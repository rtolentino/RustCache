use cache_proto::{Command, Response};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const SHARDS: usize = 16;

struct Entry {
    value: String,
    expires_at: Option<Instant>,
}

impl Entry {
    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|t| t <= now)
    }
}

/// Sharded in-memory store with TTL support. Locks are never held across `.await`.
pub struct Store {
    shards: Vec<Mutex<HashMap<String, Entry>>>,
    max_keys_per_shard: usize,
}

impl Store {
    pub fn new(max_keys: usize) -> Self {
        Store {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            max_keys_per_shard: max_keys.div_ceil(SHARDS).max(1),
        }
    }

    fn shard(&self, key: &str) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        let idx = (h.finish() as usize) % SHARDS;
        // A poisoned lock only means another thread panicked; the map is still consistent.
        self.shards[idx].lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn execute(&self, cmd: Command) -> Response {
        self.execute_at(cmd, Instant::now())
    }

    pub fn execute_at(&self, cmd: Command, now: Instant) -> Response {
        match cmd {
            Command::Ping => Response::Pong,
            Command::Get(k) => {
                let mut s = self.shard(&k);
                match s.get(&k) {
                    Some(e) if !e.expired(now) => Response::Value(e.value.clone()),
                    Some(_) => {
                        s.remove(&k);
                        Response::Nil
                    }
                    None => Response::Nil,
                }
            }
            Command::Set {
                key,
                value,
                ttl_secs,
            } => {
                let mut s = self.shard(&key);
                if !s.contains_key(&key) && s.len() >= self.max_keys_per_shard {
                    s.retain(|_, e| !e.expired(now));
                    if s.len() >= self.max_keys_per_shard {
                        return Response::Err("out of memory: key limit reached".into());
                    }
                }
                let expires_at = ttl_secs.map(|t| now + Duration::from_secs(t));
                s.insert(key, Entry { value, expires_at });
                Response::Ok
            }
            Command::Del(k) => {
                let removed = self.shard(&k).remove(&k).is_some_and(|e| !e.expired(now));
                Response::Int(removed as i64)
            }
            Command::Exists(k) => {
                let s = self.shard(&k);
                Response::Int(s.get(&k).is_some_and(|e| !e.expired(now)) as i64)
            }
            Command::Expire { key, secs } => {
                let mut s = self.shard(&key);
                match s.get_mut(&key) {
                    Some(e) if !e.expired(now) => {
                        e.expires_at = Some(now + Duration::from_secs(secs));
                        Response::Int(1)
                    }
                    _ => Response::Int(0),
                }
            }
            // -2: missing, -1: no expiry, otherwise remaining seconds (rounded up).
            Command::Ttl(k) => {
                let s = self.shard(&k);
                match s.get(&k) {
                    Some(e) if !e.expired(now) => match e.expires_at {
                        None => Response::Int(-1),
                        Some(t) => {
                            Response::Int(t.duration_since(now).as_millis().div_ceil(1000) as i64)
                        }
                    },
                    _ => Response::Int(-2),
                }
            }
            Command::Incr(k) => {
                let mut s = self.shard(&k);
                let live = s.get(&k).filter(|e| !e.expired(now));
                let (cur, expires_at) = match live {
                    Some(e) => match e.value.parse::<i64>() {
                        Ok(n) => (n, e.expires_at),
                        Err(_) => return Response::Err("value is not an integer".into()),
                    },
                    None => (0, None),
                };
                let Some(next) = cur.checked_add(1) else {
                    return Response::Err("increment overflow".into());
                };
                if live.is_none() && s.len() >= self.max_keys_per_shard {
                    return Response::Err("out of memory: key limit reached".into());
                }
                s.insert(
                    k,
                    Entry {
                        value: next.to_string(),
                        expires_at,
                    },
                );
                Response::Int(next)
            }
        }
    }

    /// Removes expired entries; returns how many were removed.
    pub fn purge_expired(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        for shard in &self.shards {
            let mut s = shard.lock().unwrap_or_else(|e| e.into_inner());
            let before = s.len();
            s.retain(|_, e| !e.expired(now));
            removed += before - s.len();
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(k: &str, v: &str, ttl: Option<u64>) -> Command {
        Command::Set {
            key: k.into(),
            value: v.into(),
            ttl_secs: ttl,
        }
    }

    #[test]
    fn set_get_del() {
        let s = Store::new(100);
        assert_eq!(s.execute(set("a", "1", None)), Response::Ok);
        assert_eq!(
            s.execute(Command::Get("a".into())),
            Response::Value("1".into())
        );
        assert_eq!(s.execute(Command::Exists("a".into())), Response::Int(1));
        assert_eq!(s.execute(Command::Del("a".into())), Response::Int(1));
        assert_eq!(s.execute(Command::Get("a".into())), Response::Nil);
        assert_eq!(s.execute(Command::Del("a".into())), Response::Int(0));
    }

    #[test]
    fn ttl_and_expiry() {
        let s = Store::new(100);
        let t0 = Instant::now();
        s.execute_at(set("a", "1", Some(10)), t0);
        assert_eq!(
            s.execute_at(Command::Ttl("a".into()), t0),
            Response::Int(10)
        );
        assert_eq!(
            s.execute_at(Command::Ttl("nope".into()), t0),
            Response::Int(-2)
        );
        let later = t0 + Duration::from_secs(11);
        assert_eq!(s.execute_at(Command::Get("a".into()), later), Response::Nil);
        s.execute_at(set("b", "1", None), t0);
        assert_eq!(
            s.execute_at(Command::Ttl("b".into()), t0),
            Response::Int(-1)
        );
        assert_eq!(
            s.execute_at(
                Command::Expire {
                    key: "b".into(),
                    secs: 5
                },
                t0
            ),
            Response::Int(1)
        );
        assert_eq!(
            s.execute_at(
                Command::Expire {
                    key: "zz".into(),
                    secs: 5
                },
                t0
            ),
            Response::Int(0)
        );
    }

    #[test]
    fn purge_removes_expired() {
        let s = Store::new(100);
        s.execute(set("a", "1", Some(0)));
        s.execute(set("b", "1", None));
        assert_eq!(s.purge_expired(), 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn incr_behaviour() {
        let s = Store::new(100);
        assert_eq!(s.execute(Command::Incr("n".into())), Response::Int(1));
        assert_eq!(s.execute(Command::Incr("n".into())), Response::Int(2));
        s.execute(set("t", "abc", None));
        assert!(matches!(
            s.execute(Command::Incr("t".into())),
            Response::Err(_)
        ));
        s.execute(set("m", &i64::MAX.to_string(), None));
        assert!(matches!(
            s.execute(Command::Incr("m".into())),
            Response::Err(_)
        ));
    }

    #[test]
    fn key_limit_enforced() {
        let s = Store::new(SHARDS); // one key per shard
        let mut errs = 0;
        for i in 0..200 {
            if matches!(
                s.execute(set(&format!("k{i}"), "v", None)),
                Response::Err(_)
            ) {
                errs += 1;
            }
        }
        assert!(errs > 0);
        assert!(s.len() <= SHARDS);
    }
}
