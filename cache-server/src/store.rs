use cache_proto::{Command, Response};
use lru::LruCache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Number of independent shards; each has its own lock to reduce contention.
const SHARDS: usize = 16;

/// Estimated per-entry bookkeeping cost in bytes (string headers, expiry, map/LRU links),
/// added to the key and value lengths when accounting memory.
pub const ENTRY_OVERHEAD: usize = 64;

/// What the store does when a write would exceed the memory or key limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Reject the write with an `ERR out of memory` reply; never remove existing keys.
    NoEviction,
    /// Remove the least recently used keys (in the same shard) until the write fits.
    #[default]
    AllKeysLru,
}

impl FromStr for EvictionPolicy {
    type Err = String;

    /// Parses `noeviction` or `allkeys-lru` (case-insensitive).
    ///
    /// # Arguments
    /// * `s` - policy name.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Ok(EvictionPolicy::NoEviction),
            "allkeys-lru" => Ok(EvictionPolicy::AllKeysLru),
            other => Err(format!(
                "unknown eviction policy '{other}' (expected 'noeviction' or 'allkeys-lru')"
            )),
        }
    }
}

/// Memory accounting size of an entry: key + value bytes + [`ENTRY_OVERHEAD`].
///
/// # Arguments
/// * `key` - the entry key.
/// * `value` - the entry value.
fn entry_size(key: &str, value: &str) -> usize {
    key.len() + value.len() + ENTRY_OVERHEAD
}

/// A stored value with its optional expiry time.
struct Entry {
    /// The stored value.
    value: String,
    /// Instant after which the entry is considered gone; `None` means never.
    expires_at: Option<Instant>,
}

impl Entry {
    /// Returns true if the entry's expiry is at or before `now`.
    ///
    /// # Arguments
    /// * `now` - the instant to compare against.
    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|t| t <= now)
    }
}

/// Per-shard limits and policy (the store-wide limits divided across shards).
#[derive(Clone, Copy)]
struct Limits {
    /// Maximum number of keys in one shard.
    max_keys: usize,
    /// Maximum accounted bytes in one shard.
    max_bytes: usize,
    /// Behaviour when a write does not fit.
    policy: EvictionPolicy,
}

/// One partition of the store: an LRU-ordered map plus its accounted memory.
struct Shard {
    /// Entries ordered by recency of use (unbounded; limits are enforced by [`Shard::store`]).
    map: LruCache<String, Entry>,
    /// Sum of [`entry_size`] over all entries.
    used: usize,
}

impl Shard {
    /// Creates an empty shard.
    fn new() -> Self {
        Shard {
            map: LruCache::unbounded(),
            used: 0,
        }
    }

    /// Removes a key and releases its accounted memory.
    ///
    /// # Arguments
    /// * `key` - key to remove.
    fn remove(&mut self, key: &str) -> Option<Entry> {
        let entry = self.map.pop(key)?;
        self.used -= entry_size(key, &entry.value);
        Some(entry)
    }

    /// Returns the live entry for `key` and marks it most recently used.
    /// An expired entry is removed and reported as missing.
    ///
    /// # Arguments
    /// * `key` - key to look up.
    /// * `now` - instant used to decide whether the entry has expired.
    fn live(&mut self, key: &str, now: Instant) -> Option<&Entry> {
        match self.map.peek(key).map(|e| e.expired(now)) {
            Some(true) => {
                self.remove(key);
                None
            }
            Some(false) => self.map.get(key),
            None => None,
        }
    }

    /// Removes every expired entry; returns how many were removed.
    ///
    /// # Arguments
    /// * `now` - instant used to decide whether entries have expired.
    fn purge_expired(&mut self, now: Instant) -> usize {
        let expired: Vec<String> = self
            .map
            .iter()
            .filter(|(_, e)| e.expired(now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.remove(key);
        }
        expired.len()
    }

    /// Frees space until an entry of `needed` bytes (a new key) fits.
    /// Expired entries are dropped first; then, under LRU, the least recently used entries.
    ///
    /// # Arguments
    /// * `needed` - accounted size of the incoming entry.
    /// * `limits` - shard limits and policy.
    /// * `now` - current instant, for expiry checks.
    /// * `evicted` - counter incremented for every key evicted by the policy.
    ///
    /// # Errors
    /// An out-of-memory message when the policy forbids eviction or nothing is left to evict.
    fn make_room(
        &mut self,
        needed: usize,
        limits: Limits,
        now: Instant,
        evicted: &AtomicU64,
    ) -> Result<(), &'static str> {
        let mut purged = false;
        loop {
            if self.used + needed <= limits.max_bytes && self.map.len() < limits.max_keys {
                return Ok(());
            }
            if !purged {
                purged = true;
                self.purge_expired(now);
                continue;
            }
            match limits.policy {
                EvictionPolicy::NoEviction => {
                    return Err("out of memory: memory or key limit reached");
                }
                EvictionPolicy::AllKeysLru => match self.map.pop_lru() {
                    Some((key, entry)) => {
                        self.used -= entry_size(&key, &entry.value);
                        evicted.fetch_add(1, Ordering::Relaxed);
                    }
                    None => return Err("out of memory: memory or key limit reached"),
                },
            }
        }
    }

    /// Inserts or replaces an entry, evicting if the policy allows. On failure the previous
    /// value (if any) is left in place.
    ///
    /// # Arguments
    /// * `key` - key to write.
    /// * `value` - value to store.
    /// * `expires_at` - optional expiry instant.
    /// * `limits` - shard limits and policy.
    /// * `now` - current instant, for expiry checks.
    /// * `evicted` - counter incremented for every key evicted by the policy.
    ///
    /// # Errors
    /// An out-of-memory message if the entry can never fit, or does not fit under `noeviction`.
    fn store(
        &mut self,
        key: String,
        value: String,
        expires_at: Option<Instant>,
        limits: Limits,
        now: Instant,
        evicted: &AtomicU64,
    ) -> Result<(), &'static str> {
        let size = entry_size(&key, &value);
        if size > limits.max_bytes {
            return Err("out of memory: entry is larger than the memory limit");
        }
        let old = self.remove(&key);
        if let Err(msg) = self.make_room(size, limits, now, evicted) {
            if let Some(old) = old {
                self.used += entry_size(&key, &old.value);
                self.map.put(key, old);
            }
            return Err(msg);
        }
        self.used += size;
        self.map.put(key, Entry { value, expires_at });
        Ok(())
    }
}

/// Sharded in-memory store with TTL, byte-based memory limits and LRU eviction.
/// Locks are never held across `.await`.
pub struct Store {
    /// Hash-partitioned shards, each behind its own mutex.
    shards: Vec<Mutex<Shard>>,
    /// Limits applied to every shard.
    limits: Limits,
    /// Total number of keys removed by the eviction policy.
    evicted: AtomicU64,
}

impl Store {
    /// Creates an empty store.
    ///
    /// Limits are split evenly across shards (rounded up, at least 1 key and enough bytes for
    /// one entry per shard), so a skewed key distribution can evict or reject slightly before
    /// the global totals are reached.
    ///
    /// # Arguments
    /// * `max_keys` - approximate total key limit.
    /// * `max_memory_bytes` - approximate total accounted memory limit in bytes
    ///   (key + value + [`ENTRY_OVERHEAD`] per entry).
    /// * `policy` - what to do when a write does not fit.
    pub fn new(max_keys: usize, max_memory_bytes: usize, policy: EvictionPolicy) -> Self {
        Store {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::new())).collect(),
            limits: Limits {
                max_keys: max_keys.div_ceil(SHARDS).max(1),
                max_bytes: max_memory_bytes.div_ceil(SHARDS).max(1),
                policy,
            },
            evicted: AtomicU64::new(0),
        }
    }

    /// Locks and returns the shard that owns `key`.
    ///
    /// # Arguments
    /// * `key` - key whose hash selects the shard.
    fn shard(&self, key: &str) -> MutexGuard<'_, Shard> {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        let idx = (h.finish() as usize) % SHARDS;
        // A poisoned lock only means another thread panicked; the map is still consistent.
        self.shards[idx].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Runs a command against the store using the current time.
    ///
    /// # Arguments
    /// * `cmd` - the command to execute.
    pub fn execute(&self, cmd: Command) -> Response {
        self.execute_at(cmd, Instant::now())
    }

    /// Runs a command with an explicit clock, which makes expiry deterministic in tests.
    ///
    /// Reply semantics: `GET` -> value or nil; `SET` -> ok, or an error if the entry cannot be
    /// stored; `DEL`/`EXISTS`/`EXPIRE` -> 1 or 0; `TTL` -> seconds, -1 (no expiry) or -2 (missing);
    /// `INCR` -> new value, or an error for non-integers, overflow or when it cannot be stored.
    /// `GET`, `SET` and `INCR` mark the key as most recently used.
    ///
    /// # Arguments
    /// * `cmd` - the command to execute.
    /// * `now` - the instant treated as "current" when checking and setting expiry.
    pub fn execute_at(&self, cmd: Command, now: Instant) -> Response {
        match cmd {
            Command::Ping => Response::Pong,
            Command::Get(k) => match self.shard(&k).live(&k, now) {
                Some(e) => Response::Value(e.value.clone()),
                None => Response::Nil,
            },
            Command::Set {
                key,
                value,
                ttl_secs,
            } => {
                let expires_at = ttl_secs.map(|t| now + Duration::from_secs(t));
                let mut s = self.shard(&key);
                match s.store(key, value, expires_at, self.limits, now, &self.evicted) {
                    Ok(()) => Response::Ok,
                    Err(msg) => Response::Err(msg.into()),
                }
            }
            Command::Del(k) => {
                let removed = self.shard(&k).remove(&k).is_some_and(|e| !e.expired(now));
                Response::Int(removed as i64)
            }
            Command::Exists(k) => {
                let s = self.shard(&k);
                Response::Int(s.map.peek(&k).is_some_and(|e| !e.expired(now)) as i64)
            }
            Command::Expire { key, secs } => {
                let mut s = self.shard(&key);
                match s.map.peek_mut(&key) {
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
                match s.map.peek(&k) {
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
                let (cur, expires_at) = match s.live(&k, now) {
                    Some(e) => match e.value.parse::<i64>() {
                        Ok(n) => (n, e.expires_at),
                        Err(_) => return Response::Err("value is not an integer".into()),
                    },
                    None => (0, None),
                };
                let Some(next) = cur.checked_add(1) else {
                    return Response::Err("increment overflow".into());
                };
                match s.store(
                    k,
                    next.to_string(),
                    expires_at,
                    self.limits,
                    now,
                    &self.evicted,
                ) {
                    Ok(()) => Response::Int(next),
                    Err(msg) => Response::Err(msg.into()),
                }
            }
        }
    }

    /// Removes expired entries; returns how many were removed.
    pub fn purge_expired(&self) -> usize {
        let now = Instant::now();
        self.shards
            .iter()
            .map(|s| {
                s.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .purge_expired(now)
            })
            .sum()
    }

    /// Number of stored entries, including expired ones not yet purged.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).map.len())
            .sum()
    }

    /// True when no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Accounted memory in bytes (key + value + [`ENTRY_OVERHEAD`] per entry).
    pub fn used_memory(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).used)
            .sum()
    }

    /// Total number of keys removed by the eviction policy since startup.
    pub fn evicted_keys(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BIG: usize = usize::MAX / 2;

    fn store() -> Store {
        Store::new(100, BIG, EvictionPolicy::AllKeysLru)
    }

    fn set(k: &str, v: &str, ttl: Option<u64>) -> Command {
        Command::Set {
            key: k.into(),
            value: v.into(),
            ttl_secs: ttl,
        }
    }

    /// Finds `n` keys that hash into the same shard as `prefix`-keys, so shard-local
    /// eviction can be tested deterministically.
    fn same_shard_keys(n: usize) -> Vec<String> {
        let target = {
            let mut h = DefaultHasher::new();
            "seed".hash(&mut h);
            (h.finish() as usize) % SHARDS
        };
        let mut keys = Vec::new();
        let mut i = 0;
        while keys.len() < n {
            let k = format!("k{i}");
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            if (h.finish() as usize) % SHARDS == target {
                keys.push(k);
            }
            i += 1;
        }
        keys
    }

    /// Store whose every shard holds exactly `entries` entries of `value_len`-byte values
    /// for keys of up to 5 bytes.
    fn tight_store(entries: usize, value_len: usize, policy: EvictionPolicy) -> Store {
        let per_shard = entries * (5 + value_len + ENTRY_OVERHEAD);
        Store::new(BIG, per_shard * SHARDS, policy)
    }

    #[test]
    fn set_get_del() {
        let s = store();
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
        let s = store();
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
        let s = store();
        s.execute(set("a", "1", Some(0)));
        s.execute(set("b", "1", None));
        assert_eq!(s.purge_expired(), 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn incr_behaviour() {
        let s = store();
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
    fn memory_is_accounted_and_released() {
        let s = store();
        assert_eq!(s.used_memory(), 0);
        s.execute(set("abc", "12345", None));
        assert_eq!(s.used_memory(), 3 + 5 + ENTRY_OVERHEAD);
        s.execute(set("abc", "1", None)); // replacing shrinks the accounting
        assert_eq!(s.used_memory(), 3 + 1 + ENTRY_OVERHEAD);
        s.execute(Command::Del("abc".into()));
        assert_eq!(s.used_memory(), 0);
        s.execute(set("x", "1", Some(0)));
        s.purge_expired();
        assert_eq!(s.used_memory(), 0);
    }

    #[test]
    fn incr_growth_is_accounted() {
        let s = store();
        s.execute(set("n", "9", None));
        let before = s.used_memory();
        s.execute(Command::Incr("n".into())); // "9" -> "10"
        assert_eq!(s.used_memory(), before + 1);
    }

    #[test]
    fn lru_evicts_least_recently_used_when_memory_full() {
        let s = tight_store(3, 10, EvictionPolicy::AllKeysLru);
        let keys = same_shard_keys(4);
        let v = "0123456789";
        for k in &keys[..3] {
            assert_eq!(s.execute(set(k, v, None)), Response::Ok);
        }
        // Touch the oldest key so the second one becomes least recently used.
        assert!(matches!(
            s.execute(Command::Get(keys[0].clone())),
            Response::Value(_)
        ));
        assert_eq!(s.execute(set(&keys[3], v, None)), Response::Ok);
        assert_eq!(s.evicted_keys(), 1);
        assert_eq!(s.execute(Command::Get(keys[1].clone())), Response::Nil);
        for k in [&keys[0], &keys[2], &keys[3]] {
            assert!(matches!(
                s.execute(Command::Get(k.clone())),
                Response::Value(_)
            ));
        }
    }

    #[test]
    fn lru_evicts_multiple_for_a_larger_entry() {
        let s = tight_store(3, 10, EvictionPolicy::AllKeysLru);
        let keys = same_shard_keys(4);
        for k in &keys[..3] {
            s.execute(set(k, "0123456789", None));
        }
        // Roughly two small entries worth of value bytes.
        let big = "x".repeat(10 + 5 + ENTRY_OVERHEAD + 10);
        assert_eq!(s.execute(set(&keys[3], &big, None)), Response::Ok);
        assert!(s.evicted_keys() >= 2);
        assert!(matches!(
            s.execute(Command::Get(keys[3].clone())),
            Response::Value(_)
        ));
    }

    #[test]
    fn expired_entries_are_dropped_before_evicting_live_ones() {
        let s = tight_store(2, 10, EvictionPolicy::AllKeysLru);
        let keys = same_shard_keys(3);
        let t0 = Instant::now();
        s.execute_at(set(&keys[0], "0123456789", Some(1)), t0);
        s.execute_at(set(&keys[1], "0123456789", None), t0);
        let later = t0 + Duration::from_secs(5);
        assert_eq!(
            s.execute_at(set(&keys[2], "0123456789", None), later),
            Response::Ok
        );
        assert_eq!(s.evicted_keys(), 0);
        assert!(matches!(
            s.execute_at(Command::Get(keys[1].clone()), later),
            Response::Value(_)
        ));
    }

    #[test]
    fn noeviction_rejects_writes_and_keeps_existing_data() {
        let s = tight_store(2, 10, EvictionPolicy::NoEviction);
        let keys = same_shard_keys(3);
        let v = "0123456789";
        assert_eq!(s.execute(set(&keys[0], v, None)), Response::Ok);
        assert_eq!(s.execute(set(&keys[1], v, None)), Response::Ok);
        assert!(matches!(
            s.execute(set(&keys[2], v, None)),
            Response::Err(m) if m.contains("out of memory")
        ));
        assert_eq!(s.evicted_keys(), 0);
        // Overwriting an existing key that fits is still allowed.
        assert_eq!(s.execute(set(&keys[0], "abcdefghij", None)), Response::Ok);
        // A failed grow leaves the old value untouched.
        let big = "y".repeat(100);
        assert!(matches!(
            s.execute(set(&keys[0], &big, None)),
            Response::Err(_)
        ));
        assert_eq!(
            s.execute(Command::Get(keys[0].clone())),
            Response::Value("abcdefghij".into())
        );
    }

    #[test]
    fn entry_larger_than_shard_limit_is_rejected_even_with_lru() {
        let s = Store::new(BIG, 16 * 200, EvictionPolicy::AllKeysLru);
        let big = "z".repeat(500);
        assert!(matches!(
            s.execute(set("k", &big, None)),
            Response::Err(m) if m.contains("larger than the memory limit")
        ));
        assert_eq!(s.used_memory(), 0);
    }

    #[test]
    fn key_limit_enforced_with_lru_and_noeviction() {
        let lru = Store::new(SHARDS, BIG, EvictionPolicy::AllKeysLru); // one key per shard
        for i in 0..200 {
            assert_eq!(lru.execute(set(&format!("k{i}"), "v", None)), Response::Ok);
        }
        assert!(lru.len() <= SHARDS);
        assert!(lru.evicted_keys() > 0);

        let strict = Store::new(SHARDS, BIG, EvictionPolicy::NoEviction);
        let errs = (0..200)
            .filter(|i| {
                matches!(
                    strict.execute(set(&format!("k{i}"), "v", None)),
                    Response::Err(_)
                )
            })
            .count();
        assert!(errs > 0);
        assert!(strict.len() <= SHARDS);
    }

    #[test]
    fn eviction_policy_parsing() {
        assert_eq!(
            "NoEviction".parse::<EvictionPolicy>(),
            Ok(EvictionPolicy::NoEviction)
        );
        assert_eq!(
            "allkeys-lru".parse::<EvictionPolicy>(),
            Ok(EvictionPolicy::AllKeysLru)
        );
        assert!("random".parse::<EvictionPolicy>().is_err());
        assert_eq!(EvictionPolicy::default(), EvictionPolicy::AllKeysLru);
    }
}
