use crate::store::{EvictionPolicy, Store};
use cache_proto::{Command, Response, MAX_LINE_LEN};
use futures_util::{SinkExt, StreamExt};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};
use tracing::{debug, info, warn};

/// Runtime settings for the TCP server.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum simultaneous client connections; extra connections are dropped.
    pub max_connections: usize,
    /// Approximate maximum number of stored keys.
    pub max_keys: usize,
    /// Approximate maximum accounted memory in bytes (key + value + per-entry overhead).
    pub max_memory_bytes: usize,
    /// What to do when a write would exceed the key or memory limit.
    pub eviction_policy: EvictionPolicy,
    /// A connection with no request for this long is closed.
    pub idle_timeout: Duration,
    /// How often expired keys are purged in the background.
    pub sweep_interval: Duration,
}

impl Default for Config {
    /// 1024 connections, 1,000,000 keys, 256 MiB memory, `allkeys-lru` eviction,
    /// 300 s idle timeout, 1 s sweep interval.
    fn default() -> Self {
        Config {
            max_connections: 1024,
            max_keys: 1_000_000,
            max_memory_bytes: 256 * 1024 * 1024,
            eviction_policy: EvictionPolicy::AllKeysLru,
            idle_timeout: Duration::from_secs(300),
            sweep_interval: Duration::from_secs(1),
        }
    }
}

/// Serves until `shutdown` resolves, then stops accepting and drains open connections.
///
/// # Arguments
/// * `listener` - bound TCP listener to accept clients on.
/// * `config` - server limits and timings.
/// * `shutdown` - future that completes when the server should stop (e.g. Ctrl-C).
pub async fn run(listener: TcpListener, config: Config, shutdown: impl Future<Output = ()>) {
    let store = Arc::new(Store::new(
        config.max_keys,
        config.max_memory_bytes,
        config.eviction_policy,
    ));
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);

    let sweeper = {
        let store = store.clone();
        let mut stop = stop_rx.clone();
        let every = config.sweep_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = tick.tick() => { let n = store.purge_expired(); if n > 0 { debug!(removed = n, "purged expired keys"); } }
                    _ = stop.changed() => break,
                }
            }
        })
    };

    let mut conns = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(v) => v,
                    Err(e) => { warn!(error = %e, "accept failed"); tokio::time::sleep(Duration::from_millis(50)).await; continue; }
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    warn!(%peer, "connection limit reached; rejecting");
                    continue;
                };
                let store = store.clone();
                let stop = stop_rx.clone();
                let idle = config.idle_timeout;
                conns.spawn(async move {
                    handle_connection(stream, store, idle, stop).await;
                    drop(permit);
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    info!("shutting down: draining connections");
    let _ = stop_tx.send(true);
    while conns.join_next().await.is_some() {}
    let _ = sweeper.await;
}

/// Reads request lines from one client and writes one reply per line until the client
/// disconnects, idles out, sends an oversized line, or shutdown is signalled.
///
/// # Arguments
/// * `stream` - the accepted client socket.
/// * `store` - shared store the commands run against.
/// * `idle` - maximum time to wait for the next request.
/// * `stop` - becomes changed when the server is shutting down.
async fn handle_connection(
    stream: TcpStream,
    store: Arc<Store>,
    idle: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut framed = Framed::new(stream, LinesCodec::new_with_max_length(MAX_LINE_LEN));
    loop {
        let line = tokio::select! {
            r = tokio::time::timeout(idle, framed.next()) => match r {
                Err(_) => { debug!("idle timeout"); return; }
                Ok(None) => return,
                Ok(Some(l)) => l,
            },
            _ = stop.changed() => return,
        };
        let reply = match line {
            Ok(l) => match Command::parse(&l) {
                Ok(cmd) => store.execute(cmd),
                Err(e) => Response::Err(e.to_string()),
            },
            Err(LinesCodecError::MaxLineLengthExceeded) => {
                let _ = framed
                    .send(Response::Err("request too large".into()).encode())
                    .await;
                return;
            }
            Err(LinesCodecError::Io(e)) => {
                debug!(error = %e, "connection error");
                return;
            }
        };
        if framed.send(reply.encode()).await.is_err() {
            return;
        }
    }
}
