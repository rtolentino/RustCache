use cache_server::server::{run, Config};
use std::time::Duration;
use tokio::net::TcpListener;

/// Reads an environment variable and parses it, falling back to a default.
///
/// # Arguments
/// * `name` - environment variable name.
/// * `default` - value used when the variable is unset or cannot be parsed.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Starts the cache server. Configured by `CACHE_ADDR`, `CACHE_MAX_CONNECTIONS`,
/// `CACHE_MAX_KEYS` and `CACHE_IDLE_TIMEOUT_SECS`; stops on Ctrl-C.
#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr = std::env::var("CACHE_ADDR").unwrap_or_else(|_| "127.0.0.1:6380".into());
    let d = Config::default();
    let config = Config {
        max_connections: env_or("CACHE_MAX_CONNECTIONS", d.max_connections),
        max_keys: env_or("CACHE_MAX_KEYS", d.max_keys),
        idle_timeout: Duration::from_secs(env_or("CACHE_IDLE_TIMEOUT_SECS", 300)),
        sweep_interval: d.sweep_interval,
    };
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "cache-server listening");
    run(listener, config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    Ok(())
}
