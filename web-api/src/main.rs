use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use web_api::{app, client::CacheClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let listen: SocketAddr = std::env::var("WEB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8443".into())
        .parse()?;
    let cache_addr = std::env::var("CACHE_ADDR").unwrap_or_else(|_| "127.0.0.1:6380".into());
    let router = app(Arc::new(CacheClient::new(cache_addr)));

    let handle = axum_server::Handle::new();
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });
    }

    match (std::env::var("TLS_CERT"), std::env::var("TLS_KEY")) {
        (Ok(cert), Ok(key)) => {
            let tls = RustlsConfig::from_pem_file(cert, key).await?;
            tracing::info!(%listen, "web-api listening (HTTPS)");
            axum_server::bind_rustls(listen, tls)
                .handle(handle)
                .serve(router.into_make_service())
                .await?;
        }
        // Plain HTTP is only for local development and must be requested explicitly.
        _ if std::env::var("WEB_ALLOW_HTTP").as_deref() == Ok("1") => {
            tracing::warn!(%listen, "TLS_CERT/TLS_KEY not set; serving plain HTTP (development only)");
            axum_server::bind(listen)
                .handle(handle)
                .serve(router.into_make_service())
                .await?;
        }
        _ => {
            return Err(
                "set TLS_CERT and TLS_KEY to serve HTTPS (or WEB_ALLOW_HTTP=1 for development)"
                    .into(),
            )
        }
    }
    Ok(())
}
