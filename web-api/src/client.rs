use cache_proto::{Command, ProtoError, Response};
use std::io;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const MAX_IDLE: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("cache server unavailable: {0}")]
    Io(#[from] io::Error),
    #[error("cache server timed out")]
    Timeout,
    #[error("bad reply from cache server: {0}")]
    Proto(#[from] ProtoError),
}

/// Small TCP connection pool to `cache-server`.
pub struct CacheClient {
    addr: String,
    idle: Mutex<Vec<BufReader<TcpStream>>>,
}

impl CacheClient {
    pub fn new(addr: impl Into<String>) -> Self {
        CacheClient {
            addr: addr.into(),
            idle: Mutex::new(Vec::new()),
        }
    }

    pub async fn execute(&self, cmd: &Command) -> Result<Response, ClientError> {
        let pooled = self.idle.lock().unwrap_or_else(|e| e.into_inner()).pop();
        if let Some(conn) = pooled {
            // A pooled connection may have been closed by the server; fall back to a fresh one.
            if let Ok(resp) = self.round_trip(conn, cmd).await {
                return Ok(resp);
            }
        }
        let stream = tokio::time::timeout(IO_TIMEOUT, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| ClientError::Timeout)??;
        self.round_trip(BufReader::new(stream), cmd).await
    }

    async fn round_trip(
        &self,
        mut conn: BufReader<TcpStream>,
        cmd: &Command,
    ) -> Result<Response, ClientError> {
        let exchange = async {
            let mut line = cmd.encode();
            line.push('\n');
            conn.get_mut().write_all(line.as_bytes()).await?;
            let mut reply = String::new();
            if conn.read_line(&mut reply).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ));
            }
            Ok(reply)
        };
        let reply = tokio::time::timeout(IO_TIMEOUT, exchange)
            .await
            .map_err(|_| ClientError::Timeout)??;
        let resp = Response::parse(&reply)?;
        let mut idle = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        if idle.len() < MAX_IDLE {
            idle.push(conn);
        }
        Ok(resp)
    }
}
