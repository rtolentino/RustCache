use cache_proto::{Command, ProtoError, Response};
use std::io;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Maximum number of idle connections kept for reuse.
const MAX_IDLE: usize = 16;
/// Timeout for connecting and for each request/reply exchange.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Connecting, reading or writing failed.
    #[error("cache server unavailable: {0}")]
    Io(#[from] io::Error),
    /// Connecting or the request/reply exchange exceeded the timeout.
    #[error("cache server timed out")]
    Timeout,
    /// The reply line could not be parsed.
    #[error("bad reply from cache server: {0}")]
    Proto(#[from] ProtoError),
}

/// Small TCP connection pool to `cache-server`.
pub struct CacheClient {
    /// Address of the cache server, e.g. `127.0.0.1:6380`.
    addr: String,
    /// Connections available for reuse.
    idle: Mutex<Vec<BufReader<TcpStream>>>,
}

impl CacheClient {
    /// Creates a client; no connection is opened until the first command.
    ///
    /// # Arguments
    /// * `addr` - cache server address as `host:port`.
    pub fn new(addr: impl Into<String>) -> Self {
        CacheClient {
            addr: addr.into(),
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Sends a command and returns the server's reply, reusing a pooled connection when possible
    /// and opening a new one if none is available or the pooled one is dead.
    ///
    /// # Arguments
    /// * `cmd` - the command to send.
    ///
    /// # Errors
    /// [`ClientError`] on I/O failure, timeout or an unparseable reply.
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

    /// Writes one command and reads one reply on `conn`, then returns the connection to the pool.
    ///
    /// # Arguments
    /// * `conn` - connection to use; dropped (not pooled) on error.
    /// * `cmd` - the command to send.
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
