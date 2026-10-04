use cache_server::server::{run, Config};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

async fn start(config: Config) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, rx) = oneshot::channel();
    let h = tokio::spawn(run(listener, config, async {
        let _ = rx.await;
    }));
    (addr, tx, h)
}

async fn roundtrip(r: &mut BufReader<TcpStream>, line: &str) -> String {
    r.get_mut()
        .write_all(format!("{line}\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    r.read_line(&mut out).await.unwrap();
    out.trim_end().to_string()
}

#[tokio::test]
async fn commands_over_tcp_and_graceful_shutdown() {
    let (addr, stop, handle) = start(Config::default()).await;
    let mut c = BufReader::new(TcpStream::connect(&addr).await.unwrap());
    assert_eq!(roundtrip(&mut c, "PING").await, "PONG");
    assert_eq!(roundtrip(&mut c, "SET greeting - hello world").await, "OK");
    assert_eq!(roundtrip(&mut c, "GET greeting").await, "VALUE hello world");
    assert_eq!(roundtrip(&mut c, "INCR n").await, "INT 1");
    assert_eq!(roundtrip(&mut c, "GET missing").await, "NIL");
    assert!(roundtrip(&mut c, "BOGUS").await.starts_with("ERR"));
    assert_eq!(roundtrip(&mut c, "SET t 1 x").await, "OK");
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(roundtrip(&mut c, "GET t").await, "NIL");

    // Partial writes are reassembled.
    c.get_mut().write_all(b"GET gree").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.get_mut().write_all(b"ting\n").await.unwrap();
    let mut out = String::new();
    c.read_line(&mut out).await.unwrap();
    assert_eq!(out.trim_end(), "VALUE hello world");

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn rejects_connections_over_limit() {
    let cfg = Config {
        max_connections: 1,
        ..Config::default()
    };
    let (addr, stop, handle) = start(cfg).await;
    let mut a = BufReader::new(TcpStream::connect(&addr).await.unwrap());
    assert_eq!(roundtrip(&mut a, "PING").await, "PONG");
    let mut b = TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 1];
    use tokio::io::AsyncReadExt;
    assert_eq!(b.read(&mut buf).await.unwrap_or(0), 0);
    stop.send(()).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn lru_eviction_keeps_server_within_memory_limit() {
    let cfg = Config {
        max_memory_bytes: 16 * 1024,
        ..Config::default()
    };
    let (addr, stop, handle) = start(cfg).await;
    let mut c = BufReader::new(TcpStream::connect(&addr).await.unwrap());
    let value = "v".repeat(100);
    for i in 0..500 {
        assert_eq!(
            roundtrip(&mut c, &format!("SET key{i} - {value}")).await,
            "OK"
        );
    }
    let mut present = 0;
    for i in 0..500 {
        if roundtrip(&mut c, &format!("GET key{i}"))
            .await
            .starts_with("VALUE")
        {
            present += 1;
        }
    }
    // ~170 bytes per entry against a 16 KiB budget: far fewer than 500 survive, but some do.
    assert!(present > 0 && present < 150, "present = {present}");
    assert!(roundtrip(&mut c, "GET key499").await.starts_with("VALUE"));
    stop.send(()).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn noeviction_returns_error_when_memory_full() {
    let cfg = Config {
        max_memory_bytes: 16 * 1024,
        eviction_policy: cache_server::store::EvictionPolicy::NoEviction,
        ..Config::default()
    };
    let (addr, stop, handle) = start(cfg).await;
    let mut c = BufReader::new(TcpStream::connect(&addr).await.unwrap());
    let value = "v".repeat(100);
    let mut errors = 0;
    for i in 0..500 {
        if roundtrip(&mut c, &format!("SET key{i} - {value}"))
            .await
            .starts_with("ERR out of memory")
        {
            errors += 1;
        }
    }
    assert!(errors > 0);
    assert!(roundtrip(&mut c, "GET key0").await.starts_with("VALUE"));
    stop.send(()).unwrap();
    handle.await.unwrap();
}
