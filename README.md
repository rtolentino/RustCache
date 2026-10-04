# RustCache

A Redis-like in-memory cache in Rust, as two applications in one Cargo workspace:

```
Client --HTTPS--> web-api --TCP--> cache-server (in-memory store)
```

- `cache-proto`: shared text protocol (parse/encode).
- `cache-server`: sharded in-memory store with TTLs, served over TCP (tokio).
- `web-api`: axum HTTPS API that calls `cache-server` through a TCP connection pool.

## Run

```sh
# dev certificate (certs/ is git-ignored)
mkdir -p certs && openssl req -x509 -newkey rsa:2048 -nodes -keyout certs/key.pem -out certs/cert.pem \
  -days 30 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cargo run -p cache-server
TLS_CERT=certs/cert.pem TLS_KEY=certs/key.pem cargo run -p web-api

curl --cacert certs/cert.pem -X PUT https://localhost:8443/v1/keys/demo \
  -H 'content-type: application/json' -d '{"value":"hi","ttl_secs":30}'
curl --cacert certs/cert.pem https://localhost:8443/v1/keys/demo
```

### Configuration (environment variables)

| Variable | App | Default |
|---|---|---|
| `CACHE_ADDR` | both (listen / upstream) | `127.0.0.1:6380` |
| `CACHE_MAX_CONNECTIONS` | cache-server | `1024` |
| `CACHE_MAX_KEYS` | cache-server | `1000000` |
| `CACHE_MAX_MEMORY_BYTES` | cache-server | `268435456` (256 MiB) |
| `CACHE_EVICTION_POLICY` | cache-server | `allkeys-lru` (or `noeviction`) |
| `CACHE_IDLE_TIMEOUT_SECS` | cache-server | `300` |
| `WEB_ADDR` | web-api | `127.0.0.1:8443` |
| `TLS_CERT`, `TLS_KEY` | web-api | required (PEM paths) |
| `WEB_ALLOW_HTTP=1` | web-api | dev only: serve plain HTTP when no TLS files are set |

## TCP protocol

One request per line (`\n` or `\r\n`); one reply line per request. Keys have no whitespace (max 256 bytes);
values are the rest of the line, with no CR/LF (max 1 MiB). Commands are case-insensitive.

| Request | Reply |
|---|---|
| `PING` | `PONG` |
| `GET <key>` | `VALUE <v>` or `NIL` |
| `SET <key> <ttl_secs or -> <value>` | `OK` |
| `DEL <key>` | `INT 1` / `INT 0` |
| `EXISTS <key>` | `INT 1` / `INT 0` |
| `EXPIRE <key> <secs>` | `INT 1` / `INT 0` |
| `TTL <key>` | `INT <secs>`, `-1` no expiry, `-2` missing |
| `INCR <key>` | `INT <n>` |

Errors reply `ERR <message>`. This is not RESP; Redis clients are not compatible.

## Memory limits and eviction

Memory is accounted per entry as key bytes + value bytes + 64 bytes of overhead, and capped by `CACHE_MAX_MEMORY_BYTES`
(and `CACHE_MAX_KEYS`). When a write does not fit:

- `allkeys-lru` (default): expired keys are dropped first, then the least recently used keys are evicted until it fits.
  `GET`, `SET` and `INCR` count as use.
- `noeviction`: the write fails with `ERR out of memory: ...` (HTTP 422 through the API) and existing data is untouched.

The limits are split evenly across 16 shards, so eviction is per shard and can start slightly before the global totals are reached.
An entry larger than a shard's budget is always rejected. An invalid `CACHE_EVICTION_POLICY` stops the server at startup.

## HTTPS API

| Route | Description |
|---|---|
| `GET /healthz` | 200 if the cache server answers `PING`, else 503 |
| `GET /v1/keys/{key}` | `{"key","value","ttl_secs"}` or 404 |
| `PUT /v1/keys/{key}` | body `{"value": "...", "ttl_secs": 30}` (ttl optional) -> 204 |
| `DELETE /v1/keys/{key}` | 204 or 404 |
| `POST /v1/keys/{key}/expire` | body `{"ttl_secs": 30}` -> 204 or 404 |
| `POST /v1/keys/{key}/incr` | `{"value": n}` |

Interactive docs: Swagger UI at `/docs`, OpenAPI 3 spec at `/openapi.json` (generated from code with `utoipa`).

Errors are JSON `{"error": "..."}`: 400 invalid input, 422 rejected by cache, 502/504 cache unreachable or slow.

## Develop

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
