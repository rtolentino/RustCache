# RustCache – Agent Instructions

## Project overview

RustCache is a Redis-like **in-memory cache** written in **Rust** using **systems programming** techniques. It is a Cargo workspace with two applications:

1. **`cache-server`** – the in-memory cache server. It owns the data store and listens on **TCP**, speaking a simple command protocol (e.g. `GET`, `SET`, `DEL`, `EXPIRE`, `TTL`).
2. **`web-api`** – a Rust Web API (axum) served over **HTTPS**. It exposes the cache as an HTTP API and calls `cache-server` over TCP. Clients never reach the store directly over HTTP.

```
Client --HTTPS--> web-api --TCP--> cache-server (in-memory store)
```

## Systems-programming priorities

- Memory efficiency: avoid unnecessary allocations and copies; prefer borrowed data and `bytes::Bytes`.
- Concurrency safety: no data races; keep lock scope small and never hold a lock across `.await`.
- Predictable latency: no blocking calls on async tasks; bound buffers, connections and key/value sizes.
- Robust I/O: handle partial reads/writes and malformed input; never `unwrap`/`expect` on I/O or parsing paths; return typed errors.
- Graceful shutdown: stop accepting connections, drain in-flight requests, then exit.
- Safe Rust by default; any `unsafe` must be justified in a comment and covered by tests.

## Technology

- Cargo workspace, Rust stable (2021 edition or later).
- `tokio` for async networking; `axum` for the Web API; `rustls` for TLS (no OpenSSL dependency).
- `tracing` for logging. Keep dependencies minimal and well-maintained.

## Conventions

- Keep the wire protocol parsing and the store in separate modules/crates so they are unit-testable without sockets.
- Add unit tests for the store and protocol, and integration tests that start `cache-server` on an ephemeral port (and `web-api` against it).
- Before finishing a change run: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.
- Never commit secrets, private keys or certificates; load TLS material and addresses from config/env.

## Documentation

- `TDD.md` is the Technical Design Document. After any code change or new design, run the `update-tdd` skill (`.github/skills/update-tdd/SKILL.md`) to write or rewrite it so it matches the code. Keep `README.md` consistent for user-facing changes.

## Scope boundaries

- The TCP command protocol and HTTP API routes are to be designed; document them in the repo when added.
- Redis wire (RESP) compatibility is **not** assumed unless explicitly requested.
- Persistence, clustering and replication are out of scope initially.
