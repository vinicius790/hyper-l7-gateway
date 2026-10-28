# ARCHITECTURE

Ver documentação completa em português: [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md).

Resumo:
- **Listener Assíncrono:** Tokio/Hyper 1.x (HTTP/1.1 & HTTP/2) em `listen` e `admin_listen` opcional, com *Graceful Shutdown* (`drain_timeout_ms`).
- **Filter Chain L7:** W3C `traceparent` + `X-Request-Id` → global rate limit → router (`RadixRouter`, host exato/wildcard, método, headers e Canary weight) → CORS preflight (`204`) → IP filter (`403`) → auth (`api_key`, `basic`, `jwt` HS256 em tempo constante) → rate limit da rota (`429` + `Retry-After`) → `max_body_bytes` (`413`) → adaptive concurrency load shedding AIMD (`503`) → circuit breaker (`Closed → Open → HalfOpen`).
- **Load Balancing & Upstream Pool:** `round_robin`, `p2c_ewma` (Power of Two Choices + Peak-EWMA em µs) e `least_conn` com guarda RAII (`ActiveRequestGuard`), health check ativo em background (`GET /__gw_health`) e ejeção passiva.
- **Encaminhamento Híbrido:** Streaming Zero-Copy (`BoxBody`) por padrão + Retries idempotentes com *Full Jitter Backoff* (`retry.rs` + `timeout.rs`).
- **Observabilidade & Operação:** Admin `/health` (JSON detalhado) + `/metrics` (Prometheus text 0.0.4 com counters, gauges e histogramas de latência) + Hot-Reload zero-downtime via watcher de `mtime` e `SIGHUP`.
- 