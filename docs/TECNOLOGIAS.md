# Tecnologias

| Peça | Escolha | Motivo |
|------|---------|--------|
| Runtime | Tokio 1.x | I/O async padrão Rust, timers, sinais Unix (`SIGHUP`/`SIGINT`) e `fs::metadata` para hot-reload |
| HTTP | Hyper 1 + hyper-util | Datapath explícito L7 com suporte a HTTP/1.1, HTTP/2 e pool de conexões persistentes |
| Body | http-body-util / bytes | Streaming zero-copy via `BoxBody` e buffers referenciados (`Bytes`) compatíveis com Hyper 1 |
| Config | serde + serde_yaml | YAML declarativo legível para operações + validação fail-fast |
| JSON | serde_json | Relatório estruturado do endpoint `/health` e decodificação de claims JWT |
| Sync | parking_lot + Atômicos | `Mutex` e `RwLock` rápidos sem poisoning + `AtomicU64`/`AtomicUsize` no caminho crítico |
| Criptografia JWT | SHA-256 + HMAC-SHA256 nativos + base64 | Validação stateless de JWT (`HS256`) em tempo constante sem adicionar crates extras fora do `Cargo.lock` |
| IDs & Tracing | uuid v4 + W3C Traceparent | Geração de `X-Request-Id` e propagação distribuída `traceparent`/`tracestate` |
| Logs | tracing + tracing-subscriber | Logs estruturados filtráveis por `RUST_LOG` |
| CLI | clap 4 | Parsing tipado de argumentos (`--config`) |
| Erros | thiserror | Enum `GatewayError` limpo e ergonômico |

**Por que não Axum/Nginx module?** Axum é ótimo para aplicações web de negócio; aqui o foco é o **datapath explícito de proxy reverso** (streaming zero-copy, load balancing P2C-EWMA, filtros de borda, circuit breaker, load shedding AIMD e retries). Usar Hyper 1.x diretamente deixa todo o controle de protocolo e orçamento de memória visível no código.

**Por que Rust (não Go)?** Alinhado ao catálogo ("estilo Envoy em Rust"): ausência de pausas de Garbage Collector no percentil `p99`, controle estrito sobre alocações de memória por requisição e segurança de concorrência garantida pelo compilador.

**Por que implementar JWT (SHA-256 / HMAC-SHA256), W3C Traceparent, Radix Trie e Hot-Reload nativamente?** Para manter o projeto 100% enxuto, auditável e estritamente compatível com `cargo test --locked` e `cargo build --locked` (`tools/dev.py`), evitando árvores de dependências pesadas ou conflitos de compilação offline.
