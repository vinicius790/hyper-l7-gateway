# API Gateway — reverse proxy HTTP (catálogo 2.2)

Gateway / reverse proxy **L7 de alta performance** em Rust (Tokio + Hyper 1.x): **streaming zero-copy**, **HTTP/1.1 & HTTP/2**, roteamento por **path/host (Radix Trie + wildcard + método + headers + Canary weight)**, balanceamento **round-robin / P2C + Peak-EWMA / least-connections**, **token bucket** (com auto-evicção e headers `X-RateLimit-*`), **API key / Basic Auth / JWT HS256 stateless**, **circuit breaker**, **adaptive concurrency (load shedding AIMD)**, **retries idempotentes com full jitter**, **CORS + IP filter**, **W3C Trace Context (`traceparent`)**, **hot-reload** (`mtime` + `SIGHUP`) e admin **`/health`** (JSON) + **`/metrics`** (Prometheus text com counters, gauges e histogramas).

> Catálogo original menciona service mesh estilo Envoy (HTTP/2, gRPC, HTTP/3, xDS). **Este repo entrega o MVP documentado e expande o datapath com recursos avançados de produção**: streaming zero-copy, pool HTTP/1.1 e HTTP/2, JWT HS256 nativo, P2C-EWMA, load shedding AIMD, retries com full jitter, W3C tracing e hot-reload — mantendo **100% de compatibilidade com `--locked`** sem dependências externas extras. **Sem claims inventados de RPS**; meça com `wrk`/`hey` no seu hardware.

**Autor:** Vinícius Fernandes \<catalog@example.com\> · **Licença:** MIT

---

## O que este repo entrega

| Requisito do catálogo / brief | Status |
|-------------------------------|--------|
| Reverse proxy HTTP/1.1 & HTTP/2 | ✅ Hyper 1.x client+server com connection pooling e `TCP_NODELAY` |
| Streaming Zero-Copy | ✅ `BoxBody<Bytes, GatewayErrorBox>` sem bufferizar payload em RAM no caminho sem retry |
| Rotas por path, host, método e headers | ✅ primeira match vence; `RadixRouter`, host exato ou wildcard (`*.api.local`), métodos e headers |
| Canary Traffic Splitting | ✅ divisão ponderada determinística (`canary_weight: 1..=100`) entre rotas de mesmo path/host |
| Upstream load balancing (3 algoritmos) | ✅ `round_robin`, `p2c_ewma` (Power of Two Choices + Peak-EWMA em µs) e `least_conn` com skip de unhealthy |
| Health check ativo e passivo | ✅ GET `{upstream}/__gw_health` (background) + ejeção passiva por falhas consecutivas; admin `/health` |
| Rate limit token bucket | ✅ por IP, global ou `header:<nome>`, auto-evicção de chaves inativas + headers `X-RateLimit-*` e `Retry-After` |
| Request ID + W3C Trace Context | ✅ gera/propaga `X-Request-Id` (UUID v4) + `traceparent` (`00-trace_id-span_id-flags`) e `tracestate` |
| Basic auth, API key ou JWT HS256 | ✅ por rota (API key em header/query, Basic Auth e JWT RFC 7519 nativo com `constant_time_eq`) |
| Circuit breaker resiliente | ✅ `Closed → Open → HalfOpen → Closed` thread-safe |
| Adaptive concurrency (Load Shedding) | ✅ controle AIMD (estilo TCP Vegas) baseado em latência alvo vs. latência real |
| Retries idempotentes com Full Jitter | ✅ `GET/HEAD/OPTIONS/PUT/DELETE` com backoff exponencial saturado e SplitMix64 jitter (`retry.rs` + `timeout.rs`) |
| Segurança de borda (CORS, IP, Body) | ✅ preflight `OPTIONS` (`204`), filtro de IP (`allow`/`deny` → `403`), `max_body_bytes` (`413`) e header policy |
| Config YAML + Hot-Reload | ✅ `config/gateway.yaml` validado no boot + reload automático por `mtime` watcher ou `SIGHUP` + graceful drain |
| `/health` + `/metrics` | ✅ JSON (status, conexões ativas, EWMA µs) + Prometheus text 0.0.4 (counters, gauges e histogramas) |
| 30 testes (unitários + integração) | ✅ `routing`, `rate_limit`, `auth`, `circuit_breaker`, `enterprise_features` + testes unitários por módulo |
| Docs densos pt-BR | ✅ `README.md` + `ARCHITECTURE.md` + `docs/ARQUITETURA.md` + `docs/TECNOLOGIAS.md` + `docs/SPEC.md` |
| MIT | ✅ |

**Escopo de protocolo fora deste binário:** transporte QUIC/HTTP/3 em kernel bypass e servidor gRPC de plano de controle xDS remoto.

---

## Arquitetura (visão rápida)

```
Cliente HTTP/1.1 ou HTTP/2
        │
        ▼
┌──────────────────────────┐
│  Listener Tokio          │  listen / admin_listen (+ graceful drain no Ctrl+C)
└────────┬─────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│  Filter chain                                                │
│  request-id + W3C traceparent → global RL → match rota       │
│  (host wildcard / path / método / headers / canary weight)   │
│  → CORS preflight → IP filter → auth (API key / Basic / JWT) │
│  → RL da rota → max_body_bytes → adaptive concurrency (AIMD) │
│  → circuit breaker (Closed → Open → HalfOpen)                │
└────────┬─────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────┐     ┌─────────────────────────────┐
│  Multi-Strategy LB       │────►│  Upstream HTTP/1.1 & HTTP/2 │
│  (RR / P2C-EWMA / LConn  │     │  (zero-copy stream, retry   │
│   + active/passive HC)   │     │   jitter, timeout, hop-by)  │
└────────┬─────────────────┘     └─────────────────────────────┘
         │
         ▼
   /health (JSON)  /metrics (Prometheus counters + gauges + histograms)
```

Detalhes: [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md) · stack: [`docs/TECNOLOGIAS.md`](docs/TECNOLOGIAS.md) · especificação: [`docs/SPEC.md`](docs/SPEC.md)

---

## Layout

```
src/
  main.rs                     # binário api-gateway (CLI, watcher YAML, SIGHUP, Tokio runtime)
  lib.rs                      # exportações públicas da biblioteca
  config.rs                   # YAML + validação fail-fast
  error.rs                    # enum GatewayError
  gateway.rs                  # datapath (zero-copy streaming, retries, CORS, IP filter, admin)
  router.rs                   # match path/host wildcard + métodos + headers + canary + strip_prefix
  loadbalancer.rs             # round-robin, P2C + Peak-EWMA, least-connections e ActiveRequestGuard
  health.rs                   # checker ativo de upstream + report JSON admin
  metrics.rs                  # counters, gauges e histogramas de latência + exposition Prometheus
  retry.rs                    # exponential backoff + full jitter determinístico
  timeout.rs                  # verificação de deadline em ms
  watcher.rs                  # hot-reload automático monitorando mtime do YAML
  middleware/
    mod.rs
    rate_limit.rs             # token bucket com auto-evicção e headers X-RateLimit-*
    auth.rs                   # API key, Basic Auth e JWT HS256 (SHA-256 + HMAC nativos)
    request_id.rs             # X-Request-Id (UUID v4)
    circuit_breaker.rs        # Closed → Open → HalfOpen
    concurrency.rs            # adaptive concurrency limiter (load shedding AIMD)
    tracing_context.rs        # W3C Trace Context (traceparent / tracestate)
config/gateway.yaml           # configuração completa de referência
examples/gateway.basic.yaml   # configuração mínima
tests/                        # routing, rate_limit, auth, circuit_breaker, enterprise_features, VECTORS.md
fixtures/vectors.bin          # fixture binária de validação
scripts/run_tests.sh          # runner de testes em bash
tools/dev.py                  # automação portátil (test | build | package) com --locked
docs/                         # ARQUITETURA.md, TECNOLOGIAS.md, SPEC.md
```

---

## Quickstart

```bash
# dependências: Rust 1.75+ (testado com 1.85, compatível com --locked)
cargo build --release --locked

# suba um backend de eco (exemplo com Python)
python3 -c "from http.server import *; HTTPServer(('127.0.0.1',9001),BaseHTTPRequestHandler).serve_forever()" &

# ajuste config/gateway.yaml (upstreams, keys) e rode:
cargo run -- -c config/gateway.yaml

# chamada autenticada com API Key:
curl -H 'X-API-Key: dev-key-change-me' [http://127.0.0.1:8080/echo/hi](http://127.0.0.1:8080/echo/hi)

# chamada propagando W3C traceparent:
curl -H 'X-API-Key: dev-key-change-me' \
     -H 'traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' \
     [http://127.0.0.1:8080/echo/hi](http://127.0.0.1:8080/echo/hi)

# endpoints administrativos:
curl [http://127.0.0.1:8080/health](http://127.0.0.1:8080/health)
curl [http://127.0.0.1:8080/metrics](http://127.0.0.1:8080/metrics)

# recarregar configuração sem derrubar conexões (além do watcher automático de arquivo):
kill -HUP $(pgrep api-gateway)
```

Testes:

```bash
cargo test --locked
# ou
./scripts/run_tests.sh
# ou
python3 tools/dev.py test
```

---

## Configuração (YAML)

Campos principais — ver `config/gateway.yaml` e `examples/gateway.basic.yaml`.

| Campo | Descrição |
|-------|-----------|
| `listen` | Bind do proxy (default `127.0.0.1:8080`) |
| `admin_listen` | Opcional; se omitido, `/health` e `/metrics` respondem na mesma porta |
| `drain_timeout_ms` | Tempo máximo (ms) aguardando conexões encerrarem no shutdown gracioso (default `5000`) |
| `health_check` | `interval_ms` (`5000`), `timeout_ms` (`500`), `path` (`/__gw_health`), `passive_failure_threshold` (`1`) |
| `global_rate_limit` | Rate limit global antes das rotas (`capacity`, `refill_per_sec`, `key`: `ip`\|`global`\|`header:<nome>`) |
| `routes[].name` | Nome identificador da rota |
| `routes[].path_prefix` | Prefixo obrigatório começando com `/` (ex.: `/api`) |
| `routes[].host` | Host opcional (case-insensitive, ignora porta; aceita exato ou wildcard `*.api.local`) |
| `routes[].strip_prefix` | Remove o prefixo ao encaminhar (`true`\|`false`) |
| `routes[].methods` | Lista opcional de verbos HTTP permitidos (ex.: `["GET", "POST"]`) |
| `routes[].match_headers` | Mapa opcional de headers `chave: valor` obrigatórios para casar a rota |
| `routes[].canary_weight` | Peso `1..=100` para divisão Canary entre rotas de mesmo `path_prefix` e `host` (default `100`) |
| `routes[].upstreams` | Lista `http://host:port` |
| `routes[].algorithm` | Algoritmo de LB: `round_robin` (default), `p2c_ewma` ou `least_conn` |
| `routes[].rate_limit` | `capacity`, `refill_per_sec`, `key` (`ip`\|`global`\|`header:<nome>`) |
| `routes[].auth` | `type: api_key` (`header`, `query_param`, `keys`), `basic` (`users`) ou `jwt` (`secret`, `issuer`, `audience`, `required_scopes`) |
| `routes[].circuit_breaker` | `failure_threshold`, `open_ms`, `half_open_success` |
| `routes[].adaptive_concurrency` | `target_latency_ms`, `min_limit`, `max_limit` (Load Shedding AIMD) |
| `routes[].retry` | `max_retries`, `base_backoff_ms`, `max_backoff_ms` (aplicado a métodos idempotentes) |
| `routes[].cors` | `allow_origins`, `allow_methods`, `allow_headers`, `max_age_secs` |
| `routes[].headers` | `request_set`, `request_remove`, `response_set`, `response_remove` |
| `routes[].ip_filter` | Listas `allow` e `deny` de IPs |
| `routes[].max_body_bytes` | Limite máximo em bytes para o corpo da requisição |
| `routes[].timeout_ms` | Timeout do proxy em ms (default `10000`) |

---

## Códigos de resposta do gateway

| Status | Código JSON (`error`) | Quando |
|--------|-----------------------|--------|
| 204 | — | Preflight CORS (`OPTIONS`) respondido diretamente pelo gateway |
| 400 | `bad_body` | Falha ao ler stream do corpo da requisição |
| 401 | `unauthorized` | Auth falhou (API key, Basic Auth ou JWT inválido/expirado/escopo insuficiente) |
| 403 | `ip_forbidden` | IP do cliente bloqueado por `ip_filter` |
| 404 | `no_route` | Nenhuma rota casou com host, path, método ou headers |
| 413 | `payload_too_large` | Corpo da requisição excedeu `max_body_bytes` |
| 429 | `rate_limited` | Rate limit excedido (inclui `X-RateLimit-*` e `Retry-After`) |
| 502 | `upstream_error` | Erro de conexão/transporte com o upstream |
| 503 | `circuit_open` / `load_shedding` / `no_upstream` | Circuit open, limite adaptativo AIMD atingido ou sem upstream saudável |
| 504 | `upstream_timeout` | Timeout aguardando resposta do upstream |

Respostas de erro JSON: `{"error":"...","request_id":"..."}` com headers `x-request-id` e `traceparent`.

---

## Uso em games (contexto do catálogo)

Game gateway / edge na frente de dedicated servers: rate limit por IP ou header de sessão, API key de sessão e **autenticação de jogador stateless via JWT (`HS256`)** (validando assinatura, expiração e escopos em tempo constante e injetando `x-authenticated-sub` para o servidor interno), circuit breaker e load shedding adaptativo (AIMD) quando um shard degrada ou cai, balanceamento `p2c_ewma` sensível à latência em tempo real, divisões Canary para novos patches, hot-reload sem derrubar conexões ativas e métricas com histogramas de latência para ops.

---

## Licença

MIT — ver [LICENSE](LICENSE).

## Projeto como repositório

Este ZIP contém o projeto `02-02-api-gateway`. Ao extrair, use a pasta `02-02-api-gateway/`
como raiz de um repositório independente no GitHub. O tipo do projeto,
seu modo de uso e os requisitos de integração estão descritos nas seções
deste README. Nem todo projeto produz um programa executável.
