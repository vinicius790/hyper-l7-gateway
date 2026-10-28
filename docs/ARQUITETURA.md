# Arquitetura — API Gateway (catálogo 2.2)

Documento denso do datapath L7. Escopo: **Proxy Reverso HTTP/1.1 & HTTP/2 de Alta Performance**, cobrindo integralmente o contrato do MVP original e as extensões de produção (Streaming Zero-Copy, P2C + Peak-EWMA, JWT HS256 nativo, Adaptive Concurrency Load Shedding, Retries com Full Jitter, Canary Routing, W3C Trace Context e Hot-Reload).


## 1. Objetivos e não-objetivos

**Objetivos**
- Proxy reverso correto e eficiente: match de rota (path, host exato/wildcard, método, headers e peso Canary), reescrita de path (`strip_prefix`), encaminhamento zero-copy de body (`BoxBody`) e higienização de headers hop-by-hop.
- Controles de borda em defesa em profundidade: rate limit (token bucket com auto-evicção), auth stateless (`api_key`, `basic` e `jwt` HS256 com comparação em tempo constante), filtro de IP (`allow`/`deny`), CORS automático, limite de payload (`max_body_bytes`), adaptive concurrency (load shedding AIMD), circuit breaker, retries idempotentes com full jitter, `X-Request-Id` e W3C `traceparent`.
- Observabilidade completa: health check ativo e passivo agregado (`/health` JSON com conexões ativas e latência EWMA por upstream) + `/metrics` Prometheus text 0.0.4 com counters, gauges e histogramas de latência.
- Operação contínua sem downtime: hot-reload atômico monitorando alterações no YAML (`mtime` watcher + `SIGHUP`) e encerramento gracioso (`SIGINT`) com drenagem de conexões em voo.
- Testabilidade e reprodutibilidade estrita (`--locked`): lógica pura isolada por módulo + testes de integração ponta a ponta com upstreams mock, usando exclusivamente os crates presentes no `Cargo.lock`.

**Não-objetivos**
- Plano de controle distribuído xDS via gRPC externo ou transporte QUIC/HTTP/3 em kernel bypass.
- Números de RPS publicados sem contexto — qualquer benchmark deve ser reproduzido localmente no hardware alvo com `wrk`, `hey` ou `vegeta`.


## 2. Pipeline de request

accept(TCP) ──► TokioIo ──► http1::serve_connection ──► Gateway::handle(req, peer)
│
├── 01. metrics.requests_total++ e metrics.active_connections++
├── 02. TraceContext::extract_or_generate(headers)
│       (injeta/propaga X-Request-Id + W3C traceparent e tracestate)
├── 03. (opcional) rotas admin GET /health | GET /metrics na mesma porta
├── 04. global token bucket (chave IP, global ou header:)
├── 05. match_route_full(routes, host, path, method, headers)
│       (host exato/wildcard, fronteira /, método, headers e Canary weight)
├── 06. CORS preflight (OPTIONS → 204 No Content imediato)
├── 07. IpFilter (deny / allow sobre peer.ip() → 403 Forbidden)
├── 08. AuthGate::authenticate(headers, query)
│       (API key header/query, Basic ou JWT HS256 → 401; injeta x-authenticated-sub)
├── 09. rate limit da rota (429 + headers X-RateLimit-Limit/Remaining e Retry-After)
├── 10. verificação de max_body_bytes (413 Payload Too Large)
├── 11. AdaptiveConcurrency::try_acquire() (AIMD load shedding → 503)
├── 12. circuit breaker.allow() (Closed/HalfOpen permitem; Open → 503)
├── 13. LoadBalancer::pick() entre saudáveis (RoundRobin | P2cEwma | LeastConn)
├── 14. rewrite path (strip_prefix) + headers (X-Forwarded-For, Host, request_set/remove)
├── 15. hyper Client → upstream (timeout por tentativa):
│       ├─ Caminho A (sem retry): Streaming Zero-Copy direto (Incoming → BoxBody)
│       └─ Caminho B (com retry idempotente): loop com Full Jitter Backoff + deadline
├── 16. on_success / on_failure no breaker + atualização passiva do LB + release AIMD
└── 17. filtra hop-by-hop; aplica response_set/remove e CORS; grava histograma e responde

**Ordem importa:**
- Auth antes de gastar quota da rota evita *DoS autenticado invertido*.
- Rate limit e Adaptive Concurrency antes do Circuit Breaker evitam abrir o circuito por flood ou rejeição de sobrecarga na borda.


## 3. Roteamento (`src/router.rs`)

`router::match_route` e `router::match_route_full`:
- **`RadixRouter`:** árvore de prefixos indexada por segmentos de path para filtragem estruturada de rotas candidatas.
- **`host` opcional:** compara hostname sem porta, ASCII lowercase. Suporta host exato (`api.local`), curinga global (`*`) e wildcard de subdomínio (`*.svc.local`).
- **`path_prefix`:** match exato ou prefixo com fronteira `/` (`/api` casa `/api` e `/api/v1`, mas **não** casa `/apitest`).
- **Métodos e Headers:** valida lista opcional `methods` (case-insensitive) e mapa `match_headers`.
- **Canary Traffic Splitting:** quando rotas adjacentes compartilham o mesmo `host` e `path_prefix`, o roteador soma os pesos `canary_weight` e usa um contador atômico monotônico (`CANARY_COUNTER`) para repartir o tráfego na proporção exata configurada (ex.: 80% rota A, 20% rota B).
- **`rewrite_path`:** se `strip_prefix: true`, remove o prefixo; path vazio vira `/`.


## 4. Load balancing e health (`src/loadbalancer.rs` & `src/health.rs`)

`RoundRobin` (também exposto como `LoadBalancer`) mantém `Vec<(String, bool, Arc<NodeMetrics>)>`, cursor atômico e estado PRNG SplitMix64.
- **Três estratégias configuráveis por rota (`algorithm`):**
  1. `round_robin`: percorre circularmente a partir do cursor atômico e devolve o próximo upstream `healthy`.
  2. `least_conn`: escolhe o upstream saudável com menor `active_requests`, desempatando pela menor latência `ewma_latency_us`.
  3. `p2c_ewma`: sorteia 2 upstreams saudáveis distintos e seleciona o de menor custo $\text{Score} = \text{ewma\_latency\_us} \times (\text{active\_requests} + 1)$.
- **Guarda RAII (`ActiveRequestGuard`):** incrementa `active_requests` na seleção e, no `Drop` ao fim da requisição, decrementa `active_requests` e recalcula a latência **Peak-EWMA** em microssegundos (peso $0.5$ para subidas rápidas de latência e $0.2$ para decaimento suave).
- **Saúde Passiva e Ativa:**
  - Falhas consecutivas de conexão atingindo `passive_failure_threshold` marcam `healthy = false` imediatamente no datapath.
  - `HealthChecker` (em background no `serve()` de produção): a cada `interval_ms` (default 5s) executa `GET {upstream}/__gw_health` com timeout de 500ms; respostas `2xx` marcam `healthy = true` e erros/timeouts marcam `healthy = false`. Testes de integração via `serve_with_listener` não ligam o checker automático para permitir mocks leves sem rota `/__gw_health`.
- **Admin `/health`:** serializa snapshot JSON por rota (`status: "ok" | "degraded"`, `url`, `healthy`, `active_requests` e `ewma_latency_us`).


## 5. Middleware (`src/middleware/`)

### 5.1 Token bucket (`rate_limit.rs`)
Por chave (`ip`, `global` ou `header:<nome>`): capacidade $C$, recarga contínua $R$ tokens/s:

tokens = min(C, tokens + Δt * R)
allow ⇔ tokens >= 1; então tokens -= 1

Thread-safe via `parking_lot::Mutex`. Se o mapa passar de 10.000 chaves, remove entradas inativas há mais de 300s (proteção anti-OOM). Em caso de bloqueio, retorna `429 Too Many Requests`, incrementa `gw_rate_limited_total` e injeta `X-RateLimit-Limit`, `X-RateLimit-Remaining` e `Retry-After`.

### 5.2 Request ID e W3C Trace Context (`request_id.rs` & `tracing_context.rs`)
- Se `X-Request-Id` estiver ausente ou vazio → gera UUID v4. Propagado ao upstream e ecoado na resposta.
- Se `traceparent` W3C válido (`00-<trace_id 32 hex>-<span_id 16 hex>-<flags 2 hex>`) estiver presente → preserva o `trace_id` original e gera um novo `span_id` de 16 dígitos hexadecimais para o gateway; caso contrário, inicia um novo `trace_id`. Propaga `traceparent` e `tracestate`.

### 5.3 Auth e Criptografia Nativa (`auth.rs`)
- Comparação em tempo constante (`constant_time_eq`) em todas as modalidades para impedir *timing attacks*.
- `api_key`: lê do header configurável (default `X-API-Key`) ou de `query_param` opcional.
- `basic`: decodifica `Authorization: Basic base64(user:pass)` contra a lista `users`.
- `jwt`: implementação nativa de **SHA-256 (FIPS 180-4)** e **HMAC-SHA256 (RFC 2104)**. Valida tokens `Bearer <jwt>` assinados com `HS256`, checando `exp`, `nbf`, `iss`, `aud` e `required_scopes`. Propaga o `sub` autenticado via header `x-authenticated-sub`.

### 5.4 Circuit breaker (`circuit_breaker.rs`)
Estados: `Closed` → ($N$ falhas) → `Open` → (após `open_ms`) → `HalfOpen` → ($M$ sucessos) → `Closed`. Falha em `HalfOpen` reabre imediatamente para `Open`.
Contagem de falha: erro de transporte, timeout ou resposta `5xx` do upstream. Sucesso: demais status HTTP.

### 5.5 Adaptive Concurrency — Load Shedding (`concurrency.rs`)
Controlador AIMD inspirado no TCP Vegas: mantém contagem atômica de requisições `in_flight` e ajusta a janela máxima de concorrência a cada 200ms comparando a latência real observada ($T_{\text{obs}}$) com `target_latency_ms` ($T_{\text{target}}$):
- Se $T_{\text{obs}} \le T_{\text{target}}$: incrementa linearmente o limite ($+2$ até `max_limit`).
- Se $T_{\text{obs}} > T_{\text{target}}$: reduz multiplicativamente o limite ($\text{limit} \times \text{clamp}(T_{\text{target}} / T_{\text{obs}}, 0.5, 0.95)$ até `min_limit`), rejeitando excessos com `503 load_shedding`.

## 6. Métricas (`src/metrics.rs`)

Exposition Prometheus text 0.0.4:
- **Counters:** `gw_requests_total`, `gw_responses_{2xx,4xx,5xx}_total`, `gw_rate_limited_total`, `gw_auth_denied_total`, `gw_circuit_open_total`, `gw_load_shed_total`, `gw_retries_total`, `gw_proxy_errors_total` e `gw_route_requests_total{route="..."}`.
- **Gauge:** `gw_active_connections`.
- **Histograma de latência:** `gw_request_duration_seconds_bucket{le="0.001|0.005|0.01|0.025|0.05|0.1|0.25|0.5|1.0|+Inf"}`, `gw_request_duration_seconds_sum` e `gw_request_duration_seconds_count`.

## 7. Configuração, validação e hot-reload (`src/config.rs` & `src/watcher.rs`)

- `GatewayConfig::validate` exige $\ge 1$ rota, `path_prefix` começando com `/`, $\ge 1$ upstream `http(s)://`, `rate_limit` com `capacity > 0` e `refill_per_sec > 0.0` e segredo JWT não vazio. Erros de configuração falham no boot (*fail-fast*).
- **Hot-Reload Zero-Downtime:** o estado ativo reside em `RwLock<Arc<RuntimeState>>`. O watcher assíncrono (`spawn_config_watcher`) verifica o `mtime` do arquivo YAML a cada 2s (e o processo também escuta `SIGHUP` no Unix). Ao detectar modificação válida, constrói o novo `RuntimeState` e substitui o ponteiro `Arc` atomicamente sem interromper conexões ativas.

## 8. Fronteiras, hop-by-hop, streaming zero-copy e retries (`src/gateway.rs`, `src/retry.rs`, `src/timeout.rs`)

- **Headers removidos no encaminhamento:** `connection`, `keep-alive`, `transfer-encoding`, `upgrade`, `te`, `trailers`, `proxy-authenticate`, `proxy-authorization` e `host` (reescrito para o host:porta do upstream). Adiciona `X-Forwarded-For` e `X-Forwarded-Proto`.
- **Streaming Zero-Copy vs. Buffer de Retry:**
  - Quando a rota não exige retry nem `max_body_bytes`, o `Incoming` do Hyper é encadeado diretamente via `.boxed()` (`BoxBody<Bytes, GatewayErrorBox>`), realizando streaming de entrada e saída com memória constante.
  - Quando `retry` está ativo em método idempotente (`GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`), o body é coletado uma única vez (respeitando `max_body_bytes`) e retransmitido em caso de falha usando `retry::jittered_backoff` (SplitMix64 full jitter) e verificação de deadline via `timeout::timed_out`.

## 9. Mapa para a versão completa do catálogo

| Peça | MVP Original | Estado Atual neste Repositório |
|------|--------------|--------------------------------|
| Protocolos | HTTP/1.1 | HTTP/1.1 + HTTP/2 com connection pooling persistente |
| Body Datapath | Bufferizado (`collect`) | **Streaming Zero-Copy (`BoxBody`)** + buffer controlado para retries |
| Roteamento | Path prefix + Host exato | `RadixRouter` + Host Wildcard (`*.svc.local`) + Métodos + Headers + **Canary Weight** |
| Auth | API key / Basic | API key (Header/Query) + Basic + **JWT HS256 Stateless** (`constant_time_eq`) |
| LB | RR + health stub | **Round-Robin**, **P2C + Peak-EWMA (µs)** e **Least-Connections** + HC ativo/passivo |
| Resiliência | Breaker por rota | Breaker + **Adaptive Concurrency (AIMD Load Shedding)** + **Retries com Full Jitter** |
| Controle | YAML estático | YAML validado + **Hot-Reload (`mtime` watcher + `SIGHUP`)** + **Graceful Shutdown** |
| Observabilidade | Counters + `X-Request-Id` | **W3C `traceparent`/`tracestate`** + `X-Request-Id` + **Histogramas Prometheus** |

## 10. Diagrama de estados do breaker e controle adaptativo

≥ N failures

Closed ──────────► Open
▲                 │
│ ≥ M successes   │ sleep open_ms
│                 ▼
└──── HalfOpen ◄──┘
│
└── failure ──► Open

[Controle AIMD de Concorrência em paralelo]
• latency_us <= target_us  ──► limit = min(max_limit, limit + 2)
• latency_us >  target_us  ──► limit = max(min_limit, limit * clamp(target / latency, 0.5, 0.95))
