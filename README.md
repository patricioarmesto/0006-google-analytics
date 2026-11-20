# ¿Cómo funciona Google Analytics?

Ejercicio 0006 de 1024 del curso **¿Cómo funciona...?**. Nivel: Medio · Track: Analytics · Fecha: 09 feb 2027 · Repo: `como-funciona/0006-google-analytics`.

Un mini Google Analytics corriendo en `localhost:3000` que podés romper a propósito para entender por qué está diseñado así.

## Qué resuelve

Google Analytics recibe eventos de muchas páginas y los convierte en agregados útiles sin que el sitio se frene. El truco no es guardar cada evento: es **no dejar que el camino caliente haga el trabajo pesado**. Acá ese mecanismo está aislado en cinco pasos observables:

1. **El demo emite eventos.** Tres páginas, `pageview` al entrar, `click` y `custom_event` en los botones, con `event_id` UUID y `session_id` estable por pestaña.
2. **El collector deduplica.** `SETNX` en Redis con TTL de 24 h: un `event_id` repetido responde `200 duplicate` y nunca se publica. Es el límite de la idempotencia en este ejercicio.
3. **Kafka desacopla.** Si el collector escribiera directo en ClickHouse, un pico de tráfico sería un pico de INSERTs. Publica a `analytics.events` con key = `session_id` y listo.
4. **El consumer junta batches.** 1000 eventos o 5 segundos, lo que llegue primero, y **un solo INSERT**. Si ClickHouse está caído, guarda el batch en memoria, loguea y reintenta sin confirmar offsets.
5. **El dashboard lee agregados.** Cuatro consultas a ClickHouse: pageviews por día, top páginas, funnel por sesiones únicas y eventos de los últimos 5 minutos con polling de 5 s.

## Cómo correr

Requiere Docker con Compose v2 y el daemon corriendo.

```bash
docker compose up
```

La primera vez construye backend y frontend; abrí **http://localhost:3000**. Para reiniciar desde cero, incluidos volúmenes:

```bash
docker compose down -v && docker compose up
```

Los puertos de host están desplazados (8084, 6384, 9094, 8126, 9003) para poder correr varios ejercicios del curso al mismo tiempo. Dentro de la red de Compose se usan los estándar (`backend:8080`, `redis:6379`, `kafka:9092`, `clickhouse:8123`).

## Arquitectura (diagrama en texto)

```
  Navegador (React) ──POST /collect──▶ frontend (nginx :80 → :3000)
        │ GET /api/stats?...                │ /collect y /api/* → backend
        └───────────────────────────────────▶┘
                                             ▼
                                   ┌────────────────────┐
                                   │ backend Go :8080   │
                                   │ -mode=api          │
                                   │ /collect /health   │
                                   │ /api/stats         │
                                   └───┬────────────┬───┘
                    SETNX event_id     │            │ produce analytics.events
                       TTL 24 h        │            │ key = session_id
                   ┌───────────────────▼──┐  ┌──────▼──────────────────┐
                   │ Redis                │  │ Kafka (KRaft)           │
                   │ dedup, sin durable   │  │ 1 partición             │
                   └──────────────────────┘  └──────┬──────────────────┘
                                                  │ consume
                                       ┌──────────▼───────────────────┐
                                       │ consumer Go                  │
                                       │ batch 1000 / 5 s             │
                                       │ 1 INSERT + commit de offsets │
                                       └──────────┬───────────────────┘
                                                  │
                                       ┌──────────▼───────────────────┐
                                       │ ClickHouse                   │
                                       │ events (MergeTree)           │
                                       └──────────────────────────────┘
```

Separación de responsabilidades: el collector valida, deduplica y publica, pero **nunca escribe en ClickHouse**; el consumer es el único que escribe; la API es la única que lee para el dashboard. Comentarios didácticos en español rioplatense explican el porqué en cada archivo.

## Flujo

1. El demo genera el evento con `crypto.randomUUID()` y `session_id` en `sessionStorage`, y hace `POST /collect`.
2. El collector valida los cinco campos. Si está mal, `400`; si falla Redis o Kafka, `503`.
3. `SET event_id 1 NX EX 86400`: si existe, `200 duplicate` y fin. Si es nuevo, publica a Kafka. Si Kafka falla, borra la clave para que el reintento no sea un falso duplicado.
4. El consumer acumula 1000 eventos o 5 s y hace un solo INSERT en `default.events`. Recién después confirma los offsets.
5. La API consulta ClickHouse; el dashboard grafica con Recharts y refresca el panel de tiempo real cada 5 s.
6. El botón **Mandar 10.000 eventos de golpe** dispara el mismo flujo con concurrencia limitada a ~100: se ven los batches, los duplicados y los reintentos.

## Stack

| Tecnología | Rol | Servicio en Compose |
| --- | --- | --- |
| Go | Collector, consumer y API dashboard | `backend`, `consumer` |
| React + TypeScript | Sitio demo y dashboard | `frontend` |
| Vite | Bundler del frontend | `frontend` |
| Tailwind CSS | Estilos utilitarios | `frontend` |
| Recharts | Gráficos del dashboard | `frontend` |
| Redis | Dedup de `event_id` con TTL 24 h | `redis` |
| Apache Kafka (KRaft) | `analytics.events`, key `session_id` | `kafka`, `kafka-init` |
| ClickHouse | Tabla `events` y agregaciones | `clickhouse` |
| nginx | Estático y proxy de `/collect` y `/api` | `frontend` |
| Docker Compose | Orquestación y healthchecks | `docker-compose.yml` |

## Decisiones clave

- **Redis es el dedup; ClickHouse es el dato durable.** Redis expira en 24 h y no es fuente de verdad. Ver [ADR-001](docs/adr/0001-collector-no-escribe-directo.md) y [ADR-002](docs/adr/0002-clickhouse-para-eventos.md).
- **Batch de 1000 o 5 s, un solo INSERT.** Un INSERT por evento generaría 10.000 parts y ahogaría los merges. Ver [ADR-003](docs/adr/0003-batch-insert.md).
- **At-least-once, con dedup en el borde.** Si ClickHouse falla, el consumer no confirma offsets y reintenta con el batch en memoria. No prometemos Exactly-Once.
- **Un binario, dos modos.** Collector/API y consumer comparten imagen pero corren en contenedores distintos para aislar el pico de ingestión.
- **`site=demo` es decorativo.** Hay un solo sitio hardcodeado; el parámetro se acepta pero no filtra. Cualquier otro valor se rechaza con `400`.

## Alcance y decisiones de diseño

El collector corre en una réplica y el consumer en otra; la consistencia apoya en Redis (dedup de ventana corta) y en el orden de confirmación de offsets de Kafka, no en coordinación distribuida. El sitio es uno solo, hardcodeado como `demo`, sin tabla de sitios ni de usuarios. El dashboard usa polling de 5 s en vez de WebSocket porque la gránularidad del ejercicio es de segundos. Todo lo que no está en esta lista queda **fuera de alcance a propósito para mantener el ejercicio legible.** No son omisiones por desconocimiento.

## Qué NO implementamos

- **Postgres.** No hay servicio ni código que conecte. Los eventos son hechos append-only: no hay stock ni órdenes que proteger, y agregar una base relacional sólo duplicaría lo que ya hace ClickHouse. Ver [ADR-005](docs/adr/0005-sin-postgres.md).
- Transactional Outbox, DLT ni Exactly-Once Semantics. La dedup de Redis es de 24 h, no una garantía global. Ver [ADR-004](docs/adr/0004-que-queda-fuera.md).
- Schema Registry, rebalancing fino de consumidores ni monitoreo de producción (métricas, trazas, alertas).
- Integración real con Google Analytics, autenticación, multi-tenant, GDPR ni escalado multi-región.
- Python, Ollama, Qdrant ni react-router.

Estos puntos están **fuera de alcance a propósito para mantener el ejercicio legible.**

## Cómo probar

**1. Health y un evento normal:**

```bash
curl -s http://localhost:8084/health
curl -s -X POST http://localhost:8084/collect -H 'Content-Type: application/json' \
  -d '{"event_id":"11111111-1111-4111-8111-111111111111","session_id":"s-demo","page":"/","type":"pageview","timestamp":"2027-02-09T13:00:00Z"}'
# {"status":"accepted"}
```

**2. Dedup (mismo event_id):**

```bash
curl -s -X POST http://localhost:8084/collect -H 'Content-Type: application/json' \
  -d '{"event_id":"11111111-1111-4111-8111-111111111111","session_id":"s-demo","page":"/","type":"pageview","timestamp":"2027-02-09T13:00:00Z"}'
# {"status":"duplicate"}
docker compose exec redis redis-cli TTL event:11111111-1111-4111-8111-111111111111
```

**3. Los cuatro modos del dashboard:**

```bash
curl -s 'http://localhost:8084/api/stats?pageviews-by-day&site=demo'
curl -s 'http://localhost:8084/api/stats?top-pages'
curl -s 'http://localhost:8084/api/stats?funnel'
curl -s 'http://localhost:8084/api/stats?realtime'
```

También directo en ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --query \
  'SELECT page, count(), uniqExact(session_id) FROM default.events GROUP BY page'
```

**4. Probar el batch:** abrí http://localhost:3000, andá a **Cómo romperlo** y apretá **Mandar 10.000 eventos de golpe**. En otra terminal:

```bash
docker compose logs -f consumer
# batch: flush OK de 1000 elementos  (≈10 veces)
```

**5. Probar el reintento:**

```bash
docker compose stop clickhouse
# apretá el botón de 10.000 eventos: el consumer loguea el error y conserva el batch
docker compose start clickhouse
# los eventos aparecen en el dashboard sin reenviarlos
```

**6. Tests del backend:**

```bash
cd backend && go test ./...
```

## ADRs

- [ADR-001 — Collector no escribe directo](docs/adr/0001-collector-no-escribe-directo.md)
- [ADR-002 — ClickHouse para eventos](docs/adr/0002-clickhouse-para-eventos.md)
- [ADR-003 — Batch insert](docs/adr/0003-batch-insert.md)
- [ADR-004 — Qué queda fuera](docs/adr/0004-que-queda-fuera.md)
- [ADR-005 — Sin Postgres](docs/adr/0005-sin-postgres.md)
