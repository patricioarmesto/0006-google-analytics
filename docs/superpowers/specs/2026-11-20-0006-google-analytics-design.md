# Diseño — 0006 Google Analytics

- Ejercicio: 0006 de 1024 del curso **¿Cómo funciona...?**
- Nivel: Medio · Track: Analytics · Fecha del ejercicio: 09 feb 2027
- Repo: `como-funciona/0006-google-analytics`
- Estado: Aceptado · Fecha de este documento: 2026-11-20

## 1. Objetivo y criterio de éxito

Construir un mini Google Analytics que corra con `docker compose up` y abra en `http://localhost:3000`. El alumno debe poder ver, medir y provocar el caso difícil: un pico de 10.000 eventos, deduplicación, batches y reintentos. El éxito es que el flujo completo funcione de punta a punta sin pasos manuales y que los comentarios didácticos en español rioplatense expliquen **por qué** cada decisión, no sólo qué hace.

No se busca parecerse al producto real en features: se busca que el mecanismo (dedup → cola → batch → columnar → dashboard) sea observable.

## 2. Alcance

**Incluido:**

- Sitio demo con 3 páginas, eventos `pageview`, `click` y `custom_event`, y botón "Mandar 10.000 eventos de golpe".
- Collector en Go: validación, dedup en Redis con `SETNX` y TTL de 24 h, publicación a Kafka con key `session_id`, `/health`.
- Consumer en Go: batch de 1000 eventos o 5 segundos, un solo `INSERT` a ClickHouse, reintento sin perder el batch.
- API dashboard en Go: cuatro modos de `GET /api/stats`.
- Dashboard en React con Recharts: pageviews por día, top páginas, funnel y panel en tiempo real con polling de 5 s.
- Compose con Redis, Kafka en modo KRaft (sin Zookeeper), ClickHouse, collector/API y consumer.
- README con el formato estándar del curso y 5 ADRs.

**Fuera de alcance a propósito:**

- **Postgres.** No hay servicio, no hay código que conecte, no hay tabla `sites` ni `users`. El sitio es uno solo y está hardcodeado como `"demo"`. Ver ADR-005.
- Integración real con Google Analytics ni con ningún producto externo.
- Transactional Outbox, DLT, Exactly-Once Semantics, Schema Registry, rebalancing fino de consumidores ni monitoreo de producción.
- Autenticación, multi-tenant, retención/TTL de eventos en ClickHouse, GDPR, escalado multi-región.
- Python, Ollama, Qdrant y cualquier otra tecnología que no figure en Compose.

Estas exclusiones son decisiones intencionales para mantener el ejercicio legible. No son omisiones por desconocimiento.

## 3. Stack y puertos

| Tecnología | Rol | Servicio |
| --- | --- | --- |
| Go | Collector, consumer y API dashboard | `backend`, `consumer` |
| React + TypeScript | Sitio demo y dashboard | `frontend` |
| Vite | Bundler y build del frontend | `frontend` |
| Tailwind CSS | Estilos utilitarios | `frontend` |
| Recharts | Gráficos del dashboard | `frontend` |
| Redis | Dedup de `event_id` con TTL 24 h | `redis` |
| Apache Kafka (KRaft) | Topic `analytics.events`, key `session_id` | `kafka`, `kafka-init` |
| ClickHouse | Tabla `events` y agregaciones | `clickhouse` |
| nginx | Servidor estático y proxy de `/collect` y `/api` | `frontend` |
| Docker Compose | Orquestación y healthchecks | `docker-compose.yml` |

Puertos de host desplazados para no chocar con los demás ejercicios del curso; dentro de la red de Compose se usan los estándar:

| Host | Contenedor | Servicio |
| --- | --- | --- |
| `3000` | `80` | frontend |
| `8084` | `8080` | backend (collector + API) |
| `6384` | `6379` | Redis |
| `9094` | `9092` | Kafka |
| `8126` | `8123` | ClickHouse HTTP |
| `9003` | `9000` | ClickHouse nativo |

## 4. Arquitectura (diagrama en texto)

```
  Navegador (React)
        │  POST /collect
        │  GET  /api/stats?...
        ▼
  ┌──────────────────────────────┐
  │ frontend (nginx :80 → :3000) │
  │ build de Vite + proxy        │
  └──────────────┬───────────────┘
                 │
  ┌──────────────▼───────────────┐
  │ backend Go :8080             │
  │ -mode=api                    │
  │ POST /collect  GET /health   │
  │ GET /api/stats               │
  └───┬──────────────────────┬───┘
      │ SETNX event_id       │ produce analytics.events
      │ TTL 24 h             │ key = session_id
  ┌───▼──────────┐    ┌──────▼───────────────────┐
  │ Redis :6379  │    │ Kafka :9092 (KRaft)      │
  │ dedup        │    │ topic analytics.events   │
  └──────────────┘    └──────┬───────────────────┘
                            │ consume
                   ┌────────▼─────────────────────┐
                   │ consumer Go                  │
                   │ batch 1000 eventos / 5 s     │
                   │ 1 INSERT, reintento          │
                   └────────┬─────────────────────┘
                            │
                   ┌────────▼─────────────────────┐
                   │ ClickHouse                   │
                   │ events (MergeTree)           │
                   │ pageviews / top / funnel /   │
                   │ realtime                     │
                   └──────────────────────────────┘
```

Separación de responsabilidades: el collector valida y dedup, pero **nunca escribe en ClickHouse**; Kafka desacopla ingestión de analítica; el consumer es el único que escribe en `events`; la API es el único que lee para el dashboard. Comentarios didácticos en español rioplatense en cada archivo Go y en los componentes React clave.

## 5. Componentes y contratos

### 5.1 Sitio demo (frontend)

- Tres páginas: `/` (Home), `/productos`, `/contacto`. Sin react-router: se cambia de vista con estado local + History API, para no agregar librerías.
- Cada visita emite un `pageview`; los botones emiten `click`; una acción explícita emite `custom_event`.
- `event_id` generado con `crypto.randomUUID()`; `session_id` persistido en `sessionStorage`; `timestamp` en ISO 8601 con `new Date().toISOString()`.
- `POST /collect` con JSON: `{ event_id, session_id, page, type, timestamp }`. `type` ∈ `{pageview, click, custom_event}`.
- Botón "Mandar 10.000 eventos de golpe" con concurrencia limitada (≈100 requests en vuelo) para no ahogar el navegador; muestra contador de enviados/duplicados/errores.

### 5.2 Collector (`backend`, `-mode=api`)

- `POST /collect`:
  1. Decodifica JSON con `DisallowUnknownFields` y valida los cinco campos: UUID válido, `session_id` no vacío, `page` no vacío, `type` enumerado y `timestamp` parseable.
  2. `SET event_id 1 NX EX 86400` en Redis. Si devuelve `0`, responde `200 {"status":"duplicate"}` y **no publica**.
  3. Si devuelve `1`, publica a `analytics.events` con key = `session_id` y responde `200 {"status":"accepted"}`.
  4. Si Redis o Kafka fallan, responde `503` y el frontend conserva el evento para reintentar: se prefiere perder un request que publicar sin dedup.
- `GET /health`: `200`.
- `GET /api/stats?...`: delega a ClickHouse.
- Comentario didáctico central: por qué el collector no escribe directo en la base (acoplamiento, picos de tráfico, latencia del request, y el batch que se viene).

### 5.3 Consumer (`consumer`, `-mode=consumer`)

- Suscripto a `analytics.events` con un consumer group fijo.
- Acumula hasta **1000 eventos o 5 segundos**, lo que llegue primero: `select` entre un `time.Ticker` y el canal de mensajes; al superar 1000 dispara el flush.
- Un solo `INSERT` multi-fila a `events (date, timestamp, event_id, session_id, page, type)`.
- Si ClickHouse falla: loguea el error, **no confirma offsets**, mantiene el batch en memoria y reintenta con backoff. Semántica at-least-once; la dedup de Redis es la primera línea de defensa.
- Comentario didáctico central: por qué batch y no insert por evento (cada insert genera un *part*; Many small parts → merge pressure → se muere el merge y después la consulta).

### 5.4 API dashboard (`backend`, `-mode=api`)

Cuatro modos, seleccionados por query param:

| Request | SQL (esencia) |
| --- | --- |
| `?pageviews-by-day&site=demo` | `SELECT toDate(timestamp) AS day, count() FROM events GROUP BY day ORDER BY day` |
| `?top-pages` | `SELECT page, count() AS n FROM events GROUP BY page ORDER BY n DESC LIMIT 10` |
| `?funnel` | Sesiones únicas por paso con orden temporal estricto `pageview → click → custom_event` dentro de la misma `session_id` |
| `?realtime` | Eventos de los últimos 5 minutos (`timestamp >= now() - INTERVAL 5 MINUTE`) |

El sitio es uno solo y está hardcodeado como `"demo"`; `site=demo` se acepta para respetar el enunciado pero no filtra: se documenta en README y ADR para que no parezca olvido.

### 5.5 Infra

- `docker-compose.yml` con `redis`, `kafka`, `kafka-init`, `clickhouse`, `backend` y `consumer`, healthchecks y `depends_on` con condiciones.
- Kafka KRaft sin Zookeeper; `kafka-init` crea `analytics.events` con 1 partición y factor de replicación 1.
- `clickhouse/init.sql` con la tabla exacta:

```sql
CREATE TABLE IF NOT EXISTS default.events
(
    date       Date,
    timestamp  DateTime64(3),
    event_id   String,
    session_id String,
    page       String,
    type       String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, page, timestamp);
```

- Dockerfile multi-stage para Go y para el frontend; nginx proxy de `/collect` y `/api/*` a `backend:8080`.

## 6. Flujo de datos

1. El demo genera un evento y hace `POST /collect`.
2. El collector valida y hace `SETNX` en Redis. Duplicado → `200 duplicate`, fin.
3. Nuevo → publica a Kafka con key `session_id` (la key mantiene los eventos de una sesión juntos y ordenados dentro de la partición).
4. El consumer junta 1000 eventos o 5 s y hace un solo INSERT en ClickHouse.
5. La API consulta ClickHouse y el dashboard grafica; el panel de tiempo real hace polling cada 5 s.
6. El botón de 10.000 eventos dispara el mismo flujo en escala: se ven los `duplicate`, los batches de 1000 y, si se cae ClickHouse, los reintentos en `docker compose logs -f consumer`.

## 7. Manejo de errores

| Fallo | Comportamiento |
| --- | --- |
| Evento inválido | `400` con el campo que falló; no se publica. |
| `event_id` repetido | `200 duplicate`; no se publica. |
| Redis caído | `503`; el frontend reintenta. No se publica sin dedup. |
| Kafka caído | `503`; el frontend reintenta. |
| ClickHouse caído | El consumer loguea, no confirma offset, reintenta con el batch en memoria. La API devuelve `502` con mensaje claro. |
| Canal del batch lleno | No aplica: el flush está gobernado por tamaño y tiempo, no por un canal con pérdida. |

## 8. Documentación

README con, en este orden: título y metadata del ejercicio, **Qué resuelve**, **Cómo correr**, **Arquitectura** (texto), **Flujo**, **Stack**, **Decisiones clave**, **Alcance y decisiones de diseño**, **Qué NO implementamos**, **Cómo probar**, **ADRs**.

ADRs (formato estándar: Contexto / Decisión / Consecuencias / Alternativas / Referencias):

1. `0001-collector-no-escribe-directo.md` — collector no escribe directo en la base.
2. `0002-clickhouse-para-eventos.md` — ClickHouse para eventos.
3. `0003-batch-insert.md` — batch insert de 1000 o 5 s.
4. `0004-que-queda-fuera.md` — qué queda fuera (Outbox, DLT, Exactly-Once, Schema Registry, rebalancing fino, monitoreo de producción).
5. `0005-sin-postgres.md` — por qué no usamos Postgres en este ejercicio.

Tono profesional y no defensivo; frases obligatorias: "Alcance y decisiones de diseño" y "fuera de alcance a propósito".

## 9. Verificación

- `go build ./...` y `go test ./...` en `backend/`.
- `npm run build` en `frontend/`.
- `docker compose config` y `docker compose up --build`.
- Manual: `curl` a `/health`, un `POST /collect` válido, el mismo `event_id` repetido (ver `duplicate`), los cuatro modos de `/api/stats`, y el botón de 10.000 con `docker compose logs -f consumer`.

## 10. Decisiones que cierran ambigüedades del enunciado

- **`site=demo`:** parámetro aceptado, sin filtro real; un solo sitio hardcodeado. No hay tabla `sites`.
- **Funnel:** requiere orden temporal estricto dentro de la misma sesión; no alcanza con que la sesión haya emitido los tres tipos en cualquier orden.
- **Dedup vs. entrega:** Redis es dedup de ventana corta (24 h), no una garantía de exactly-once. Ante su caída, se rechaza el evento; no se publica "a ciegas".
- **Un binario, dos modos:** collector/API y consumer comparten Dockerfile y código, pero corren como contenedores distintos para aislar el pico de ingestión del consumo.
- **Sin librerías de más:** no se agrega react-router ni state manager; estado local y History API alcanzan.
