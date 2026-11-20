# 0006 Google Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir el mini Google Analytics del ejercicio 0006: demo React → collector Go con dedup en Redis → Kafka → consumer con batch a ClickHouse → API y dashboard con Recharts, corriendo con `docker compose up` en `localhost:3000`.

**Architecture:** Un solo binario Go con dos modos (`-mode=api` para collector + dashboard, `-mode=consumer` para Kafka → ClickHouse) y dos contenedores separados. El collector valida y deduplica en Redis pero nunca escribe en la base; Kafka desacopla la ingestión del análisis; el consumer agrupa 1000 eventos o 5 segundos y hace un solo INSERT; la API consulta ClickHouse por HTTP. El frontend es un build de Vite servido por nginx, que hace de proxy de `/collect` y `/api`.

**Tech Stack:** Go 1.23, `github.com/redis/go-redis/v9`, `github.com/segmentio/kafka-go`, `github.com/google/uuid`, ClickHouse vía interfaz HTTP (sin driver), React 18 + TypeScript + Vite 5 + Tailwind 3 + Recharts 2, Docker Compose v2, Redis 7, Kafka 3.8 KRaft, ClickHouse 24.3.

**Spec:** `docs/superpowers/specs/2026-11-20-0006-google-analytics-design.md`

## Global Constraints

- Sin Postgres: no hay servicio, DSN, driver ni tabla `sites`/`users`. Sitio único hardcodeado como `"demo"`.
- Sin Python, Ollama, Qdrant, react-router ni state manager.
- Sin Transactional Outbox, DLT, Exactly-Once Semantics, Schema Registry, rebalancing fino ni monitoreo de producción.
- Comentarios didácticos en **español rioplatense** en todo el código, explicando el **por qué**, no sólo el qué.
- Tono profesional y no defensivo en documentación. Frases obligatorias: "Alcance y decisiones de diseño" y "fuera de alcance a propósito".
- Puertos de host: frontend `3000`, backend `8084`, Redis `6384`, Kafka `9094`, ClickHouse HTTP `8126` / nativo `9003`. Internos: `80`, `8080`, `6379`, `9092`, `8123`, `9000`.
- Topic Kafka: `analytics.events`, 1 partición, key = `session_id`.
- Tabla ClickHouse exacta: `events (date Date, timestamp DateTime64(3), event_id String, session_id String, page String, type String) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (date, page, timestamp)`.
- Dedup: `SET event_id NX EX 86400` en Redis. TTL 24 h.
- Batch: 1000 eventos o 5 segundos, lo que llegue primero; un solo INSERT; si falla, conservar el batch y no confirmar offsets.
- Dashboard: polling de 5 s; botón de 10.000 eventos con concurrencia limitada.

## Review Focus

Estas son las entradas/fallas que el spec implica pero que ningún test "feliz" cubre. Cada línea tiene su test en la tarea que posee el código.

1. **`event_id` repetido** → debe responder `200 duplicate` y NO publicar a Kafka. Test: `TestCollectorDuplicate` (Task 3).
2. **Kafka falla después del `SETNX`** → debe borrar la clave de Redis para que el reintento no sea tratado como duplicado. Test: `TestCollectorRollsBackDedupOnProduceError` (Task 3).
3. **Flush fallido** → el batch no se descarta y el `commit` de offsets no ocurre. Test: `TestBatcherKeepsBatchOnFlushError` (Task 4) y `TestConsumerFlushCommitsOnlyOnSuccess` (Task 5).
4. **ClickHouse caído** → reintento con backoff y log, sin pérdida del batch en memoria. Test: `TestClickHouseInsertBatchRetries` (Task 5).
5. **Evento inválido y `site` raro** → `400` para evento inválido y para `site` distinto de `demo`; `503` para fallo de infra. Tests: `TestValidateEvent` (Task 2) y `TestStatsHandlerRejectsUnknownSite` (Task 6).

---

### Task 1: Infraestructura base

**Files:**
- Create: `docker-compose.yml`
- Create: `.gitignore`
- Create: `clickhouse/init.sql`
- Create: `backend/Dockerfile`
- Create: `frontend/Dockerfile`
- Create: `frontend/nginx.conf`

**Interfaces:**
- Consumes: nada.
- Produces: servicios `redis:6379`, `kafka:9092`, `clickhouse:8123`, `backend:8080`, `frontend:80`; tabla `default.events`.

- [ ] **Step 1: Crear `.gitignore`**

```gitignore
node_modules/
dist/
*.log
.DS_Store
```

- [ ] **Step 2: Crear `clickhouse/init.sql`**

```sql
-- events es la única tabla del ejercicio: hechos append-only, uno por evento.
-- MergeTree con ORDER BY (date, page, timestamp) deja el gráfico de pageviews
-- por día y el top de páginas casi gratis: los datos ya llegan agrupados en parts.
CREATE TABLE IF NOT EXISTS default.events
(
    date       Date,
    timestamp  DateTime64(3, 'UTC'),
    event_id   String,
    session_id String,
    page       String,
    type       String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, page, timestamp);
```

- [ ] **Step 3: Crear `backend/Dockerfile`**

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /app/server .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /app/server /app/server
EXPOSE 8080
ENTRYPOINT ["/app/server"]
```

- [ ] **Step 4: Crear `frontend/Dockerfile`**

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
```

- [ ] **Step 5: Crear `frontend/nginx.conf`**

```nginx
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    # El navegador usa rutas relativas: mismo origen, sin CORS.
    # /collect y /api van al collector/API en Go.
    location /collect {
        proxy_pass http://backend:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_read_timeout 30s;
    }

    location /api/ {
        proxy_pass http://backend:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_read_timeout 30s;
    }
}
```

- [ ] **Step 6: Crear `docker-compose.yml`**

```yaml
name: como-funciona-0006-google-analytics

# Seis servicios con un solo `docker compose up`. Los healthchecks y los
# depends_on con conditions fijan el orden: primero Redis/Kafka/ClickHouse,
# después el init del topic, recién ahí collector, consumer y frontend.
x-app-env: &app-env
  KAFKA_BROKERS: "kafka:9092"
  KAFKA_TOPIC: "analytics.events"
  CLICKHOUSE_URL: "http://clickhouse:8123"

services:
  redis:
    image: redis:7-alpine
    container_name: ga-redis
    ports:
      - "6384:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 20

  kafka:
    image: apache/kafka:3.8.0
    container_name: ga-kafka
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    ports:
      - "9094:9092"
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 30
      start_period: 15s

  # KRaft no tiene Zookeeper, pero el topic sí hay que crearlo. Este servicio
  # termina y sale; api y consumer esperan a que termine.
  kafka-init:
    image: apache/kafka:3.8.0
    container_name: ga-kafka-init
    entrypoint:
      - /bin/bash
      - -c
      - |
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic analytics.events --partitions 1 --replication-factor 1
    depends_on:
      kafka:
        condition: service_healthy
    restart: "no"

  clickhouse:
    image: clickhouse/clickhouse-server:24.3-alpine
    container_name: ga-clickhouse
    ports:
      - "8126:8123"
      - "9003:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD-SHELL", "clickhouse-client --query 'SELECT 1' || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 10s

  backend:
    build: ./backend
    container_name: ga-backend
    command: ["-mode=api"]
    environment:
      <<: *app-env
      PORT: "8080"
      REDIS_ADDR: "redis:6379"
    ports:
      - "8084:8080"
    depends_on:
      redis:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
      clickhouse:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 10s

  # Mismo binario, otro proceso: un pico de 10.000 eventos no compite con el
  # consume por los mismos goroutines.
  consumer:
    build: ./backend
    container_name: ga-consumer
    command: ["-mode=consumer"]
    environment:
      <<: *app-env
    depends_on:
      kafka-init:
        condition: service_completed_successfully
      clickhouse:
        condition: service_healthy
    restart: on-failure

  frontend:
    build: ./frontend
    container_name: ga-frontend
    ports:
      - "3000:80"
    depends_on:
      backend:
        condition: service_healthy

volumes:
  redis_data:
  kafka_data:
  clickhouse_data:
```

- [ ] **Step 7: Validar la configuración**

Run: `docker compose config >/dev/null && echo OK`
Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add .gitignore docker-compose.yml clickhouse/init.sql backend/Dockerfile frontend/Dockerfile frontend/nginx.conf
git commit -m "feat: add compose infra for google analytics exercise"
```

---

### Task 2: Modelo de evento y validación

**Files:**
- Create: `backend/go.mod`
- Create: `backend/event.go`
- Test: `backend/event_test.go`

**Interfaces:**
- Consumes: nada.
- Produces: `Event`, `ErrInvalidEvent`, `Validate() error`, constantes `TypePageview`, `TypeClick`, `TypeCustomEvent`, `Site = "demo"`.

- [ ] **Step 1: Crear `backend/go.mod`**

```go
module github.com/como-funciona/0006-google-analytics/backend

go 1.23

require (
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.7.3
	github.com/segmentio/kafka-go v0.4.47
)
```

`go.sum` se genera con `go mod tidy` en el Step 5.

- [ ] **Step 2: Crear `backend/event.go`**

```go
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Site es el único sitio del ejercicio. Está hardcodeado a propósito: meter una
// tabla sites con una fila sería decoración, no una decisión de diseño.
const Site = "demo"

const (
	TypePageview    = "pageview"
	TypeClick       = "click"
	TypeCustomEvent = "custom_event"
)

var ErrInvalidEvent = errors.New("evento inválido")

// Event es el contrato entre el demo de React y todo el pipeline. Va en el
// body de POST /collect y, serializado, en el value del mensaje de Kafka.
type Event struct {
	EventID   string    `json:"event_id"`
	SessionID string    `json:"session_id"`
	Page      string    `json:"page"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// Validate chequea los cinco campos antes de tocar Redis. Validar acá y no en
// ClickHouse esbarre el error al borde: un evento mal formado nunca entra a la
// cola y nunca ocupa un lugar en un batch.
func (e Event) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("%w: event_id no es UUID: %v", ErrInvalidEvent, err)
	}
	if e.SessionID == "" {
		return fmt.Errorf("%w: session_id vacío", ErrInvalidEvent)
	}
	if e.Page == "" {
		return fmt.Errorf("%w: page vacía", ErrInvalidEvent)
	}
	switch e.Type {
	case TypePageview, TypeClick, TypeCustomEvent:
	default:
		return fmt.Errorf("%w: type %q no soportado", ErrInvalidEvent, e.Type)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp cero", ErrInvalidEvent)
	}
	return nil
}
```

- [ ] **Step 3: Crear `backend/event_test.go`**

```go
package main

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validEvent() Event {
	return Event{
		EventID:   uuid.NewString(),
		SessionID: "s-1",
		Page:      "/productos",
		Type:      TypeClick,
		Timestamp: time.Date(2027, 2, 9, 13, 0, 0, 0, time.UTC),
	}
}

func TestValidateEvent(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Event)
		want   bool
	}{
		{"valido", func(e *Event) {}, false},
		{"uuid malo", func(e *Event) { e.EventID = "no-uuid" }, true},
		{"sin session", func(e *Event) { e.SessionID = "" }, true},
		{"sin page", func(e *Event) { e.Page = "" }, true},
		{"type desconocido", func(e *Event) { e.Type = "scroll" }, true},
		{"sin timestamp", func(e *Event) { e.Timestamp = time.Time{} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(&e)
			err := e.Validate()
			if got := err != nil; got != tc.want {
				t.Fatalf("Validate() error = %v, want error = %v", err, tc.want)
			}
			if tc.want && !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Validate() = %v, no envuelve ErrInvalidEvent", err)
			}
		})
	}
}
```

- [ ] **Step 4: Correr el test y verlo fallar**

Run: `cd backend && go test ./...`
Expected: falla con "no required module provides package github.com/google/uuid" (el `go.sum` todavía no existe).

- [ ] **Step 5: Generar `go.sum` y verlo pasar**

Run: `cd backend && go mod tidy && go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/go.mod backend/go.sum backend/event.go backend/event_test.go
git commit -m "feat: add event model and validation"
```

---

### Task 3: Collector con dedup en Redis y publicación a Kafka

**Files:**
- Create: `backend/dedup.go`
- Create: `backend/collector.go`
- Test: `backend/collector_test.go`

**Interfaces:**
- Consumes: `Event`, `ErrInvalidEvent`, `Site` (Task 2).
- Produces: `Deduper` (interfaz con `SetNX`, `Delete`), `RedisDeduper`, `Collector`, `CollectResult`, `ResultAccepted`, `ResultDuplicate`, `Collector.Collect(ctx, Event) (CollectResult, error)`, `HandleCollect(w, r)`.

- [ ] **Step 1: Crear `backend/dedup.go`**

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduper es chico a propósito: el collector no necesita conocer Redis para
// ser testeable. En producción lo implementa Redis; en tests, un fake en memoria.
type Deduper interface {
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Delete(ctx context.Context, keys ...string) error
}

type RedisDeduper struct{ client *redis.Client }

func NewRedisDeduper(client *redis.Client) RedisDeduper { return RedisDeduper{client: client} }

func (d RedisDeduper) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return d.client.SetNX(ctx, key, value, ttl).Result()
}

func (d RedisDeduper) Delete(ctx context.Context, keys ...string) error {
	return d.client.Del(ctx, keys...).Err()
}
```

- [ ] **Step 2: Crear `backend/collector.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer también es una interfaz chica: kafka-go.Writer la cumple sin adapters.
type Producer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type CollectResult string

const (
	ResultAccepted  CollectResult = "accepted"
	ResultDuplicate CollectResult = "duplicate"
)

type Collector struct {
	deduper  Deduper
	producer Producer
	topic    string
	ttl      time.Duration
}

func NewCollector(d Deduper, p Producer, topic string, ttl time.Duration) *Collector {
	return &Collector{deduper: d, producer: p, topic: topic, ttl: ttl}
}

// Collect es el camino caliente. El orden importa:
//  1. validar (barato, sin IO);
//  2. SETNX con TTL 24 h: si el event_id ya estuvo, respuesta 200 y a casa;
//  3. publicar a Kafka con key = session_id.
//
// ¿Por qué el collector NO escribe directo en ClickHouse? Porque cada insert
// abriría una conexión desde el borde, convertiría un pico de tráfico en un
// pico de parts en ClickHouse y ataría la latencia del request al disco. Acá
// sólo valida, deduplica y publica: el trabajo pesado lo hace el consumer.
func (c *Collector) Collect(ctx context.Context, e Event) (CollectResult, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}

	key := "event:" + e.EventID
	first, err := c.deduper.SetNX(ctx, key, 1, c.ttl)
	if err != nil {
		return "", fmt.Errorf("dedup en Redis: %w", err)
	}
	if !first {
		return ResultDuplicate, nil
	}

	body, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	if err := c.producer.WriteMessages(ctx, kafka.Message{
		Topic: c.topic,
		Key:   []byte(e.SessionID), // junta los eventos de una sesión en una partición
		Value: body,
	}); err != nil {
		// Si Kafka falla después del SETNX, la clave queda y un reintento sería
		// tratado como duplicado: se borró para que el reintento pueda publicar.
		// Esto no es Exactly-Once: es sentido común para no perder el evento.
		_ = c.deduper.Delete(ctx, key)
		return "", fmt.Errorf("publicar en Kafka: %w", err)
	}
	return ResultAccepted, nil
}

// HandleCollect traduce errores de dominio a HTTP: 400 si el evento está mal,
// 503 si falló la infra. El 503 invita al frontend a reintentar.
func HandleCollect(c *Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var e Event
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&e); err != nil {
			http.Error(w, "JSON inválido: "+err.Error(), http.StatusBadRequest)
			return
		}
		result, err := c.Collect(r.Context(), e)
		if err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, ErrInvalidEvent) {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": string(result)})
	}
}
```

- [ ] **Step 3: Crear `backend/collector_test.go`**

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeDeduper struct {
	keys   map[string]bool
	setErr error
}

func (d *fakeDeduper) SetNX(_ context.Context, key string, _ any, _ time.Duration) (bool, error) {
	if d.setErr != nil {
		return false, d.setErr
	}
	if d.keys[key] {
		return false, nil
	}
	d.keys[key] = true
	return true, nil
}

func (d *fakeDeduper) Delete(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(d.keys, k)
	}
	return nil
}

type fakeProducer struct {
	msgs   []kafka.Message
	err    error
}

func (p *fakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func newCollectorForTest(d Deduper, p Producer) *Collector {
	return NewCollector(d, p, "analytics.events", 24*time.Hour)
}

func TestCollectorAcceptsAndUsesSessionAsKey(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{}
	e := validEvent()

	got, err := newCollectorForTest(d, p).Collect(context.Background(), e)
	if err != nil || got != ResultAccepted {
		t.Fatalf("Collect() = %q, %v; quería accepted, nil", got, err)
	}
	if len(p.msgs) != 1 {
		t.Fatalf("mensajes publicados = %d, quería 1", len(p.msgs))
	}
	if string(p.msgs[0].Key) != e.SessionID {
		t.Fatalf("key = %q, quería %q", p.msgs[0].Key, e.SessionID)
	}
}

func TestCollectorDuplicate(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{}
	c := newCollectorForTest(d, p)

	e := validEvent()
	e.EventID = "11111111-1111-4111-8111-111111111111"

	first, err := c.Collect(context.Background(), e)
	if err != nil || first != ResultAccepted {
		t.Fatalf("primer Collect() = %q, %v", first, err)
	}
	second, err := c.Collect(context.Background(), e)
	if err != nil || second != ResultDuplicate {
		t.Fatalf("segundo Collect() = %q, %v; quería duplicate, nil", second, err)
	}
	if len(p.msgs) != 1 {
		t.Fatalf("mensajes publicados = %d, quería 1", len(p.msgs))
	}
}

func TestCollectorRollsBackDedupOnProduceError(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{err: errors.New("kafka caído")}
	c := newCollectorForTest(d, p)
	e := validEvent()

	if _, err := c.Collect(context.Background(), e); err == nil {
		t.Fatal("quería error de Kafka")
	}
	if d.keys["event:"+e.EventID] {
		t.Fatal("la clave de dedup quedó seteada; el reintento sería duplicate")
	}

	// El reintento con Kafka ya arriba debe publicar.
	p.err = nil
	got, err := c.Collect(context.Background(), e)
	if err != nil || got != ResultAccepted {
		t.Fatalf("reintento = %q, %v; quería accepted, nil", got, err)
	}
}

func TestCollectorRejectsInvalidEvent(t *testing.T) {
	c := newCollectorForTest(&fakeDeduper{keys: map[string]bool{}}, &fakeProducer{})
	e := validEvent()
	e.Type = "scroll"
	if _, err := c.Collect(context.Background(), e); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("Collect() = %v, quería ErrInvalidEvent", err)
	}
}
```

- [ ] **Step 4: Correr los tests**

Run: `cd backend && go test ./...`
Expected: PASS (4 tests nuevos + los de `event_test.go`).

- [ ] **Step 5: Commit**

```bash
git add backend/dedup.go backend/collector.go backend/collector_test.go
git commit -m "feat: add collector with redis dedup and kafka publish"
```

---

### Task 4: Batcher genérico de 1000 eventos o 5 segundos

**Files:**
- Create: `backend/batcher.go`
- Test: `backend/batcher_test.go`

**Interfaces:**
- Consumes: nada (genérico).
- Produces: `RunBatcher[T any](ctx, in <-chan T, maxSize int, every time.Duration, flush func([]T) error, logf func(string, ...any))`.

- [ ] **Step 1: Crear `backend/batcher.go`**

```go
package main

import (
	"context"
	"time"
)

// RunBatcher junta hasta maxSize elementos o los espera `every`, lo que pase
// primero. Es genérico porque el consumer necesita meter el mensaje de Kafka
// junto al evento: así puede hacer commit de los offsets recién cuando el
// INSERT salió bien.
//
// ¿Por qué batch y no un INSERT por evento? ClickHouse escribe un "part" por
// insert. A 10.000 eventos, insertar de a uno genera 10.000 parts y el merge
// se ahoga; con batches de 1000 son 10 inserts y 10 parts. Esa es la diferencia
// entre andar y no andar, y es lo que el botón de "romperlo" deja ver.
//
// Si flush falla, el batch NO se descarta: se loguea y se reintenta en el
// próximo tick. Perder un batch en memoria sería perder eventos para siempre.
func RunBatcher[T any](
	ctx context.Context,
	in <-chan T,
	maxSize int,
	every time.Duration,
	flush func([]T) error,
	logf func(string, ...any),
) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	batch := make([]T, 0, maxSize)
	doFlush := func() {
		if len(batch) == 0 {
			return
		}
		if err := flush(batch); err != nil {
			logf("batch: flush falló con %d elementos, conservo el batch: %v", len(batch), err)
			return
		}
		logf("batch: flush OK de %d elementos", len(batch))
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-in:
			if !ok {
				doFlush()
				return
			}
			batch = append(batch, item)
			if len(batch) >= maxSize {
				doFlush()
			}
		case <-ticker.C:
			doFlush()
		}
	}
}
```

- [ ] **Step 2: Crear `backend/batcher_test.go`**

```go
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBatcherFlushesBySize(t *testing.T) {
	in := make(chan int, 10)
	var mu sync.Mutex
	var batches [][]int
	done := make(chan struct{})

	go func() {
		RunBatcher(context.Background(), in, 2, time.Hour, func(b []int) error {
			mu.Lock()
			batches = append(batches, append([]int(nil), b...))
			mu.Unlock()
			if len(batches) == 2 {
				close(done)
			}
			return nil
		}, func(string, ...any) {})
	}()

	in <- 1
	in <- 2
	in <- 3
	in <- 4

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("no hubo dos flush por tamaño")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 2 {
		t.Fatalf("batches = %v, quería [[1 2] [3 4]]", batches)
	}
}

func TestBatcherFlushesByTime(t *testing.T) {
	in := make(chan int, 1)
	got := make(chan []int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunBatcher(ctx, in, 1000, 20*time.Millisecond, func(b []int) error {
		got <- append([]int(nil), b...)
		return nil
	}, func(string, ...any) {})

	in <- 7
	select {
	case b := <-got:
		if len(b) != 1 || b[0] != 7 {
			t.Fatalf("batch = %v, quería [7]", b)
		}
	case <-time.After(time.Second):
		t.Fatal("no hubo flush por tiempo")
	}
}

func TestBatcherKeepsBatchOnFlushError(t *testing.T) {
	in := make(chan int, 1)
	attempts := make(chan []int, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fail := true

	go RunBatcher(ctx, in, 1000, 15*time.Millisecond, func(b []int) error {
		cp := append([]int(nil), b...)
		attempts <- cp
		if fail {
			return errors.New("clickhouse caído")
		}
		return nil
	}, func(string, ...any) {})

	in <- 42

	first := <-attempts
	if len(first) != 1 || first[0] != 42 {
		t.Fatalf("primer intento = %v", first)
	}
	second := <-attempts
	if len(second) != 1 || second[0] != 42 {
		t.Fatalf("segundo intento = %v, el batch se perdió", second)
	}

	fail = false
	third := <-attempts
	if len(third) != 1 || third[0] != 42 {
		t.Fatalf("tercer intento = %v", third)
	}
}
```

- [ ] **Step 3: Correr los tests**

Run: `cd backend && go test ./... -run TestBatcher -v`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add backend/batcher.go backend/batcher_test.go
git commit -m "feat: add size/time batcher that keeps failed batches"
```

---

### Task 5: Cliente ClickHouse por HTTP e inserción con reintento

**Files:**
- Create: `backend/clickhouse.go`
- Test: `backend/clickhouse_test.go`

**Interfaces:**
- Consumes: `Event` (Task 2).
- Produces: `ClickHouse`, `NewClickHouse(url, user, password string) *ClickHouse`, `InsertBatch(ctx, []Event) error`.

- [ ] **Step 1: Crear `backend/clickhouse.go`**

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClickHouse se usa por su interfaz HTTP. El insert viaja como JSONEachRow:
// un solo POST con N líneas, o sea un solo INSERT para todo el batch.
// No hace falta un driver nativo para un ejercicio que sólo hace INSERT y
// SELECT simples, y así el dependency list queda corto.
type ClickHouse struct {
	url    string
	user   string
	password string
	client *http.Client
}

func NewClickHouse(rawURL, user, password string) *ClickHouse {
	return &ClickHouse{
		url:      strings.TrimRight(rawURL, "/"),
		user:     user,
		password: password,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

type clickRow struct {
	Date      string `json:"date"`
	Timestamp string `json:"timestamp"`
	EventID   string `json:"event_id"`
	SessionID string `json:"session_id"`
	Page      string `json:"page"`
	Type      string `json:"type"`
}

// InsertBatch hace UN solo INSERT para las N filas. Si falla, reintenta con
// backoff hasta que el contexto se cancela: el consumer mantiene el batch en
// memoria, así que un ClickHouse caído no significa eventos perdidos.
func (ch *ClickHouse) InsertBatch(ctx context.Context, events []Event) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, e := range events {
		utc := e.Timestamp.UTC()
		if err := enc.Encode(clickRow{
			Date:      utc.Format("2006-01-02"),
			Timestamp: utc.Format("2006-01-02 15:04:05.000"),
			EventID:   e.EventID,
			SessionID: e.SessionID,
			Page:      e.Page,
			Type:      e.Type,
		}); err != nil {
			return err
		}
	}
	// En la interfaz HTTP de ClickHouse la query y los datos van en el MISMO
	// body: primero el INSERT, después las filas JSONEachRow.
	query := "INSERT INTO default.events (date, timestamp, event_id, session_id, page, type) FORMAT JSONEachRow\n"
	payload := append([]byte(query), body.Bytes()...)

	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := ch.exec(ctx, payload, nil)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("clickhouse: insert falló (intento %d, %d eventos): %v", attempt, len(events), err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (ch *ClickHouse) exec(ctx context.Context, body []byte, out any) error {
	endpoint := ch.url + "/"
	values := url.Values{}
	// ClickHouse cita los UInt64 como strings en JSON; los counts del dashboard
	// se decodifican como números en Go.
	values.Set("output_format_json_quote_64bit_integers", "0")
	endpoint += "?" + values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if ch.user != "" {
		req.SetBasicAuth(ch.user, ch.password)
	}
	resp, err := ch.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("ClickHouse %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
```

- [ ] **Step 2: Crear `backend/clickhouse_test.go`**

```go
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickHouseInsertBatchRetries(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "FORMAT JSONEachRow") {
			t.Errorf("el body no tiene el INSERT: %q", body)
		}
		if strings.Count(string(body), "event_id") != 2 {
			t.Errorf("el batch no tiene las 2 filas: %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(context.Background(), []Event{validEvent(), validEvent()}); err != nil {
		t.Fatalf("InsertBatch() = %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("llamadas = %d, quería 2 (1 fallo + 1 ok)", got)
	}
}

func TestClickHouseInsertBatchSendsOnePostForWholeBatch(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(context.Background(), []Event{validEvent(), validEvent(), validEvent()}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("llamadas = %d, quería 1 INSERT para 3 eventos", got)
	}
}

func TestClickHouseInsertBatchHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(ctx, []Event{validEvent()}); err == nil {
		t.Fatal("quería error por contexto cancelado")
	}
}
```

- [ ] **Step 3: Correr los tests**

Run: `cd backend && go test ./... -run TestClickHouse -v`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add backend/clickhouse.go backend/clickhouse_test.go
git commit -m "feat: add clickhouse http client with batch insert and retry"
```

---

### Task 6: Consumer de Kafka con commit después del INSERT

**Files:**
- Create: `backend/consumer.go`
- Test: `backend/consumer_test.go`

**Interfaces:**
- Consumes: `Event`, `RunBatcher`, `ClickHouse.InsertBatch`.
- Produces: `RunConsumer(ctx, brokers []string, topic, group string, ch *ClickHouse, batchSize int, every time.Duration) error`, `offsetCommitter`, `insertAndCommit` (testeable).

- [ ] **Step 1: Crear `backend/consumer.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type queuedEvent struct {
	event   Event
	message kafka.Message
}

// offsetCommitter es la interfaz mínima que cumple *kafka.Reader. Meterla acá
// permite testear el orden insert → commit sin un broker real.
type offsetCommitter interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// insertAndCommit inserta el batch y recién después confirma los offsets. Si el
// INSERT falla, InsertBatch ya reintentó y devolvió error: no confirmamos nada
// y el batch sigue en memoria. Al reiniciar, Kafka nos vuelve a entregar esos
// mensajes. Es at-least-once: Redis ya descartó los duplicados en el borde.
type flushFunc func(ctx context.Context, ch *ClickHouse, committer offsetCommitter, batch []queuedEvent) error

var insertAndCommit flushFunc = func(ctx context.Context, ch *ClickHouse, committer offsetCommitter, batch []queuedEvent) error {
	events := make([]Event, 0, len(batch))
	messages := make([]kafka.Message, 0, len(batch))
	for _, q := range batch {
		events = append(events, q.event)
		messages = append(messages, q.message)
	}
	if err := ch.InsertBatch(ctx, events); err != nil {
		return err
	}
	return committer.CommitMessages(ctx, messages...)
}

// RunConsumer es el bucle de por vida del contenedor `consumer`.
func RunConsumer(ctx context.Context, brokers []string, topic, group string, ch *ClickHouse, batchSize int, every time.Duration) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     group,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
	})
	defer reader.Close()

	queue := make(chan queuedEvent, batchSize*2)
	go RunBatcher(ctx, queue, batchSize, every, func(batch []queuedEvent) error {
		return insertAndCommit(ctx, ch, reader, batch)
	}, log.Printf)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch de Kafka: %w", err)
		}
		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			log.Printf("consumer: mensaje inválido, se salta: %v", err)
			continue
		}
		queue <- queuedEvent{event: e, message: msg}
	}
}
```

- [ ] **Step 2: Crear `backend/consumer_test.go`**

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeCommitter struct {
	calls int
}

func (f *fakeCommitter) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	f.calls++
	return nil
}

func testBatch() []queuedEvent {
	return []queuedEvent{
		{event: validEvent(), message: kafka.Message{Offset: 1}},
		{event: validEvent(), message: kafka.Message{Offset: 2}},
	}
}

func TestConsumerFlushCommitsOnlyOnSuccess(t *testing.T) {
	var posts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&posts, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // primer intento falla
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	committer := &fakeCommitter{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := insertAndCommit(ctx, ch, committer, testBatch()); err != nil {
		t.Fatalf("insertAndCommit() = %v", err)
	}
	if got := atomic.LoadInt32(&posts); got < 2 {
		t.Fatalf("POSTs = %d, quería al menos 2 (fallo + reintento)", got)
	}
	if committer.calls != 1 {
		t.Fatalf("commits = %d, quería 1", committer.calls)
	}
}

func TestConsumerFlushDoesNotCommitWhenInsertFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	committer := &fakeCommitter{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := insertAndCommit(ctx, ch, committer, testBatch()); err == nil {
		t.Fatal("quería error de ClickHouse")
	}
	if committer.calls != 0 {
		t.Fatalf("commits = %d, quería 0: no se puede confirmar un INSERT fallido", committer.calls)
	}
}
```

- [ ] **Step 3: Correr los tests**

Run: `cd backend && go test ./... -run TestConsumer -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add backend/consumer.go backend/consumer_test.go
git commit -m "feat: add kafka consumer that commits after insert"
```

---

### Task 7: API de estadísticas (4 modos)

**Files:**
- Create: `backend/stats.go`
- Test: `backend/stats_test.go`
- Modify: `backend/clickhouse.go` (agregar los 4 métodos de consulta)

**Interfaces:**
- Consumes: `ClickHouse.exec`, `Site` (Task 2).
- Produces: `StatsHandler`, `DayStat`, `PageStat`, `FunnelStat`, `RealtimeStat`, `HandleStats(ch) http.Handler`.

- [ ] **Step 1: Agregar los métodos de consulta a `backend/clickhouse.go`**

```go
type DayStat struct {
	Day   string `json:"day"`
	Views uint64 `json:"views"`
}

type PageStat struct {
	Page  string `json:"page"`
	Views uint64 `json:"views"`
}

type FunnelStat struct {
	Pageviews    uint64 `json:"pageviews"`
	Clicks       uint64 `json:"clicks"`
	CustomEvents uint64 `json:"custom_events"`
}

type RealtimeStat struct {
	Events uint64 `json:"events"`
	Latest []Event `json:"latest"`
}

func (ch *ClickHouse) PageviewsByDay(ctx context.Context) ([]DayStat, error) {
	q := `SELECT toString(toDate(timestamp)) AS day, count() AS views
		FROM default.events GROUP BY day ORDER BY day FORMAT JSON`
	var out struct {
		Data []DayStat `json:"data"`
	}
	if err := ch.exec(ctx, []byte(q), &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (ch *ClickHouse) TopPages(ctx context.Context) ([]PageStat, error) {
	q := `SELECT page, count() AS views FROM default.events
		GROUP BY page ORDER BY views DESC LIMIT 10 FORMAT JSON`
	var out struct {
		Data []PageStat `json:"data"`
	}
	if err := ch.exec(ctx, []byte(q), &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// El funnel cuenta sesiones únicas por paso y exige orden temporal estricto
// dentro de la misma sesión. minIf con un timestamp 0 como centinela evita
// contar un click de una sesión que nunca hizo pageview.
func (ch *ClickHouse) Funnel(ctx context.Context) (FunnelStat, error) {
	q := `SELECT
			uniqExactIf(session_id, pv > toDateTime64('1970-01-01 00:00:00.000', 3)) AS pageviews,
			uniqExactIf(session_id, pv > toDateTime64('1970-01-01 00:00:00.000', 3)
				AND cl > pv) AS clicks,
			uniqExactIf(session_id, pv > toDateTime64('1970-01-01 00:00:00.000', 3)
				AND cl > pv AND ce > cl) AS custom_events
		FROM (
			SELECT session_id,
				minIf(timestamp, type = 'pageview') AS pv,
				minIf(timestamp, type = 'click') AS cl,
				minIf(timestamp, type = 'custom_event') AS ce
			FROM default.events GROUP BY session_id
		) FORMAT JSON`
	var out struct {
		Data []FunnelStat `json:"data"`
	}
	if err := ch.exec(ctx, []byte(q), &out); err != nil {
		return FunnelStat{}, err
	}
	if len(out.Data) == 0 {
		return FunnelStat{}, nil
	}
	return out.Data[0], nil
}

func (ch *ClickHouse) Realtime(ctx context.Context) (RealtimeStat, error) {
	q := `SELECT count() AS events FROM default.events
		WHERE timestamp >= now() - INTERVAL 5 MINUTE FORMAT JSON`
	var out struct {
		Data []struct {
			Events uint64 `json:"events"`
		} `json:"data"`
	}
	if err := ch.exec(ctx, []byte(q), &out); err != nil {
		return RealtimeStat{}, err
	}
	result := RealtimeStat{Latest: []Event{}}
	if len(out.Data) > 0 {
		result.Events = out.Data[0].Events
	}
	return result, nil
}
```

- [ ] **Step 2: Crear `backend/stats.go`**

```go
package main

import (
	"encoding/json"
	"net/http"
)

// HandleStats expone los cuatro modos del dashboard. `site=demo` se acepta
// porque está en el enunciado, pero no filtra: hay un solo sitio y está
// hardcodeado en Site. Cualquier otro valor se rechaza para no mentir.
func HandleStats(ch *ClickHouse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if site := r.URL.Query().Get("site"); site != "" && site != Site {
			http.Error(w, "sitio no soportado: "+site, http.StatusBadRequest)
			return
		}
		q := r.URL.Query()
		var payload any
		var err error
		switch {
		case q.Has("pageviews-by-day"):
			payload, err = ch.PageviewsByDay(r.Context())
		case q.Has("top-pages"):
			payload, err = ch.TopPages(r.Context())
		case q.Has("funnel"):
			payload, err = ch.Funnel(r.Context())
		case q.Has("realtime"):
			payload, err = ch.Realtime(r.Context())
		default:
			http.Error(w, "modo no soportado: usar pageviews-by-day, top-pages, funnel o realtime", http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, "ClickHouse: "+err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}
}
```

- [ ] **Step 3: Crear `backend/stats_test.go`**

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatsHandlerRejectsUnknownSite(t *testing.T) {
	h := HandleStats(NewClickHouse("http://localhost:8123", "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?pageviews-by-day&site=otro", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, quería 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "sitio no soportado") {
		t.Fatalf("body = %q", w.Body.String())
	}
}

func TestStatsHandlerRejectsUnknownMode(t *testing.T) {
	h := HandleStats(NewClickHouse("http://localhost:8123", "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?nada", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, quería 400", w.Code)
	}
}

func TestStatsHandlerAcceptsDemoSite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()

	h := HandleStats(NewClickHouse(server.URL, "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?pageviews-by-day&site=demo", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, quería 200: %q", w.Code, w.Body.String())
	}
}
```

- [ ] **Step 4: Correr los tests**

Run: `cd backend && go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/stats.go backend/stats_test.go backend/clickhouse.go
git commit -m "feat: add four dashboard stats endpoints"
```

---

### Task 8: Wiring de `main.go` (modos api y consumer)

**Files:**
- Create: `backend/main.go`

**Interfaces:**
- Consumes: todo lo anterior.
- Produces: binario `/app/server` con `-mode=api|consumer`.

- [ ] **Step 1: Crear `backend/main.go`**

```go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	mode := flag.String("mode", "api", "api o consumer")
	flag.Parse()

	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("KAFKA_TOPIC", "analytics.events")
	ch := NewClickHouse(env("CLICKHOUSE_URL", "http://clickhouse:8123"), "", "")

	if *mode == "consumer" {
		log.Println("modo consumer: esperando eventos de", topic)
		if err := RunConsumer(context.Background(), brokers, topic, "ga-consumer", ch, 1000, 5*time.Second); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Modo api: collector + dashboard en el mismo proceso. Redis y Kafka se
	// conectan acá; el consumer es el único que habla con ClickHouse para
	// escribir, api sólo lee.
	redisClient := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "redis:6379")})
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
	defer writer.Close()
	defer redisClient.Close()

	collector := NewCollector(NewRedisDeduper(redisClient), writer, topic, 24*time.Hour)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/collect", HandleCollect(collector))
	mux.HandleFunc("/api/stats", HandleStats(ch))

	addr := ":" + env("PORT", "8080")
	log.Println("modo api: escuchando en", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
```

- [ ] **Step 2: Verificar build y tests**

Run: `cd backend && gofmt -w *.go && go vet ./... && go test ./... && go build ./...`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add backend/main.go
git commit -m "feat: wire api and consumer modes"
```

---

### Task 9: Frontend base, sitio demo y cliente de eventos

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/tsconfig.json`
- Create: `frontend/vite.config.ts`
- Create: `frontend/tailwind.config.js`
- Create: `frontend/postcss.config.js`
- Create: `frontend/index.html`
- Create: `frontend/src/main.tsx`
- Create: `frontend/src/index.css`
- Create: `frontend/src/analytics.ts`
- Create: `frontend/src/DemoSite.tsx`

**Interfaces:**
- Consumes: `POST /collect`.
- Produces: `sendEvent`, `sendBurst`, `PAGES`, `Page`, `EventType`, `DemoSite` component.

- [ ] **Step 1: Crear la configuración del frontend**

`frontend/package.json`:

```json
{
  "name": "como-funciona-0006-google-analytics-frontend",
  "private": true,
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc --noEmit && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "recharts": "^2.12.7"
  },
  "devDependencies": {
    "@types/react": "^18.3.3",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.1",
    "autoprefixer": "^10.4.19",
    "postcss": "^8.4.39",
    "tailwindcss": "^3.4.17",
    "typescript": "^5.5.4",
    "vite": "^5.4.0"
  }
}
```

`frontend/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true
  },
  "include": ["src"]
}
```

`frontend/vite.config.ts`:

```ts
// El proxy sólo existe para `npm run dev` en la laptop; en Docker, nginx es
// quien reenvía /collect y /api al backend. Mismo contrato en los dos lados.
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/collect': 'http://localhost:8084',
      '/api': 'http://localhost:8084',
    },
  },
})
```

`frontend/tailwind.config.js`:

```js
/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: { extend: {} },
  plugins: [],
}
```

`frontend/postcss.config.js`:

```js
export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
```

`frontend/index.html`:

```html
<!doctype html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>¿Cómo funciona Google Analytics?</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 2: Crear `frontend/src/index.css`**

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

body {
  @apply bg-slate-950 text-slate-100 antialiased;
}
```

- [ ] **Step 3: Crear `frontend/src/analytics.ts`**

```ts
// El evento se arma en el navegador, igual que lo haría el script real de GA.
// crypto.randomUUID() da el event_id; la session_id vive en sessionStorage para
// que las tres páginas compartan sesión sin backend de sesiones.
export const SITE = 'demo'
export const PAGES = ['/', '/productos', '/contacto'] as const

export type Page = (typeof PAGES)[number]
export type EventType = 'pageview' | 'click' | 'custom_event'
export type SendResult = 'accepted' | 'duplicate' | 'error'

const sessionKey = 'ga-session-id'
let sessionId = sessionStorage.getItem(sessionKey)
if (!sessionId) {
  sessionId = crypto.randomUUID()
  sessionStorage.setItem(sessionKey, sessionId)
}

export async function sendEvent(page: Page, type: EventType): Promise<SendResult> {
  const event = {
    event_id: crypto.randomUUID(),
    session_id: sessionId,
    page,
    type,
    timestamp: new Date().toISOString(),
  }
  try {
    const res = await fetch('/collect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event),
    })
    if (!res.ok) return 'error'
    const body = (await res.json()) as { status: SendResult }
    return body.status
  } catch {
    return 'error'
  }
}

export type BurstProgress = { done: number; accepted: number; duplicate: number; error: number }

// 10.000 requests de golpe pueden ahogar al navegador y al collector. Con
// ~100 en vuelo el pico es constante y se ve el efecto real: dedup en Redis,
// batches de 1000 en el consumer y, si se cae ClickHouse, reintentos en logs.
export async function sendBurst(
  page: Page,
  onProgress: (p: BurstProgress) => void,
  total = 10_000,
  concurrency = 100,
): Promise<BurstProgress> {
  const progress: BurstProgress = { done: 0, accepted: 0, duplicate: 0, error: 0 }
  let next = 0
  async function worker() {
    while (next < total) {
      next++
      const result = await sendEvent(page, 'custom_event')
      progress.done++
      if (result === 'accepted') progress.accepted++
      else if (result === 'duplicate') progress.duplicate++
      else progress.error++
      if (progress.done % 100 === 0) onProgress({ ...progress })
    }
  }
  await Promise.all(Array.from({ length: concurrency }, worker))
  onProgress({ ...progress })
  return progress
}
```

- [ ] **Step 4: Crear `frontend/src/DemoSite.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { PAGES, sendEvent, type Page } from './analytics'

// Sin react-router: tres vistas con estado + History API. La librería de
// routing no aporta nada para un demo de tres páginas y sumaría ruido.
export function DemoSite({ onViewChange }: { onViewChange: (page: Page) => void }) {
  const [page, setPage] = useState<Page>(() => (PAGES.includes(location.pathname as Page) ? (location.pathname as Page) : '/'))

  useEffect(() => {
    history.pushState({}, '', page)
    onViewChange(page)
    void sendEvent(page, 'pageview')
  }, [page, onViewChange])

  const go = (next: Page) => setPage(next)

  return (
    <section className="space-y-6">
      <nav className="flex gap-2">
        {PAGES.map((p) => (
          <button
            key={p}
            onClick={() => go(p)}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
              page === p ? 'bg-sky-500 text-white' : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
            }`}
          >
            {p === '/' ? 'Home' : p.slice(1)}
          </button>
        ))}
      </nav>

      <div className="rounded-2xl border border-slate-800 bg-slate-900 p-8">
        <h2 className="text-2xl font-semibold capitalize">{page === '/' ? 'Home' : page.slice(1)}</h2>
        <p className="mt-2 text-slate-400">
          Cada vez que entrás a esta página se manda un <code className="text-sky-300">pageview</code>. Los botones de abajo mandan otros tipos.
        </p>
        <div className="mt-6 flex flex-wrap gap-3">
          <button
            onClick={() => void sendEvent(page, 'click')}
            className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold hover:bg-emerald-500"
          >
            Botón de click
          </button>
          <button
            onClick={() => void sendEvent(page, 'custom_event')}
            className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold hover:bg-violet-500"
          >
            Custom event
          </button>
        </div>
      </div>
    </section>
  )
}
```

- [ ] **Step 5: Crear `frontend/src/main.tsx`**

```tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import { App } from './App'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
```

`App.tsx` se crea en el Task 10; para poder compilar antes, crealo con este contenido mínimo y se reemplaza en el próximo task:

```tsx
export function App() {
  return <main className="p-8">Cargando…</main>
}
```

- [ ] **Step 6: Instalar dependencias y verificar el build**

Run: `cd frontend && npm install && npm run build`
Expected: PASS; genera `frontend/package-lock.json` y `frontend/dist/`.

- [ ] **Step 7: Commit**

```bash
git add frontend/package.json frontend/package-lock.json frontend/tsconfig.json frontend/vite.config.ts frontend/tailwind.config.js frontend/postcss.config.js frontend/index.html frontend/src
git commit -m "feat: add demo site and event client"
```

---

### Task 10: Dashboard con Recharts, tiempo real y "Cómo romperlo"

**Files:**
- Create: `frontend/src/Dashboard.tsx`
- Create: `frontend/src/BreakIt.tsx`
- Modify: `frontend/src/App.tsx`

**Interfaces:**
- Consumes: `SITE` de `analytics.ts`, `/api/stats`.
- Produces: `Dashboard`, `BreakIt`, `App` con tres vistas.

- [ ] **Step 1: Crear `frontend/src/Dashboard.tsx`**

```tsx
import { useCallback, useEffect, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'
import { SITE } from './analytics'

type Day = { day: string; views: number }
type TopPage = { page: string; views: number }
type Funnel = { pageviews: number; clicks: number; custom_events: number }
type Realtime = { events: number; latest: unknown[] }

async function get<T>(mode: string): Promise<T> {
  const res = await fetch(`/api/stats?${mode}&site=${SITE}`)
  if (!res.ok) throw new Error(`${mode}: ${res.status}`)
  return (await res.json()) as T
}

export function Dashboard({ refreshKey }: { refreshKey: number }) {
  const [days, setDays] = useState<Day[]>([])
  const [top, setTop] = useState<TopPage[]>([])
  const [funnel, setFunnel] = useState<Funnel>({ pageviews: 0, clicks: 0, custom_events: 0 })
  const [realtime, setRealtime] = useState<Realtime>({ events: 0, latest: [] })
  const [error, setError] = useState('')

  // Polling de 5 s, igual que el panel en vivo de GA: no hace falta WebSocket
  // para un ejercicio donde la gránularidad es de segundos.
  const load = useCallback(async () => {
    try {
      const [d, t, f, r] = await Promise.all([
        get<Day[]>('pageviews-by-day'),
        get<TopPage[]>('top-pages'),
        get<Funnel>('funnel'),
        get<Realtime>('realtime'),
      ])
      setDays(d)
      setTop(t)
      setFunnel(f)
      setRealtime(r)
      setError('')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => {
    void load()
    const id = setInterval(() => void load(), 5000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const funnelData = [
    { step: 'pageview', value: funnel.pageviews },
    { step: 'click', value: funnel.clicks },
    { step: 'custom_event', value: funnel.custom_events },
  ]

  return (
    <section className="space-y-6">
      {error && <p className="rounded-lg bg-red-950 p-3 text-sm text-red-300">Error: {error}</p>}

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Pageviews por día</h2>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={days}>
              <CartesianGrid stroke="#1e293b" />
              <XAxis dataKey="day" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} allowDecimals={false} />
              <Tooltip />
              <Line type="monotone" dataKey="views" stroke="#38bdf8" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Embudo (sesiones únicas)</h2>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={funnelData}>
              <CartesianGrid stroke="#1e293b" />
              <XAxis dataKey="step" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} allowDecimals={false} />
              <Tooltip />
              <Bar dataKey="value" fill="#a78bfa" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Top páginas</h2>
          <table className="w-full text-sm">
            <thead className="text-left text-slate-400">
              <tr><th className="pb-2">Página</th><th className="pb-2 text-right">Views</th></tr>
            </thead>
            <tbody>
              {top.map((p) => (
                <tr key={p.page} className="border-t border-slate-800">
                  <td className="py-2 font-mono text-sky-300">{p.page}</td>
                  <td className="py-2 text-right tabular-nums">{p.views}</td>
                </tr>
              ))}
              {top.length === 0 && <tr><td colSpan={2} className="py-4 text-slate-500">Sin datos todavía</td></tr>}
            </tbody>
          </table>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Tiempo real</h2>
            <span className="flex items-center gap-2 text-xs text-emerald-400">
              <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-400" />Actualiza cada 5 s
            </span>
          </div>
          <p className="mt-6 text-5xl font-semibold tabular-nums text-emerald-300">{realtime.events}</p>
          <p className="mt-1 text-sm text-slate-400">eventos en los últimos 5 minutos</p>
          <p className="mt-6 text-xs text-slate-500">
            El contador viene de ClickHouse, no de Redis: lo que ves es lo que ya se persistió.
          </p>
        </div>
      </div>
    </section>
  )
}
```

- [ ] **Step 2: Crear `frontend/src/BreakIt.tsx`**

```tsx
import { useState } from 'react'
import { sendBurst, type BurstProgress } from './analytics'

const empty: BurstProgress = { done: 0, accepted: 0, duplicate: 0, error: 0 }

export function BreakIt({ onBurst }: { onBurst: () => void }) {
  const [progress, setProgress] = useState<BurstProgress>(empty)
  const [running, setRunning] = useState(false)

  async function run() {
    setRunning(true)
    setProgress(empty)
    await sendBurst('/productos', setProgress)
    setRunning(false)
    onBurst()
  }

  return (
    <section className="grid gap-6 lg:grid-cols-2">
      <div className="rounded-2xl border border-amber-800/60 bg-amber-950/30 p-6">
        <h2 className="text-lg font-semibold text-amber-200">Mandar 10.000 eventos de golpe</h2>
        <p className="mt-3 text-sm text-amber-100/80">
          Dispara 10.000 <code className="text-amber-300">POST /collect</code> con ~100 requests en vuelo.
          Es el mismo flujo de siempre, pero en escala: el cuello no está en el collector, está en lo que pasa después.
        </p>
        <button
          disabled={running}
          onClick={() => void run()}
          className="mt-5 rounded-lg bg-amber-500 px-5 py-3 text-sm font-bold text-slate-950 hover:bg-amber-400 disabled:opacity-50"
        >
          {running ? `Enviando… ${progress.done}/10000` : 'Mandar 10.000 eventos de golpe'}
        </button>
        <dl className="mt-6 grid grid-cols-3 gap-3 text-center text-sm">
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Aceptados</dt><dd className="text-lg text-emerald-300">{progress.accepted}</dd></div>
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Duplicados</dt><dd className="text-lg text-sky-300">{progress.duplicate}</dd></div>
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Errores</dt><dd className="text-lg text-red-300">{progress.error}</dd></div>
        </dl>
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 text-sm text-slate-300">
        <h2 className="mb-3 text-lg font-semibold text-slate-100">Qué mirar en los logs</h2>
        <pre className="mb-4 overflow-x-auto rounded-lg bg-slate-950 p-3 text-xs text-emerald-300">
          docker compose logs -f consumer
        </pre>
        <ul className="list-disc space-y-2 pl-5">
          <li><strong className="text-sky-300">batch: flush OK de 1000 elementos</strong> — aparece unas 10 veces. Un solo INSERT por batch, no 10.000.</li>
          <li><strong className="text-sky-300">duplicate</strong> — si apretás dos veces, Redis reconoce los <code>event_id</code> y el collector no publica.</li>
          <li><strong className="text-amber-300">ClickHouse caído</strong> — pará <code>docker compose stop clickhouse</code>, mandá eventos y mirá cómo el consumer conserva el batch y reintenta con backoff. Al levantarlo, los datos aparecen.</li>
          <li>Después del pico, el dashboard se actualiza solo: es polling de 5 s sobre ClickHouse.</li>
        </ul>
      </div>
    </section>
  )
}
```

- [ ] **Step 3: Reemplazar `frontend/src/App.tsx`**

```tsx
import { useState } from 'react'
import { BreakIt } from './BreakIt'
import { Dashboard } from './Dashboard'
import { DemoSite } from './DemoSite'
import type { Page } from './analytics'

type Tab = 'demo' | 'dashboard' | 'romper'

export function App() {
  const [tab, setTab] = useState<Tab>('demo')
  const [refreshKey, setRefreshKey] = useState(0)
  const [currentPage, setCurrentPage] = useState<Page>('/')

  const tabs: { id: Tab; label: string }[] = [
    { id: 'demo', label: 'Sitio demo' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'romper', label: 'Cómo romperlo' },
  ]

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <header className="mb-8">
        <p className="text-xs uppercase tracking-widest text-sky-400">¿Cómo funciona...?</p>
        <h1 className="mt-1 text-3xl font-semibold">Google Analytics</h1>
        <p className="mt-2 text-sm text-slate-400">
          Sitio: <code className="text-sky-300">demo</code> · Vista actual: <code className="text-sky-300">{currentPage}</code> · Sesión: la misma para las tres páginas · Eventos: pageview, click, custom_event
        </p>
      </header>

      <nav className="mb-8 flex gap-2 border-b border-slate-800 pb-4">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
              tab === t.id ? 'bg-sky-500 text-white' : 'bg-slate-900 text-slate-300 hover:bg-slate-800'
            }`}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {tab === 'demo' && <DemoSite onViewChange={setCurrentPage} />}
      {tab === 'dashboard' && <Dashboard refreshKey={refreshKey} />}
      {tab === 'romper' && <BreakIt onBurst={() => setRefreshKey((k) => k + 1)} />}
    </main>
  )
}
```

- [ ] **Step 4: Verificar el build**

Run: `cd frontend && npm run build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/Dashboard.tsx frontend/src/BreakIt.tsx frontend/src/App.tsx
git commit -m "feat: add recharts dashboard and break-it panel"
```

---

### Task 11: README y 5 ADRs

**Files:**
- Create: `README.md`
- Create: `docs/adr/0001-collector-no-escribe-directo.md`
- Create: `docs/adr/0002-clickhouse-para-eventos.md`
- Create: `docs/adr/0003-batch-insert.md`
- Create: `docs/adr/0004-que-queda-fuera.md`
- Create: `docs/adr/0005-sin-postgres.md`

**Interfaces:**
- Consumes: spec y todo el código.
- Produces: documentación final del curso.

- [ ] **Step 1: Crear `README.md`**

```md
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
```

- [ ] **Step 2: Crear los ADRs**

`docs/adr/0001-collector-no-escribe-directo.md`:

```md
# ADR-001: El collector no escribe directo en la base
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

`POST /collect` está en el camino caliente: cada visita y cada click del demo lo llama, y el botón de 10.000 eventos lo prueba. Si el handler hiciera un INSERT directo, la latencia del request dependería de ClickHouse y un pico de tráfico se convertiría en un pico de escrituras.

## Decisión

El collector valida, deduplica en Redis y publica a `analytics.events` con key = `session_id`. Nunca habla con ClickHouse. El consumidor es el único que inserta.

## Consecuencias (A favor / En contra)

A favor:
- El request de ingestión es corto y predecible: dos operaciones de red y listo.
- Un pico de 10.000 eventos no se traduce en 10.000 INSERTs.
- El consumidor puede agrupar, reintentar y confirmar offsets sin involucrar al navegador.

En contra:
- Aparece un servicio más (Kafka) y una ventana entre publicar y persistir.
- El dashboard puede ir unos segundos detrás de lo que ya se envió; por eso el panel es de tiempo real con polling, no instantáneo.

## Alternativas consideradas

- **INSERT directo desde el collector:** más simple, pero acopla latencia al request y pierde el batch que se estudia.
- **Escribir en Redis y que un worker lea Redis:** cambia Kafka por una cola con semántica distinta y sin orden por sesión.

## Referencias

- Implementación: `backend/collector.go`.
- Decisión complementaria: [ADR-003 — Batch insert](0003-batch-insert.md).
```

`docs/adr/0002-clickhouse-para-eventos.md`:

```md
# ADR-002: ClickHouse para los eventos
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

Los eventos son append-only y las preguntas del dashboard son siempre agregaciones: por día, por página, por sesión, por ventana de tiempo. La tabla crece rápido cuando se aprieta el botón de 10.000.

## Decisión

`default.events` usa `MergeTree()` con `PARTITION BY toYYYYMM(date)` y `ORDER BY (date, page, timestamp)`. Todas las consultas del API son `GROUP BY` sobre esa tabla. No hay tabla de eventos en otra base.

## Consecuencias (A favor / En contra)

A favor:
- `ORDER BY (date, page, timestamp)` hace que pageviews por día y top páginas lean datos contiguos.
- `uniqExact` resuelve el funnel de sesiones únicas sin tabla auxiliar.
- El storage columnar comprime bien columnas repetidas como `type` y `page`.

En contra:
- No es una base transaccional: no hay `UPDATE` ni índices únicos para dedup. Por eso el `event_id` se deduplica en Redis.
- Requiere un servicio más y un esquema declarado en `clickhouse/init.sql`.

## Alternativas consideradas

- **Postgres para eventos:** bueno para transacciones, malo para agregaciones de millones de filas. Además no hay nada transaccional que proteger acá. Ver [ADR-005](0005-sin-postgres.md).
- **Sólo Redis:** rápido, pero volatil; no sirve para el histórico del dashboard.

## Referencias

- Esquema: `clickhouse/init.sql`.
- Consultas: `backend/clickhouse.go`.
```

`docs/adr/0003-batch-insert.md`:

```md
# ADR-003: Batch insert de 1000 eventos o 5 segundos
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

ClickHouse escribe un *part* por INSERT. Con 10.000 eventos, insertar de a uno genera 10.000 parts; el merge se atrasa y el dashboard empieza a leer mal. El botón de "romperlo" existe para hacer visible ese problema.

## Decisión

El consumer junta hasta 1000 eventos o 5 segundos, lo que llegue primero, y hace un solo INSERT con `FORMAT JSONEachRow`. Si el INSERT falla, conserva el batch en memoria, loguea y reintenta con backoff. Los offsets de Kafka se confirman recién después del INSERT.

## Consecuencias (A favor / En contra)

A favor:
- 10.000 eventos ⇒ ~10 INSERTs en vez de 10.000.
- El reintento no pierde eventos: el batch sigue en memoria y Kafka no confirmó.
- El log del consumer muestra cada flush, así que el mecanismo es observable.

En contra:
- Hasta 5 s de retardo antes de que un evento aparezca en el dashboard; es el precio del batch.
- Un batch en memoria se pierde si el contenedor muere antes del INSERT; Kafka lo vuelve a entregar porque el offset no se confirmó.

## Alternativas consideradas

- **Insert por evento:** simple, pero mata el merge de ClickHouse bajo el pico.
- **Batch por tiempo solamente:** si el tráfico para, el último batch espera los 5 s; con el criterio de tamaño, el pico se drena apenas llega a 1000.

## Referencias

- Implementación: `backend/batcher.go` y `backend/consumer.go`.
- Implementación del INSERT: `backend/clickhouse.go` (`InsertBatch`).
```

`docs/adr/0004-que-queda-fuera.md`:

```md
# ADR-004: Qué queda fuera
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

El curso tiene ejercicios específicos para patrones avanzados (Outbox en 0103, DLT en 0099, Exactly-Once en 0102). Meterlos acá mezclaría conceptos y haría el flujo ilegible.

## Decisión

Quedan **fuera de alcance a propósito**:

- Transactional Outbox: publicar dentro de la transacción no aplica porque no hay transacción de negocio; la dedup es de Redis y la entrega es at-least-once.
- DLT: un mensaje inválido se loguea y se salta; no hay topic de muerte.
- Exactly-Once Semantics: Redis da dedup de 24 h, Kafka da at-least-once; no prometemos más.
- Schema Registry: el evento es un JSON de cinco campos y el contrato vive en `backend/event.go`.
- Rebalancing fino de consumidores: hay una partición y un consumidor; la configuración por default alcanza.
- Monitoreo de producción: no hay métricas, trazas ni alertas; los `log.Printf` del consumer son suficientes para la demo.
- Integración real con Google Analytics, auth, multi-tenant, GDPR y multi-región.

## Consecuencias (A favor / En contra)

A favor:
- El alumno ve un solo mecanismo a la vez: dedup → cola → batch → agregación.
- No hay dependencias que expliquen mejor en otro ejercicio.

En contra:
- Si el proceso muere entre el `SETNX` y el publish puede perderse un evento (el collector borra la clave si Kafka falla, pero no hay reintento persistente). Está documentado y es intencional.

## Alternativas consideradas

- Implementar Outbox + DLT: pertenece a los ejercicios 0103 y 0099, no a este.
- Prometer Exactly-Once: contradice la decisión de dedup efímera en Redis.

## Referencias

- Alcance: [README.md § Alcance y decisiones de diseño](../../README.md).
```

`docs/adr/0005-sin-postgres.md`:

```md
# ADR-005: Sin Postgres en este ejercicio
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

El stack del curso incluye Postgres en varios ejercicios. En 0003-shopify es la fuente de verdad porque hay stock, órdenes e idempotency keys: estado transaccional que hay que proteger. En este ejercicio los eventos son hechos append-only: no hay saldo que descontar ni orden que confirmar.

Los usos candidatos para Postgres acá serían una tabla `sites` con una fila, una tabla `sessions` redundante con el `session_id` de cada evento, o un landing para Outbox. El primero es decoración; el segundo crea una segunda fuente de verdad; el tercero está explícitamente fuera de alcance (ver ADR-004).

## Decisión

No hay servicio Postgres en `docker-compose.yml`, no hay DSN, no hay tabla `sites` ni `users`. El sitio demo es uno solo y está hardcodeado como `demo` en el código. Redis es el único estado efímero (dedup con TTL) y ClickHouse el único estado durable (eventos).

## Consecuencias (A favor / En contra)

A favor:
- Cada servicio del Compose tiene un rol real: nada está ahí por costumbre.
- No aparece una fuente de verdad relacional que compita con ClickHouse.
- La explicación del flujo cabe en un diagrama: menos servicios, menos ruido.

En contra:
- Si mañana el ejercicio pidiera múltiples sitios o usuarios, habría que introducir una base y migrar el `site` hardcodeado. Ese es el ejercicio correcto para hacerlo, no este.
- `?site=demo` se acepta pero no filtra; se documenta para que no parezca un olvido.

## Alternativas consideradas

- **Tabla `sites` con una fila:** da un uso formal al parámetro `site`, pero con un solo sitio el filtro siempre es verdadero.
- **Tabla `sessions` en Postgres:** permitiría reconstruir sesiones, pero `uniqExact(session_id)` en ClickHouse ya lo hace sin sincronizar dos bases.
- **Postgres como landing antes de Kafka:** es el patrón Outbox, fuera de alcance y tema del ejercicio 0103.

## Referencias

- Stack: [README.md § Stack](../../README.md).
- Decisión complementaria: [ADR-002 — ClickHouse para eventos](0002-clickhouse-para-eventos.md).
```

- [ ] **Step 3: Revisar frases obligatorias**

Run: `grep -rn "Alcance y decisiones de diseño\|fuera de alcance a propósito" README.md docs/adr/`
Expected: ambas frases aparecen en README.md y en ADR-004/0005.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/adr/
git commit -m "docs: add README and five ADRs"
```

---

### Task 12: Verificación end-to-end

**Files:**
- Ninguno. Verificación y correcciones mínimas si aparece un error.

**Interfaces:**
- Consumes: todo.
- Produces: stack funcionando en `localhost:3000`.

- [ ] **Step 1: Verificación local sin Docker**

Run: `cd backend && gofmt -l . && go vet ./... && go test ./... && cd ../frontend && npm run build && cd .. && docker compose config >/dev/null`
Expected: `gofmt -l` sin salida; tests PASS; build PASS; compose config sin errores.

- [ ] **Step 2: Levantar el stack**

Run: `docker compose up --build -d && sleep 25 && docker compose ps`
Expected: los seis servicios arriba; `kafka-init` con estado `exited (0)`; `backend`, `consumer`, `frontend`, `redis`, `kafka` y `clickhouse` corriendo.

- [ ] **Step 3: Probar el flujo completo**

Run:

```bash
curl -s http://localhost:8084/health
curl -s -X POST http://localhost:8084/collect -H 'Content-Type: application/json' \
  -d '{"event_id":"22222222-2222-4222-8222-222222222222","session_id":"s-verify","page":"/productos","type":"pageview","timestamp":"2027-02-09T13:00:00Z"}'
sleep 7
curl -s 'http://localhost:8084/api/stats?pageviews-by-day&site=demo'
curl -s 'http://localhost:8084/api/stats?top-pages'
curl -s 'http://localhost:8084/api/stats?funnel'
curl -s 'http://localhost:8084/api/stats?realtime'
```

Expected: `ok`; `{"status":"accepted"}`; los cuatro modos devuelven JSON con el evento cargado (realtime puede dar 0 si la fecha del evento está fuera de la ventana de 5 min — usar `new Date()` para la prueba manual de realtime).

- [ ] **Step 4: Verificar la UI**

Abrir http://localhost:3000. Esperado: pestañas "Sitio demo", "Dashboard", "Cómo romperlo"; las tres páginas mandan eventos; el dashboard grafica y el panel en vivo se actualiza solo.

- [ ] **Step 5: Verificar el pico y el reintento**

Apretar "Mandar 10.000 eventos de golpe" y correr `docker compose logs -f consumer`: esperado, ≈10 líneas `batch: flush OK de 1000 elementos`. Después `docker compose stop clickhouse`, mandar otro burst, confirmar logs de error con el batch conservado, y `docker compose start clickhouse`: los eventos aparecen al levantar.

- [ ] **Step 6: Bajar y commit final si hubo correcciones**

Run: `docker compose down && git status --short`
Expected: árbol limpio o sólo los archivos corregidos; si los hay, commitear con `fix: ...`.
