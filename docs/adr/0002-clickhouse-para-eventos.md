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
- **Sólo Redis:** rápido, pero volátil; no sirve para el histórico del dashboard.

## Referencias

- Esquema: `clickhouse/init.sql`.
- Consultas: `backend/clickhouse.go`.
