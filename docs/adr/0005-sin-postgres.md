# ADR-005: Sin Postgres en este ejercicio
- Ejercicio: 0006 de 1024 · Fecha: 2027-02-09 · Estado: Aceptado

## Contexto

El stack del curso incluye Postgres en varios ejercicios. En 0003-shopify es la fuente de verdad porque hay stock, órdenes e idempotency keys: estado transaccional que hay que proteger. En este ejercicio los eventos son hechos append-only: no hay saldo que descontar ni orden que confirmar.

Los usos candidatos para Postgres acá serían una tabla `sites` con una fila, una tabla `sessions` redundante con el `session_id` de cada evento, o un landing para Outbox. El primero es decoración; el segundo crea una segunda fuente de verdad; el tercero está **fuera de alcance a propósito** (ver ADR-004).

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
