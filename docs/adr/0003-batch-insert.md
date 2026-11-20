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
