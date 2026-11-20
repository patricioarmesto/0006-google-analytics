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
