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
