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
