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
