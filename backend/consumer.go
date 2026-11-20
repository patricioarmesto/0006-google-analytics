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
