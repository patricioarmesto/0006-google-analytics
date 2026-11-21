package main

import (
	"testing"
	"time"
)

// kafka-go exige que el tópico esté en un solo lugar: Writer.Topic o
// Message.Topic. Collector lo pone en cada mensaje, así que el Writer de
// producción debe dejarlo vacío. Este test evita que el wiring vuelva a romperse.
func TestKafkaWriterLeavesTopicToCollectorMessage(t *testing.T) {
	t.Setenv("KAFKA_TOPIC", "analytics.events")

	w := newKafkaWriter([]string{"kafka:9092"})
	if w.Topic != "" {
		t.Fatalf("writer.Topic = %q, quería vacío: Collector ya setea Topic en cada kafka.Message", w.Topic)
	}
}

// Con el default de kafka-go (1 s), un POST con poca concurrencia espera a que
// se llene el batch de 100. En el navegador hay ~6 conexiones, así que cada
// evento tardaba 1 s y el polling del dashboard quedaba encolado.
func TestKafkaWriterFlushesQuicklyWhenBatchIsNotFull(t *testing.T) {
	w := newKafkaWriter([]string{"kafka:9092"})
	// 0 significa "usar el default de kafka-go", que es 1 segundo.
	if w.BatchTimeout <= 0 || w.BatchTimeout > 50*time.Millisecond {
		t.Fatalf("BatchTimeout = %v, quería un valor explícito <= 50ms", w.BatchTimeout)
	}
}
