package main

import "testing"

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
