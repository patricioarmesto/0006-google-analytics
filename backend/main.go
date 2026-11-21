package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newKafkaWriter construye el producer usado por el collector. El tópico se
// define en cada mensaje (ver Collector.Collect), no acá.
func newKafkaWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...),
		// El default de kafka-go es 1 s: con poca concurrencia (el navegador
		// abre ~6 conexiones) cada request esperaría ese segundo a completar
		// el batch de 100. Con 10 ms el collector sigue siendo rápido y el
		// polling del dashboard no queda encolado detrás del burst.
		BatchTimeout: 10 * time.Millisecond,
		Balancer:     &kafka.Hash{},
	}
}

// newClickHouseFromEnv arma el cliente HTTP con las credenciales del usuario
// de aplicación creado en clickhouse/users.xml.
func newClickHouseFromEnv() *ClickHouse {
	return NewClickHouse(
		env("CLICKHOUSE_URL", "http://clickhouse:8123"),
		env("CLICKHOUSE_USER", ""),
		env("CLICKHOUSE_PASSWORD", ""),
	)
}

func main() {
	mode := flag.String("mode", "api", "api o consumer")
	flag.Parse()

	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("KAFKA_TOPIC", "analytics.events")
	ch := newClickHouseFromEnv()

	if *mode == "consumer" {
		log.Println("modo consumer: esperando eventos de", topic)
		if err := RunConsumer(context.Background(), brokers, topic, "ga-consumer", ch, 1000, 5*time.Second); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Modo api: collector + dashboard en el mismo proceso. Redis y Kafka se
	// conectan acá; el consumer es el único que habla con ClickHouse para
	// escribir, api sólo lee.
	redisClient := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "redis:6379")})
	writer := newKafkaWriter(brokers)
	defer writer.Close()
	defer redisClient.Close()

	collector := NewCollector(NewRedisDeduper(redisClient), writer, topic, 24*time.Hour)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/collect", HandleCollect(collector))
	mux.HandleFunc("/api/stats", HandleStats(ch))

	addr := ":" + env("PORT", "8080")
	log.Println("modo api: escuchando en", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
