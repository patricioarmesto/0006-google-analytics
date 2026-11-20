package main

import (
	"context"
	"time"
)

// RunBatcher junta hasta maxSize elementos o los espera `every`, lo que pase
// primero. Es genérico porque el consumer necesita meter el mensaje de Kafka
// junto al evento: así puede hacer commit de los offsets recién cuando el
// INSERT salió bien.
//
// ¿Por qué batch y no un INSERT por evento? ClickHouse escribe un "part" por
// insert. A 10.000 eventos, insertar de a uno genera 10.000 parts y el merge
// se ahoga; con batches de 1000 son 10 inserts y 10 parts. Esa es la diferencia
// entre andar y no andar, y es lo que el botón de "romperlo" deja ver.
//
// Si flush falla, el batch NO se descarta: se loguea y se reintenta en el
// próximo tick. Perder un batch en memoria sería perder eventos para siempre.
func RunBatcher[T any](
	ctx context.Context,
	in <-chan T,
	maxSize int,
	every time.Duration,
	flush func([]T) error,
	logf func(string, ...any),
) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	batch := make([]T, 0, maxSize)
	doFlush := func() {
		if len(batch) == 0 {
			return
		}
		if err := flush(batch); err != nil {
			logf("batch: flush falló con %d elementos, conservo el batch: %v", len(batch), err)
			return
		}
		logf("batch: flush OK de %d elementos", len(batch))
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-in:
			if !ok {
				doFlush()
				return
			}
			batch = append(batch, item)
			if len(batch) >= maxSize {
				doFlush()
			}
		case <-ticker.C:
			doFlush()
		}
	}
}
