package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeDeduper struct {
	keys   map[string]bool
	setErr error
}

func (d *fakeDeduper) SetNX(_ context.Context, key string, _ any, _ time.Duration) (bool, error) {
	if d.setErr != nil {
		return false, d.setErr
	}
	if d.keys[key] {
		return false, nil
	}
	d.keys[key] = true
	return true, nil
}

func (d *fakeDeduper) Delete(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(d.keys, k)
	}
	return nil
}

type fakeProducer struct {
	msgs []kafka.Message
	err  error
}

func (p *fakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func newCollectorForTest(d Deduper, p Producer) *Collector {
	return NewCollector(d, p, "analytics.events", 24*time.Hour)
}

func TestCollectorAcceptsAndUsesSessionAsKey(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{}
	e := validEvent()

	got, err := newCollectorForTest(d, p).Collect(context.Background(), e)
	if err != nil || got != ResultAccepted {
		t.Fatalf("Collect() = %q, %v; quería accepted, nil", got, err)
	}
	if len(p.msgs) != 1 {
		t.Fatalf("mensajes publicados = %d, quería 1", len(p.msgs))
	}
	if string(p.msgs[0].Key) != e.SessionID {
		t.Fatalf("key = %q, quería %q", p.msgs[0].Key, e.SessionID)
	}
}

func TestCollectorDuplicate(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{}
	c := newCollectorForTest(d, p)

	e := validEvent()
	e.EventID = "11111111-1111-4111-8111-111111111111"

	first, err := c.Collect(context.Background(), e)
	if err != nil || first != ResultAccepted {
		t.Fatalf("primer Collect() = %q, %v", first, err)
	}
	second, err := c.Collect(context.Background(), e)
	if err != nil || second != ResultDuplicate {
		t.Fatalf("segundo Collect() = %q, %v; quería duplicate, nil", second, err)
	}
	if len(p.msgs) != 1 {
		t.Fatalf("mensajes publicados = %d, quería 1", len(p.msgs))
	}
}

func TestCollectorRollsBackDedupOnProduceError(t *testing.T) {
	d := &fakeDeduper{keys: map[string]bool{}}
	p := &fakeProducer{err: errors.New("kafka caído")}
	c := newCollectorForTest(d, p)
	e := validEvent()

	if _, err := c.Collect(context.Background(), e); err == nil {
		t.Fatal("quería error de Kafka")
	}
	if d.keys["event:"+e.EventID] {
		t.Fatal("la clave de dedup quedó seteada; el reintento sería duplicate")
	}

	// El reintento con Kafka ya arriba debe publicar.
	p.err = nil
	got, err := c.Collect(context.Background(), e)
	if err != nil || got != ResultAccepted {
		t.Fatalf("reintento = %q, %v; quería accepted, nil", got, err)
	}
}

func TestCollectorRejectsInvalidEvent(t *testing.T) {
	c := newCollectorForTest(&fakeDeduper{keys: map[string]bool{}}, &fakeProducer{})
	e := validEvent()
	e.Type = "scroll"
	if _, err := c.Collect(context.Background(), e); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("Collect() = %v, quería ErrInvalidEvent", err)
	}
}
