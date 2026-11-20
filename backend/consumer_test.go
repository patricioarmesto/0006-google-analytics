package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeCommitter struct {
	calls int
}

func (f *fakeCommitter) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	f.calls++
	return nil
}

func testBatch() []queuedEvent {
	return []queuedEvent{
		{event: validEvent(), message: kafka.Message{Offset: 1}},
		{event: validEvent(), message: kafka.Message{Offset: 2}},
	}
}

func TestConsumerFlushCommitsOnlyOnSuccess(t *testing.T) {
	var posts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&posts, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // primer intento falla
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	committer := &fakeCommitter{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := insertAndCommit(ctx, ch, committer, testBatch()); err != nil {
		t.Fatalf("insertAndCommit() = %v", err)
	}
	if got := atomic.LoadInt32(&posts); got < 2 {
		t.Fatalf("POSTs = %d, quería al menos 2 (fallo + reintento)", got)
	}
	if committer.calls != 1 {
		t.Fatalf("commits = %d, quería 1", committer.calls)
	}
}

func TestConsumerFlushDoesNotCommitWhenInsertFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	committer := &fakeCommitter{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := insertAndCommit(ctx, ch, committer, testBatch()); err == nil {
		t.Fatal("quería error de ClickHouse")
	}
	if committer.calls != 0 {
		t.Fatalf("commits = %d, quería 0: no se puede confirmar un INSERT fallido", committer.calls)
	}
}
