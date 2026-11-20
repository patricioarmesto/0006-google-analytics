package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickHouseInsertBatchRetries(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "FORMAT JSONEachRow") {
			t.Errorf("el body no tiene el INSERT: %q", body)
		}
		if strings.Count(string(body), `"event_id":`) != 2 {
			t.Errorf("el batch no tiene las 2 filas: %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(context.Background(), []Event{validEvent(), validEvent()}); err != nil {
		t.Fatalf("InsertBatch() = %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("llamadas = %d, quería 2 (1 fallo + 1 ok)", got)
	}
}

func TestClickHouseInsertBatchSendsOnePostForWholeBatch(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(context.Background(), []Event{validEvent(), validEvent(), validEvent()}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("llamadas = %d, quería 1 INSERT para 3 eventos", got)
	}
}

func TestClickHouseInsertBatchHonorsContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ch := NewClickHouse(server.URL, "", "")
	if err := ch.InsertBatch(ctx, []Event{validEvent()}); err == nil {
		t.Fatal("quería error por contexto cancelado")
	}
}
