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

// En el demo es fácil mandar un custom event antes del click y otro después.
// El funnel debe encontrar una cadena pageview -> click -> custom_event en
// cualquier posición, no exigir que el PRIMER custom sea posterior al primer
// click (lo que hacía minIf y descartaba la sesión para siempre).
func TestFunnelFindsOrderedChainNotFirstOfEachType(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"pageviews":1,"clicks":1,"custom_events":1}]}`)
	}))
	defer server.Close()

	ch := NewClickHouse(server.URL, "", "")
	stat, err := ch.Funnel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stat.CustomEvents != 1 {
		t.Fatalf("custom_events = %d, quería 1", stat.CustomEvents)
	}
	if !strings.Contains(query, "arrayExists(pv ->") {
		t.Fatalf("el funnel no parte de un pageview real: %q", query)
	}
	if strings.Contains(query, "minIf(timestamp, type = 'custom_event')") {
		t.Fatalf("el funnel volvió a comparar solo el primer custom_event: %q", query)
	}
	if strings.Contains(query, "1970") || strings.Contains(query, "toDateTime64(0") {
		t.Fatalf("el funnel usa un centinela: un click sin pageview podría contar: %q", query)
	}
}
