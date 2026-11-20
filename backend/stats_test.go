package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatsHandlerRejectsUnknownSite(t *testing.T) {
	h := HandleStats(NewClickHouse("http://localhost:8123", "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?pageviews-by-day&site=otro", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, quería 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "sitio no soportado") {
		t.Fatalf("body = %q", w.Body.String())
	}
}

func TestStatsHandlerRejectsUnknownMode(t *testing.T) {
	h := HandleStats(NewClickHouse("http://localhost:8123", "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?nada", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, quería 400", w.Code)
	}
}

func TestStatsHandlerAcceptsDemoSite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()

	h := HandleStats(NewClickHouse(server.URL, "", ""))
	r := httptest.NewRequest(http.MethodGet, "/api/stats?pageviews-by-day&site=demo", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, quería 200: %q", w.Code, w.Body.String())
	}
}
