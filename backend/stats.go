package main

import (
	"encoding/json"
	"net/http"
)

// HandleStats expone los cuatro modos del dashboard. `site=demo` se acepta
// porque está en el enunciado, pero no filtra: hay un solo sitio y está
// hardcodeado en Site. Cualquier otro valor se rechaza para no mentir.
func HandleStats(ch *ClickHouse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if site := r.URL.Query().Get("site"); site != "" && site != Site {
			http.Error(w, "sitio no soportado: "+site, http.StatusBadRequest)
			return
		}
		q := r.URL.Query()
		var payload any
		var err error
		switch {
		case q.Has("pageviews-by-day"):
			payload, err = ch.PageviewsByDay(r.Context())
		case q.Has("top-pages"):
			payload, err = ch.TopPages(r.Context())
		case q.Has("funnel"):
			payload, err = ch.Funnel(r.Context())
		case q.Has("realtime"):
			payload, err = ch.Realtime(r.Context())
		default:
			http.Error(w, "modo no soportado: usar pageviews-by-day, top-pages, funnel o realtime", http.StatusBadRequest)
			return
		}
		if err != nil {
			http.Error(w, "ClickHouse: "+err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}
}
