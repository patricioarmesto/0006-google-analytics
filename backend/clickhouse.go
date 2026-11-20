package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClickHouse se usa por su interfaz HTTP. El insert viaja como JSONEachRow:
// un solo POST con N líneas, o sea un solo INSERT para todo el batch.
// No hace falta un driver nativo para un ejercicio que sólo hace INSERT y
// SELECT simples, y así el dependency list queda corto.
type ClickHouse struct {
	url      string
	user     string
	password string
	client   *http.Client
}

func NewClickHouse(rawURL, user, password string) *ClickHouse {
	return &ClickHouse{
		url:      strings.TrimRight(rawURL, "/"),
		user:     user,
		password: password,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

type clickRow struct {
	Date      string `json:"date"`
	Timestamp string `json:"timestamp"`
	EventID   string `json:"event_id"`
	SessionID string `json:"session_id"`
	Page      string `json:"page"`
	Type      string `json:"type"`
}

// InsertBatch hace UN solo INSERT para las N filas. Si falla, reintenta con
// backoff hasta que el contexto se cancela: el consumer mantiene el batch en
// memoria, así que un ClickHouse caído no significa eventos perdidos.
func (ch *ClickHouse) InsertBatch(ctx context.Context, events []Event) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, e := range events {
		utc := e.Timestamp.UTC()
		if err := enc.Encode(clickRow{
			Date:      utc.Format("2006-01-02"),
			Timestamp: utc.Format("2006-01-02 15:04:05.000"),
			EventID:   e.EventID,
			SessionID: e.SessionID,
			Page:      e.Page,
			Type:      e.Type,
		}); err != nil {
			return err
		}
	}
	// En la interfaz HTTP de ClickHouse la query y los datos van en el MISMO
	// body: primero el INSERT, después las filas JSONEachRow.
	query := "INSERT INTO default.events (date, timestamp, event_id, session_id, page, type) FORMAT JSONEachRow\n"
	payload := append([]byte(query), body.Bytes()...)

	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := ch.exec(ctx, payload, nil)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("clickhouse: insert falló (intento %d, %d eventos): %v", attempt, len(events), err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (ch *ClickHouse) exec(ctx context.Context, body []byte, out any) error {
	endpoint := ch.url + "/"
	values := url.Values{}
	// ClickHouse cita los UInt64 como strings en JSON; los counts del dashboard
	// se decodifican como números en Go.
	values.Set("output_format_json_quote_64bit_integers", "0")
	endpoint += "?" + values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if ch.user != "" {
		req.SetBasicAuth(ch.user, ch.password)
	}
	resp, err := ch.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("ClickHouse %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
