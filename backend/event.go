package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Site es el único sitio del ejercicio. Está hardcodeado a propósito: meter una
// tabla sites con una fila sería decoración, no una decisión de diseño.
const Site = "demo"

const (
	TypePageview    = "pageview"
	TypeClick       = "click"
	TypeCustomEvent = "custom_event"
)

var ErrInvalidEvent = errors.New("evento inválido")

// Event es el contrato entre el demo de React y todo el pipeline. Va en el
// body de POST /collect y, serializado, en el value del mensaje de Kafka.
type Event struct {
	EventID   string    `json:"event_id"`
	SessionID string    `json:"session_id"`
	Page      string    `json:"page"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// Validate chequea los cinco campos antes de tocar Redis. Validar acá y no en
// ClickHouse esbarre el error al borde: un evento mal formado nunca entra a la
// cola y nunca ocupa un lugar en un batch.
func (e Event) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("%w: event_id no es UUID: %v", ErrInvalidEvent, err)
	}
	if e.SessionID == "" {
		return fmt.Errorf("%w: session_id vacío", ErrInvalidEvent)
	}
	if e.Page == "" {
		return fmt.Errorf("%w: page vacía", ErrInvalidEvent)
	}
	switch e.Type {
	case TypePageview, TypeClick, TypeCustomEvent:
	default:
		return fmt.Errorf("%w: type %q no soportado", ErrInvalidEvent, e.Type)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp cero", ErrInvalidEvent)
	}
	return nil
}
