package main

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validEvent() Event {
	return Event{
		EventID:   uuid.NewString(),
		SessionID: "s-1",
		Page:      "/productos",
		Type:      TypeClick,
		Timestamp: time.Date(2027, 2, 9, 13, 0, 0, 0, time.UTC),
	}
}

func TestValidateEvent(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Event)
		want   bool
	}{
		{"valido", func(e *Event) {}, false},
		{"uuid malo", func(e *Event) { e.EventID = "no-uuid" }, true},
		{"sin session", func(e *Event) { e.SessionID = "" }, true},
		{"sin page", func(e *Event) { e.Page = "" }, true},
		{"type desconocido", func(e *Event) { e.Type = "scroll" }, true},
		{"sin timestamp", func(e *Event) { e.Timestamp = time.Time{} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(&e)
			err := e.Validate()
			if got := err != nil; got != tc.want {
				t.Fatalf("Validate() error = %v, want error = %v", err, tc.want)
			}
			if tc.want && !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("Validate() = %v, no envuelve ErrInvalidEvent", err)
			}
		})
	}
}
