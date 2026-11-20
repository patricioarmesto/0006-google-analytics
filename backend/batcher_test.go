package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBatcherFlushesBySize(t *testing.T) {
	in := make(chan int, 10)
	var mu sync.Mutex
	var batches [][]int
	done := make(chan struct{})

	go func() {
		RunBatcher(context.Background(), in, 2, time.Hour, func(b []int) error {
			mu.Lock()
			batches = append(batches, append([]int(nil), b...))
			mu.Unlock()
			if len(batches) == 2 {
				close(done)
			}
			return nil
		}, func(string, ...any) {})
	}()

	in <- 1
	in <- 2
	in <- 3
	in <- 4

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("no hubo dos flush por tamaño")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 2 {
		t.Fatalf("batches = %v, quería [[1 2] [3 4]]", batches)
	}
}

func TestBatcherFlushesByTime(t *testing.T) {
	in := make(chan int, 1)
	got := make(chan []int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunBatcher(ctx, in, 1000, 20*time.Millisecond, func(b []int) error {
		got <- append([]int(nil), b...)
		return nil
	}, func(string, ...any) {})

	in <- 7
	select {
	case b := <-got:
		if len(b) != 1 || b[0] != 7 {
			t.Fatalf("batch = %v, quería [7]", b)
		}
	case <-time.After(time.Second):
		t.Fatal("no hubo flush por tiempo")
	}
}

func TestBatcherKeepsBatchOnFlushError(t *testing.T) {
	in := make(chan int, 1)
	attempts := make(chan []int, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fail := true

	go RunBatcher(ctx, in, 1000, 15*time.Millisecond, func(b []int) error {
		cp := append([]int(nil), b...)
		attempts <- cp
		if fail {
			return errors.New("clickhouse caído")
		}
		return nil
	}, func(string, ...any) {})

	in <- 42

	first := <-attempts
	if len(first) != 1 || first[0] != 42 {
		t.Fatalf("primer intento = %v", first)
	}
	second := <-attempts
	if len(second) != 1 || second[0] != 42 {
		t.Fatalf("segundo intento = %v, el batch se perdió", second)
	}

	fail = false
	third := <-attempts
	if len(third) != 1 || third[0] != 42 {
		t.Fatalf("tercer intento = %v", third)
	}
}
