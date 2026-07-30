package rpc

import (
	"sync"
	"testing"
)

func TestOnReconnectRunsEveryHandler(t *testing.T) {
	c := &Client{}

	var mu sync.Mutex
	var fired []string
	done := make(chan struct{}, 2)
	record := func(name string) func() {
		return func() {
			mu.Lock()
			fired = append(fired, name)
			mu.Unlock()
			done <- struct{}{}
		}
	}

	c.OnReconnect(record("a"))
	c.OnReconnect(record("b"))

	c.notifyReconnect()
	<-done
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(fired) != 2 {
		t.Fatalf("both handlers must run, got %v", fired)
	}
}

func TestOnReconnectUnregisterStopsHandler(t *testing.T) {
	c := &Client{}
	kept := make(chan struct{}, 1)
	removed := make(chan struct{}, 1)

	c.OnReconnect(func() { kept <- struct{}{} })
	off := c.OnReconnect(func() { removed <- struct{}{} })
	off()

	c.notifyReconnect()
	<-kept

	select {
	case <-removed:
		t.Fatal("unregistered handler must not run")
	default:
	}
}

func TestOnReconnectIsolatesPanics(t *testing.T) {
	c := &Client{}
	ok := make(chan struct{}, 1)

	c.OnReconnect(func() { panic("boom") })
	c.OnReconnect(func() { ok <- struct{}{} })

	// a panicking handler must neither crash the process nor block the others
	c.notifyReconnect()
	<-ok
}
