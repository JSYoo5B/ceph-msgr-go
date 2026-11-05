package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The dial hold keeps every packet of a held dial out of a capture gap: no
// connection attempt reaches the listener while the hold file exists, and the
// trace records the hold before the dial start.
func TestDialHoldSendsNothingWhileHeld(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan time.Time, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- time.Now().UTC()
			c.Close()
		}
	}()
	hold := filepath.Join(t.TempDir(), "dial-hold")
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{phase: "peer-restart"}
	dial := rec.dialer(hold)

	// A held dial whose context ends sends nothing.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	if _, err := dial(ctx, "tcp", ln.Addr().String()); err == nil {
		t.Fatal("a held dial succeeded")
	}
	cancel()
	select {
	case <-accepted:
		t.Fatal("a held dial reached the listener")
	case <-time.After(100 * time.Millisecond):
	}

	released := make(chan time.Time, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		released <- time.Now().UTC()
		os.Remove(hold)
	}()
	conn, err := dial(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	at := <-accepted
	if rel := <-released; at.Before(rel) {
		t.Fatalf("accepted at %s before the hold was released at %s", at, rel)
	}
	tr := rec.trace.Connections
	if len(tr) != 2 || tr[0].DialError == "" || tr[0].HeldFrom.IsZero() || tr[1].HeldFrom.IsZero() ||
		tr[1].HeldUntil.Before(tr[1].HeldFrom) || tr[1].DialStart.Before(tr[1].HeldUntil) || tr[1].Local == "" {
		t.Fatalf("trace %+v", tr)
	}
	// Without a hold file the dial is immediate and records no hold.
	conn, err = dial(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if last := rec.trace.Connections[2]; !last.HeldFrom.IsZero() || !last.HeldUntil.IsZero() {
		t.Fatalf("unheld dial recorded a hold: %+v", last)
	}
}
