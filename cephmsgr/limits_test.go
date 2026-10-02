package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestCommandLimitWaitsWithoutSendingAndReleasesOnCancellation(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxInFlight = 1
	blocked := make(chan struct{})
	var sent atomic.Int32
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, command: func(m msgr.MessageData) {
			sent.Add(1)
			if strings.Contains(string(m.Front), "block") {
				close(blocked)
			}
		}})
		return client, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, stop := context.WithCancel(ctx)
	defer stop()
	result := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(first, Command{JSON: []byte(`{"prefix":"block"}`)})
		result <- err
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	second, stopSecond := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stopSecond()
	_, err = c.MonCommand(second, Command{JSON: []byte(`{"prefix":"status"}`)})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || sent.Load() != 1 {
		t.Fatal("command waiting for a slot was sent", err, sent.Load())
	}
	stop()
	if err := <-result; !errors.As(err, &unknown) {
		t.Fatal("sent command lost its uncertain outcome", err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("canceled call did not release slot", err)
	}
}
