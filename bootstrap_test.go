package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
)

func TestBootstrapRecoversTemporaryAuthenticationEOF(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	var attempts atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if attempts.Add(1) <= 2 {
			return nil, fmt.Errorf("ceph messenger authentication: %w", io.EOF)
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("temporary setup loss prevented bootstrap", err)
	}
	defer c.Close()
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatal("unexpected setup attempts", attempts.Load())
	}
}

func TestBootstrapRetryHonorsContextAndSetupLimit(t *testing.T) {
	for _, callerDeadline := range []bool{true, false} {
		t.Run(fmt.Sprint(callerDeadline), func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			options.ConnectTimeout = 30 * time.Millisecond
			options.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, io.EOF }
			ctx := context.Background()
			if callerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			started := time.Now()
			_, err := Dial(ctx, options)
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, io.EOF) {
				t.Fatal("setup loss or timeout cause missing", err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("bootstrap retry exceeded its setup bound")
			}
		})
	}
}

func TestBootstrapDoesNotRetryExplicitAuthenticationRejection(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	var attempts atomic.Int32
	rejection := &cephx.AuthenticationError{Method: 2, Code: -13}
	options.DialContext = func(context.Context, string, string) (net.Conn, error) {
		attempts.Add(1)
		// One transient leaf must not make a joined explicit rejection retryable.
		return nil, errors.Join(io.EOF, rejection)
	}
	_, err := Dial(context.Background(), options)
	if !errors.Is(err, rejection) || attempts.Load() != 1 {
		t.Fatal("explicit rejection was retried or lost", attempts.Load(), err)
	}
}
