package cephmsgr

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
)

type nextStreamFixture struct {
	next   func(context.Context) error
	accept func()
	stop   func(error)
}

// Each adapter uses the real stream implementation, including its queue and
// stop method. Empty maps, digests and batches must not become "no item".
var nextStreamFixtures = map[string]func(*Client, context.Context, context.CancelFunc) nextStreamFixture{
	"config": func(c *Client, ctx context.Context, cancel context.CancelFunc) nextStreamFixture {
		s := &ConfigStream{client: c, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
		return nextStreamFixture{
			next:   func(ctx context.Context) error { _, err := s.Next(ctx); return err },
			accept: func() { s.pending = map[string]string{} }, stop: s.stop,
		}
	},
	"digest": func(c *Client, ctx context.Context, cancel context.CancelFunc) nextStreamFixture {
		s := &DigestStream{client: c, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
		return nextStreamFixture{
			next:   func(ctx context.Context) error { _, err := s.Next(ctx); return err },
			accept: func() { s.pending = &ClusterDigest{} }, stop: s.stop,
		}
	},
	"service map": func(c *Client, ctx context.Context, cancel context.CancelFunc) nextStreamFixture {
		s := &ServiceMapStream{client: c, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
		return nextStreamFixture{
			next:   func(ctx context.Context) error { _, err := s.Next(ctx); return err },
			accept: func() { s.pending = &ServiceMap{} }, stop: s.stop,
		}
	},
	"log": func(c *Client, ctx context.Context, cancel context.CancelFunc) nextStreamFixture {
		s := &LogStream{client: c, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
		return nextStreamFixture{
			next:   func(ctx context.Context) error { _, err := s.Next(ctx); return err },
			accept: func() { s.queue = []queuedLogBatch{{}} }, stop: s.stop,
		}
	},
	"OSD map": func(c *Client, ctx context.Context, cancel context.CancelFunc) nextStreamFixture {
		s := &OSDMapStream{client: c, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
		return nextStreamFixture{
			next:   func(ctx context.Context) error { _, err := s.Next(ctx); return err },
			accept: func() { s.queue = []queuedOSDMap{{}} }, stop: s.stop,
		}
	},
}

func TestStreamNextCanceledWaitPreservesEmptyItemAndFirstCause(t *testing.T) {
	for name, makeStream := range nextStreamFixtures {
		t.Run(name, func(t *testing.T) {
			watch, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := &Client{ctx: context.Background()}
			s := makeStream(c, watch, cancel)
			s.accept()
			cause := errors.New("first stream cause")
			s.stop(cause)
			s.stop(errors.New("later stream cause"))
			canceled, cancelWait := context.WithCancel(context.Background())
			cancelWait()
			if err := s.next(canceled); err != context.Canceled {
				t.Fatal("canceled wait did not win without consuming data", err)
			}
			if err := s.next(context.Background()); err != nil {
				t.Fatal("accepted empty item did not precede terminal cause", err)
			}
			for range 2 {
				if err := s.next(context.Background()); err != cause {
					t.Fatal("first terminal cause was not stable", err)
				}
			}
		})
	}
}

func TestStreamNextConcurrentWaitersTransferEmptyItemOnce(t *testing.T) {
	for name, makeStream := range nextStreamFixtures {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			watch, stopWatch := context.WithCancel(context.Background())
			defer stopWatch()
			c := &Client{ctx: context.Background()}
			s := makeStream(c, watch, stopWatch)
			const waiters = 16
			results := make(chan error, waiters)
			var ready sync.WaitGroup
			ready.Add(waiters)
			for range waiters {
				go func() { ready.Done(); results <- s.next(ctx) }()
			}
			ready.Wait()
			c.mu.Lock()
			s.accept()
			c.mu.Unlock()
			cause := errors.New("accepted before terminal")
			s.stop(cause)
			accepted := 0
			for range waiters {
				select {
				case err := <-results:
					if err == nil {
						accepted++
					} else if err != cause {
						t.Error("waiter lost first terminal cause", err)
					}
				case <-ctx.Done():
					t.Fatal("concurrent Next did not finish", ctx.Err())
				}
			}
			if accepted != 1 {
				t.Fatal("empty item transferred more or less than once", accepted)
			}
		})
	}
}

func TestStreamNextLifetimeStopRunsOutsideClientLock(t *testing.T) {
	for name, makeStream := range nextStreamFixtures {
		for _, clientStop := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/watch", true: "/client"}[clientStop], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				client, cancelClient := context.WithCancel(context.Background())
				defer cancelClient()
				watch, cancelWatch := context.WithCancel(context.Background())
				defer cancelWatch()
				c := &Client{ctx: client, auth: &cephx.Client{}}
				// Reentrant cancellation must not deadlock on c.mu.
				s := makeStream(c, watch, func() { c.Snapshot(); cancelWatch() })
				want := error(context.Canceled)
				if clientStop {
					want = ErrClosed
					cancelClient()
				} else {
					cancelWatch()
				}
				result := make(chan error, 1)
				go func() { result <- s.next(ctx) }()
				select {
				case err := <-result:
					if err != want {
						t.Fatal("lifetime stop lost cause", err, want)
					}
				case <-ctx.Done():
					t.Fatal("lifetime stop held the client lock", ctx.Err())
				}
			})
		}
	}
}
