package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type snapshotWatchHandle struct {
	next  func(context.Context) (string, error)
	close func() error
	stop  func(error) // the shared terminal transition, as a stale callback would reach it
}

type snapshotWatchCase struct {
	watch    func(*Client, context.Context) (snapshotWatchHandle, error)
	register func(*testing.T, context.Context, *configTestPeer, *Client)
	message  func(int) msgr.MessageData
	active   error
	closed   error
}

// Independently assembled MServiceMap: a v1 envelope around epoch, modified
// time and an empty service collection.
func serviceMapTestMessage(epoch uint32) msgr.MessageData {
	var body, front wire.Encoder
	body.U32(epoch)
	body.U32(0)
	body.U32(0)
	body.U32(0)
	front.Struct(1, 1, body.Data)
	return msgr.MessageData{Type: msgr.ServiceMapMessage, Version: 1, CompatVersion: 1, Front: front.Data}
}

func serviceMapTestRegistration(t *testing.T, ctx context.Context, p *configTestPeer) {
	t.Helper()
	select {
	case sub := <-p.serviceMapSubscriptions:
		if sub.name != "servicemap" || sub.start != 0 || sub.flags != 0 || len(sub.all) != 3 || sub.all["monmap"] != 0 || sub.all["mgrmap"] != 0 {
			t.Fatal("service map subscription lost map or continuous semantics", sub)
		}
	case <-ctx.Done():
		t.Fatal("service map subscription was not observed", ctx.Err())
	}
}

var snapshotWatchCases = map[string]snapshotWatchCase{
	"config": {
		watch: func(c *Client, ctx context.Context) (snapshotWatchHandle, error) {
			s, err := c.WatchConfig(ctx, ConfigOptions{})
			if err != nil {
				return snapshotWatchHandle{}, err
			}
			return snapshotWatchHandle{next: func(ctx context.Context) (string, error) {
				m, err := s.Next(ctx)
				return m["value"], err
			}, close: s.Close, stop: s.watch.stop}, nil
		},
		register: func(t *testing.T, ctx context.Context, p *configTestPeer, c *Client) {
			p.registration(t, ctx, c.options.Identity)
		},
		message: func(n int) msgr.MessageData { return configTestMessage("value", strconv.Itoa(n)) },
		active:  ErrConfigWatchActive, closed: ErrConfigStreamClosed,
	},
	"digest": {
		watch: func(c *Client, ctx context.Context) (snapshotWatchHandle, error) {
			s, err := c.WatchDigest(ctx, DigestOptions{})
			if err != nil {
				return snapshotWatchHandle{}, err
			}
			return snapshotWatchHandle{next: func(ctx context.Context) (string, error) {
				d, err := s.Next(ctx)
				return string(d.MonStatus), err
			}, close: s.Close, stop: s.watch.stop}, nil
		},
		register: func(t *testing.T, ctx context.Context, p *configTestPeer, _ *Client) {
			digestTestRegistration(t, ctx, p)
		},
		message: func(n int) msgr.MessageData { return digestTestMessage(strconv.Itoa(n), "") },
		active:  ErrDigestWatchActive, closed: ErrDigestStreamClosed,
	},
	"service map": {
		watch: func(c *Client, ctx context.Context) (snapshotWatchHandle, error) {
			s, err := c.WatchServiceMap(ctx, ServiceMapOptions{})
			if err != nil {
				return snapshotWatchHandle{}, err
			}
			return snapshotWatchHandle{next: func(ctx context.Context) (string, error) {
				m, err := s.Next(ctx)
				if err != nil {
					return "", err
				}
				return strconv.FormatUint(uint64(m.Epoch), 10), nil
			}, close: s.Close, stop: s.watch.stop}, nil
		},
		register: func(t *testing.T, ctx context.Context, p *configTestPeer, _ *Client) {
			serviceMapTestRegistration(t, ctx, p)
		},
		message: func(n int) msgr.MessageData { return serviceMapTestMessage(uint32(n)) },
		active:  ErrServiceMapWatchActive, closed: ErrServiceMapStreamClosed,
	},
}

// Each public snapshot watch shares one terminal transition. A stale source or
// a stopped predecessor cannot terminate its replacement, accepted data drains
// before the first cause, and an already canceled Next consumes nothing.
func TestMonSnapshotWatchesShareTerminalTransition(t *testing.T) {
	for name, test := range snapshotWatchCases {
		t.Run(name, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			first, err := test.watch(c, ctx)
			if err != nil {
				t.Fatal(err)
			}
			test.register(t, ctx, p, c)
			if _, err := test.watch(c, ctx); err != test.active {
				t.Fatal("a second watch took an occupied slot", err)
			}
			p.send(t, ctx, test.message(1))
			logTestBarrier(t, ctx, c)
			c.mu.Lock()
			current := c.mon
			c.mu.Unlock()
			fatal := fmt.Errorf("unit fatal source: %w", msgr.ErrFrame)
			c.stopMonSnapshotsForFailedSession(new(session.Session), fatal)
			if err := first.close(); err != nil {
				t.Fatal(err)
			}
			if got, err := first.next(ctx); err != nil || got != "1" {
				t.Fatal("close discarded accepted data", got, err)
			}
			for range 2 {
				if got, err := first.next(ctx); got != "" || err != test.closed {
					t.Fatal("stale source replaced the close cause", got, err)
				}
			}

			replacement, err := test.watch(c, ctx)
			if err != nil {
				t.Fatal("closed watch retained its slot", err)
			}
			defer replacement.close()
			test.register(t, ctx, p, c)
			first.stop(errors.New("stale predecessor callback"))
			p.send(t, ctx, test.message(2))
			logTestBarrier(t, ctx, c)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := replacement.next(canceled); got != "" || !errors.Is(err, context.Canceled) {
				t.Fatal("canceled Next consumed data", got, err)
			}
			c.stopMonSnapshotsForFailedSession(current, fatal)
			if got, err := replacement.next(ctx); err != nil || got != "2" {
				t.Fatal("replacement lost accepted data", got, err)
			}
			if err := replacement.close(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if got, err := replacement.next(ctx); got != "" || err != fatal {
					t.Fatal("registered source cause was not first", got, err)
				}
			}
			again, err := test.watch(c, ctx)
			if err != nil {
				t.Fatal("source failure retained the watch slot", err)
			}
			again.close()
		})
	}
}
