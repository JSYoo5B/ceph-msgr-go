package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// serviceMapLimitsMessage independently assembles an MServiceMap holding one
// service "svc" with one daemon "d" (GID 4242) whose metadata is one "pad"
// value of the given length, or claimed metadata entries without content
// when claimed is nonzero.
func serviceMapLimitsMessage(epoch uint32, pad int, claimed uint32) msgr.MessageData {
	var daemon, svc, body, front wire.Encoder
	daemon.U64(4242)
	msgr.Address{}.Encode(&daemon)
	daemon.U32(epoch)
	daemon.U32(0)
	daemon.U32(0)
	if claimed != 0 {
		daemon.U32(claimed)
	} else {
		daemon.U32(1)
		daemon.String("pad")
		daemon.String(strings.Repeat("p", pad))
	}
	daemon.U32(0)
	svc.U32(1)
	svc.String("d")
	svc.Struct(2, 1, daemon.Data)
	svc.String("")
	body.U32(epoch)
	body.U32(0)
	body.U32(0)
	body.U32(1)
	body.String("svc")
	body.Struct(1, 1, svc.Data)
	front.Struct(1, 1, body.Data)
	return msgr.MessageData{Type: msgr.ServiceMapMessage, Version: 1, CompatVersion: 1, Front: front.Data}
}

func serviceMapLimitsSession(c *Client) *session.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mon
}

// A decodable map above the watch's byte budget ends only that watch. The
// map accepted before it is still delivered first, the terminal cause is
// stable, the shared MON session stays admitted without a reconnect, and a
// new watch with a larger budget receives the same map on that session.
func TestServiceMapWatchBudgetOverflowIsIsolated(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	const budget = 2 << 10
	// The budget counts the raw map plus 512 bytes: find the largest admitted
	// padding, so that one more byte is the first refused size.
	admitted := 0
	for pad := 0; len(serviceMapLimitsMessage(1, pad, 0).Front)+512 <= budget; pad++ {
		admitted = pad
	}
	atBudget, overBudget := serviceMapLimitsMessage(2, admitted, 0), serviceMapLimitsMessage(3, admitted+1, 0)
	if len(atBudget.Front)+512 != budget || len(overBudget.Front)+512 != budget+1 {
		t.Fatal("synthetic maps do not bracket the budget", len(atBudget.Front), len(overBudget.Front))
	}
	watch, err := c.WatchServiceMap(ctx, ServiceMapOptions{MaxBufferedBytes: budget})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	serviceMapTestRegistration(t, ctx, p)
	source := serviceMapLimitsSession(c)
	p.send(t, ctx, serviceMapLimitsMessage(1, 0, 0))
	p.send(t, ctx, atBudget)
	logTestBarrier(t, ctx, c)
	p.send(t, ctx, overBudget)
	logTestBarrier(t, ctx, c)

	got, err := watch.Next(ctx)
	if err != nil || got.Epoch != 2 || len(got.Raw)+512 != budget || !bytes.Equal(got.Raw, atBudget.Front) {
		t.Fatal("map at the budget was not retained and drained before the overflow", got.Epoch, len(got.Raw), err)
	}
	if d := got.Services["svc"].Daemons["d"]; d.GID != 4242 || len(d.Metadata["pad"]) != admitted {
		t.Fatal("drained map lost its daemon GID or raw metadata", d.GID, len(d.Metadata["pad"]))
	}
	for range 2 {
		if m, err := watch.Next(ctx); err != ErrServiceMapOverflow || m.Raw != nil || m.Services != nil {
			t.Fatal("overflow was not the stable terminal cause", m.Epoch, err)
		}
	}
	// Only the watch ended: same admitted session, no failure, no new dial.
	if current := serviceMapLimitsSession(c); current != source || source.Err() != nil {
		t.Fatal("watch budget overflow disturbed the shared MON session", source.Err())
	}
	select {
	case <-peers:
		t.Fatal("watch budget overflow caused a MON reconnect")
	default:
	}
	logTestBarrier(t, ctx, c)

	replacement, err := c.WatchServiceMap(ctx, ServiceMapOptions{MaxBufferedBytes: budget + 1})
	if err != nil {
		t.Fatal("overflowed watch kept the client's slot", err)
	}
	defer replacement.Close()
	serviceMapTestRegistration(t, ctx, p)
	p.send(t, ctx, overBudget)
	logTestBarrier(t, ctx, c)
	got, err = replacement.Next(ctx)
	if err != nil || got.Epoch != 3 || !bytes.Equal(got.Raw, overBudget.Front) || len(got.Services["svc"].Daemons["d"].Metadata["pad"]) != admitted+1 {
		t.Fatal("larger budget did not admit the same map on the same session", got.Epoch, len(got.Raw), err)
	}
	// The delivered snapshot belongs to the caller: changing it reaches
	// neither the stream nor a later map.
	for i := range got.Raw {
		got.Raw[i] = 0
	}
	got.Services["svc"].Daemons["d"].Metadata["pad"] = "changed"
	delete(got.Services, "svc")
	p.send(t, ctx, serviceMapLimitsMessage(4, 7, 0))
	logTestBarrier(t, ctx, c)
	if next, err := replacement.Next(ctx); err != nil || next.Epoch != 4 || next.Services["svc"].Daemons["d"].Metadata["pad"] != "ppppppp" || !bytes.Equal(next.Raw, serviceMapLimitsMessage(4, 7, 0).Front) {
		t.Fatal("caller changes to a delivered snapshot reached a later map", next.Epoch, err)
	}
}

// Input the session itself cannot admit is not a watch-budget overflow: a
// collection above its fixed cap, or a frame above MaxFrameSize, fails the
// MON session that carried it. The watch drains its accepted map, then
// reports that session's limit error as its stable terminal cause. The
// client recovers on another MON without resubscribing the ended watch,
// commands work there, and the watch slot is free again.
func TestServiceMapSessionLimitEndsWatchAndRecoversClient(t *testing.T) {
	for name, over := range map[string]func(*Client) msgr.MessageData{
		"metadata collection cap": func(*Client) msgr.MessageData { return serviceMapLimitsMessage(2, 0, (1<<16)+1) },
		"frame limit": func(c *Client) msgr.MessageData {
			return serviceMapLimitsMessage(2, int(c.options.MaxFrameSize), 0)
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			watch, err := c.WatchServiceMap(ctx, ServiceMapOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer watch.Close()
			serviceMapTestRegistration(t, ctx, p)
			source := serviceMapLimitsSession(c)
			p.send(t, ctx, serviceMapLimitsMessage(1, 3, 0))
			logTestBarrier(t, ctx, c)
			// The client may refuse the frame and close the connection while
			// the peer is still writing it, so this send can fail.
			rejected := logTestSend{message: over(c), result: make(chan error, 1)}
			select {
			case p.sends <- rejected:
			case <-ctx.Done():
				t.Fatal("inadmissible map was not sent", ctx.Err())
			}
			select {
			case <-source.Done():
			case <-ctx.Done():
				t.Fatal("inadmissible map did not end its MON session", ctx.Err())
			}
			if err := source.Err(); !errors.Is(err, ErrLimitExceeded) {
				t.Fatal("session did not fail with the limit error", err)
			}
			got, err := watch.Next(ctx)
			if err != nil || got.Epoch != 1 || got.Services["svc"].Daemons["d"].Metadata["pad"] != "ppp" {
				t.Fatal("accepted map was not drained before the session cause", got.Epoch, err)
			}
			var first error
			for range 2 {
				m, err := watch.Next(ctx)
				if !errors.Is(err, ErrLimitExceeded) || errors.Is(err, ErrServiceMapOverflow) || m.Raw != nil || first != nil && err != first {
					t.Fatal("session limit was not the stable terminal cause", m.Epoch, err, first)
				}
				first = err
			}
			recovered := configTestNextPeer(t, ctx, peers)
			logTestBarrier(t, ctx, c)
			if current := serviceMapLimitsSession(c); current == source || current.Err() != nil {
				t.Fatal("client did not recover on a new admitted MON session")
			}
			select {
			case sub := <-recovered.serviceMapSubscriptions:
				t.Fatal("ended watch resubscribed on the recovered session", sub)
			default:
			}
			replacement, err := c.WatchServiceMap(ctx, ServiceMapOptions{})
			if err != nil {
				t.Fatal("ended watch kept the client's slot", err)
			}
			defer replacement.Close()
			serviceMapTestRegistration(t, ctx, recovered)
			recovered.send(t, ctx, serviceMapLimitsMessage(5, 1, 0))
			if m, err := replacement.Next(ctx); err != nil || m.Epoch != 5 {
				t.Fatal("new watch on the recovered session", m.Epoch, err)
			}
		})
	}
}

// Next with a canceled context consumes nothing from an overflowed watch:
// the accepted map and the terminal cause both remain for a live caller.
func TestServiceMapOverflowKeepsAcceptedMapForLiveNext(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	watch, err := c.WatchServiceMap(ctx, ServiceMapOptions{MaxBufferedBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()
	serviceMapTestRegistration(t, ctx, p)
	p.send(t, ctx, serviceMapLimitsMessage(1, 1, 0))
	p.send(t, ctx, serviceMapLimitsMessage(2, 1<<10, 0))
	logTestBarrier(t, ctx, c)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if m, err := watch.Next(canceled); !errors.Is(err, context.Canceled) || m.Raw != nil {
		t.Fatal("canceled Next consumed from an overflowed watch", m.Epoch, err)
	}
	if err := watch.Close(); err != nil {
		t.Fatal(err)
	}
	if m, err := watch.Next(ctx); err != nil || m.Epoch != 1 {
		t.Fatal("Close or a canceled Next discarded the accepted map", m.Epoch, err)
	}
	if _, err := watch.Next(ctx); err != ErrServiceMapOverflow {
		t.Fatal("Close replaced the earlier overflow cause", err)
	}
}
