package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestConfigWatchCoalescesFullMapsAndTransfersOwnership(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	if second, err := c.WatchConfig(ctx, ConfigOptions{}); second != nil || !errors.Is(err, ErrConfigWatchActive) {
		t.Fatal("second config watch was admitted", err)
	}
	p.send(t, ctx, configTestMessage("deleted", "old", "unchanged", "before"))
	p.send(t, ctx, configTestMessage("raw\x00key\xff", "  value\x00\xff\n", "empty", ""))
	logTestBarrier(t, ctx, c)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if config, err := stream.Next(canceled); config != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Next consumed a full map", config, err)
	}
	got, err := stream.Next(ctx)
	want := map[string]string{"raw\x00key\xff": "  value\x00\xff\n", "empty": ""}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("full-map replacement merged deleted keys, lost raw strings or queued old maps", got, err)
	}
	got["raw\x00key\xff"] = "caller mutation"
	delete(got, "empty")
	p.send(t, ctx, configTestMessage())
	logTestBarrier(t, ctx, c)
	empty, err := stream.Next(ctx)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal("empty full map was treated as missing data", empty, err)
	}
	if got["raw\x00key\xff"] != "caller mutation" || len(got) != 1 {
		t.Fatal("subsequent delivery retained the caller's previous map", got)
	}
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if config, err := stream.Next(wait); config != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("local wait did not leave the watch active", config, err)
	}
	p.send(t, ctx, configTestMessage("after wait", "delivered"))
	if config, err := stream.Next(ctx); err != nil || config["after wait"] != "delivered" {
		t.Fatal("local wait canceled the watch", config, err)
	}
	if c.Snapshot().Manager.Ready {
		t.Fatal("config watch opened a MGR connection")
	}
}

func TestConfigWatchIndependentOfLogsAndCommandCapacity(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	mutation, cancel := context.WithCancel(ctx)
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(mutation, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
		finished <- err
	}()
	select {
	case <-p.commands:
	case <-ctx.Done():
		t.Fatal("mutation did not occupy the only command slot", ctx.Err())
	}
	stream, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal("full command capacity prevented config registration", err)
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	logs, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal("config watch occupied the independent log slot", err)
	}
	defer logs.Close()
	p.subscription(t, ctx, "log-info", 0)
	p.send(t, ctx, configTestMessage("beside mutation", "value"))
	p.send(t, ctx, logTestMessage([16]byte{1}, 7, logTestRawEntry("beside config")))
	if config, err := stream.Next(ctx); err != nil || config["beside mutation"] != "value" {
		t.Fatal("pending mutation blocked config receive", config, err)
	}
	logTestBatch(t, ctx, logs, 7, logTestRawEntry("beside config"))
	cancel()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) {
			t.Fatal("config/log delivery changed uncertain mutation cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("mutation local wait did not end", ctx.Err())
	}
	logTestBarrier(t, ctx, c)
	stream.Close()
	p.send(t, ctx, logTestMessage([16]byte{1}, 8, logTestRawEntry("after config close")))
	logTestBatch(t, ctx, logs, 8, logTestRawEntry("after config close"))
	if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil || result.Code != 0 {
		t.Fatal("config/log dispatch interfered with independent MGR path", result, err)
	}
}

func TestConfigWatchOverflowKeepsAcceptedMapAndFirstCause(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "first map oversized", true: "replacement oversized"}[pending], func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			stream, err := c.WatchConfig(ctx, ConfigOptions{MaxBufferedBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.registration(t, ctx, c.options.Identity)
			if pending {
				p.send(t, ctx, configTestMessage("v", strings.Repeat("x", 383)))
			} // exact retained estimate: 512+128+1+383.
			p.send(t, ctx, configTestMessage("v", strings.Repeat("x", 384)))
			logTestBarrier(t, ctx, c)
			stream.Close()
			if pending {
				if config, err := stream.Next(ctx); err != nil || len(config) != 1 || config["v"] != strings.Repeat("x", 383) {
					t.Fatal("overflow discarded accepted map or accepted rejected replacement", config, err)
				}
			}
			configTestTerminal(t, ctx, stream, ErrConfigOverflow)
			logTestBarrier(t, ctx, c)
			replacement, err := c.WatchConfig(ctx, ConfigOptions{})
			if err != nil {
				t.Fatal("overflow retained the watch slot", err)
			}
			defer replacement.Close()
			p.registration(t, ctx, c.options.Identity)
			p.send(t, ctx, configTestMessage("fresh", "full map"))
			if config, err := replacement.Next(ctx); err != nil || config["fresh"] != "full map" {
				t.Fatal("same-session reopen did not request full map", config, err)
			}
		})
	}
}

func TestConfigWatchLifetimeCancellationAndClientCloseJoin(t *testing.T) {
	for _, mode := range []string{"lifetime", "client"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			lifetime, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := c.WatchConfig(lifetime, ConfigOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.registration(t, ctx, c.options.Identity)
			p.send(t, ctx, configTestMessage("accepted", "before terminal"))
			logTestBarrier(t, ctx, c)
			cause := error(context.Canceled)
			if mode == "lifetime" {
				cancel()
			} else {
				cause = ErrClosed
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-stream.done:
			case <-ctx.Done():
				t.Fatal("terminal did not join config worker", ctx.Err())
			}
			stream.Close()
			if config, err := stream.Next(ctx); err != nil || config["accepted"] != "before terminal" {
				t.Fatal("terminal discarded accepted map", config, err)
			}
			configTestTerminal(t, ctx, stream, cause)
			if mode == "lifetime" {
				logTestBarrier(t, ctx, c)
			}
		})
	}
}

func TestConfigWatchRejectsOptionsAndDoesNotManufactureDeliveryFromAck(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	for _, options := range []ConfigOptions{{MaxBufferedBytes: 1}, {MaxBufferedBytes: (1 << 30) + 1}} {
		if stream, err := c.WatchConfig(ctx, options); err == nil {
			stream.Close()
			t.Fatal("invalid config options accepted", options)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if stream, err := c.WatchConfig(canceled, ConfigOptions{}); stream != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled watch started", err)
	}
	select {
	case <-p.configSubscriptions:
		t.Fatal("invalid/canceled watch sent a subscription")
	default:
	}
	stream, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	front := append(logTestU32(nil, 30), make([]byte, 16)...)
	p.send(t, ctx, msgr.MessageData{Type: msgr.SubscribeAckMessage, Version: 1, Front: front})
	logTestBarrier(t, ctx, c)
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	if config, err := stream.Next(wait); config != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("SubscribeAck manufactured config delivery/permission", config, err)
	}
	stop()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	configTestTerminal(t, ctx, stream, ErrClosed)
	for _, options := range []ConfigOptions{{}, {MaxBufferedBytes: 1}} {
		if stream, err := c.WatchConfig(ctx, options); stream != nil || !errors.Is(err, ErrClosed) {
			t.Fatal("closed client validated options before lifetime", options, err)
		}
		if stream, err := c.WatchConfig(canceled, options); stream != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("closed client hid caller cancellation", err)
		}
	}
}
