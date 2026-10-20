package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestLogWatchRawFieldsMixedCommandsAndLocalWait(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-info", 0)
	if _, err := c.WatchLogs(ctx, LogOptions{Level: LogDebug}); !errors.Is(err, ErrLogWatchActive) {
		t.Fatal("second log watch was admitted", err)
	}
	wait, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stream.Next(wait); !errors.Is(err, context.Canceled) {
		t.Fatal("local log wait ignored its context", err)
	}
	entry := logTestRawEntry("raw\x00message\xff\n")
	p.send(t, ctx, logTestMessage([16]byte{1}, 7, entry))
	logTestBarrier(t, ctx, c)
	logTestBatch(t, ctx, stream, 7, entry)
	if result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil || result.Code != 0 || !bytes.Equal(result.Data, []byte(`{"ok":true}`)) {
		t.Fatal("MON log dispatch interfered with the MGR path", result, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	logTestTerminal(t, ctx, stream, ErrLogStreamClosed)
	replacement, err := c.WatchLogs(ctx, LogOptions{Level: LogSec})
	if err != nil {
		t.Fatal("closing watch did not release its slot", err)
	}
	defer replacement.Close()
	p.subscription(t, ctx, "log-sec", 0)
}
