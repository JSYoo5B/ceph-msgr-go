package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func digestTestRegistration(t *testing.T, ctx context.Context, p *configTestPeer) {
	t.Helper()
	select {
	case sub := <-p.digestSubscriptions:
		if sub.name != "mgrdigest" || sub.start != 0 || sub.flags != 0 || len(sub.all) != 3 || sub.all["monmap"] != 0 || sub.all["mgrmap"] != 0 {
			t.Fatal("digest subscription lost map or continuous semantics", sub)
		}
	case <-ctx.Done():
		t.Fatal("digest subscription was not observed", ctx.Err())
	}
}

// Independent MMgrDigest wire payload: two uint32-length raw byte buffers.
func digestTestMessage(mon, health string) msgr.MessageData {
	front := logTestString(nil, mon)
	front = logTestString(front, health)
	return msgr.MessageData{Type: 0x705, Version: 1, CompatVersion: 1, Front: front}
}

func digestTestNext(t *testing.T, ctx context.Context, stream *DigestStream, mon, health string) {
	t.Helper()
	got, err := stream.Next(ctx)
	if err != nil || !bytes.Equal(got.MonStatus, []byte(mon)) || !bytes.Equal(got.Health, []byte(health)) {
		t.Fatal("digest lost its raw pair", got, err)
	}
}

func digestTestTerminal(t *testing.T, ctx context.Context, stream *DigestStream, cause error) {
	t.Helper()
	for range 2 {
		got, err := stream.Next(ctx)
		if len(got.MonStatus) != 0 || len(got.Health) != 0 || !errors.Is(err, cause) {
			t.Fatal("digest terminal changed or returned rejected data", got, err, cause)
		}
	}
}

func TestDigestWatchCoalescesRawPairsAndTransfersOwnership(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchDigest(ctx, DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	digestTestRegistration(t, ctx, p)
	if second, err := c.WatchDigest(ctx, DigestOptions{}); second != nil || !errors.Is(err, ErrDigestWatchActive) {
		t.Fatal("second digest watch was admitted", err)
	}
	p.send(t, ctx, digestTestMessage("old mon", "old health"))
	mon, health := "  mon\x00\xff\n", "not JSON\x00\xff"
	p.send(t, ctx, digestTestMessage(mon, health))
	logTestBarrier(t, ctx, c)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if got, err := stream.Next(canceled); len(got.MonStatus) != 0 || len(got.Health) != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled Next consumed a digest", got, err)
	}
	got, err := stream.Next(ctx)
	if err != nil || string(got.MonStatus) != mon || string(got.Health) != health {
		t.Fatal("replacement merged pairs, parsed JSON or queued older data", got, err)
	}
	got.MonStatus[0] = 'x'
	if string(got.Health) != health {
		t.Fatal("raw buffers share writable storage", got)
	}
	p.send(t, ctx, digestTestMessage("", ""))
	logTestBarrier(t, ctx, c)
	digestTestNext(t, ctx, stream, "", "")
	if got.MonStatus[0] != 'x' {
		t.Fatal("later publication reused caller-owned storage", got)
	}
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	if _, err := stream.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("local wait did not end", err)
	}
	stop()
	p.send(t, ctx, digestTestMessage("after local wait", "health"))
	digestTestNext(t, ctx, stream, "after local wait", "health")
	stream.Close()
	digestTestTerminal(t, ctx, stream, ErrDigestStreamClosed)
	reopened, err := c.WatchDigest(ctx, DigestOptions{})
	if err != nil {
		t.Fatal("closed watch retained its slot", err)
	}
	defer reopened.Close()
	digestTestRegistration(t, ctx, p)
	p.send(t, ctx, digestTestMessage("same-session reopen", "fresh"))
	digestTestNext(t, ctx, reopened, "same-session reopen", "fresh")
	if state := c.Snapshot(); state.Manager.Ready || state.Manager.Endpoint != "" {
		t.Fatal("digest watch opened a MGR connection", state)
	}
}

func TestDigestWatchIndependentOfConfigLogsAndCommandCapacity(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	mutation, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(mutation, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
		finished <- err
	}()
	select {
	case <-p.commands:
	case <-ctx.Done():
		t.Fatal("mutation did not occupy the command slot", ctx.Err())
	}
	stream, err := c.WatchDigest(ctx, DigestOptions{})
	if err != nil {
		t.Fatal("full command capacity blocked digest registration", err)
	}
	defer stream.Close()
	digestTestRegistration(t, ctx, p)
	configs, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer configs.Close()
	p.registration(t, ctx, c.options.Identity)
	logs, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	p.subscription(t, ctx, "log-info", 0)
	p.send(t, ctx, digestTestMessage("beside commands", "health"))
	p.send(t, ctx, configTestMessage("beside digest", "value"))
	p.send(t, ctx, logTestMessage([16]byte{1}, 7, logTestRawEntry("beside digest")))
	digestTestNext(t, ctx, stream, "beside commands", "health")
	if config, err := configs.Next(ctx); err != nil || config["beside digest"] != "value" {
		t.Fatal("digest blocked config delivery", config, err)
	}
	logTestBatch(t, ctx, logs, 7, logTestRawEntry("beside digest"))
	cancel()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) {
			t.Fatal("digest changed an already started command's cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("command wait did not end", ctx.Err())
	}
	stream.Close()
	p.send(t, ctx, configTestMessage("after digest close", "value"))
	p.send(t, ctx, logTestMessage([16]byte{1}, 8, logTestRawEntry("after digest close")))
	if config, err := configs.Next(ctx); err != nil || config["after digest close"] != "value" {
		t.Fatal("digest Close ended config watch", config, err)
	}
	logTestBatch(t, ctx, logs, 8, logTestRawEntry("after digest close"))
	logTestBarrier(t, ctx, c)
}

func TestDigestWatchOverflowPreservesAcceptedPairAndFirstCause(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "replacement"}[pending], func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			stream, err := c.WatchDigest(ctx, DigestOptions{MaxBufferedBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			digestTestRegistration(t, ctx, p)
			if pending {
				p.send(t, ctx, digestTestMessage("", strings.Repeat("x", 512)))
			}
			p.send(t, ctx, digestTestMessage("", strings.Repeat("x", 513)))
			logTestBarrier(t, ctx, c)
			stream.Close()
			if pending {
				digestTestNext(t, ctx, stream, "", strings.Repeat("x", 512))
			}
			digestTestTerminal(t, ctx, stream, ErrDigestOverflow)
			logTestBarrier(t, ctx, c)
		})
	}
}

func TestDigestWatchLifetimeAndClientClosePreserveAcceptedPair(t *testing.T) {
	for _, mode := range []string{"lifetime", "client"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			lifetime, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := c.WatchDigest(lifetime, DigestOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			digestTestRegistration(t, ctx, p)
			p.send(t, ctx, digestTestMessage("accepted", "before terminal"))
			logTestBarrier(t, ctx, c)
			cause := error(context.Canceled)
			if mode == "lifetime" {
				cancel()
			} else {
				cause = ErrClosed
				c.Close()
			}
			awaitCleanup(t, ctx, stream.done)
			stream.Close()
			digestTestNext(t, ctx, stream, "accepted", "before terminal")
			digestTestTerminal(t, ctx, stream, cause)
			if mode == "lifetime" {
				logTestBarrier(t, ctx, c)
			}
		})
	}
}

func TestDigestWatchRegistrationValidationAndAckIsNotDelivery(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	for _, limit := range []uint32{1, (1 << 30) + 1} {
		if stream, err := c.WatchDigest(ctx, DigestOptions{MaxBufferedBytes: limit}); err == nil {
			stream.Close()
			t.Fatal("invalid digest limit accepted", limit)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if stream, err := c.WatchDigest(canceled, DigestOptions{}); stream != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled watch was registered", err)
	}
	stream, err := c.WatchDigest(ctx, DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	digestTestRegistration(t, ctx, p)
	front := append(logTestU32(nil, 30), make([]byte, 16)...)
	p.send(t, ctx, msgr.MessageData{Type: msgr.SubscribeAckMessage, Version: 1, Front: front})
	logTestBarrier(t, ctx, c)
	wait, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	if _, err := stream.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("SubscribeAck manufactured digest delivery", err)
	}
	stop()
	c.Close()
	if stream, err := c.WatchDigest(ctx, DigestOptions{MaxBufferedBytes: 1}); stream != nil || !errors.Is(err, ErrClosed) {
		t.Fatal("closed client validated options before lifetime", err)
	}
}
