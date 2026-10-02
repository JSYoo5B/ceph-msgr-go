package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephStreamCancellationIntegration(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) { testStreamCancellation(t, mgr) })
	}
}

func testStreamCancellation(t *testing.T, mgr bool) {
	t.Helper()
	observer, ctx := fixtureClient(t, 40*time.Second)
	input := bytes.Repeat([]byte{0, 255, 13, 10}, 10<<10)
	marker := fmt.Sprintf("native-stream-marker-%d", time.Now().UnixNano())
	mutation := marker + "-canceled"
	command := func(prefix, key string) cephmsgr.Command {
		encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "key": key})
		return cephmsgr.Command{JSON: encoded}
	}
	if !mgr {
		seed := command("config-key set", marker)
		seed.Input = input
		if _, err := observer.MonCommand(ctx, seed); err != nil {
			t.Fatal("seed independent binary response marker", err)
		}
	}
	options := integrationOptions(t)
	options.ConnectTimeout = 5 * time.Second
	dial := options.DialContext
	var stream atomic.Pointer[testcluster.StreamRelay]
	var wrapped atomic.Bool
	var targetDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, port, _ := net.SplitHostPort(endpoint)
		isMgr := port == "36800" || port == "36801"
		if isMgr == mgr {
			targetDials.Add(1)
			if wrapped.CompareAndSwap(false, true) {
				relay := testcluster.NewStreamRelay(conn)
				stream.Store(relay)
				t.Cleanup(func() { relay.Close(); relay.Wait() })
				return relay, nil
			}
		}
		return conn, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call, warm := c.MonCommand, c.WaitMonReady
	query := cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}
	if mgr {
		call, warm = c.MgrCommand, c.WaitMgrReady
		query.JSON = []byte(`{"prefix":"pg stat","format":"json"}`)
	}
	if err := warm(ctx); err != nil {
		t.Fatal("authenticate fragmented connection", err)
	}
	relay := stream.Load()
	if relay == nil {
		t.Fatal("target connection did not use the development relay")
	}
	connections := targetDials.Load()
	bulk := command("config-key set", mutation)
	if mgr {
		bulk = query
	}
	bulk.Input = input
	relay.ArmWritePause()
	canceled, stop := context.WithCancel(ctx)
	defer stop()
	finished := make(chan error, 1)
	go func() { _, err := call(canceled, bulk); finished <- err }()
	select {
	case <-relay.WriteBlocked:
	case err := <-finished:
		t.Fatal("bulk call finished before the partial-write fault", err)
	case <-ctx.Done():
		t.Fatal("partial-write fault did not activate", ctx.Err())
	}
	stop()
	select {
	case err := <-finished:
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
			t.Fatal("partial-write cancellation lost uncertainty", err)
		}
	case <-ctx.Done():
		t.Fatal("local cancellation waited for the blocked frame", ctx.Err())
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err = call(short, query)
	cancel()
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("queued untransmitted call lost its known outcome", err)
	}
	if !mgr {
		result, err := observer.MonCommand(ctx, command("config-key get", mutation))
		var missing *cephmsgr.CommandError
		if !errors.As(err, &missing) || missing.Code != -2 || result.Code != -2 {
			t.Fatal("incomplete frame already applied the mutation", err)
		}
	}

	// Different output forms make assigning a canceled or mismatched reply to
	// a live transaction observable. All eight calls share the delayed stream.
	live := make(chan error, 8)
	for i := 0; i < cap(live); i++ {
		go func(binary bool) {
			cmd := query
			if binary {
				cmd = command("config-key get", marker)
				if mgr {
					cmd.JSON = []byte(`{"prefix":"pg getmap"}`)
				}
			}
			result, err := call(ctx, cmd)
			if err == nil {
				valid := json.Valid(result.Data)
				if binary {
					valid = bytes.Equal(result.Data, input)
					if mgr {
						valid = len(result.Data) >= 16 && !json.Valid(result.Data) && bytes.ContainsRune(result.Data, 0)
					}
				}
				if !valid {
					err = fmt.Errorf("mismatched live response: binary=%t code=%d size=%d", binary, result.Code, len(result.Data))
				}
			}
			live <- err
		}(i%2 == 0)
	}
	relay.ArmReadPause()
	relay.ResumeWrite()
	select {
	case <-relay.ReadBlocked:
	case <-ctx.Done():
		t.Fatal("response-delay fault did not activate", ctx.Err())
	}
	select {
	case err := <-live:
		t.Fatal("live request finished while server receive bytes were paused", err)
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	relay.ResumeRead()
	for i := 0; i < cap(live); i++ {
		select {
		case err := <-live:
			if err != nil {
				t.Fatal("local cancellation disrupted another request", err)
			}
		case <-ctx.Done():
			t.Fatal("live requests did not complete after resuming the stream", ctx.Err())
		}
	}
	if !mgr {
		for {
			result, err := observer.MonCommand(ctx, command("config-key get", mutation))
			if err == nil {
				if !bytes.Equal(result.Data, input) {
					t.Fatal("completed canceled frame changed binary input")
				}
				break
			}
			var missing *cephmsgr.CommandError
			if !errors.As(err, &missing) || missing.Code != -2 {
				t.Fatal("independent canceled mutation query", err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("resumed canceled frame never applied", ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	if result, err := call(ctx, query); err != nil || !json.Valid(result.Data) {
		t.Fatal("subsequent call could not consume the same stream", err)
	}
	if targetDials.Load() != connections || relay.ReadChunks.Load() <= 1 || relay.WriteChunks.Load() <= 1 {
		t.Fatal("cancellation replaced the connection or bypassed stream fragmentation", connections, targetDials.Load())
	}
	c.Close()
	relay.Wait()
	t.Logf("partial-frame cancellation, known queued timeout and delayed fragmented responses preserved 8 live requests on the same connection; read chunks=%d write chunks=%d", relay.ReadChunks.Load(), relay.WriteChunks.Load())
}
