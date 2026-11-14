package cephmsgr

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// sentTellCancellationSelector exclusively selects the cell: it ends MON and
// MGR sessions on purpose, so broad selections skip it.
const sentTellCancellationSelector = "^TestCephSentTellCancellationIntegration$"

// sentTellVersionCommand is the read-only daemon-local command of the cell.
// Padding makes its one local write recognizable without decoding a frame.
func sentTellVersionCommand(padding int) Command {
	return Command{JSON: []byte(`{"prefix":"version","format":"json"}` + strings.Repeat(" ", padding))}
}

// A Tell whose command was completely written and whose reply never reaches
// the caller, ended by the caller: once by canceling the call's context and
// once by Client.Close. Each of MonTell, MonTellTo, MgrTell and MgrTellTo must
// return OutcomeUnknownError with that cause and must not transmit the command
// again. The command is read-only, so no daemon state is compared; "sent"
// means that the one padded local write returned and that bytes then arrived
// on its connection and were withheld. Nothing is decoded, so the cell runs in
// CRC and secure modes alike and attributes no byte to a reply.
func TestCephSentTellCancellationIntegration(t *testing.T) {
	if selected := flag.Lookup("test.run"); selected == nil || selected.Value.String() != sentTellCancellationSelector {
		t.Skip("sent Tell cancellation requires its exclusive selector " + sentTellCancellationSelector)
	}
	if os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" {
		t.Skip("requires the ordinary two-MGR disposable fixture")
	}
	options := integrationOptions(t)
	observer, ctx := fixtureClient(t, 3*time.Minute)
	if _, err := observer.MgrTell(ctx, sentTellVersionCommand(0)); err != nil {
		t.Fatal("read the current active MGR", err)
	}
	observer.mu.Lock()
	manager := observer.mgrMap.Name
	observer.mu.Unlock()
	if !mgrTellTargets[manager] {
		t.Fatal("the active MGR is not an owned fixture daemon", manager)
	}
	const monitor = "b"
	apis := []struct {
		name string
		// receiver reports whether a fixture role may carry the command.
		receiver func(role string) bool
		call     func(context.Context, *Client, Command) (Result, error)
	}{
		{"MonTell", func(role string) bool { return strings.HasPrefix(role, "mon.") },
			func(ctx context.Context, c *Client, command Command) (Result, error) { return c.MonTell(ctx, command) }},
		{"MonTellTo", func(role string) bool { return role == "mon."+monitor },
			func(ctx context.Context, c *Client, command Command) (Result, error) {
				return c.MonTellTo(ctx, monitor, command)
			}},
		{"MgrTell", func(role string) bool { return role == "mgr."+manager },
			func(ctx context.Context, c *Client, command Command) (Result, error) { return c.MgrTell(ctx, command) }},
		{"MgrTellTo", func(role string) bool { return role == "mgr."+manager },
			func(ctx context.Context, c *Client, command Command) (Result, error) {
				return c.MgrTellTo(ctx, manager, command)
			}},
	}
	for _, api := range apis {
		for _, ending := range []string{"cancel", "close"} {
			t.Run(api.name+"_"+ending, func(t *testing.T) {
				subject := options
				dial := subject.DialContext
				var writes atomic.Int32
				var connectionID atomic.Uint64
				// Only a connection to a daemon that may carry this API's command
				// can bind the gate.
				gate := newPaddedReplyGate(4096, api.receiver)
				subject.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
					conn, err := dial(ctx, network, endpoint)
					if err != nil {
						return nil, err
					}
					_, peer, _ := testcluster.SplitPeer(endpoint)
					return &commandWriteProbe{Conn: gate.wrap(conn, connectionID.Add(1), peer), writes: &writes}, nil
				}
				c, err := Dial(ctx, subject)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				// Control: the unpadded command through the same API is answered,
				// and no write so far reached the padded size.
				if result, err := api.call(ctx, c, sentTellVersionCommand(0)); err != nil || result.Code != 0 || !strings.Contains(string(result.Data), "version") {
					t.Fatal("read-only control Tell", err, result.Code, len(result.Data))
				}
				if _, _, bound := gate.binding(); bound || writes.Load() != 0 {
					t.Fatal("a write before the subject command reached the padded size", bound, writes.Load())
				}
				call, stop := context.WithCancel(ctx)
				defer stop()
				type outcome struct {
					result Result
					err    error
				}
				finished := make(chan outcome, 1)
				go func() {
					result, err := api.call(call, c, sentTellVersionCommand(40<<10))
					finished <- outcome{result, err}
				}()
				// Sent: the padded write was issued, it returned, and bytes that
				// arrived on its connection afterwards were withheld.
				for _, signal := range []struct {
					name string
					done <-chan struct{}
				}{{"issued", gate.armed}, {"followed by withheld received bytes", gate.received}} {
					select {
					case <-signal.done:
					case got := <-finished:
						t.Fatal("the padded Tell ended before it was "+signal.name, got.err)
					case <-ctx.Done():
						t.Fatal("the padded Tell was never "+signal.name, ctx.Err())
					}
				}
				connection, receiver, _ := gate.binding()
				if count := awaitPaddedWrites(t, ctx, &writes, 1); count != 1 || !api.receiver(receiver) {
					t.Fatal("the padded Tell was not written exactly once to its target", count, receiver)
				}
				select {
				case got := <-finished:
					t.Fatal("the caller ended while every received byte was withheld", got.err)
				default:
				}
				want := context.Canceled
				if ending == "cancel" {
					stop()
				} else {
					want = ErrClosed
					if err := c.Close(); err != nil {
						t.Fatal("Close with a sent Tell outstanding", err)
					}
				}
				select {
				case got := <-finished:
					var unknown *OutcomeUnknownError
					if !errors.As(got.err, &unknown) || !errors.Is(got.err, want) {
						t.Fatalf("a sent %s ended by %s lost its unknown outcome or its cause: %v", api.name, ending, got.err)
					}
					if got.result.Code != 0 || got.result.Message != "" || len(got.result.Data) != 0 {
						t.Fatal("an unanswered Tell gained an acknowledged server result", got.result)
					}
					t.Logf("%s to %s on connection %d, ended by %s after its one padded write: %v", api.name, receiver, connection, ending, got.err)
				case <-ctx.Done():
					t.Fatal("ending the caller did not release the sent Tell", ctx.Err())
				}
				read := func() (Result, error) { return api.call(ctx, c, sentTellVersionCommand(0)) }
				answered := func(result Result, err error) bool {
					return err == nil && result.Code == 0 && strings.Contains(string(result.Data), "version")
				}
				if ending == "cancel" {
					// The withheld connection cannot deliver another reply. End
					// it. A session that still owns it learns of the loss from
					// its next use, so the first new invocation may fail on the
					// ended connection, as uncertain or as a transport error
					// without a result. The invocation after such a failure
					// must be answered.
					if err := gate.closeHeld(); err != nil {
						t.Fatal("close the withheld connection", err)
					}
					result, err := read()
					if !answered(result, err) {
						if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) || result.Code != 0 || len(result.Data) != 0 {
							t.Fatal("the invocation that met the ended connection returned a result or another cause", err, result.Code, len(result.Data))
						}
						t.Log("the first new invocation met the ended connection:", err)
						if result, err = read(); !answered(result, err) {
							t.Fatal("a new invocation after the canceled Tell", err, result.Code, len(result.Data))
						}
					}
				} else if result, err := read(); !errors.Is(err, ErrClosed) || errors.As(err, new(*OutcomeUnknownError)) || len(result.Data) != 0 {
					t.Fatal("an invocation on the closed client was not a known-unsent refusal", err)
				}
				// Nothing transmitted the padded command again, and the gate still
				// holds only its connection.
				if held, _, _ := gate.binding(); writes.Load() != 1 || held != connection {
					t.Fatal("the padded Tell was written again or the gate was rebound", writes.Load(), held, connection)
				}
			})
		}
	}
}
