package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A raw OSD command and a PG command to an OSD that was SIGKILLed have one of
// two classifications, and which one depends only on whether the client's
// session had already recorded the end of its connection when the command
// was issued. The test puts one session on each side on purpose and asserts
// both; it does not wait for either to happen by itself.
//
//   - Session already ended: the client closed the OSD connection, which it
//     does only after recording the terminal cause. The command is refused
//     before it is submitted: an error that is neither an unknown outcome nor
//     ErrClosed, with no transaction.
//   - Session still unaware: from before the kill, everything the client would
//     receive on the OSD connection is parked, the end of the peer included,
//     and its keepalive is too slow to probe. The command is admitted and its
//     transmission starts, so it ends as an unknown outcome with its
//     transaction, never with a result.
//
// Neither side invents a result, and the old handle never recovers by itself.
// After the daemon restarts, a new connection to its new address answers.
func TestCephOSDCommandCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon restart needs the BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	version := cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}
	pg := cephmsgr.OSDCommandPG{Pool: uint64(base.Pool), Seed: base.PG, Primary: base.ID, MapEpoch: base.Epoch}
	query := cephmsgr.Command{JSON: []byte(fmt.Sprintf(`{"prefix":"query","pgid":%q}`, base.PGID))}
	forms := []string{"Command", "PGCommand"}
	call := func(ctx context.Context, o *cephmsgr.OSDConnection, form string) (cephmsgr.OSDCommandResult, error) {
		if form == "PGCommand" {
			return o.PGCommand(ctx, pg, query)
		}
		return o.Command(ctx, version)
	}
	// The commands of the unaware side are padded, so that their one local
	// write is told apart from acknowledgements without decoding a frame.
	// A little over 8 KiB fits the socket buffer of a peer that is gone.
	padded := func(ctx context.Context, o *cephmsgr.OSDConnection, form string) (cephmsgr.OSDCommandResult, error) {
		padding := strings.Repeat(" ", 8<<10)
		if form == "PGCommand" {
			return o.PGCommand(ctx, pg, cephmsgr.Command{JSON: append(append([]byte(nil), query.JSON...), padding...)})
		}
		return o.Command(ctx, cephmsgr.Command{JSON: append(append([]byte(nil), version.JSON...), padding...)})
	}
	o := mutationConnection(t, c, ctx, base)
	first, err := o.Command(ctx, version)
	if err != nil || first.Code != 0 || len(first.Data) == 0 {
		t.Fatal("command before the crash", err)
	}
	o.Close()
	// One client per side. ended only reports the close of its OSD
	// connection; unaware (one per command form, since its session serves a
	// single command) parks every read from before the kill.
	type side struct {
		hold   *commandHold
		handle *cephmsgr.OSDConnection
	}
	open := func(configure func(*cephmsgr.Options)) side {
		t.Helper()
		hold := newCommandHold(4096, daemonEndpoint(base.Address))
		options := integrationOptions(t)
		options.EnableOSD = true
		options.DialContext = hold.dialer(options.DialContext)
		configure(&options)
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		handle := mutationConnection(t, client, ctx, base)
		for _, form := range forms {
			if result, err := call(ctx, handle, form); err != nil || result.Code != 0 || len(result.Data) == 0 {
				t.Fatal(form, "before the crash", err)
			}
		}
		return side{hold, handle}
	}
	// A prompt keepalive bounds the wait should the kill not end the
	// connection by itself.
	ended := open(func(options *cephmsgr.Options) {
		options.KeepaliveInterval, options.KeepaliveTimeout = time.Second, 3*time.Second
	})
	unaware := map[string]side{}
	for _, form := range forms {
		unaware[form] = open(func(options *cephmsgr.Options) {
			options.KeepaliveInterval, options.KeepaliveTimeout = 10*time.Minute, 11*time.Minute
		})
		unaware[form].hold.holdNow()
	}
	name := strconv.FormatInt(int64(base.ID), 10)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		if !restarted {
			recoverMutationFixture(t, c, base, original, name, true)
		}
	}()
	select {
	case <-ended.hold.closed:
	case <-ctx.Done():
		t.Fatal("the session of the killed OSD never ended", ctx.Err())
	}
	// The error the transport of the ended session was lost with, as its
	// connection returned it before the client closed it; nil when the
	// keepalive ended the session instead.
	loss := ended.hold.transportLoss()
	t.Logf("the ended session's connection to the killed OSD was lost with: %v", loss)
	for round := 0; round < 2; round++ {
		for _, form := range forms {
			result, err := call(ctx, ended.handle, form)
			if !knownUnsent(result.Result, err) || errors.Is(err, cephmsgr.ErrClosed) || result.Submission.Transaction != 0 {
				t.Fatalf("%s after its session had ended was not a known-unsent refusal: %+v %v", form, result.Submission, err)
			}
			if !endedWithSessionCause(err, loss) {
				t.Fatalf("%s after its session had ended lost the session's terminal cause %v: %v", form, loss, err)
			}
			if round == 0 {
				t.Logf("%s on the ended session of the killed OSD: known unsent: %v", form, err)
			}
		}
	}
	for _, form := range forms {
		type outcome struct {
			result cephmsgr.OSDCommandResult
			err    error
		}
		hold := unaware[form].hold
		finished := make(chan outcome, 1)
		go func() {
			result, err := padded(ctx, unaware[form].handle, form)
			finished <- outcome{result, err}
		}()
		// The padded command's local write returned, with or without an
		// error. Only then may the session learn that its peer is gone.
		var got *outcome
		select {
		case <-hold.wrote:
		case early := <-finished:
			got = &early
		case <-ctx.Done():
			t.Fatal(form, "to the killed OSD was never written", ctx.Err())
		}
		select {
		case <-hold.closed:
			t.Log(form, "on the unaware session: the local write failed and ended the session")
		default:
			t.Log(form, "on the unaware session: the local write returned; releasing the parked end of the peer")
		}
		hold.releaseHeld()
		if got == nil {
			select {
			case late := <-finished:
				got = &late
			case <-ctx.Done():
				t.Fatal(form, "to the killed OSD never ended", ctx.Err())
			}
		}
		submission := got.result.Submission
		_, large, _ := hold.counts()
		if !unknownOutcome(got.result.Result, got.err) || errors.Is(got.err, cephmsgr.ErrClosed) || submission.Transaction == 0 ||
			(form == "PGCommand") != (submission.PG != nil) || large != 1 {
			t.Fatalf("%s admitted on a session unaware of the kill was not an unknown outcome with its transaction: %+v %v", form, submission, got.err)
		}
		t.Logf("%s on the unaware session of the killed OSD: unknown outcome, transaction %d: %v", form, submission.Transaction, got.err)
		// The handle does not recover by itself.
		if result, err := call(ctx, unaware[form].handle, form); !knownUnsent(result.Result, err) || result.Submission.Transaction != 0 {
			t.Fatalf("%s on the failed handle was not refused: %+v %v", form, result.Submission, err)
		}
	}
	recoverMutationFixture(t, c, base, original, name, true)
	restarted = true
	fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
	if fresh.Address == base.Address {
		t.Fatal("the restarted OSD kept its address nonce")
	}
	// The old handles stay failed after the restart.
	for _, handle := range []*cephmsgr.OSDConnection{ended.handle, unaware["Command"].handle} {
		if result, err := handle.Command(ctx, version); !knownUnsent(result.Result, err) {
			t.Fatal("an old connection silently recovered", err)
		}
	}
	n := mutationConnection(t, c, ctx, fresh)
	defer n.Close()
	again, err := n.Command(ctx, version)
	if err != nil || again.Code != 0 || string(again.Data) != string(first.Data) || again.Submission.Transaction <= first.Submission.Transaction {
		t.Fatal("command on a new connection after restart", err)
	}
}

// A long OSD command (bench overwriting 1 GiB in 4 KiB blocks) is in flight
// when its OSD is SIGKILLed, and in a second round when it is stopped (SIGTERM,
// docker stop). A SIGKILL ends the call as an unknown outcome carrying its
// transaction, never with a result. A stop must not report success either; a
// known error the daemon sends while shutting down is accepted and logged.
// Nothing is resent to the restarted daemon, and a new connection answers.
func TestCephOSDCommandCrashInFlightIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon restart needs the BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	current := base
	for _, action := range []string{"kill", "stop"} {
		o := mutationConnection(t, c, ctx, current)
		// 262144 overwrites of 4 KiB inside one 4 MiB object (no device
		// growth). 4 KiB blocks are bounded by osd_bench_small_size_max_iops
		// (100 by default, 12 MiB per bench). The limit is raised in the
		// target daemon's runtime configuration only; the restart drops it.
		if r, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"config set","var":"osd_bench_small_size_max_iops","val":["100000"]}`)}); err != nil || r.Code != 0 {
			t.Fatal("raise the bench limit", err)
		}
		bench := cephmsgr.Command{JSON: []byte(`{"prefix":"bench","count":1073741824,"size":4096,"object_size":4194304,"object_num":1}`)}
		type outcome struct {
			result cephmsgr.OSDCommandResult
			err    error
		}
		done := make(chan outcome, 1)
		started := time.Now()
		go func() {
			call, stop := context.WithTimeout(ctx, 120*time.Second)
			defer stop()
			result, err := o.Command(call, bench)
			done <- outcome{result, err}
		}()
		select {
		case r := <-done:
			t.Fatalf("%s: bench finished in %s before the daemon action: code %d %v", action, time.Since(started), r.result.Code, r.err)
		case <-time.After(500 * time.Millisecond):
		}
		name := strconv.FormatInt(int64(current.ID), 10)
		if err := testcluster.ControlDaemon(ctx, dir, action, "osd", name); err != nil {
			t.Fatal(action, err)
		}
		restarted := false
		restore := func() {
			o.Close()
			recoverMutationFixture(t, c, current, original, name, true)
			restarted = true
		}
		defer func() {
			if !restarted {
				restore()
			}
		}()
		r := <-done
		var unknown *cephmsgr.OutcomeUnknownError
		var refused *cephmsgr.CommandError
		switch {
		case errors.As(r.err, &unknown) && r.result.Code == 0 && len(r.result.Data) == 0 && r.result.Submission.Transaction != 0:
			t.Logf("%s: bench in flight for %s ended unknown (transaction %d): %v", action, time.Since(started).Round(time.Millisecond), r.result.Submission.Transaction, r.err)
		case action == "stop" && errors.As(r.err, &refused) && refused.Code < 0:
			t.Logf("%s: bench in flight for %s was refused by the stopping daemon: code %d %q", action, time.Since(started).Round(time.Millisecond), refused.Code, r.result.Message)
		default:
			t.Fatalf("%s: a command in flight must not report a result: code %d, transaction %d: %v", action, r.result.Code, r.result.Submission.Transaction, r.err)
		}
		restore()
		fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
		current = adoptMutationPlacement(t, current, fresh)
		n := mutationConnection(t, c, ctx, current)
		if again, err := n.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || again.Code != 0 {
			t.Fatal(action, "command on a new connection after restart", err)
		}
		n.Close()
	}
}
