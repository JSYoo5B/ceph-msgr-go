package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDMutationCloseIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	observer, _, oracle, original := osdFixture(t)
	defer func() { recoverMutationFixture(t, observer, oracle, original, "", false) }()
	baseline := nativeMutation(t, ctx, map[string]any{"compare": false})
	if baseline.Actual.Code != 0 || !bytes.Equal(baseline.Actual.Data, original) {
		t.Fatal("native baseline for Close acceptance differs")
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	faultDaemon := "osd." + strconv.Itoa(int(oracle.ID))
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, daemon, _ := testcluster.SplitPeer(endpoint)
		if daemon != faultDaemon {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	armed.Store(true)
	tail := []byte{0, 255, 128, 10, 'C', 'l', 'o', 's', 'e', 0, 254, 127}
	request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	request.ClientIncarnation = 31
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000400, Nanoseconds: 123456789}
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan completion, 1)
	// Submit exactly once. The receive fault withholds the response from
	// the Go session while native readback independently observes execution.
	go func() {
		reply, err := o.Request(ctx, request)
		finished <- completion{reply, err}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("OSD receive fault was not activated", ctx.Err())
	}
	want := append(bytes.Clone(original), tail...)
	var executed mutationOracle
	for {
		executed = nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
		if executed.Actual.Code == 0 && bytes.Equal(executed.Actual.Data, want) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the one append was not independently observed", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if executed.Actual.Version <= baseline.Actual.Version || executed.Actual.Seconds != request.ModificationTime.Seconds || executed.Actual.Nanos != request.ModificationTime.Nanoseconds {
		t.Fatal("native execution lacked the append version or explicit mtime")
	}
	select {
	case result := <-finished:
		t.Fatalf("mutation completed before Close despite blocked receive: %v", result.err)
	default:
	}
	// Keep the caller context live: this exercises handle shutdown, rather
	// than cancellation of the operation's caller-owned wait.
	if err := o.Close(); err != nil {
		t.Fatal("close OSD with a sent mutation", err)
	}
	var result completion
	select {
	case result = <-finished:
	case <-ctx.Done():
		t.Fatal("Close did not release the pending mutation", ctx.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	submission := result.reply.Submission
	if ctx.Err() != nil || !errors.As(result.err, &unknown) || !errors.Is(result.err, cephmsgr.ErrClosed) || submission.Transaction == 0 || submission.GlobalID != c.Snapshot().GlobalID || submission.ClientIncarnation != 31 || submission.MapEpoch != request.MapEpoch || submission.Attempt != 0 || result.reply.Transaction != 0 || result.reply.GlobalID != 0 || result.reply.Flags != 0 || len(result.reply.Operations) != 0 || len(result.reply.RawFront) != 0 || len(result.reply.RawData) != 0 {
		t.Fatalf("Close lost unknown outcome/local identity: tid=%d gid=%d epoch=%d attempt=%d error=%v caller=%v", submission.Transaction, submission.GlobalID, submission.MapEpoch, submission.Attempt, result.err, ctx.Err())
	}
	if reply, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 1})); !errors.Is(err, cephmsgr.ErrClosed) || reply.Submission.Transaction != 0 {
		t.Fatal("closed handle admitted another request", err)
	}
	armed.Store(false)
	oracle = adoptMutationPlacement(t, oracle, executed.Placement)
	publishReplicaOSDRouting(t, os.Getenv("CEPH_MSGR_CONTROL_DIR"), oracle)
	fresh := mutationConnection(t, c, ctx, oracle)
	defer fresh.Close()
	read, err := fresh.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)}))
	if err != nil {
		t.Fatal("fresh caller-selected OSD read after Close", err)
	}
	checkOSDMetadata(t, read, oracle)
	if read.Transaction <= submission.Transaction || read.UserVersion != executed.Actual.Version || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, want) {
		t.Fatal("fresh session changed request identity or independently observed object")
	}
	final := nativeMutation(t, ctx, map[string]any{"compare": false})
	if final.Actual.Code != 0 || final.Actual.Version != executed.Actual.Version || !bytes.Equal(final.Actual.Data, want) || final.Actual.Seconds != executed.Actual.Seconds || final.Actual.Nanos != executed.Actual.Nanos {
		t.Fatal("Close/reopen replayed the append or changed native object metadata")
	}
	t.Log("native readback proved one append before handle Close; live caller received OutcomeUnknown+ErrClosed with identity; fresh explicit session preserved native bytes/version without replay")
}
