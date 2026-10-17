package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A COPY_FROM whose reply never reaches the client has executed on the daemon.
// The local wait ends with an unknown outcome, the request identity survives,
// and a fresh session neither replays the copy nor observes a second one.
func TestCephOSDCopyFromLostReplyIntegration(t *testing.T) {
	observer, ctx, base, _ := osdFixture(t)
	_ = observer
	ctx, cancelAll := context.WithTimeout(ctx, 240*time.Second)
	defer cancelAll()
	baseline := []byte{0, 0xff, 'l', 'o', 's', 't', 10, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	attrs := []nativeXattrEntry{{Name: []byte("lost-attr"), Value: []byte{0xff, 0, 'L'}}}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned copy-from objects were not removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: attrs}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("native copy source seed differs", owned.SeedCode, owned.Objects)
	}
	src, dst := owned.Objects[0], owned.Objects[1]
	target := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": dst, "namespace": ""}).Placement
	if target.Name != dst || target.Pool != base.Pool || target.ID < 0 {
		t.Fatal("native destination placement differs", target)
	}

	options := integrationOptions(t)
	options.EnableOSD = true
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, kind, _ := testcluster.SplitPeer(endpoint)
		if kind != "osd."+strconv.Itoa(int(target.ID)) {
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
	o := mutationConnection(t, c, ctx, target)
	armed.Store(true)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan result, 1)
	op := cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src}}
	go func() {
		r, e := o.Request(callCtx, osdObjectRequest(target, op))
		finished <- result{r, e}
	}()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault not activated", ctx.Err())
	}
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: dst}).State
	}
	var copied omapWriteState
	for {
		copied = observe()
		if copied.Code == 0 && bytes.Equal(copied.Data, baseline) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("copy not visible", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if len(copied.Attributes) != 1 || !bytes.Equal(copied.Attributes[0].Value, attrs[0].Value) {
		t.Fatal("native destination attributes differ after the lost reply", copied.Attributes)
	}
	cancel()
	got := <-finished
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction == 0 || got.reply.Submission.GlobalID == 0 || got.reply.Submission.MapEpoch != target.Epoch || got.reply.Transaction != 0 {
		t.Fatalf("lost response metadata: %+v %v", got.reply, got.err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	armed.Store(false)
	o = mutationConnection(t, c, ctx, target)
	read, err := o.Request(ctx, osdObjectRequest(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(baseline) + 32)}))
	if err != nil || read.Transaction <= got.reply.Submission.Transaction || !bytes.Equal(read.Operations[0].Data, baseline) {
		t.Fatal("fresh session identity/readback", err)
	}
	time.Sleep(500 * time.Millisecond)
	after := observe()
	if after.Version != copied.Version || after.Seconds != copied.Seconds || after.Nanos != copied.Nanos || !bytes.Equal(after.Data, baseline) {
		t.Fatal("copy was replayed or altered", copied.Version, after.Version)
	}
	t.Log("native observation proved one copy; local cancellation kept the request identity and the fresh session did not replay")
}
