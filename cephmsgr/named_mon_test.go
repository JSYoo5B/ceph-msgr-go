package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type namedTestSnapshotContext struct {
	context.Context
	client *Client
	calls  atomic.Uint32
	drift  atomic.Bool
}

func (c *namedTestSnapshotContext) Err() error {
	c.calls.Add(1)
	state := c.client.Snapshot()
	if state.GlobalID != 42 || state.FSID != "01000000-0000-0000-0000-000000000000" || state.AuthRejection != nil {
		c.drift.Store(true)
	}
	return c.Context.Err()
}

func TestNamedMonitorTellPreservesPrimaryIdentityCommandsAndLogs(t *testing.T) {
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	if err := f.c.WaitMgrReady(f.ctx); err != nil {
		t.Fatal(err)
	}
	stream, err := f.c.WatchLogs(f.ctx, LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	f.c.mu.Lock()
	primary, mgr, auth := f.c.mon, f.c.mgr, f.c.auth
	f.c.mu.Unlock()
	before := f.c.Snapshot()
	ctx := &namedTestSnapshotContext{Context: f.ctx, client: f.c}
	json := []byte(" \n{\"prefix\":\"version\"}\t")
	input := []byte{0, 255, '\r', '\n'}
	for _, command := range []Command{{JSON: json, Input: input}, {JSON: []byte(`{"prefix":"deny"}`), Input: input}} {
		result, err := f.c.MonTellTo(ctx, "b", command)
		var server *CommandError
		var rejection *AuthenticationError
		var unknown *OutcomeUnknownError
		denied := bytes.Contains(command.JSON, []byte("deny"))
		if result.Message != "named status" || !bytes.Equal(result.Data, input) || errors.As(err, &rejection) || errors.As(err, &unknown) || denied && (!errors.As(err, &server) || result.Code != -13) || !denied && (err != nil || result.Code != 0) {
			t.Fatal("named tell lost raw output or command-error classification", result, err)
		}
		p := namedTestTarget(t, f)
		select {
		case initial := <-p.initialID:
			if initial != 0 || p.clientID != 84 {
				t.Fatal("directed authentication reused the primary identity or ignored its fresh assigned ID", initial, p.clientID)
			}
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
		get := p.next(t, f.ctx, 5)
		if get.Version != 1 || get.CompatVersion != 0 || len(get.Front) != 0 || len(get.Middle) != 0 || len(get.Data) != 0 || get.Transaction != 0 {
			t.Fatal("directed MON map request is not an empty MMonGetMap", get)
		}
		request := p.next(t, f.ctx, 97)
		want := append([]byte(nil), []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
		want = binary.LittleEndian.AppendUint32(want, 1)
		want = namedTestString(want, string(command.JSON))
		if request.Version != 1 || request.CompatVersion != 0 || request.Transaction == 0 || !bytes.Equal(request.Front, want) || !bytes.Equal(request.Data, input) || p.tellCount.Load() != 1 {
			t.Fatal("directed tell did not preserve independent FSID/vector/input bytes", request)
		}
		select {
		case <-p.done:
		case <-f.ctx.Done():
			t.Fatal("private session outlived completed tell", f.ctx.Err())
		}
	}
	namedTestPrimaryHealthy(t, f)
	if result, err := f.c.MgrCommand(f.ctx, Command{JSON: []byte(`{"prefix":"status"}`), Input: input}); err != nil || !bytes.Equal(result.Data, input) {
		t.Fatal("named tell changed the retained MGR", result, err)
	}
	entry := logTestRawEntry("main watch after named tell")
	f.primary.send(t, f.ctx, logTestMessage([16]byte{1}, 1, entry))
	logTestBatch(t, f.ctx, stream, 1, entry)
	f.c.mu.Lock()
	preserved := f.c.mon == primary && f.c.mgr == mgr && f.c.auth == auth
	f.c.mu.Unlock()
	if !preserved || !reflect.DeepEqual(before, f.c.Snapshot()) || f.monDials.Load() != 1 || f.namedDials.Load() != 2 || f.mgrDials.Load() != 1 || ctx.calls.Load() == 0 || ctx.drift.Load() {
		t.Fatal("named tell replaced primary state or reentrant callback was unsafe", before, f.c.Snapshot(), preserved, ctx.calls.Load())
	}
}
