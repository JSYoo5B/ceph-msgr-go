package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// hello.write_return_data stages xattr "foo" and, given input, then fails with
// -EINVAL. With FAILOK the vector continues; native observation shows whether
// the failed method's staged xattr is committed with the later operations
// (observed: it is).
func TestCephOSDCallFailOKIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("call-failok")}, Reserve: 0})
	prepared = true
	object := owned.Objects[0]
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": object, "namespace": ""}).Placement
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: object}).State
	}
	call := func(flags cephmsgr.OSDOperationFlags) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Flags: flags, Data: []byte("x"),
			Class: cephmsgr.OSDClassInput{Class: "hello", Method: "write_return_data", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 256}}
	}
	marker := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "after"}, Data: []byte("1")}
	send := func(ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := osdObjectRequest(route, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700008000}
		return o.Request(ctx, r)
	}
	before := observe()
	_, err := send(call(0), marker)
	var server *cephmsgr.OSDError
	if !errors.As(err, &server) || server.Code != -22 {
		t.Fatal("control: the failing method without FAILOK must refuse the vector", err)
	}
	if after := observe(); after.Version != before.Version || len(xattrInventory(after.nativeXattrSnapshot)) != len(xattrInventory(before.nativeXattrSnapshot)) {
		t.Fatal("control vector changed the object")
	}
	reply, err := send(call(cephmsgr.OSDFailOK), marker)
	after := observe()
	attrs := xattrInventory(after.nativeXattrSnapshot)
	t.Logf("FAILOK: err=%v code=%d results=%v output=%q version %d->%d attrs=%v", err, reply.Code, []int32{reply.Operations[0].Result, reply.Operations[1].Result}, reply.Operations[0].Data, before.Version, after.Version, attrs)
	// Observed Tentacle behavior: the vector commits with ONDISK, the failed
	// method's per-op result and error output are preserved, and the xattr it
	// staged before failing is committed together with the later operation.
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Operations[0].Result != -22 || reply.Operations[1].Result != 0 ||
		string(reply.Operations[0].Data) != "too much input data!" || attrs["foo"] != "bar" || attrs["after"] != "1" || reply.UserVersion != after.Version {
		t.Fatal("FAILOK CALL semantics changed")
	}
}
