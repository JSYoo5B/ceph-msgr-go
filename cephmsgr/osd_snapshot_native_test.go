package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func nativeSnapRequest(ops ...OSDOperation) OSDRequest {
	return OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
		Object: OSDObject{Pool: 7, Name: "snap-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: ops}
}

func nativeSnapWrite(r OSDRequest) OSDRequest {
	r.ModificationTime = OSDTime{Seconds: 1739999999, Nanoseconds: 5}
	r.SnapContext = OSDSnapContext{Sequence: 7, Snapshots: []uint64{7, 4, 2}}
	return r
}

// Each public request must submit the fresh native front/data; the matching
// native reply then completes it with the same identity, results and outputs.
func TestOSDSnapshotPublicRequestsMatchFreshNativePayloads(t *testing.T) {
	read := nativeSnapRequest(OSDOperation{Code: OSDRead, Length: 4}, OSDOperation{Code: OSDStat})
	read.ReadSnapshot = 4
	ordered := nativeSnapWrite(nativeSnapRequest(OSDOperation{Code: OSDWriteFull, Length: 2, Data: []byte("v2")}))
	ordered.OrderSnapshots = true
	for _, tc := range []struct {
		request, reply string
		input          OSDRequest
		code           int32
		check          func(*testing.T, OSDReply)
	}{
		{"snap-native-request-v6-read", "snap-native-reply-v6-read", read, 0, func(t *testing.T, r OSDReply) {
			if string(r.Operations[0].Data) != "v1v1" || len(r.Operations[1].Data) != 16 || r.UserVersion != 3 {
				t.Fatal("native read-at-snapshot reply changed", r.Operations)
			}
		}},
		{"snap-native-request-v6-write", "snap-native-reply-v6-eoldsnapc", ordered, OSDOldSnapContext, func(t *testing.T, r OSDReply) {
			if r.UserVersion != 0 || r.Operations[0].Result != 0 || r.Flags&uint64(osd.FlagAck) != 0 {
				t.Fatal("native EOLDSNAPC refusal changed", r)
			}
		}},
		{"snap-native-request-v6-rollback", "snap-native-reply-v6-rollback", nativeSnapWrite(nativeSnapRequest(OSDOperation{Code: OSDRollback, Snapshot: 4})), 0, func(t *testing.T, r OSDReply) {
			if r.Operations[0].Snapshot != 4 || r.UserVersion != 21 || r.Flags&uint64(osd.FlagOnDisk) == 0 {
				t.Fatal("native ROLLBACK completion changed", r)
			}
		}},
		{"snap-native-request-v6-list", "snap-native-reply-v6-list", nativeSnapRequest(OSDOperation{Code: OSDListSnapshots, ListSnapshots: OSDListSnapshotsInput{MaxOutputBytes: 512}}), 0, func(t *testing.T, r OSDReply) {
			l := r.Operations[0].Snapshots
			if l.Sequence != 7 || len(l.Clones) != 3 || l.Clones[1].ID != 7 || len(l.Clones[1].Overlap) != 2 || l.Clones[1].Overlap[1] != (OSDExtent{4, 4}) ||
				l.Clones[2].ID != OSDHeadSnapshot || l.Clones[2].Size != 11 || len(r.Operations[0].Data) != 180 {
				t.Fatalf("native listing not decoded: %+v", l)
			}
		}},
	} {
		t.Run(tc.request, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, tc.input); done <- response{reply, err} }()
			request := peer.next(t)
			if !bytes.Equal(request.Front, nativeAllocHintFixture(t, tc.request, "front")) || !bytes.Equal(request.Data, nativeAllocHintFixture(t, tc.request, "data")) {
				t.Fatalf("public snapshot request differs from fresh native front/data\nfront %x", request.Front)
			}
			front, data := nativeAllocHintFixture(t, tc.reply, "front"), nativeAllocHintFixture(t, tc.reply, "data")
			_ = osdMapsWrite(peer, msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Transaction: request.Transaction, Front: front, Data: data})
			got := <-done
			var server *OSDError
			if tc.code < 0 && (!errors.As(got.err, &server) || server.Code != tc.code) || tc.code == 0 && got.err != nil {
				t.Fatal("native snapshot reply outcome changed", got.err)
			}
			if got.reply.Code != tc.code || got.reply.Transaction != request.Transaction || !bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) {
				t.Fatal("native snapshot reply identity or raw payload changed", got.reply)
			}
			tc.check(t, got.reply)
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("native snapshot reply invalidated or reconnected the session")
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed a snapshot request", replay.Transaction)
			default:
			}
		})
	}
}

// Product validation stays strict against the same native replies.
func TestOSDSnapshotNativeRepliesRejectContradictoryRequests(t *testing.T) {
	decode := func(name string) osd.Reply {
		m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Front: nativeAllocHintFixture(t, name, "front"), Data: nativeAllocHintFixture(t, name, "data")}
		r, err := osd.DecodeReply(m, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	request := func(ops ...osd.Operation) osd.Request {
		return osd.Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "snap-codec",
			Locator: osd.Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, MTimeSeconds: 1739999999, Operations: ops}
	}
	rollback := decode("snap-native-reply-v6-rollback")
	if err := validateOSDReply(request(osd.Operation{Code: osd.Rollback, Snap: 4}), rollback, false); err != nil {
		t.Fatal("native ROLLBACK reply rejected for its own request", err)
	}
	if err := validateOSDReply(request(osd.Operation{Code: osd.Rollback, Snap: 2}), rollback, false); err == nil {
		t.Fatal("native ROLLBACK echo accepted for another snap id")
	}
	rollback.Flags &^= osd.FlagOnDisk
	if err := validateOSDReply(request(osd.Operation{Code: osd.Rollback, Snap: 4}), rollback, false); err == nil {
		t.Fatal("ROLLBACK success without ONDISK accepted")
	}
	list := decode("snap-native-reply-v6-list")
	listing := osd.Operation{Code: osd.ListSnaps, ListSnaps: osd.ListSnapsInput{MaxOutputBytes: 180}}
	listRequest := request(listing)
	listRequest.MTimeSeconds = 0
	if err := validateOSDReply(listRequest, list, false); err != nil {
		t.Fatal("native listing rejected at its exact budget", err)
	}
	listRequest.Operations[0].ListSnaps.MaxOutputBytes = 179
	if err := validateOSDReply(listRequest, list, false); err == nil {
		t.Fatal("native listing accepted above the local budget")
	}
}
