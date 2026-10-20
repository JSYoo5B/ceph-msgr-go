package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestOSDSnapshotInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	write := []OSDOperation{{Code: OSDWrite, Length: 1, Data: []byte{1}}}
	rollback := []OSDOperation{{Code: OSDRollback, Snapshot: 4}}
	list := []OSDOperation{{Code: OSDListSnapshots, ListSnapshots: OSDListSnapshotsInput{MaxOutputBytes: 256}}}
	for _, tc := range []struct {
		name string
		edit func(*OSDRequest)
		want error
	}{
		{"write at a snapshot", func(r *OSDRequest) { r.Operations, r.ReadSnapshot = write, 4 }, osd.ErrRequest},
		{"SnapContext on a read", func(r *OSDRequest) { r.SnapContext = OSDSnapContext{Sequence: 2, Snapshots: []uint64{2}} }, osd.ErrRequest},
		{"ORDERSNAP on a read", func(r *OSDRequest) { r.OrderSnapshots = true }, osd.ErrRequest},
		{"unordered snapshots", func(r *OSDRequest) {
			r.Operations, r.SnapContext = write, OSDSnapContext{Sequence: 5, Snapshots: []uint64{2, 5}}
		}, osd.ErrRequest},
		{"rollback beside a guard", func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDAssertVersion, Version: 1}, rollback[0]}
		}, ErrUnsupportedOSDOperation},
		{"rollback to snap 0", func(r *OSDRequest) { r.Operations = []OSDOperation{{Code: OSDRollback}} }, osd.ErrRequest},
		{"listing at a snapshot", func(r *OSDRequest) { r.Operations, r.ReadSnapshot = list, 3 }, osd.ErrRequest},
		{"listing beside STAT", func(r *OSDRequest) { r.Operations = append([]OSDOperation{{Code: OSDStat}}, list...) }, ErrUnsupportedOSDOperation},
		{"read at SNAPDIR", func(r *OSDRequest) { r.ReadSnapshot = ^uint64(0) }, osd.ErrRequest},
	} {
		input := osdValidationRequest()
		tc.edit(&input)
		if input.Operations[0].Code == OSDWrite || input.Operations[0].Code == OSDRollback {
			input.ModificationTime = OSDTime{Seconds: 1}
		}
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if !errors.Is(err, tc.want) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s reached submission or lost cause: %v", tc.name, err)
		}
		select {
		case m := <-peer.requests:
			t.Fatalf("%s was transmitted as %d", tc.name, m.Transaction)
		default:
		}
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local snapshot rejection failed/reconnected the session")
	}
}

func osdSnapListBytes(clones []OSDClone, seq uint64) []byte {
	body := wire.Encoder{}
	body.U32(uint32(len(clones)))
	for _, c := range clones {
		entry := wire.Encoder{}
		entry.U64(c.ID)
		entry.U32(uint32(len(c.Snapshots)))
		for _, s := range c.Snapshots {
			entry.U64(s)
		}
		entry.U32(uint32(len(c.Overlap)))
		for _, o := range c.Overlap {
			entry.U64(o.Offset)
			entry.U64(o.Length)
		}
		entry.U64(c.Size)
		body.Struct(1, 1, entry.Data)
	}
	body.U64(seq)
	out := wire.Encoder{}
	out.Struct(2, 1, body.Data)
	return out.Data
}

func TestOSDSnapshotSentRequestsPreserveContextAndResults(t *testing.T) {
	ondisk := uint64(osd.FlagOnDisk | osd.FlagAck)
	listing := osdSnapListBytes([]OSDClone{{ID: 2, Snapshots: []uint64{2}, Overlap: []OSDExtent{{0, 3}}, Size: 3}, {ID: OSDHeadSnapshot, Size: 9}}, 2)
	rollback := OSDOperation{Code: OSDRollback, Snapshot: 4}
	list := OSDOperation{Code: OSDListSnapshots, ListSnapshots: OSDListSnapshotsInput{MaxOutputBytes: 256}}
	for _, tc := range []struct {
		name    string
		input   func(*OSDRequest)
		reply   OSDOperation // synthetic union: Offset carries the ROLLBACK snap id
		output  []byte
		code    int32
		flags   uint64
		snap    uint64
		invalid bool
	}{
		{name: "read at a snapshot", input: func(r *OSDRequest) { r.ReadSnapshot = 4; r.Operations = []OSDOperation{{Code: OSDStat}} },
			reply: OSDOperation{Code: OSDStat}, output: make([]byte, 16), flags: ondisk, snap: 4},
		{name: "ordered write refused as old", input: func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDWriteFull, Length: 2, Data: []byte("v2")}}
			r.SnapContext, r.OrderSnapshots = OSDSnapContext{Sequence: 3, Snapshots: []uint64{3}}, true
		}, reply: OSDOperation{Code: OSDWriteFull, Length: 2}, code: OSDOldSnapContext, flags: uint64(osd.FlagOnDisk), snap: osd.NoSnap},
		{name: "rollback committed", input: func(r *OSDRequest) {
			r.Operations, r.SnapContext = []OSDOperation{rollback}, OSDSnapContext{Sequence: 7, Snapshots: []uint64{7, 4}}
		}, reply: OSDOperation{Code: OSDRollback, Offset: 4}, flags: ondisk, snap: osd.NoSnap},
		{name: "rollback echo changed", input: func(r *OSDRequest) { r.Operations = []OSDOperation{rollback} },
			reply: OSDOperation{Code: OSDRollback, Offset: 5}, flags: ondisk, snap: osd.NoSnap, invalid: true},
		{name: "rollback without ONDISK", input: func(r *OSDRequest) { r.Operations = []OSDOperation{rollback} },
			reply: OSDOperation{Code: OSDRollback, Offset: 4}, flags: uint64(osd.FlagAck), snap: osd.NoSnap, invalid: true},
		{name: "snapshot listing", input: func(r *OSDRequest) { r.Operations = []OSDOperation{list} },
			reply: OSDOperation{Code: OSDListSnapshots}, output: listing, flags: ondisk, snap: osd.SnapDir},
		{name: "listing above budget", input: func(r *OSDRequest) {
			r.Operations = []OSDOperation{{Code: OSDListSnapshots, ListSnapshots: OSDListSnapshotsInput{MaxOutputBytes: uint32(len(listing) - 1)}}}
		}, reply: OSDOperation{Code: OSDListSnapshots}, output: listing, flags: ondisk, snap: osd.SnapDir, invalid: true},
		{name: "malformed listing", input: func(r *OSDRequest) { r.Operations = []OSDOperation{list} },
			reply: OSDOperation{Code: OSDListSnapshots}, output: listing[:len(listing)-1], flags: ondisk, snap: osd.SnapDir, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			tc.input(&input)
			if input.Operations[0].Code != OSDStat && input.Operations[0].Code != OSDListSnapshots {
				input.ModificationTime = OSDTime{Seconds: 1700000000}
			}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); done <- response{reply, err} }()
			m := peer.next(t)
			// hobj.snap follows the 38-byte operations of the request.
			tail := 113 + 38*len(input.Operations)
			if got := binary.LittleEndian.Uint64(m.Front[tail : tail+8]); got != tc.snap {
				t.Fatalf("transmitted hobj.snap %x, want %x", got, tc.snap)
			}
			if input.OrderSnapshots != (binary.LittleEndian.Uint32(m.Front[8:12])&uint32(osd.FlagOrderSnap) != 0) {
				t.Fatal("ORDERSNAP flag differs from the request")
			}
			if seq := binary.LittleEndian.Uint64(m.Front[tail+8 : tail+16]); seq != input.SnapContext.Sequence {
				t.Fatal("snap_seq differs", seq)
			}
			reply := osdXattrSyntheticReply(t, []OSDOperation{tc.reply}, [][]byte{tc.output}, []int32{0}, tc.code, tc.flags)
			reply.Transaction = m.Transaction
			_ = osdMapsWrite(peer, reply)
			got := <-done
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("contradictory snapshot reply completed", got.err)
				}
				return
			case tc.code < 0:
				if !errors.As(got.err, &server) || server.Code != tc.code {
					t.Fatal("known snapshot refusal changed", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid snapshot reply rejected", got.err)
				}
			}
			op := got.reply.Operations[0]
			switch tc.reply.Code {
			case OSDRollback:
				if op.Snapshot != 4 || len(op.Data) != 0 {
					t.Fatal("rollback echo lost", op)
				}
			case OSDListSnapshots:
				l := op.Snapshots
				if l.Sequence != 2 || len(l.Clones) != 2 || l.Clones[0].ID != 2 || l.Clones[0].Overlap[0] != (OSDExtent{0, 3}) || l.Clones[1].ID != OSDHeadSnapshot || l.Clones[1].Size != 9 || len(op.Data) != len(listing) {
					t.Fatalf("snapshot listing not decoded: %+v", l)
				}
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("snapshot request was resent", replay.Transaction)
			default:
			}
		})
	}
}
