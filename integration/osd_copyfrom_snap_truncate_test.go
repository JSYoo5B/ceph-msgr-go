package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Cross both copy opcodes with a truncated source head and its untruncated
// snapshot clone, the COPY_FROM2 TRUNCATE_SEQ override, and destination
// SnapContexts (empty, current, newer, stale with and without ORDERSNAP,
// newer on an absent destination). Every expectation is derived from the
// pinned Tentacle source and checked against independent native librados
// reads, object_info and rados listsnaps, never against the Go client's view.
//
//fixture:isolated allocates self-managed snapshots, leaving the fixture pool flag set
func TestCephOSDCopyFromSnapTruncateIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	type state struct {
		Read snapOracleRead `json:"read"`
		List snapToolResult `json:"list_snaps"`
	}
	type cell struct {
		Name        string                    `json:"name"`
		Input       cephmsgr.OSDCopyFromInput `json:"input"`
		SnapContext cephmsgr.OSDSnapContext   `json:"snap_context"`
		OrderSnap   bool                      `json:"order_snap"`
		Seeds       []cephmsgr.OSDReply       `json:"seed_replies"`
		Before      state                     `json:"before"`
		Reply       cephmsgr.OSDReply         `json:"reply"`
		Error       string                    `json:"error,omitempty"`
		After       state                     `json:"after"`
		Clone       *snapOracleRead           `json:"destination_clone,omitempty"`
	}
	receipt := struct {
		Snaps          []uint64          `json:"snaps"`
		SourceTruncate cephmsgr.OSDReply `json:"source_truncate_reply"`
		Clone          snapOracleRead    `json:"source_clone"`
		Head           snapOracleRead    `json:"source_head"`
		Source         snapToolResult    `json:"source_list_snaps"`
		Cells          []cell            `json:"cells"`
		Refusals       []string          `json:"local_refusals"`
		Cleaned        bool              `json:"cleaned"`
	}{}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
			receipt.Cleaned = !t.Failed()
		}
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-snap-truncate-results.json"), raw)
		}
		if err != nil {
			t.Error("snapshot/truncate copy evidence", err)
		}
	})

	// One native source and eight absent reserved destinations: per opcode a
	// truncate destination plus current/newer, stale and absent destinations.
	baseline := []byte("clone-bytes-0123456789abcdefghijklmnopqrstuvwxyz\x00\xff")
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: []nativeXattrEntry{{Name: []byte("marker"), Value: []byte("clone")}}}, Reserve: 8})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 9 {
		t.Fatal("native seed", owned)
	}
	src := owned.Objects[0]
	place := func(name string) osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	read := func(t *testing.T, name string, snap uint64) snapOracleRead {
		return nativeSnapRead(t, ctx, base, name, snap, nil)
	}
	list := func(t *testing.T, name string) snapToolResult { return nativeListSnaps(t, ctx, base, name) }
	observe := func(t *testing.T, name string) state { return state{read(t, name, snapOracleHead), list(t, name)} }
	// headOnly requires a listing with no clone and the given snapset seq.
	headOnly := func(t *testing.T, what string, l snapCLIListing, seq uint64, size uint64) {
		t.Helper()
		if l.Seq != int64(seq) || len(l.Clones) != 1 || !l.Clones[0].ID.Head || l.Clones[0].Size != size || len(l.Clones[0].Overlaps) != 0 {
			t.Fatalf("%s: native listsnaps differs from head-only seq %d size %d: %+v", what, seq, size, l)
		}
	}

	snaps := nativeSnapCreate(t, ctx, base, 2)
	s1, s2 := snaps[0], snaps[1]
	receipt.Snaps = snaps
	if s2 <= s1 {
		t.Fatal("native snap ids must increase", snaps)
	}
	cur := cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1}}
	newer := cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s2, s1}}

	// Truncate the source head under snapshot s1: make_writeable clones the
	// untruncated object first, and TRUNCATE with a newer sequence records
	// truncate_seq/truncate_size in the head's object_info.
	const sourceSeq, sourceSize = uint32(3), uint64(20)
	srcRoute := place(src)
	sourceConn := mutationConnection(t, c, ctx, srcRoute)
	defer sourceConn.Close()
	truncate := request(srcRoute,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: sourceSize, TruncateSize: sourceSize, TruncateSequence: sourceSeq},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte("head")})
	truncate.SnapContext = cur
	truncate.ModificationTime = cephmsgr.OSDTime{Seconds: 1700050000, Nanoseconds: 7}
	truncateReply, err := sourceConn.Request(ctx, truncate)
	receipt.SourceTruncate = truncateReply
	if err != nil || truncateReply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("source truncate", truncateReply, err)
	}
	t.Logf("source truncate reply ops: %+v", truncateReply.Operations)
	receipt.Clone, receipt.Head, receipt.Source = read(t, src, s1), read(t, src, snapOracleHead), list(t, src)
	for _, s := range []snapOracleRead{receipt.Clone, receipt.Head} {
		if s.Aggregate != 0 || len(s.Operations) != 5 || s.Version == 0 || s.ObjectInfo == nil || s.ObjectInfo.UserVersion != s.Version || s.Operations[0].Seconds == nil {
			t.Fatal("native source precondition", s)
		}
	}
	ci, hi := receipt.Clone.ObjectInfo, receipt.Head.ObjectInfo
	if ci.OID.Snap != int64(s1) || hi.OID.Snap != -2 ||
		!bytes.Equal(receipt.Clone.Operations[1].Data, baseline) || ci.Size != uint64(len(baseline)) || ci.TruncateSeq != 0 || ci.TruncateSize != 0 ||
		!bytes.Equal(receipt.Head.Operations[1].Data, baseline[:sourceSize]) || hi.Size != sourceSize || hi.TruncateSeq != sourceSeq || hi.TruncateSize != sourceSize ||
		string(receipt.Clone.Xattrs()["marker"]) != "clone" || string(receipt.Head.Xattrs()["marker"]) != "head" || receipt.Clone.Version >= receipt.Head.Version {
		t.Fatalf("native truncated head and untruncated clone differ from the source-derived model: clone=%+v head=%+v", ci, hi)
	}
	sl := receipt.Source.listing(t)
	if sl.Seq != int64(s1) || len(sl.Clones) != 2 || sl.Clones[0].ID.Head || sl.Clones[0].ID.ID != s1 || len(sl.Clones[0].Snapshots) != 1 || sl.Clones[0].Snapshots[0].ID != s1 ||
		sl.Clones[0].Size != uint64(len(baseline)) || len(sl.Clones[0].Overlaps) != 1 || sl.Clones[0].Overlaps[0].Start != 0 || sl.Clones[0].Overlaps[0].Length != sourceSize ||
		!sl.Clones[1].ID.Head || sl.Clones[1].Size != sourceSize {
		t.Fatalf("native source listsnaps differs: %s", receipt.Source.JSON)
	}
	sources := map[string]struct {
		snap  uint64
		state snapOracleRead
	}{"clone": {s1, receipt.Clone}, "head": {0, receipt.Head}}

	// copied checks the native destination head against the selected source:
	// bytes, size, user xattrs (destination-only removed), user version, mtime
	// and the truncate pair, which is the client's when TRUNCATE_SEQ is set
	// and the source object_info's otherwise.
	copied := func(t *testing.T, what string, after snapOracleRead, source snapOracleRead, seq uint32, size uint64, reply cephmsgr.OSDReply) {
		t.Helper()
		oi := after.ObjectInfo
		so := source.Operations[0]
		if after.Aggregate != 0 || oi == nil || !bytes.Equal(after.Operations[1].Data, source.Operations[1].Data) || oi.Size != uint64(len(source.Operations[1].Data)) ||
			!reflect.DeepEqual(copyClassificationXattrs(after.Xattrs()), copyClassificationXattrs(source.Xattrs())) ||
			after.Version != source.Version || oi.UserVersion != source.Version || reply.UserVersion != after.Version ||
			*after.Operations[0].Seconds != *so.Seconds || *after.Operations[0].Nanos != *so.Nanos ||
			oi.TruncateSeq != seq || oi.TruncateSize != size {
			t.Fatalf("%s: native destination differs from the copied source (want truncate %d/%d): after=%+v oi=%+v reply-uv=%d", what, seq, size, after.Operations[:3], oi, reply.UserVersion)
		}
	}
	accepted := func(t *testing.T, what string, reply cephmsgr.OSDReply, err error, dst string, input cephmsgr.OSDCopyFromInput, code cephmsgr.OSDOpcode, route osdFixtureOracle) {
		t.Helper()
		wantSnap := input.SourceSnapshot
		if wantSnap == 0 {
			wantSnap = cephmsgr.OSDHeadSnapshot
		}
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction ||
			reply.Object != dst || reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != 1 || len(reply.RawData) != 0 {
			t.Fatalf("%s: copy reply %+v %v", what, reply, err)
		}
		r := reply.Operations[0]
		if r.Code != code || r.Result != 0 || r.CopyFromSnapshot != wantSnap || r.CopyFromVersion != 0 || r.PayloadLength != 0 || len(r.Data) != 0 {
			t.Fatalf("%s: per-op echo %+v", what, r)
		}
	}
	mtime := uint32(0)
	seed := func(t *testing.T, o *cephmsgr.OSDConnection, route osdFixtureOracle, label string, snapc cephmsgr.OSDSnapContext, seq uint32) cephmsgr.OSDReply {
		t.Helper()
		data := []byte(label)
		ops := []cephmsgr.OSDOperation{
			{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
			{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: data},
			{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "destination-only"}, Data: data},
		}
		if seq != 0 {
			ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: uint64(len(data)), TruncateSize: uint64(len(data)), TruncateSequence: seq})
		}
		r := request(route, ops...)
		r.SnapContext = snapc
		mtime++
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700060000, Nanoseconds: mtime}
		reply, err := o.Request(ctx, r)
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("destination seed", label, reply, err)
		}
		return reply
	}
	// seeded verifies a seed natively: bytes, both xattrs, version and the
	// truncate pair (the seed's own sequence and length when sequenced).
	seeded := func(t *testing.T, what string, s state, label string, reply cephmsgr.OSDReply, seq uint32) {
		t.Helper()
		oi := s.Read.ObjectInfo
		want := map[string]string{"marker": label, "destination-only": label}
		wantSize := uint64(0)
		if seq != 0 {
			wantSize = uint64(len(label))
		}
		if s.Read.Aggregate != 0 || oi == nil || !bytes.Equal(s.Read.Operations[1].Data, []byte(label)) || !reflect.DeepEqual(copyClassificationXattrs(s.Read.Xattrs()), want) ||
			s.Read.Version != reply.UserVersion || oi.TruncateSeq != seq || oi.TruncateSize != wantSize {
			t.Fatalf("%s: native seed differs: %+v %+v", what, s.Read.Operations[:3], oi)
		}
		for name, source := range sources {
			if bytes.Equal(s.Read.Operations[1].Data, source.state.Operations[1].Data) || reflect.DeepEqual(want, copyClassificationXattrs(source.state.Xattrs())) {
				t.Fatalf("%s: seed is not distinguishable from source %s", what, name)
			}
		}
	}

	for index, code := range []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2} {
		t.Run(fmt.Sprintf("opcode-%x", code), func(t *testing.T) {
			dsts := owned.Objects[1+4*index : 5+4*index]
			routes := make([]osdFixtureOracle, len(dsts))
			conns := make([]*cephmsgr.OSDConnection, len(dsts))
			for i, dst := range dsts {
				routes[i] = place(dst)
				conns[i] = mutationConnection(t, c, ctx, routes[i])
				defer conns[i].Close()
			}
			good := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src}}
			truncated := good
			if code == cephmsgr.OSDCopyFrom2 {
				truncated.CopyFrom.TruncateSequence, truncated.CopyFrom.TruncateSize = 9, 4
			}

			// Known-unsent combinations: a truncate before or after the copy in
			// one vector, and SnapContexts SnapContext::is_valid (or this
			// client's stricter zero-id rule) rejects. TIDs stay consecutive.
			t.Run("local-refusals", func(t *testing.T) {
				o, route, dst := conns[0], routes[0], dsts[0]
				seed(t, o, route, fmt.Sprintf("refusal-%x", code), cephmsgr.OSDSnapContext{}, 0)
				readReq := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
				first, err := o.Request(ctx, readReq)
				if err != nil {
					t.Fatal(err)
				}
				before := observe(t, dst)
				trunc := cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: 4, TruncateSize: 4, TruncateSequence: 9}
				refusals := []struct {
					name string
					edit func(*cephmsgr.OSDRequest)
				}{
					{"copy-then-truncate", func(r *cephmsgr.OSDRequest) { r.Operations = append(r.Operations, trunc) }},
					{"truncate-then-copy", func(r *cephmsgr.OSDRequest) {
						r.Operations = append([]cephmsgr.OSDOperation{trunc}, r.Operations...)
					}},
					{"snap-above-seq", func(r *cephmsgr.OSDRequest) {
						r.SnapContext = cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s2}}
					}},
					{"ascending-snaps", func(r *cephmsgr.OSDRequest) {
						r.SnapContext = cephmsgr.OSDSnapContext{Sequence: s2, Snapshots: []uint64{s1, s2}}
					}},
					{"zero-snap", func(r *cephmsgr.OSDRequest) {
						r.SnapContext = cephmsgr.OSDSnapContext{Sequence: s1, Snapshots: []uint64{s1, 0}}
					}},
					{"seq-above-maxsnap", func(r *cephmsgr.OSDRequest) {
						r.SnapContext = cephmsgr.OSDSnapContext{Sequence: cephmsgr.OSDMaxSnapshot + 1}
					}},
				}
				for _, refusal := range refusals {
					r := request(route, truncated)
					refusal.edit(&r)
					reply, err := o.Request(ctx, r)
					var server *cephmsgr.OSDError
					var unknown *cephmsgr.OutcomeUnknownError
					if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
						t.Fatal("local refusal was submitted", refusal.name, reply, err)
					}
					receipt.Refusals = append(receipt.Refusals, fmt.Sprintf("%x/%s", code, refusal.name))
				}
				next, err := o.Request(ctx, readReq)
				if err != nil || next.Transaction != first.Transaction+1 || !reflect.DeepEqual(before, observe(t, dst)) {
					t.Fatal("refusal consumed a TID, altered the object or broke the connection", next, err)
				}
			})
			if t.Failed() {
				return
			}

			// Truncated source: the destination receives the selected
			// object's bytes and size; TRUNCATE_SEQ replaces only the
			// truncate pair, never the data, and is not compared with the
			// source's sequence. No SnapContext means no destination clone.
			t.Run("truncate", func(t *testing.T) {
				type tcase struct {
					name     string
					source   string
					seq      uint32
					size     uint64
					override bool
				}
				cases := []tcase{{"head", "head", sourceSeq, sourceSize, false}, {"clone", "clone", 0, 0, false}}
				if code == cephmsgr.OSDCopyFrom2 {
					cases = append(cases,
						tcase{"head-truncate-seq", "head", 7, 4, true},
						tcase{"clone-truncate-seq", "clone", 7, 4, true},
						tcase{"head-lower-truncate-seq", "head", 1, 5, true})
				}
				o, route, dst := conns[0], routes[0], dsts[0]
				for i, tc := range cases {
					source := sources[tc.source]
					op := good
					op.CopyFrom.SourceSnapshot = source.snap
					if tc.override {
						op.CopyFrom.TruncateSequence, op.CopyFrom.TruncateSize = tc.seq, tc.size
					}
					label := fmt.Sprintf("truncate-sentinel-%x-%s", code, tc.name)
					// The sequenced seed always exceeds every copied sequence,
					// so each case starts from a distinct truncate pair.
					seedSeq := uint32(200 + i)
					seedReply := seed(t, o, route, label, cephmsgr.OSDSnapContext{}, seedSeq)
					before := observe(t, dst)
					seeded(t, tc.name, before, label, seedReply, seedSeq)
					headOnly(t, tc.name+" before", before.List.listing(t), 0, uint64(len(label)))
					r := request(route, op)
					r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700070000}
					reply, err := o.Request(ctx, r)
					after := observe(t, dst)
					entry := cell{Name: fmt.Sprintf("%x/truncate/%s", code, tc.name), Input: op.CopyFrom, Seeds: []cephmsgr.OSDReply{seedReply}, Before: before, Reply: reply, After: after}
					if err != nil {
						entry.Error = err.Error()
					}
					receipt.Cells = append(receipt.Cells, entry)
					accepted(t, tc.name, reply, err, dst, op.CopyFrom, code, route)
					copied(t, tc.name, after.Read, source.state, tc.seq, tc.size, reply)
					headOnly(t, tc.name+" after", after.List.listing(t), 0, uint64(len(source.state.Operations[1].Data)))
					t.Logf("%s: source=%s truncate=%d/%d size=%d uv=%d tid=%d", tc.name, tc.source, after.Read.ObjectInfo.TruncateSeq, after.Read.ObjectInfo.TruncateSize, after.Read.ObjectInfo.Size, after.Read.Version, reply.Transaction)
				}
			})
			if t.Failed() {
				return
			}

			// Destination SnapContexts with the snapshot clone as source.
			clone := sources["clone"]
			op := good
			op.CopyFrom.SourceSnapshot = s1
			run := func(t *testing.T, name string, o *cephmsgr.OSDConnection, route osdFixtureOracle, dst string, snapc cephmsgr.OSDSnapContext, order bool, seeds []cephmsgr.OSDReply) (cell, error) {
				t.Helper()
				before := observe(t, dst)
				r := request(route, op)
				r.SnapContext, r.OrderSnapshots = snapc, order
				r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700080000}
				reply, err := o.Request(ctx, r)
				entry := cell{Name: fmt.Sprintf("%x/snapc/%s", code, name), Input: op.CopyFrom, SnapContext: snapc, OrderSnap: order, Seeds: seeds, Before: before, Reply: reply, After: observe(t, dst)}
				if err != nil {
					entry.Error = err.Error()
				}
				t.Logf("%s: aggregate=%d tid=%d uv=%d err=%v", name, reply.Code, reply.Transaction, reply.UserVersion, err)
				return entry, err
			}
			record := func(entry cell) { receipt.Cells = append(receipt.Cells, entry) }
			t.Run("snapc", func(t *testing.T) {
				// Current: the seed write sets the destination snapset seq to s1;
				// a copy under the same context has snaps[0] == seq, no clone.
				o, route, dst := conns[1], routes[1], dsts[1]
				label := fmt.Sprintf("current-sentinel-%x", code)
				first := seed(t, o, route, label, cur, 0)
				entry, err := run(t, "current", o, route, dst, cur, false, []cephmsgr.OSDReply{first})
				record(entry)
				seeded(t, "current", entry.Before, label, first, 0)
				headOnly(t, "current before", entry.Before.List.listing(t), s1, uint64(len(label)))
				accepted(t, "current", entry.Reply, err, dst, op.CopyFrom, code, route)
				copied(t, "current", entry.After.Read, clone.state, 0, 0, entry.Reply)
				headOnly(t, "current after", entry.After.List.listing(t), s1, uint64(len(baseline)))

				// Newer: reseed without a clone, then copy under {s2, [s2, s1]}.
				// make_writeable clones the pre-copy head as clone s2 holding
				// snaps [s2]; finish_copyfrom marked [0, old size) modified, so
				// the clone keeps no overlap with the new head.
				label = fmt.Sprintf("newer-sentinel-%x", code)
				second := seed(t, o, route, label, cur, 0)
				entry, err = run(t, "newer", o, route, dst, newer, false, []cephmsgr.OSDReply{second})
				destClone := read(t, dst, s2)
				entry.Clone = &destClone
				record(entry)
				seeded(t, "newer", entry.Before, label, second, 0)
				headOnly(t, "newer before", entry.Before.List.listing(t), s1, uint64(len(label)))
				accepted(t, "newer", entry.Reply, err, dst, op.CopyFrom, code, route)
				copied(t, "newer", entry.After.Read, clone.state, 0, 0, entry.Reply)
				l := entry.After.List.listing(t)
				if l.Seq != int64(s2) || len(l.Clones) != 2 || l.Clones[0].ID.Head || l.Clones[0].ID.ID != s2 || len(l.Clones[0].Snapshots) != 1 || l.Clones[0].Snapshots[0].ID != s2 ||
					l.Clones[0].Size != uint64(len(label)) || len(l.Clones[0].Overlaps) != 0 || !l.Clones[1].ID.Head || l.Clones[1].Size != uint64(len(baseline)) {
					t.Fatalf("newer: native destination listsnaps differs: %s", entry.After.List.JSON)
				}
				co := destClone.ObjectInfo
				if destClone.Aggregate != 0 || co == nil || co.OID.Snap != int64(s2) || !bytes.Equal(destClone.Operations[1].Data, []byte(label)) ||
					!reflect.DeepEqual(copyClassificationXattrs(destClone.Xattrs()), copyClassificationXattrs(entry.Before.Read.Xattrs())) ||
					destClone.Version != entry.Before.Read.Version || *destClone.Operations[0].Seconds != *entry.Before.Read.Operations[0].Seconds ||
					*destClone.Operations[0].Nanos != *entry.Before.Read.Operations[0].Nanos {
					t.Fatalf("newer: native destination clone is not the pre-copy head: %+v %+v", destClone.Operations[:3], co)
				}

				// Stale with ORDERSNAP: the seed sets seq s2; {s1, [s1]} is older,
				// so execute_ctx replies EOLDSNAPC before the copy starts and the
				// per-op result keeps its unexecuted zero.
				o, route, dst = conns[2], routes[2], dsts[2]
				label = fmt.Sprintf("stale-sentinel-%x", code)
				third := seed(t, o, route, label, newer, 0)
				entry, err = run(t, "stale-ordersnap", o, route, dst, cur, true, []cephmsgr.OSDReply{third})
				record(entry)
				seeded(t, "stale-ordersnap", entry.Before, label, third, 0)
				headOnly(t, "stale-ordersnap before", entry.Before.List.listing(t), s2, uint64(len(label)))
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if !errors.As(err, &server) || server.Code != cephmsgr.OSDOldSnapContext || errors.As(err, &unknown) || entry.Reply.Code != cephmsgr.OSDOldSnapContext || entry.Reply.UserVersion != 0 || len(entry.Reply.Operations) != 1 ||
					entry.Reply.Operations[0].Result != 0 || entry.Reply.Transaction == 0 {
					t.Fatalf("stale-ordersnap: reply %+v %s", entry.Reply, entry.Error)
				}
				if after, err := o.Request(ctx, request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})); err != nil || after.Transaction != entry.Reply.Transaction+1 {
					t.Fatal("connection after EOLDSNAPC", after, err)
				}
				if !reflect.DeepEqual(entry.Before, entry.After) {
					t.Fatal("stale-ordersnap: refused copy changed the destination or its clone list")
				}

				// Stale without ORDERSNAP: the write proceeds, snaps[0] = s1 is not
				// newer than seq s2, so there is no clone and seq stays s2.
				entry, err = run(t, "stale", o, route, dst, cur, false, nil)
				record(entry)
				accepted(t, "stale", entry.Reply, err, dst, op.CopyFrom, code, route)
				if !reflect.DeepEqual(entry.Before, receipt.Cells[len(receipt.Cells)-2].After) {
					t.Fatal("stale: precondition differs from the refused cell's native after state")
				}
				copied(t, "stale", entry.After.Read, clone.state, 0, 0, entry.Reply)
				headOnly(t, "stale after", entry.After.List.listing(t), s2, uint64(len(baseline)))

				// Absent destination under the newer context: no head existed,
				// so there is nothing to clone; seq becomes s2.
				o, route, dst = conns[3], routes[3], dsts[3]
				entry, err = run(t, "absent-newer", o, route, dst, newer, false, nil)
				record(entry)
				if entry.Before.Read.Aggregate != -2 || entry.Before.List.Exit == 0 || entry.Before.List.Code == nil || *entry.Before.List.Code != -2 {
					t.Fatal("absent-newer: destination was not natively absent", entry.Before.Read.Aggregate, entry.Before.List)
				}
				accepted(t, "absent-newer", entry.Reply, err, dst, op.CopyFrom, code, route)
				copied(t, "absent-newer", entry.After.Read, clone.state, 0, 0, entry.Reply)
				headOnly(t, "absent-newer after", entry.After.List.listing(t), s2, uint64(len(baseline)))
			})
		})
	}
	if !reflect.DeepEqual(receipt.Clone, read(t, src, s1)) || !reflect.DeepEqual(receipt.Head, read(t, src, snapOracleHead)) || !reflect.DeepEqual(receipt.Source, list(t, src)) {
		t.Fatal("copies altered the source clone, head or clone list")
	}
}
