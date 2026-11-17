package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Independently read a real clone and head before crossing source snapshots
// with version assertions. This extends the separate head-version and unpinned
// snapshot cases, without sending any unsupported copy flag to the daemon.
//
//fixture:isolated allocates self-managed snapshots, leaving the fixture pool flag set
func TestCephOSDCopyFromClassificationIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	type cell struct {
		Name   string                    `json:"name"`
		Input  cephmsgr.OSDCopyFromInput `json:"input"`
		Reply  cephmsgr.OSDReply         `json:"reply"`
		Before omapWriteState            `json:"before"`
		After  omapWriteState            `json:"after"`
	}
	receipt := struct {
		Clone    snapOracleRead `json:"clone"`
		Head     snapOracleRead `json:"head"`
		Cells    []cell         `json:"cells"`
		Refusals int            `json:"local_refusals"`
		Cleaned  bool           `json:"cleaned"`
	}{}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
			receipt.Cleaned = !t.Failed()
		}
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-classification-results.json"), raw)
		}
		if err != nil {
			t.Error("classification evidence", err)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("clone-data\x00\xff"), Attributes: []nativeXattrEntry{{Name: []byte("marker"), Value: []byte("clone")}}}, Reserve: 2})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 3 {
		t.Fatal("native seed", owned)
	}
	src := owned.Objects[0]
	place := func(name string) osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	srcRoute := place(src)
	sourceConn := mutationConnection(t, c, ctx, srcRoute)
	defer sourceConn.Close()
	// Raise the source user version so the lower nonzero assertion is distinct
	// from version zero, which disables checking.
	setup := request(srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("header")}})
	if _, err := sourceConn.Request(ctx, setup); err != nil {
		t.Fatal(err)
	}
	snap := nativeSnapCreate(t, ctx, base, 1)[0]
	overwrite := request(srcRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 9, Data: []byte("head-data")}, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte("head")})
	overwrite.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	overwrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700010000}
	if _, err := sourceConn.Request(ctx, overwrite); err != nil {
		t.Fatal(err)
	}
	receipt.Clone = nativeSnapRead(t, ctx, base, src, snap, nil)
	receipt.Head = nativeSnapRead(t, ctx, base, src, snapOracleHead, nil)
	for _, s := range []snapOracleRead{receipt.Clone, receipt.Head} {
		if s.Aggregate != 0 || len(s.Operations) < 5 || s.Version < 2 || s.ObjectInfo == nil || s.ObjectInfo.UserVersion != s.Version {
			t.Fatal("native version precondition", s)
		}
	}
	if receipt.Clone.Version >= receipt.Head.Version || bytes.Equal(receipt.Clone.Operations[1].Data, receipt.Head.Operations[1].Data) {
		t.Fatal("clone and head must differ")
	}
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name}).State
	}
	for index, code := range []cephmsgr.OSDOpcode{cephmsgr.OSDCopyFrom, cephmsgr.OSDCopyFrom2} {
		t.Run(fmt.Sprintf("opcode-%x", code), func(t *testing.T) {
			dst := owned.Objects[index+1]
			route := place(dst)
			o := mutationConnection(t, c, ctx, route)
			defer o.Close()
			if _, err := o.Request(ctx, request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 4, Data: []byte("seed")})); err != nil {
				t.Fatal(err)
			}
			good := cephmsgr.OSDOperation{Code: code, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src}}
			read := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
			first, err := o.Request(ctx, read)
			if err != nil {
				t.Fatal(err)
			}
			beforeRefusals := observe(dst)
			edits := []func(*cephmsgr.OSDRequest){
				func(r *cephmsgr.OSDRequest) { r.Operations[0].Flags = cephmsgr.OSDFailOK },
				func(r *cephmsgr.OSDRequest) { r.Operations[0].Flags = cephmsgr.OSDExclusiveCreate },
				func(r *cephmsgr.OSDRequest) { r.Operations[0].CopyFrom.SourceFadvise = cephmsgr.OSDFailOK },
				func(r *cephmsgr.OSDRequest) { r.Operations[0].CopyFrom.SourceSnapshot = math.MaxUint64 },
				func(r *cephmsgr.OSDRequest) { r.Operations[0].CopyFrom.TruncateSize = 1 },
				func(r *cephmsgr.OSDRequest) {
					r.Operations[0].CopyFrom.TruncateSequence = 1
					r.Operations[0].CopyFrom.TruncateSize = math.MaxUint64
				},
				func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap },
			}
			if code == cephmsgr.OSDCopyFrom {
				edits = append(edits, func(r *cephmsgr.OSDRequest) { r.Operations[0].CopyFrom.TruncateSequence = 2 })
			}
			for _, edit := range edits {
				r := request(route, good)
				edit(&r)
				reply, err := o.Request(ctx, r)
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if err == nil || errors.As(err, &server) || errors.As(err, &unknown) || reply.Submission != (cephmsgr.OSDSubmission{}) || reply.Transaction != 0 {
					t.Fatal("local refusal was submitted", reply, err)
				}
				receipt.Refusals++
			}
			next, err := o.Request(ctx, read)
			if err != nil || next.Transaction != first.Transaction+1 || !reflect.DeepEqual(beforeRefusals, observe(dst)) {
				t.Fatal("refusal consumed TID, altered object or broke connection", next, err)
			}
			for _, source := range []struct {
				name  string
				snap  uint64
				state snapOracleRead
			}{{"clone", snap, receipt.Clone}, {"head", 0, receipt.Head}} {
				for _, version := range []struct {
					name  string
					value uint64
					code  int32
				}{{"disabled", 0, 0}, {"exact", source.state.Version, 0}, {"older", source.state.Version - 1, -34}, {"newer", source.state.Version + 1, -75}} {
					t.Run(source.name+"/"+version.name, func(t *testing.T) {
						op := good
						op.CopyFrom.SourceSnapshot = source.snap
						op.CopyFrom.SourceVersion = version.value
						before := observe(dst)
						r := request(route, op)
						r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700020000}
						reply, err := o.Request(ctx, r)
						after := observe(dst)
						receipt.Cells = append(receipt.Cells, cell{Name: fmt.Sprintf("%x/%s/%s", code, source.name, version.name), Input: op.CopyFrom, Reply: reply, Before: before, After: after})
						var server *cephmsgr.OSDError
						var unknown *cephmsgr.OutcomeUnknownError
						if version.code == 0 && err != nil || version.code != 0 && (!errors.As(err, &server) || server.Code != version.code || errors.As(err, &unknown)) {
							t.Fatal("copy result", reply, err)
						}
						wantSnap := source.snap
						if wantSnap == 0 {
							wantSnap = cephmsgr.OSDHeadSnapshot
						}
						if reply.Code != version.code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != dst || reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != 1 || len(reply.RawData) != 0 {
							t.Fatal("raw receipt", reply)
						}
						result := reply.Operations[0]
						wantOp := int32(0)
						if version.code != 0 {
							wantOp = -115
						}
						if result.Code != code || result.Result != wantOp || result.CopyFromSnapshot != wantSnap || result.CopyFromVersion != version.value || result.PayloadLength != 0 || len(result.Data) != 0 {
							t.Fatal("union/result echo", result)
						}
						if version.code != 0 {
							if !reflect.DeepEqual(before, after) {
								t.Fatal("refused copy changed populated destination")
							}
						} else {
							if reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || after.Code != 0 || after.Version != source.state.Version || reply.UserVersion != after.Version || !bytes.Equal(after.Data, source.state.Operations[1].Data) || !reflect.DeepEqual(xattrInventory(after.nativeXattrSnapshot), stringXattrs(source.state.Xattrs())) {
								t.Fatal("native copy state", after, reply)
							}
							// The native pre-copy source observation and post-copy destination
							// observation include mtime and OMAP/header below.
							if source.state.Operations[0].Seconds == nil || source.state.Operations[0].Nanos == nil || after.Seconds != *source.state.Operations[0].Seconds || after.Nanos != *source.state.Operations[0].Nanos || after.Header == nil || !bytes.Equal(*after.Header, []byte("header")) || len(omapWriteInventory(t, after)) != 0 {
								t.Fatal("copy metadata", after)
							}
						}
						t.Logf("snapshot=%d source_version=%d aggregate=%d op=%d tid=%d epoch=%d", source.snap, version.value, reply.Code, result.Result, reply.Transaction, reply.MapEpoch)
					})
				}
			}
			final, err := o.Request(ctx, read)
			if err != nil || final.Code != 0 {
				t.Fatal("connection after known server errors", err)
			}
		})
	}
	if !reflect.DeepEqual(receipt.Clone, nativeSnapRead(t, ctx, base, src, snap, nil)) || !reflect.DeepEqual(receipt.Head, nativeSnapRead(t, ctx, base, src, snapOracleHead, nil)) {
		t.Fatal("copy altered source clone/head")
	}
}

func stringXattrs(input map[string][]byte) map[string]string {
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = string(v)
	}
	return out
}
