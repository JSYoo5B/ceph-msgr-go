package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

func TestCephOSDMutationConcurrencyIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, _, oracle, original := osdFixture(t)
	defer func() { recoverMutationFixture(t, c, oracle, original, "", false) }()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	const count, recordSize = 16, 64
	record := func(id int) []byte {
		data := make([]byte, recordSize)
		copy(data, []byte{0, 255, 128, 10})
		binary.LittleEndian.PutUint32(data[4:8], uint32(id))
		for j := 8; j < len(data); j++ {
			data[j] = byte(id*19 + j*7)
		}
		return data
	}
	for _, appendMode := range []bool{false, true} {
		name := "independent compound write extents"
		if appendMode {
			name = "atomic compound append records"
		}
		t.Run(name, func(t *testing.T) {
			baseline := make([]byte, count*recordSize)
			if appendMode {
				baseline = []byte{0, 255, 128, 10, 1, 254, 0, 127}
			}
			seed := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(baseline)), Data: baseline})
			seed.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000299, Nanoseconds: 123456789}
			before, err := o.Request(ctx, seed)
			if err != nil {
				t.Fatal("seed concurrent object", err)
			}
			nativeBefore := nativeMutation(t, ctx, map[string]any{"compare": false})
			if nativeBefore.Actual.Code != 0 || nativeBefore.Actual.Version != before.UserVersion || !bytes.Equal(nativeBefore.Actual.Data, baseline) {
				t.Fatal("native concurrent baseline differs")
			}
			type completion struct {
				id    int
				reply cephmsgr.OSDReply
				err   error
			}
			start := make(chan struct{})
			finished := make(chan completion, count)
			var workers sync.WaitGroup
			mtime := cephmsgr.OSDTime{Seconds: 1700000300, Nanoseconds: 987654321}
			for id := 1; id <= count; id++ {
				workers.Add(1)
				go func(id int) {
					defer workers.Done()
					data := record(id)
					code, offset := cephmsgr.OSDWrite, uint64((id-1)*recordSize)
					secondOffset := offset + recordSize/2
					if appendMode {
						code, offset, secondOffset = cephmsgr.OSDAppend, 0, 0
					}
					request := osdObjectRequest(oracle,
						cephmsgr.OSDOperation{Code: code, Offset: offset, Length: recordSize / 2, Data: data[:recordSize/2]},
						cephmsgr.OSDOperation{Code: code, Offset: secondOffset, Length: recordSize / 2, Data: data[recordSize/2:]})
					request.ClientIncarnation, request.ModificationTime = 37, mtime
					<-start
					reply, err := o.Request(ctx, request)
					finished <- completion{id, reply, err}
				}(id)
			}
			close(start)
			workers.Wait()
			close(finished)
			var tids []uint64
			versions := make(map[uint64]bool, count)
			var maxVersion uint64
			for result := range finished {
				if result.err != nil {
					t.Fatalf("concurrent request %d: %v (submission tid=%d)", result.id, result.err, result.reply.Submission.Transaction)
				}
				reply := result.reply
				checkOSDMetadata(t, reply, oracle)
				if reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.ClientIncarnation != 37 || reply.Submission.MapEpoch != oracle.Epoch || reply.Submission.Attempt != 0 || len(reply.Operations) != 2 || reply.UserVersion <= before.UserVersion || versions[reply.UserVersion] {
					t.Fatalf("concurrent completion %d lacks unique committed identity/version", result.id)
				}
				for i, op := range reply.Operations {
					code, offset := cephmsgr.OSDWrite, uint64((result.id-1)*recordSize+i*recordSize/2)
					if appendMode {
						code, offset = cephmsgr.OSDAppend, 0
					}
					if op.Code != code || op.Flags != 0 || op.Offset != offset || op.Length != recordSize/2 || op.Result != 0 || op.PayloadLength != 0 || len(op.Data) != 0 {
						t.Fatalf("concurrent request %d operation %d lost completion metadata", result.id, i)
					}
				}
				if len(reply.RawData) != 0 {
					t.Fatal("mutation-only vector unexpectedly returned data")
				}
				tids = append(tids, reply.Transaction)
				versions[reply.UserVersion] = true
				maxVersion = max(maxVersion, reply.UserVersion)
			}
			// Replies can complete in any goroutine order. Check the allocated
			// identities as a set, bounded by sequential requests before/after.
			slices.Sort(tids)
			for i, tid := range tids {
				if tid <= before.Transaction || i > 0 && tid <= tids[i-1] {
					t.Fatal("duplicate or regressed concurrent submission identity", tids)
				}
			}
			native := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
			if native.Actual.Code != 0 || native.Actual.Version != maxVersion || native.Actual.Seconds != mtime.Seconds || native.Actual.Nanos != mtime.Nanoseconds || native.Placement.ID != oracle.ID || native.Placement.Address != oracle.Address || native.Placement.Pool != oracle.Pool || native.Placement.Name != oracle.Name || native.Placement.Hash != oracle.Hash || native.Placement.Epoch < oracle.Epoch {
				t.Fatalf("native concurrent result: code=%d bytes=%d version=%d mtime=%d.%09d placement=%+v", native.Actual.Code, len(native.Actual.Data), native.Actual.Version, native.Actual.Seconds, native.Actual.Nanos, native.Placement)
			}
			data := native.Actual.Data
			if appendMode {
				if len(data) != len(baseline)+count*recordSize || !bytes.Equal(data[:len(baseline)], baseline) {
					t.Fatal("concurrent append changed the prefix or record count")
				}
				seen := make(map[uint32]bool, count)
				for tail := data[len(baseline):]; len(tail) > 0; tail = tail[recordSize:] {
					id := binary.LittleEndian.Uint32(tail[4:8])
					if id < 1 || id > count || seen[id] || !bytes.Equal(tail[:recordSize], record(int(id))) {
						t.Fatal("concurrent append duplicated, lost or tore an atomic record", id)
					}
					seen[id] = true
				}
			} else {
				var want []byte
				for id := 1; id <= count; id++ {
					want = append(want, record(id)...)
				}
				if !bytes.Equal(data, want) {
					t.Fatal("concurrent independent extents differ from native readback")
				}
			}
			read, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(data))}))
			if err != nil || read.Transaction <= tids[len(tids)-1] || read.UserVersion != native.Actual.Version || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, data) {
				t.Fatal("Go read after concurrent writes differs from native bytes/version", err)
			}
			t.Logf("%d concurrent two-op requests: unique TIDs %d..%d, native version %d; complete binary records and ONDISK results matched", count, tids[0], tids[len(tids)-1], maxVersion)
		})
		if t.Failed() {
			break
		}
	}
}
