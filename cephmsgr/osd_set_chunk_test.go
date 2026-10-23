package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func setChunkTestOperation() OSDOperation {
	return OSDOperation{Code: OSDSetChunk, Flags: OSDWithReference, Chunk: OSDChunkInput{SourceLength: 8, TargetPool: 9, TargetName: "chunk-target", TargetNamespace: "tenant", TargetOffset: 4096}}
}

func TestOSDSetChunkPreflight(t *testing.T) {
	o := backoffUnitConnection()
	good := OSDRequest{MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{setChunkTestOperation()}}
	// SET_CHUNK on a plain pool needs neither EnableOSDCachePools nor bit 35.
	if err := o.validateSetChunk(good); err != nil {
		t.Fatal("native SET_CHUNK shape refused", err)
	}
	for _, r := range []OSDRequest{{Operations: []OSDOperation{{Code: OSDRead, Length: 1}}}, {Operations: []OSDOperation{{Code: OSDCacheFlush}}}} {
		if err := o.validateSetChunk(r); err != nil {
			t.Fatal("request without SET_CHUNK changed", err)
		}
	}
	if err := o.validateSetChunk(OSDRequest{Operations: []OSDOperation{{Code: OSDRead, Length: 1, Chunk: OSDChunkInput{SourceLength: 1}}}}); !errors.Is(err, osd.ErrRequest) {
		t.Fatal("chunk input admitted outside OSDSetChunk", err)
	}
}

func TestOSDSetChunkRefusesBeforeSlotMonitorAndIdentity(t *testing.T) {
	op := func(r *OSDRequest) *OSDOperation { return &r.Operations[0] }
	edits := map[string]func(*OSDRequest){
		"without-reference": func(r *OSDRequest) { op(r).Flags = 0 },
		"failok":            func(r *OSDRequest) { op(r).Flags |= OSDFailOK },
		"nonzero-offset":    func(r *OSDRequest) { op(r).Chunk.SourceOffset = 16 },
		"zero-length":       func(r *OSDRequest) { op(r).Chunk.SourceLength = 0 },
		"no-target":         func(r *OSDRequest) { op(r).Chunk.TargetName = "" },
		"negative-pool":     func(r *OSDRequest) { op(r).Chunk.TargetPool = -1 },
		"self-target": func(r *OSDRequest) {
			op(r).Chunk = OSDChunkInput{SourceLength: 8, TargetPool: r.Object.Pool, TargetName: r.Object.Name}
		},
		"extent":    func(r *OSDRequest) { op(r).Length = 1 },
		"input":     func(r *OSDRequest) { op(r).Data = []byte{1} },
		"redirect":  func(r *OSDRequest) { op(r).Redirect.TargetName = "x" },
		"compound":  func(r *OSDRequest) { r.Operations = append(r.Operations, OSDOperation{Code: OSDStat}) },
		"mtime":     func(r *OSDRequest) { r.ModificationTime.Seconds = 1 },
		"resubmit":  func(r *OSDRequest) { r.Resubmit = &OSDSubmission{Transaction: 1} },
		"retry":     func(r *OSDRequest) { r.RetryAttempt = 1 },
		"snapshot":  func(r *OSDRequest) { r.ReadSnapshot = 1 },
		"snapc":     func(r *OSDRequest) { r.SnapContext.Sequence = 1 },
		"rwordered": func(r *OSDRequest) { r.OrderReadsWrites = true },
		"skiplocks": func(r *OSDRequest) { r.SkipRWLocks = true },
		"ignore":    func(r *OSDRequest) { r.IgnoreCache = true },
		"overlay":   func(r *OSDRequest) { r.IgnoreOverlay = true },
		"balance":   func(r *OSDRequest) { r.BalanceReads = true },
		"fulltry":   func(r *OSDRequest) { r.FullTry = true },
		"erasure": func(r *OSDRequest) {
			if r.Placement != nil {
				r.Placement.Erasure, r.Placement.Shard = true, 0
			} else {
				r.Resubmit = &OSDSubmission{Transaction: 1}
			}
		},
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for name, edit := range edits {
			t.Run(profileNameForCacheTest(profile)+"/"+name, func(t *testing.T) {
				o := backoffUnitConnection()
				o.client.options.OSDProfile = profile
				// A foreign owner occupies the sole slot; MON and writer are unstarted.
				o.client.calls = make(chan struct{}, 1)
				o.client.calls <- struct{}{}
				r := OSDRequest{MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{setChunkTestOperation()}}
				if profile.replicatedPG() {
					r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
				}
				edit(&r)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result, err := o.Request(ctx, r)
				if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || result.Submission.Transaction != 0 || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
					t.Fatal("invalid SET_CHUNK passed local preflight", err, result.Submission.Transaction, o.client.osdNext.Load(), len(o.client.calls))
				}
			})
		}
	}
}

// TestOSDSetChunkPublicTransportAndUnknownOutcome sends one SET_CHUNK per
// profile, checks its request and per-operation flags and indata, keeps a
// signed server refusal, and leaves a sent request whose connection is lost
// unknown with its transaction: no redial and no resend.
func TestOSDSetChunkPublicTransportAndUnknownOutcome(t *testing.T) {
	type result struct {
		r OSDReply
		e error
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		t.Run(profileNameForCacheTest(profile), func(t *testing.T) {
			var o *OSDConnection
			var peer *osdTestPeer
			var dials *atomic.Uint32
			ctx := context.Background()
			input := osdValidationRequest()
			input.ClientIncarnation = 23
			input.Operations = []OSDOperation{setChunkTestOperation()}
			offset := 8
			if profile == OSDProfileObjectHash {
				c, peers, d := osdTestClient(t)
				var err error
				if o, err = c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"}); err != nil {
					t.Fatal(err)
				}
				peer, dials = <-peers, d
			} else {
				_, o, peer, ctx, dials = replicatedReplyV8ProfileOpen(t, profile)
				input.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
				offset = 32
			}
			done := make(chan result, 1)
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			sent := peer.next(t)
			want := uint32(osd.FlagRead | osd.FlagOnDisk | osd.FlagReturnVec)
			if binary.LittleEndian.Uint32(sent.Front[offset:]) != want {
				t.Fatalf("SET_CHUNK request flags %#x, want %#x", binary.LittleEndian.Uint32(sent.Front[offset:]), want)
			}
			packed := binary.LittleEndian.AppendUint16(nil, osd.SetChunk)
			packed = binary.LittleEndian.AppendUint32(packed, osd.OpWithReference)
			if !bytes.Contains(sent.Front, append(packed, make([]byte, 28)...)) || !bytes.Contains(sent.Data, []byte("chunk-target")) || binary.LittleEndian.Uint64(sent.Data[8:]) != 8 {
				t.Fatal("SET_CHUNK operation or indata not transmitted", sent.Data)
			}
			var reply msgr.MessageData
			if profile == OSDProfileObjectHash {
				reply = osdXattrSyntheticReply(t, input.Operations, make([][]byte, 1), []int32{-2}, -2, OSDReplyOnDisk)
			} else {
				reply = replyV8ProfileMessage(input, make([][]byte, 1), []int32{-2}, -2, OSDReplyOnDisk, false)
			}
			reply.Transaction = sent.Transaction
			if err := osdMapsWrite(peer, reply); err != nil {
				t.Fatal(err)
			}
			got := <-done
			var server *OSDError
			var unknown *OutcomeUnknownError
			if !errors.As(got.e, &server) || server.Code != -2 || errors.As(got.e, &unknown) || got.r.Operations[0].Result != -2 || got.r.Submission.Transaction != sent.Transaction {
				t.Fatal("signed SET_CHUNK refusal lost", got.r, got.e)
			}
			// A sent SET_CHUNK whose connection is lost stays unknown.
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			lost := peer.next(t)
			peer.conn.Close()
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("lost SET_CHUNK did not complete locally")
			}
			if !errors.As(got.e, &unknown) || got.r.Submission.Transaction != lost.Transaction || lost.Transaction == sent.Transaction || dials.Load() != 1 {
				t.Fatal("uncertain SET_CHUNK lost identity or redialed", got.r.Submission, got.e, dials.Load())
			}
			select {
			case extra := <-peer.requests:
				t.Fatal("SET_CHUNK was resent", extra.Transaction)
			default:
			}
		})
	}
}
