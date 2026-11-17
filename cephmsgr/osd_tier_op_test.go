package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

// tierTestRequest is the native request shape of one tier operation:
// promote is a write (an mtime is the caller's); flush and evict are reads
// with IgnoreCache and IgnoreOverlay.
func tierTestRequest(code OSDOpcode) OSDRequest {
	r := OSDRequest{MapEpoch: 17, Object: OSDObject{Pool: 7, Name: "object"}, Operations: []OSDOperation{{Code: code}}}
	if code == OSDTierPromote {
		r.ModificationTime = OSDTime{Seconds: 1700000000}
	} else {
		r.IgnoreCache, r.IgnoreOverlay = true, true
	}
	return r
}

func TestOSDTierOperationPreflight(t *testing.T) {
	o := backoffUnitConnection()
	// Tier operations need neither EnableOSDCachePools nor bit 35.
	if o.client.options.EnableOSDCachePools || o.features&osd.FeatureCachePool != 0 {
		t.Fatal("unit connection unexpectedly carries the cache-pool capability")
	}
	for _, code := range []OSDOpcode{OSDTierPromote, OSDTierFlush, OSDTierEvict} {
		r := tierTestRequest(code)
		for name, check := range map[string]func(OSDRequest) error{"tier": o.validateTierOperation, "ignore-cache": o.validateIgnoreCache, "cache": o.validateCacheOperation} {
			if err := check(r); err != nil {
				t.Fatalf("%#x native shape refused by %s preflight: %v", code, name, err)
			}
		}
	}
	for _, r := range []OSDRequest{{Operations: []OSDOperation{{Code: OSDRead, Length: 1}}}, {Operations: []OSDOperation{{Code: OSDSetChunk}}}} {
		if err := o.validateTierOperation(r); err != nil {
			t.Fatal("request without a tier operation changed", err)
		}
	}
	// IgnoreOverlay and IgnoreCache stay refused or gated elsewhere.
	if err := o.validateCacheOperation(OSDRequest{Operations: []OSDOperation{{Code: OSDRead, Length: 1}}, IgnoreOverlay: true}); !errors.Is(err, osd.ErrRequest) {
		t.Fatal("IgnoreOverlay admitted on a READ", err)
	}
	if err := o.validateIgnoreCache(OSDRequest{Operations: []OSDOperation{{Code: OSDRead, Length: 1}}, IgnoreCache: true}); !errors.Is(err, ErrUnsupportedFeatures) {
		t.Fatal("IgnoreCache READ admitted without EnableOSDCachePools", err)
	}
}

func TestOSDTierOperationRefusesBeforeSlotMonitorAndIdentity(t *testing.T) {
	op := func(r *OSDRequest) *OSDOperation { return &r.Operations[0] }
	edits := map[string]func(*OSDRequest){
		"flags":          func(r *OSDRequest) { op(r).Flags = OSDFailOK },
		"with-reference": func(r *OSDRequest) { op(r).Flags = OSDWithReference },
		"extent":         func(r *OSDRequest) { op(r).Length = 1 },
		"input":          func(r *OSDRequest) { op(r).Data = []byte{1} },
		"chunk":          func(r *OSDRequest) { op(r).Chunk.SourceLength = 1 },
		"redirect":       func(r *OSDRequest) { op(r).Redirect.TargetName = "x" },
		"compound":       func(r *OSDRequest) { r.Operations = append(r.Operations, OSDOperation{Code: OSDStat}) },
		"resubmit":       func(r *OSDRequest) { r.Resubmit = &OSDSubmission{Transaction: 1} },
		"retry":          func(r *OSDRequest) { r.RetryAttempt = 1 },
		"snapshot":       func(r *OSDRequest) { r.ReadSnapshot = 1 },
		"snapc":          func(r *OSDRequest) { r.SnapContext.Sequence = 1 },
		"rwordered":      func(r *OSDRequest) { r.OrderReadsWrites = true },
		"skiplocks":      func(r *OSDRequest) { r.SkipRWLocks = true },
		"redirect-flag":  func(r *OSDRequest) { r.IgnoreRedirect = true },
		"balance":        func(r *OSDRequest) { r.BalanceReads = true },
		"fullforce":      func(r *OSDRequest) { r.FullForce = true },
		// Flush and evict must carry both native flags and no mtime; promote
		// must carry neither flag.
		"flags-flipped": func(r *OSDRequest) { r.IgnoreCache, r.IgnoreOverlay = !r.IgnoreCache, !r.IgnoreOverlay },
		"overlay-only":  func(r *OSDRequest) { r.IgnoreCache = !r.IgnoreCache },
		"cache-only":    func(r *OSDRequest) { r.IgnoreOverlay = !r.IgnoreOverlay },
		"erasure": func(r *OSDRequest) {
			if r.Placement != nil {
				r.Placement.Erasure, r.Placement.Shard = true, 0
			} else {
				r.Resubmit = &OSDSubmission{Transaction: 1}
			}
		},
	}
	readEdits := map[string]func(*OSDRequest){"mtime": func(r *OSDRequest) { r.ModificationTime.Seconds = 1 }}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, code := range []OSDOpcode{OSDTierPromote, OSDTierFlush, OSDTierEvict} {
			cases := map[string]func(*OSDRequest){}
			for name, edit := range edits {
				cases[name] = edit
			}
			if code != OSDTierPromote {
				for name, edit := range readEdits {
					cases[name] = edit
				}
			}
			for name, edit := range cases {
				t.Run(fmt.Sprintf("%s/%#x/%s", profileNameForCacheTest(profile), uint16(code), name), func(t *testing.T) {
					o := backoffUnitConnection()
					o.client.options.OSDProfile = profile
					// A foreign owner occupies the sole slot; MON and writer are unstarted.
					o.client.calls = make(chan struct{}, 1)
					o.client.calls <- struct{}{}
					r := tierTestRequest(code)
					if profile.replicatedPG() {
						r.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
					}
					edit(&r)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					result, err := o.Request(ctx, r)
					if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || result.Submission.Transaction != 0 || o.client.osdNext.Load() != 0 || len(o.client.calls) != 1 {
						t.Fatal("invalid tier operation passed local preflight", err, result.Submission.Transaction, o.client.osdNext.Load(), len(o.client.calls))
					}
				})
			}
		}
	}
}

// tierTestOpen opens one OSD connection of the profile and returns the
// request input with its placement and the request-flag offset of the front.
func tierTestOpen(t *testing.T, profile OSDProfile, code OSDOpcode) (*OSDConnection, *osdTestPeer, context.Context, *atomic.Uint32, OSDRequest, int) {
	t.Helper()
	ctx := context.Background()
	input := osdValidationRequest()
	input.ClientIncarnation = 23
	input.Operations = []OSDOperation{{Code: code}}
	input.ModificationTime = OSDTime{}
	input.IgnoreCache, input.IgnoreOverlay = false, false
	if code == OSDTierPromote {
		input.ModificationTime = OSDTime{Seconds: 1700000000, Nanoseconds: 5}
	} else {
		input.IgnoreCache, input.IgnoreOverlay = true, true
	}
	if profile == OSDProfileObjectHash {
		c, peers, dials := osdTestClient(t)
		o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
		if err != nil {
			t.Fatal(err)
		}
		return o, <-peers, ctx, dials, input, 8
	}
	_, o, peer, ctx, dials := replicatedReplyV8ProfileOpen(t, profile)
	input.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
	return o, peer, ctx, dials, input, 32
}

func tierTestReply(t *testing.T, profile OSDProfile, input OSDRequest, perOp, code int32, flags uint64) msgr.MessageData {
	t.Helper()
	if profile == OSDProfileObjectHash {
		return osdXattrSyntheticReply(t, input.Operations, make([][]byte, 1), []int32{perOp}, code, flags)
	}
	return replyV8ProfileMessage(input, make([][]byte, 1), []int32{perOp}, code, flags, false)
}

// TestOSDTierOperationPublicTransportAndUnknownOutcome sends each tier
// operation per profile without the cache-pool capability, checks its exact
// request flags, mtime and packed operation, keeps a signed server refusal,
// and leaves a sent request whose connection is lost unknown with its
// transaction: no redial and no resend.
func TestOSDTierOperationPublicTransportAndUnknownOutcome(t *testing.T) {
	type result struct {
		r OSDReply
		e error
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, tc := range []struct {
			code  OSDOpcode
			flags uint64
			// refusal is a source-derived signed result of the handler.
			refusal int32
		}{
			{OSDTierPromote, osd.FlagWrite, -2},
			{OSDTierFlush, osd.FlagRead | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay, -22},
			{OSDTierEvict, osd.FlagRead | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay, -22},
		} {
			t.Run(fmt.Sprintf("%s/%#x", profileNameForCacheTest(profile), uint16(tc.code)), func(t *testing.T) {
				o, peer, ctx, dials, input, offset := tierTestOpen(t, profile, tc.code)
				done := make(chan result, 1)
				go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
				sent := peer.next(t)
				want := uint32(tc.flags | osd.FlagOnDisk | osd.FlagReturnVec)
				if got := binary.LittleEndian.Uint32(sent.Front[offset:]); got != want {
					t.Fatalf("tier request flags %#x, want %#x", got, want)
				}
				if len(sent.Data) != 0 {
					t.Fatal("tier operation carried input bytes")
				}
				if !containsPackedInputless(sent.Front, uint16(tc.code)) {
					t.Fatal("tier operation not transmitted as one inputless packed op")
				}
				reply := tierTestReply(t, profile, input, tc.refusal, tc.refusal, OSDReplyAck|OSDReplyOnDisk)
				reply.Transaction = sent.Transaction
				if err := osdMapsWrite(peer, reply); err != nil {
					t.Fatal(err)
				}
				got := <-done
				var server *OSDError
				var unknown *OutcomeUnknownError
				if !errors.As(got.e, &server) || server.Code != tc.refusal || errors.As(got.e, &unknown) || got.r.Submission.Transaction != sent.Transaction {
					t.Fatal("signed tier refusal lost", got.r, got.e)
				}
				// A sent tier operation whose connection is lost stays unknown.
				go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
				lost := peer.next(t)
				peer.conn.Close()
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("lost tier operation did not complete locally")
				}
				if !errors.As(got.e, &unknown) || got.r.Submission.Transaction != lost.Transaction || lost.Transaction == sent.Transaction || dials.Load() != 1 {
					t.Fatal("uncertain tier operation lost identity or redialed", got.r.Submission, got.e, dials.Load())
				}
				select {
				case extra := <-peer.requests:
					t.Fatal("tier operation was resent", extra.Transaction)
				default:
				}
			})
		}
	}
}

// containsPackedInputless finds the packed op: u16 count 1, u16 opcode, u32
// per-op flags 0, a zero 28-byte union and payload length 0.
func containsPackedInputless(front []byte, code uint16) bool {
	packed := binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint16(nil, 1), code)
	packed = append(packed, make([]byte, 4+28+4)...)
	count := 0
	for i := 0; i+len(packed) <= len(front); i++ {
		if string(front[i:i+len(packed)]) == string(packed) {
			count++
		}
	}
	return count == 1
}

// TestOSDTierOperationSuccessRequiresOnDisk keeps the native READ or WRITE
// mode but requires OSD ONDISK before a successful tier operation completes:
// each can change the object's data, manifest or dirty state. ACK-only and
// ONNVRAM-only successes leave the outcome unknown on v6, v8/v8 and v9/v8;
// a signed refusal without ONDISK stays a server result.
func TestOSDTierOperationSuccessRequiresOnDisk(t *testing.T) {
	type result struct {
		r OSDReply
		e error
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, code := range []OSDOpcode{OSDTierPromote, OSDTierFlush, OSDTierEvict} {
			for _, tc := range []struct {
				name  string
				flags uint64
				code  int32
				ok    bool
			}{{"ack-only", OSDReplyAck, 0, false}, {"onnvram-only", OSDReplyOnNVRAM, 0, false}, {"ack-onnvram", OSDReplyAck | OSDReplyOnNVRAM, 0, false},
				{"ondisk", OSDReplyOnDisk, 0, true}, {"ack-ondisk", OSDReplyAck | OSDReplyOnDisk, 0, true}, {"ack-only-refusal", OSDReplyAck, -2, true}} {
				t.Run(fmt.Sprintf("%s/%#x/%s", profileNameForCacheTest(profile), uint16(code), tc.name), func(t *testing.T) {
					o, peer, ctx, _, input, _ := tierTestOpen(t, profile, code)
					done := make(chan result, 1)
					go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
					sent := peer.next(t)
					reply := tierTestReply(t, profile, input, tc.code, tc.code, tc.flags)
					reply.Transaction = sent.Transaction
					if err := osdMapsWrite(peer, reply); err != nil {
						t.Fatal(err)
					}
					var got result
					select {
					case got = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("tier reply did not complete locally")
					}
					var unknown *OutcomeUnknownError
					var server *OSDError
					switch {
					case tc.ok && tc.code < 0:
						if !errors.As(got.e, &server) || server.Code != tc.code || errors.As(got.e, &unknown) {
							t.Fatal("signed tier refusal without ONDISK was not kept", got.r.Flags, got.e)
						}
					case tc.ok:
						if got.e != nil || got.r.Code != 0 || got.r.Flags&OSDReplyOnDisk == 0 {
							t.Fatal("ONDISK tier success refused", got.r.Flags, got.e)
						}
					default:
						if !errors.As(got.e, &unknown) || got.r.Submission.Transaction != sent.Transaction {
							t.Fatal("tier success without ONDISK was accepted", got.r.Flags, got.e)
						}
						select {
						case extra := <-peer.requests:
							t.Fatal("tier operation was resent", extra.Transaction)
						default:
						}
					}
				})
			}
		}
	}
}
