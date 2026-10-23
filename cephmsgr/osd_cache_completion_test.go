package cephmsgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

// These synthetic transport cases inject weaker completion and signed errors.
// They do not identify a live daemon branch or establish crash persistence.
func TestOSDCacheOperationCompletion(t *testing.T) {
	type result struct {
		reply OSDReply
		err   error
	}
	operations := []struct {
		name  string
		code  OSDOpcode
		flags uint64
	}{
		{"flush", OSDCacheFlush, osd.FlagRead | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay},
		{"try-flush", OSDCacheTryFlush, osd.FlagRead | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay | osd.FlagSkipRWLocks},
		{"evict", OSDCacheEvict, osd.FlagRead | osd.FlagIgnoreCache | osd.FlagIgnoreOverlay | osd.FlagSkipRWLocks},
		{"pin", OSDCachePin, osd.FlagWrite | osd.FlagIgnoreCache},
		{"unpin", OSDCacheUnpin, osd.FlagWrite | osd.FlagIgnoreCache},
	}
	completions := []struct {
		name     string
		flags    uint64
		code     int32
		complete bool
	}{
		{"no-completion", 0, 0, false}, {"ack-only", OSDReplyAck, 0, false},
		{"onnvram-only", OSDReplyOnNVRAM, 0, false}, {"ack-onnvram", OSDReplyAck | OSDReplyOnNVRAM, 0, false},
		{"ondisk", OSDReplyOnDisk, 0, true}, {"ack-ondisk", OSDReplyAck | OSDReplyOnDisk, 0, true},
		{"signed-enoent-no-completion", 0, -2, true}, {"signed-enoent-ack-only", OSDReplyAck, -2, true},
	}
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPGReplyV8, OSDProfileReplicatedPGRequestV9ReplyV8} {
		for _, op := range operations {
			for _, tc := range completions {
				t.Run(profileNameForCacheTest(profile)+"/"+op.name+"/"+tc.name, func(t *testing.T) {
					_, o, peer, ctx, dials := replicatedReplyV8ProfileOpenWithCache(t, profile, true)
					request := osdValidationRequest()
					request.ClientIncarnation = 23
					request.Operations = []OSDOperation{{Code: op.code}}
					request.IgnoreCache = true
					request.IgnoreOverlay = op.code == OSDCacheFlush || op.code == OSDCacheTryFlush || op.code == OSDCacheEvict
					request.SkipRWLocks = op.code == OSDCacheTryFlush || op.code == OSDCacheEvict
					if op.code == OSDCachePin || op.code == OSDCacheUnpin {
						request.ModificationTime = OSDTime{Seconds: 1700000900}
					}
					offset := 8
					if profile.replicatedPG() {
						request.Placement = &OSDPlacement{PG: OSDPG{Pool: 7, Seed: 3}, Primary: o.target.ID}
						offset = 32
					}
					done := make(chan result, 1)
					go func() { r, e := o.Request(ctx, request); done <- result{r, e} }()
					sent := peer.next(t)
					if flags := binary.LittleEndian.Uint32(sent.Front[offset:]); flags != uint32(op.flags|osd.FlagOnDisk|osd.FlagReturnVec) {
						t.Fatal("cache completion changed native request mode/flags", flags)
					}
					perOp := int32(0)
					if tc.code < 0 && (op.code == OSDCachePin || op.code == OSDCacheUnpin) {
						perOp = tc.code
					}
					var response msgr.MessageData
					if profile == OSDProfileObjectHash {
						response = osdXattrSyntheticReply(t, request.Operations, make([][]byte, 1), []int32{perOp}, tc.code, tc.flags)
					} else {
						response = replyV8ProfileMessage(request, make([][]byte, 1), []int32{perOp}, tc.code, tc.flags, false)
					}
					response.Transaction = sent.Transaction
					if err := osdMapsWrite(peer, response); err != nil {
						t.Fatal(err)
					}
					var got result
					select {
					case got = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("cache reply did not complete locally")
					}
					if got.reply.Submission.Transaction != sent.Transaction || got.reply.Submission.ClientIncarnation != 23 || got.reply.Submission.MapEpoch != request.MapEpoch || got.reply.Flags != tc.flags ||
						!bytes.Equal(got.reply.RawFront, response.Front) || len(got.reply.Operations) != 1 || got.reply.Operations[0].Result != perOp || dials.Load() != 1 {
						t.Fatal("cache completion lost raw fields or original submission", got.reply.Submission, got.err)
					}
					var unknown *OutcomeUnknownError
					var server *OSDError
					switch {
					case !tc.complete:
						if !errors.As(got.err, &unknown) {
							t.Fatal("cache success without ONDISK completed", got.err, got.reply.Flags)
						}
					case tc.code < 0:
						if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
							t.Fatal("signed refusal was reclassified", got.err)
						}
					default:
						if got.err != nil || got.reply.Code != 0 {
							t.Fatal("ONDISK cache success refused", got.err)
						}
					}
					select {
					case extra := <-peer.requests:
						t.Fatal("cache operation was automatically resent", extra.Transaction)
					default:
					}
				})
			}
		}
	}
}
