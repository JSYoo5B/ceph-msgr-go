package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

// The public source-read flags become the copy union's flag byte on both
// opcodes, beside TRUNCATE_SEQ, and the reply must echo that byte. The cache
// flags need EnableOSDCachePools and a clone id needs a snapshot; both
// refusals happen before submission and consume no TID.
func TestOSDCopyFromSourceReadFlags(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     OSDOpcode
		edit     func(*OSDCopyFromInput)
		cache    bool
		want     uint8
		badEcho  bool
		refusal  error
		snapshot uint64
	}{
		{name: "clone id copy-from", code: OSDCopyFrom, edit: func(in *OSDCopyFromInput) { in.SourceCloneID = true }, snapshot: 6, want: osd.CopyFromMapSnapClone},
		{name: "clone id copy-from2 truncate", code: OSDCopyFrom2, edit: func(in *OSDCopyFromInput) {
			in.SourceCloneID, in.TruncateSequence, in.TruncateSize = true, 5, 9
		}, snapshot: 6, want: osd.CopyFromMapSnapClone | osd.CopyFromTruncateSequence},
		{name: "ignore overlay", code: OSDCopyFrom2, edit: func(in *OSDCopyFromInput) { in.SourceIgnoreOverlay = true }, cache: true, want: osd.CopyFromIgnoreOverlay},
		{name: "ignore cache", code: OSDCopyFrom, edit: func(in *OSDCopyFromInput) { in.SourceIgnoreCache = true }, cache: true, want: osd.CopyFromIgnoreCache},
		{name: "every read flag", code: OSDCopyFrom2, edit: func(in *OSDCopyFromInput) {
			in.SourceCloneID, in.SourceIgnoreOverlay, in.SourceIgnoreCache = true, true, true
		}, cache: true, snapshot: 9, want: osd.CopyFromMapSnapClone | osd.CopyFromIgnoreOverlay | osd.CopyFromIgnoreCache},
		{name: "changed flag echo", code: OSDCopyFrom, edit: func(in *OSDCopyFromInput) { in.SourceCloneID = true }, snapshot: 6, want: osd.CopyFromMapSnapClone, badEcho: true},
		{name: "ignore cache without opt-in", code: OSDCopyFrom2, edit: func(in *OSDCopyFromInput) { in.SourceIgnoreCache = true }, refusal: ErrUnsupportedFeatures},
		{name: "ignore overlay without opt-in", code: OSDCopyFrom, edit: func(in *OSDCopyFromInput) { in.SourceIgnoreOverlay = true }, refusal: ErrUnsupportedFeatures},
		{name: "clone id at the head", code: OSDCopyFrom2, edit: func(in *OSDCopyFromInput) { in.SourceCloneID = true }, refusal: osd.ErrRequest},
		{name: "clone id at nosnap", code: OSDCopyFrom, edit: func(in *OSDCopyFromInput) { in.SourceCloneID = true }, snapshot: OSDHeadSnapshot, refusal: osd.ErrRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			o.client.options.EnableOSDCachePools = tc.cache
			peer := <-peers
			input := osdValidationRequest()
			input.Operations = []OSDOperation{copyFromRequest(tc.code)}
			input.Operations[0].CopyFrom.SourceSnapshot = tc.snapshot
			tc.edit(&input.Operations[0].CopyFrom)
			if tc.refusal != nil {
				r, err := o.Request(ctx, input)
				var unknown *OutcomeUnknownError
				if !errors.Is(err, tc.refusal) || errors.As(err, &unknown) || r.Transaction != 0 || r.Submission != (OSDSubmission{}) {
					t.Fatal("refused copy flag was submitted", r, err)
				}
				select {
				case m := <-peer.requests:
					t.Fatal("refused copy flag reached the OSD", m.Transaction)
				default:
				}
				return
			}
			type result struct {
				r OSDReply
				e error
			}
			done := make(chan result, 1)
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			sent := peer.next(t)
			// The packed op: code, op flags, snapid, source version, copy flags.
			var packed [22]byte
			binary.LittleEndian.PutUint16(packed[:], uint16(tc.code))
			snap := tc.snapshot
			if snap == 0 {
				snap = OSDHeadSnapshot
			}
			binary.LittleEndian.PutUint64(packed[6:], snap)
			binary.LittleEndian.PutUint64(packed[14:], input.Operations[0].CopyFrom.SourceVersion)
			at := bytes.Index(sent.Front, packed[:])
			if at < 0 || bytes.Count(sent.Front, packed[:]) != 1 || sent.Front[at+22] != tc.want {
				t.Fatalf("copy flag byte at %d of %x, want %02x", at, sent.Front, tc.want)
			}
			start := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
			reply := copyFromSyntheticReply(t, input, []int32{0}, 0, OSDReplyOnDisk, false)
			reply.Transaction = sent.Transaction
			if tc.badEcho {
				reply.Front[start+6+16] ^= osd.CopyFromIgnoreCache
			}
			if err := osdMapsWrite(peer, reply); err != nil {
				t.Fatal(err)
			}
			got := <-done
			var unknown *OutcomeUnknownError
			if tc.badEcho {
				if !errors.As(got.e, &unknown) {
					t.Fatal("a changed flag echo was accepted", got.e)
				}
				return
			}
			if got.e != nil || got.r.Operations[0].CopyFromSnapshot != snap {
				t.Fatal("flagged copy reply", got.r, got.e)
			}
		})
	}
}
