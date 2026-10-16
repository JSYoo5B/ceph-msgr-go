package cephmsgr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func assertReplicatedRequestProfile(t *testing.T, profile OSDProfile, m msgr.MessageData) {
	t.Helper()
	want := uint16(8)
	if profile.requestV9() {
		want = 9
	}
	if m.Type != osd.RequestMessage || m.Version != want || m.CompatVersion != 3 || m.Transaction == 0 {
		t.Fatal("profile selected another request envelope", profile, m)
	}
	if profile.requestV9() {
		if len(m.Front) < 94 || !bytes.Equal(m.Front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
			t.Fatal("public v9 did not emit canonical empty OTEL")
		}
		if binary.LittleEndian.Uint64(m.Front[len(m.Front)-8:])&(1<<49|1<<57|1<<58) != (1<<49 | 1<<57 | 1<<58) {
			t.Fatal("v9 fixed features lost")
		}
	}
}

func TestOSDRequestV9ProfileRequestResultsAndNoReplay(t *testing.T) {
	// Same authenticated synthetic transport and signed-result/completion cases
	// as v8; only the explicit request profile differs. Native proof is separate.
	testReplicatedReplyV8RequestResults(t, OSDProfileReplicatedPGRequestV9ReplyV8)
}

func TestOSDRequestV9ProfileListPGStrictReplyAndNoFallback(t *testing.T) {
	testReplicatedReplyV8PGListResults(t, OSDProfileReplicatedPGRequestV9ReplyV8)
}

func TestOSDRequestV9ProfileAdmissionAndStrictReplySelection(t *testing.T) {
	p := OSDProfileReplicatedPGRequestV9ReplyV8
	if p != 3 || !p.valid() || !p.replicatedPG() || !p.replyV8() || !p.requestV9() {
		t.Fatal("fixed v9 enum/capability changed")
	}
	if err := validateOSDProfile(Options{OSDProfile: p}); !errors.Is(err, ErrOSDDisabled) {
		t.Fatal(err)
	}
	if err := validateOSDProfile(Options{EnableOSD: true, OSDProfile: p}); err != nil {
		t.Fatal(err)
	}
	o := backoffUnitConnection()
	o.client.options.OSDProfile = p
	r := backoffTransportRequest()
	r.Placement.Primary = o.target.ID
	if err := o.validatePlacement(r); err != nil {
		t.Fatal(err)
	}
	r.Placement = nil
	if err := o.validatePlacement(r); err == nil {
		t.Fatal("v9 accepted missing actual PG")
	}
	o.features = 1<<49 | 1<<57
	m := nativeReplyV8Message(t, "reply-v8-native-success")
	if _, err := o.decodeReply(m); !errors.Is(err, osd.ErrFeatures) {
		t.Fatal("v9 request feature substituted reply-v8 capability", err)
	}
	o.features |= 1 << 58
	if _, err := o.decodeReply(m); err != nil {
		t.Fatal("strict v8 native reply refused", err)
	}
	m.Version = 6
	if _, err := o.decodeReply(m); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("v9 profile downgraded reply", err)
	}
}

func TestOSDRequestV9BackoffPGNLSUsesEmptyOIDAndExplicitHash(t *testing.T) {
	o := backoffUnitConnection()
	o.client.options.OSDProfile = OSDProfileReplicatedPGRequestV9ReplyV8
	o.backoffState.gateInstalled = true
	point := osd.PGCursor{Pool: 7, Hash: 3, Namespace: "tenant", Snap: osd.NoSnap}
	block := osd.Backoff{PG: osd.PG{Pool: 7, Seed: 3}, Shard: -1, MapEpoch: 15, ID: 101, Op: osd.BackoffBlock, Begin: point, End: point}
	m, err := osd.EncodeBackoff(block, 4096)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := osd.DecodeBackoff(m, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.backoffState.accept(o, decoded, o.generation, o.globalID); err != nil {
		t.Fatal(err)
	}
	input := osd.PGListRequest{PG: osd.PG{Pool: 7, Seed: 3}, ClientInc: 9, MapEpoch: 11, Count: 1, Namespace: "tenant", MaxOutputBytes: osd.MinPGListBytes}
	features := uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<49 | 1<<56 | 1<<57 | 1<<58)
	m, err = osd.EncodePGListRequestV9(input, features, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(m); !errors.Is(err, ErrOSDBackoff) {
		t.Fatal("PGNLS point escaped exact head-object range", err)
	}
	for _, edit := range []func(*osd.PGListRequest){func(r *osd.PGListRequest) { r.Namespace = "unrelated" }, func(r *osd.PGListRequest) { r.PG.Seed++ }} {
		r := input
		edit(&r)
		unrelated, err := osd.EncodePGListRequestV9(r, features, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if err := o.beforeWriteBackoff(unrelated); err != nil {
			t.Fatal("unrelated PGNLS was blocked or failed to decode", err)
		}
	}
	old, err := osd.EncodePGListRequestV8(input, features&^(1<<49), 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.beforeWriteBackoff(old); !errors.Is(err, ErrMalformedMessage) {
		t.Fatal("v9 backoff fell back to v8 parser", err)
	}
	m.Front = bytes.Clone(m.Front)
	m.Front[93] = 1
	if err := o.beforeWriteBackoff(m); !errors.Is(err, ErrMalformedMessage) {
		t.Fatal("nonempty trace escaped installed v9 writer gate", err)
	}
}
