package cephmsgr

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func nativeReplyV8Connection(profile OSDProfile) *OSDConnection {
	// The fixed request/placement/backoff capabilities are separate from the
	// newly selected reply encoding. No MON admission features are included.
	features := session.Features | 1<<8 | 1<<9
	if profile == OSDProfileReplicatedPG || profile == OSDProfileReplicatedPGReplyV8 {
		features |= 1<<21 | 1<<30 | 1<<56 | 1<<57
	}
	if profile == OSDProfileReplicatedPGReplyV8 {
		features |= 1 << 58
	}
	return &OSDConnection{client: &Client{options: Options{OSDProfile: profile, MaxFrameSize: wire.DefaultLimit}}, features: features}
}

func nativeReplyV8Message(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	header := nativeAllocHintFixture(t, name, "header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, nativeAllocHintFixture(t, name, "front"), nil, nativeAllocHintFixture(t, name, "data")}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != msgr.OSDOpReplyMessage || m.CompatVersion != 2 || m.Transaction != 77 || m.Priority != 127 || m.Sequence != 1 || m.AckSequence != 3 || !bytes.Equal(m.Frame().Segments[0], header) {
		t.Fatal("fresh native reply envelope changed", m)
	}
	return m
}

func TestOSDReplyV8ProfilePreservesFreshNativeReplies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     int32
		redirect bool
		mutation bool
	}{
		{name: "success"},
		{name: "enoent", code: -2},
		{name: "unknown-error", code: -12345},
		{name: "mutation-ack", mutation: true},
		{name: "redirect", redirect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeReplyV8Message(t, "reply-v8-native-"+tc.name)
			if m.Version != 8 {
				t.Fatal("native v8 reply unexpectedly selected a different codec", m.Version)
			}
			front, data := bytes.Clone(m.Front), bytes.Clone(m.Data)
			o := nativeReplyV8Connection(OSDProfileReplicatedPGReplyV8)
			r, err := o.decodeReply(m)
			if tc.redirect {
				if !errors.Is(err, ErrOSDRedirect) {
					t.Fatal("native conditional redirect lost its error", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			got := publicOSDReply(r)
			want := OSDReply{
				Object: "reply-v8-codec", PG: OSDPG{Pool: 7, Seed: 0x44},
				Flags: osd.FlagRead | osd.FlagReturnVec | osd.FlagAck | osd.FlagOnDisk,
				Code:  tc.code, MapEpoch: 12, UserVersion: 13,
				LegacyVersion: OSDVersion{Epoch: 8, Version: 13}, ReplayVersion: OSDVersion{Epoch: 8, Version: 91},
				Redirect: OSDRedirect{Pool: -1, Hash: -1}, RawFront: front, RawData: data,
				Operations: []OSDOperationResult{
					{Code: OSDStat, Result: tc.code, Data: []byte{}},
					{Code: OSDRead, Offset: 2, Length: 5, Data: []byte{}},
				},
			}
			request := osd.Request{Object: "reply-v8-codec", Locator: osd.Locator{Pool: 7}, Hash: 0x44,
				Operations: []osd.Operation{{Code: osd.Stat}, {Code: osd.Read, Offset: 2, Length: 5}}}
			if tc.name == "success" {
				want.Operations[0].PayloadLength = 16
				want.Operations[0].Data = []byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 241, 83, 101, 21, 205, 91, 7}
				want.Operations[1].PayloadLength = 5
				want.Operations[1].Data = []byte{0, 255, 'a', 0, 'b'}
			}
			if tc.mutation {
				want.Flags = osd.FlagWrite | osd.FlagReturnVec | osd.FlagAck
				want.Operations = []OSDOperationResult{{Code: OSDWriteFull, Length: 3, Data: []byte{}}}
				request.Operations = []osd.Operation{{Code: osd.WriteFull, Length: 3, Data: []byte("abc")}}
			}
			if tc.redirect {
				want.Operations[0].Result = 0
				want.Redirect = OSDRedirect{Pool: 9, Key: "redirect-key", Namespace: "redirect-namespace", Hash: -1, Object: "redirect-object", LegacyInstructions: []byte{}}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("public v8 reply lost native signed status, flags, versions, operation output or redirect\ngot:  %+v\nwant: %+v", got, want)
			}
			if err := validateOSDReply(request, r, tc.redirect); tc.mutation {
				if err == nil || !strings.Contains(err.Error(), "ONDISK") {
					t.Fatal("native ACK-only mutation must fail specifically for absent ONDISK completion", err)
				}
			} else if err != nil {
				t.Fatal("matching native reply did not validate", err)
			}
			request.Hash++
			if err := validateOSDReply(request, r, tc.redirect); err == nil {
				t.Fatal("reply with another actual PG was accepted")
			}
			// Decoder ownership must survive input buffer reuse; public slices
			// may retain the decoder's owned views but never the caller's input.
			for i := range m.Front {
				m.Front[i] ^= 0xff
			}
			for i := range m.Data {
				m.Data[i] ^= 0xff
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("public reply changed after input buffers were reused")
			}
		})
	}
}

func TestOSDReplyV8ProfileRejectsWrongEncodingAndMissingFeature(t *testing.T) {
	m := nativeReplyV8Message(t, "reply-v8-native-success")
	for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG} {
		o := nativeReplyV8Connection(profile)
		// Even an inconsistent local feature mask must not auto-select v8.
		o.features |= 1 << 58
		if _, err := o.decodeReply(m); !errors.Is(err, wire.ErrVersion) {
			t.Fatal("old profile accepted a v8 reply or chose its codec from features", profile, err)
		}
	}
	o := nativeReplyV8Connection(OSDProfileReplicatedPGReplyV8)
	o.features &^= 1 << 58
	if _, err := o.decodeReply(m); !errors.Is(err, osd.ErrFeatures) {
		t.Fatal("v8 codec selected without its negotiated feature", err)
	}
	for _, envelope := range [][2]uint16{{6, 2}, {7, 2}, {9, 2}, {8, 0}, {8, 1}, {8, 3}} {
		bad := m
		bad.Version, bad.CompatVersion = envelope[0], envelope[1]
		o := nativeReplyV8Connection(OSDProfileReplicatedPGReplyV8)
		if _, err := o.decodeReply(bad); !errors.Is(err, wire.ErrVersion) {
			t.Fatal("v8 profile accepted another version or compatibility", envelope, err)
		}
	}
}

func TestOSDReplyV8ProfileDoesNotFallbackToNativeV6(t *testing.T) {
	// These pre-existing fresh native vectors remain unchanged. The selected
	// v8 profile must reject them, while both prior profiles retain v6 decoding.
	for _, name := range []string{
		"mutation-native-reply-v6-all-success",
		"mutation-native-reply-v6-exclusive-eexist",
		"mutation-native-reply-v6-cmpext-mismatch",
		"mutation-native-reply-v6-truncate",
		"snap-native-reply-v6-read",
	} {
		t.Run(name, func(t *testing.T) {
			m := nativeReplyV8Message(t, name)
			if m.Version != 6 {
				t.Fatal("native v6 regression vector changed", m.Version)
			}
			for _, profile := range []OSDProfile{OSDProfileObjectHash, OSDProfileReplicatedPG} {
				r, err := nativeReplyV8Connection(profile).decodeReply(m)
				if err != nil || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) || len(r.Operations) == 0 {
					t.Fatal("prior profile lost native v6 support", profile, r, err)
				}
			}
			if _, err := nativeReplyV8Connection(OSDProfileReplicatedPGReplyV8).decodeReply(m); !errors.Is(err, wire.ErrVersion) {
				t.Fatal("v8 profile fell back to native v6", err)
			}
		})
	}
}
