package mds

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// nativeReplyInodes decodes every InodeStat of one fresh native reply vector,
// labelled by its place in the reply.
func nativeReplyInodes(t *testing.T, name string) map[string]InodeStat {
	t.Helper()
	reply, err := DecodeClientReply(nativeRequestMessage(t, name, ClientReplyMessage, 1), wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := DecodeTrace(reply.Trace, reply.IsDentry, reply.IsTarget)
	if err != nil {
		t.Fatal(err)
	}
	inodes := map[string]InodeStat{}
	if trace.DirInode != nil {
		inodes["dir"] = *trace.DirInode
	}
	if trace.Target != nil {
		inodes["target"] = *trace.Target
	}
	if reply.Op == OpReaddir {
		listing, err := DecodeReaddir(reply.Extra)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range listing.Entries {
			inodes["entry "+entry.Name] = entry.Inode
		}
	}
	return inodes
}

// The fresh native READDIR target is the one charmap directory of the
// request-family vectors (case-insensitive, NFKC, UTF-8); every other native
// InodeStat is a zero-option control. The receipt comparison in
// TestDecodeFreshNativeClientReplies binds both to the native decoder; this
// test pins the reviewed values and refuses every truncation of the native
// option bytes.
func TestDecodeOptionalMetadataMatchesFreshNative(t *testing.T) {
	var charmap []byte
	for _, name := range []string{"mds-request-native-reply-lookup", "mds-request-native-reply-readdir"} {
		for where, inode := range nativeReplyInodes(t, name) {
			opt, err := inode.DecodeOptionalMetadata()
			if err != nil {
				t.Fatal(name, where, err)
			}
			if name == "mds-request-native-reply-readdir" && where == "target" {
				if len(opt.Options) != 1 {
					t.Fatalf("native charmap directory has %d options", len(opt.Options))
				}
				want := OptionalMetadata{Options: []OptionalMetadataOption{{Kind: OptionalMetadataCharmap, Version: 1, Compat: 1, Payload: opt.Options[0].Payload}},
					Charmap: &Charmap{CaseSensitive: false, Normalization: "nfkc", Encoding: "utf8"}}
				if !reflect.DeepEqual(opt, want) {
					t.Fatalf("native charmap directory: %+v", opt)
				}
				charmap = inode.OptionalMetadata
				continue
			}
			if !bytes.Equal(inode.OptionalMetadata, []byte{0, 0, 0, 0}) || opt.Options != nil || opt.Charmap != nil {
				t.Fatalf("%s %s is not the zero-option control: %x %+v", name, where, inode.OptionalMetadata, opt)
			}
		}
	}
	if charmap == nil {
		t.Fatal("the native READDIR target carries no charmap")
	}
	for cut := range len(charmap) {
		if _, err := DecodeOptionalMetadata(charmap[:cut]); err == nil {
			t.Fatal("truncated native charmap accepted at", cut)
		}
	}
	if _, err := DecodeOptionalMetadata(append(bytes.Clone(charmap), 0)); err == nil {
		t.Fatal("trailing byte after the native option vector accepted")
	}
}

// optionVector builds an option vector from kind and envelope triples.
func optionVector(options ...any) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(options) / 4))
	for i := 0; i+3 < len(options); i += 4 {
		e.U64(options[i].(uint64))
		e.Struct(options[i+1].(uint8), options[i+2].(uint8), options[i+3].([]byte))
	}
	return e.Data
}

func charmapBody(sensitive uint8, normalization, encoding string, extra ...byte) []byte {
	e := wire.Encoder{}
	e.U8(sensitive)
	e.String(normalization)
	e.String(encoding)
	e.Raw(extra)
	return e.Data
}

func TestDecodeOptionalMetadataForms(t *testing.T) {
	empty, err := DecodeOptionalMetadata([]byte{0, 0, 0, 0})
	if err != nil || empty.Options != nil || empty.Charmap != nil {
		t.Fatal("zero-option vector", empty, err)
	}
	// Unknown kinds are kept raw with any version, before and after a
	// charmap; a newer charmap version is read by its leading fields.
	unknown := []byte{0xde, 0xad}
	raw := optionVector(uint64(0), uint8(9), uint8(7), unknown,
		OptionalMetadataCharmap, uint8(2), uint8(1), charmapBody(1, "nfd", "utf8", 0xaa, 0xbb),
		uint64(1<<40), uint8(1), uint8(1), []byte(nil))
	got, err := DecodeOptionalMetadata(raw)
	want := OptionalMetadata{
		Options: []OptionalMetadataOption{{Kind: 0, Version: 9, Compat: 7, Payload: unknown},
			{Kind: OptionalMetadataCharmap, Version: 2, Compat: 1, Payload: charmapBody(1, "nfd", "utf8", 0xaa, 0xbb)},
			{Kind: 1 << 40, Version: 1, Compat: 1, Payload: []byte{}}},
		Charmap: &Charmap{CaseSensitive: true, Normalization: "nfd", Encoding: "utf8"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed options\n got %+v (%v)\nwant %+v", got, err, want)
	}
	// Options are copies: changing the input leaves them intact.
	raw[len(raw)-1-len(unknown)] ^= 0xff
	if !reflect.DeepEqual(got, want) {
		t.Fatal("decoded options alias the input")
	}
	// An empty normalization and encoding are the values an rmxattr leaves
	// before the MDS restores its defaults; the decoder reports them raw.
	blank, err := DecodeOptionalMetadata(optionVector(OptionalMetadataCharmap, uint8(1), uint8(1), charmapBody(0, "", "")))
	if err != nil || *blank.Charmap != (Charmap{}) {
		t.Fatal("blank charmap", blank, err)
	}
}

func TestDecodeOptionalMetadataRefusesMalformedInput(t *testing.T) {
	cm := charmapBody(0, "nfkc", "utf8")
	over := wire.Encoder{}
	over.U32(MaxOptionalMetadataOptions + 1)
	for range MaxOptionalMetadataOptions + 1 {
		over.U64(7)
		over.Struct(1, 1, nil)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		want error
	}{
		{"empty", nil, nil},
		{"count beyond the data", []byte{1, 0, 0, 0}, wire.ErrLimit},
		{"count beyond the bound", over.Data, wire.ErrLimit},
		{"charmap compat 2", optionVector(OptionalMetadataCharmap, uint8(2), uint8(2), cm), wire.ErrVersion},
		{"charmap v1 with trailing bytes", optionVector(OptionalMetadataCharmap, uint8(1), uint8(1), charmapBody(0, "nfkc", "utf8", 0)), nil},
		{"charmap missing encoding", optionVector(OptionalMetadataCharmap, uint8(2), uint8(1), cm[:len(cm)-6]), nil},
		{"charmap case byte 2", optionVector(OptionalMetadataCharmap, uint8(1), uint8(1), charmapBody(2, "nfkc", "utf8")), nil},
		{"repeated charmap", optionVector(OptionalMetadataCharmap, uint8(1), uint8(1), cm, OptionalMetadataCharmap, uint8(1), uint8(1), cm), ErrOptionalMetadata},
		{"repeated unknown kind", optionVector(uint64(5), uint8(1), uint8(1), []byte(nil), uint64(5), uint8(1), uint8(1), []byte(nil)), ErrOptionalMetadata},
		{"envelope past the vector", append(optionVector(uint64(5), uint8(1), uint8(1), []byte{1, 2})[:4+8+2], 9, 0, 0, 0, 1), nil},
		{"trailing byte", append(optionVector(uint64(5), uint8(1), uint8(1), []byte(nil)), 0), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeOptionalMetadata(tc.raw)
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) || got.Options != nil || got.Charmap != nil {
				t.Fatalf("got %+v, %v; want refusal %v", got, err, tc.want)
			}
		})
	}
}
