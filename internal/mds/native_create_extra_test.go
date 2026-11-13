package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Fresh native Tentacle openc_response_t vectors: both registered
// ceph-dencoder test instances of the pinned release, each exported by the
// release encoder and dumped by a separate decoder invocation. See
// mds-openc-native-provenance.json. They are codec evidence only; no daemon
// produced them.

type nativeOpencProof struct {
	Commit    string `json:"ceph_commit"`
	Image     string `json:"image"`
	Type      string `json:"native_type"`
	Instances int    `json:"native_test_instances"`
	Fixtures  map[string]struct {
		Encode []string `json:"encode_argv"`
		Decode []string `json:"decode_argv"`
		Bytes  int      `json:"extra_bytes"`
		SHA256 string   `json:"extra_sha256"`
		Dump   struct {
			CreatedIno uint64 `json:"created_ino"`
			Delegated  string `json:"delegated_inos"`
		} `json:"native_dump"`
	} `json:"fixtures"`
}

// nativeIntervalSet parses the interval_set stream form "[0x1~0xa,0x20~0x2]"
// that openc_response_t::dump prints.
func nativeIntervalSet(t *testing.T, text string) []InodeRange {
	t.Helper()
	inner, ok := strings.CutPrefix(text, "[")
	inner, closed := strings.CutSuffix(inner, "]")
	if !ok || !closed {
		t.Fatal("native interval_set dump shape differs", text)
	}
	if inner == "" {
		return nil
	}
	var ranges []InodeRange
	for _, item := range strings.Split(inner, ",") {
		start, length, found := strings.Cut(item, "~")
		s, errS := strconv.ParseUint(strings.TrimPrefix(start, "0x"), 16, 64)
		l, errL := strconv.ParseUint(strings.TrimPrefix(length, "0x"), 16, 64)
		if !found || errS != nil || errL != nil {
			t.Fatal("native interval differs", item)
		}
		ranges = append(ranges, InodeRange{Start: s, Length: l})
	}
	return ranges
}

func TestDecodeCreateExtraMatchesFreshNative(t *testing.T) {
	raw, err := os.ReadFile("testdata/mds-openc-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof nativeOpencProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !strings.HasSuffix(proof.Image, "@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9") ||
		proof.Type != "openc_response_t" || proof.Instances != 2 || len(proof.Fixtures) != 2 {
		t.Fatal("native openc_response_t provenance differs", proof)
	}
	for name, want := range map[string]struct {
		instance string
		extra    CreateExtra
	}{
		"mds-openc-native-empty": {"1", CreateExtra{Ino: 0, Delegation: true}},
		"mds-openc-native-range": {"2", CreateExtra{Ino: 1, Delegation: true, Delegated: []InodeRange{{Start: 1, Length: 10}}}},
	} {
		t.Run(name, func(t *testing.T) {
			fixture, ok := proof.Fixtures[name]
			extra := nativeMapBytes(t, name, "extra")
			if !ok || len(extra) != fixture.Bytes || fmt.Sprintf("%x", sha256.Sum256(extra)) != fixture.SHA256 {
				t.Fatal("native extra bytes differ from their provenance")
			}
			if len(fixture.Encode) != 8 || fixture.Encode[2] != proof.Type || fixture.Encode[3] != "select_test" || fixture.Encode[4] != want.instance ||
				len(fixture.Decode) != 7 || fixture.Decode[2] != proof.Type || fixture.Decode[5] != "decode" {
				t.Fatal("native invocation differs", fixture.Encode, fixture.Decode)
			}
			got, err := DecodeCreateExtra(extra)
			if err != nil || !reflect.DeepEqual(got, want.extra) {
				t.Fatalf("decoded %+v, %v; want %+v", got, err, want.extra)
			}
			// The separate native decoder's dump is the independent reading.
			if got.Ino != fixture.Dump.CreatedIno || !reflect.DeepEqual(got.Delegated, nativeIntervalSet(t, fixture.Dump.Delegated)) {
				t.Fatalf("decoded %+v differs from the native dump %+v", got, fixture.Dump)
			}
			// Every proper prefix is refused, including the 8-byte one, which
			// would otherwise read as a bare inode.
			for cut := range len(extra) {
				if cut == bareCreatedInodeBytes {
					continue
				}
				if _, err := DecodeCreateExtra(extra[:cut]); err == nil {
					t.Fatal("truncated native extra decoded", cut)
				}
			}
			if _, err := DecodeCreateExtra(append(bytes.Clone(extra), 0)); err == nil {
				t.Fatal("trailing byte after the native envelope decoded")
			}
		})
	}
}

// opencExtra hand-builds an openc_response_t with a caller-chosen declared
// count, independently of the decoder.
func opencExtra(version, compat uint8, ino uint64, declared uint32, pairs ...uint64) []byte {
	body := binary.LittleEndian.AppendUint64(nil, ino)
	body = binary.LittleEndian.AppendUint32(body, declared)
	for _, v := range pairs {
		body = binary.LittleEndian.AppendUint64(body, v)
	}
	out := []byte{version, compat}
	out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}

func TestDecodeCreateExtraForms(t *testing.T) {
	bare, err := DecodeCreateExtra(binary.LittleEndian.AppendUint64(nil, 0x10000000001))
	if err != nil || !reflect.DeepEqual(bare, CreateExtra{Ino: 0x10000000001}) {
		t.Fatal("bare created inode differs", bare, err)
	}
	// An empty grant is still the delegated form, distinct from the bare one.
	empty, err := DecodeCreateExtra(opencExtra(1, 1, 0x10000000001, 0))
	if err != nil || !reflect.DeepEqual(empty, CreateExtra{Ino: 0x10000000001, Delegation: true}) {
		t.Fatal("empty delegation differs", empty, err)
	}
	const top = ^uint64(0)
	many, err := DecodeCreateExtra(opencExtra(1, 1, 7, 3, 0x100, 2, 0x102, 1, top-1, 1))
	if err != nil || !reflect.DeepEqual(many, CreateExtra{Ino: 7, Delegation: true, Delegated: []InodeRange{{0x100, 2}, {0x102, 1}, {top - 1, 1}}}) {
		t.Fatal("raw delegated ranges were not preserved", many, err)
	}
}

func TestDecodeCreateExtraRejectsMalformedInput(t *testing.T) {
	const top = ^uint64(0)
	for name, tc := range map[string]struct {
		raw  []byte
		want error
	}{
		"empty blob":                   {nil, nil},
		"seven bytes":                  {make([]byte, 7), nil},
		"nine bytes":                   {make([]byte, 9), nil},
		"struct version 2":             {opencExtra(2, 1, 1, 0), wire.ErrVersion},
		"compat 2":                     {opencExtra(2, 2, 1, 0), wire.ErrVersion},
		"count without pairs":          {opencExtra(1, 1, 1, 1), wire.ErrLimit},
		"truncated pair":               {opencExtra(1, 1, 1, 1, 5), wire.ErrLimit},
		"second pair truncated":        {opencExtra(1, 1, 1, 2, 5, 1, 9), wire.ErrLimit},
		"count beyond the bound":       {opencExtra(1, 1, 1, MaxDelegatedInodeRanges+1), wire.ErrLimit},
		"huge count":                   {opencExtra(1, 1, 1, 0xffffffff, 5, 1), wire.ErrLimit},
		"pair beyond the count":        {opencExtra(1, 1, 1, 1, 5, 1, 9, 1), nil},
		"empty interval":               {opencExtra(1, 1, 1, 1, 5, 0), ErrRequest},
		"end reaches 2^64":             {opencExtra(1, 1, 1, 1, top, 1), ErrRequest},
		"end overflows":                {opencExtra(1, 1, 1, 1, top-4, 10), ErrRequest},
		"overlapping intervals":        {opencExtra(1, 1, 1, 2, 5, 4, 8, 1), ErrRequest},
		"descending intervals":         {opencExtra(1, 1, 1, 2, 9, 1, 5, 1), ErrRequest},
		"envelope shorter than fields": {[]byte{1, 1, 4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil},
	} {
		got, err := DecodeCreateExtra(tc.raw)
		if err == nil || tc.want != nil && !errors.Is(err, tc.want) || !reflect.DeepEqual(got, CreateExtra{}) {
			t.Errorf("%s: got %+v, %v; want error %v", name, got, err, tc.want)
		}
	}
	// The bound is checked before the ranges are allocated: a maximal count
	// with its pairs decodes, one more does not.
	pairs := make([]uint64, 0, 2*MaxDelegatedInodeRanges)
	for i := range uint64(MaxDelegatedInodeRanges) {
		pairs = append(pairs, 2*i+1, 1)
	}
	if got, err := DecodeCreateExtra(opencExtra(1, 1, 1, MaxDelegatedInodeRanges, pairs...)); err != nil || uint32(len(got.Delegated)) != MaxDelegatedInodeRanges {
		t.Fatal("maximal interval count refused", len(got.Delegated), err)
	}
	if _, err := DecodeCreateExtra(opencExtra(1, 1, 1, MaxDelegatedInodeRanges+1, append(pairs, 2*uint64(MaxDelegatedInodeRanges)+1, 1)...)); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("interval count above the bound was not refused", err)
	}
}
