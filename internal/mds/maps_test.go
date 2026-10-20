package mds

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"net/netip"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func testFSMap() msgr.MessageData {
	body := wire.Encoder{}
	body.U32(17)
	body.U32(1)
	body.U32(2)
	for _, f := range []Filesystem{{1, "one"}, {7, "binary\x00name"}} {
		item := wire.Encoder{}
		item.U32(uint32(f.ID))
		item.String(f.Name)
		body.Struct(1, 1, item.Data)
	}
	e := wire.Encoder{}
	e.U32(17)
	e.Struct(1, 1, body.Data)
	return msgr.MessageData{Type: FSMapUserMessage, Version: 1, CompatVersion: 1, Front: e.Data}
}
func emptyCompat(e *wire.Encoder) {
	for i := 0; i < 3; i++ {
		e.U64(0)
		e.U32(0)
	}
}
func testMDSMap() ([]byte, map[string]int) {
	body := wire.Encoder{}
	offsets := map[string]int{}
	mark := func(name string) { offsets[name] = 6 + len(body.Data) }
	body.U32(23)
	body.U32(0x80000112)
	body.U32(19)
	body.U32(0)
	body.U32(61)
	body.U32(301)
	body.U64(1 << 42)
	body.U32(3)
	mark("daemon_count")
	body.U32(1)
	mark("gid")
	body.U64(50)
	daemon := wire.Encoder{}
	daemon.U64(50)
	daemon.String("mds-a")
	daemon.U32(0)
	daemon.U32(7)
	daemon.U32(777)
	daemon.U64(150)
	msgr.EncodeAddresses(&daemon, []msgr.Address{{Type: 2, Nonce: 42, Endpoint: netip.MustParseAddrPort("192.0.2.10:6800")}, {Type: 2, Nonce: 43, Endpoint: netip.MustParseAddrPort("[2001:db8::10]:6801")}})
	daemon.U32(1700000002)
	daemon.U32(1)
	daemon.U32(math.MaxUint32)
	daemon.String("")
	daemon.U32(2)
	daemon.U32(2)
	daemon.U32(3)
	daemon.U64(1<<63 | 5)
	daemon.U32(7)
	daemon.U8(0)
	daemon.U64(1<<63 | 1)
	emptyCompat(&daemon)
	mark("daemon_struct")
	body.Struct(10, 4, daemon.Data)
	body.U32(2)
	body.U64(12)
	body.U64(11)
	body.U64(math.MaxUint64)
	mark("extension")
	body.U16(19)
	mark("compat")
	emptyCompat(&body)
	body.U64(10)
	body.U32(1700000000)
	body.U32(123456789)
	body.U32(1700000001)
	body.U32(987654321)
	body.U32(1)
	body.U32(2)
	body.U32(0)
	body.U32(1)
	body.U32(1)
	body.U32(0)
	body.U32(math.MaxUint32)
	body.U32(2)
	body.U32(0)
	body.U64(50)
	body.U32(1)
	body.U64(51)
	body.U32(1)
	body.U32(2)
	body.U32(1)
	body.U32(3)
	body.U32(31)
	body.U8(0xb2)
	body.U8(0xa2)
	mark("inline_bool")
	body.U8(1)
	mark("enabled_bool")
	body.U8(1)
	body.String("native-fs")
	body.U32(1)
	body.U32(4)
	body.String("balance-policy")
	body.U32(math.MaxUint32)
	body.U32(2)
	body.U8(0)
	mark("feature_bytes")
	body.U32(16)
	body.Raw([]byte{2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})
	body.String("0-2")
	body.U64(131072)
	body.U64(50)
	body.U32(2)
	body.U64(51)
	mark("last_member")
	body.U64(50)
	e := wire.Encoder{}
	e.Struct(5, 4, body.Data)
	return e.Data, offsets
}
func testMDSMessage(blob []byte) msgr.MessageData {
	e := wire.Encoder{}
	e.Raw([]byte{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255})
	e.U32(23)
	e.Bytes(blob)
	e.String("wire-map-name")
	return msgr.MessageData{Type: MDSMapMessageType, Version: 2, CompatVersion: 1, Front: e.Data}
}

func TestFSMapUserWireIdentityAndOwnership(t *testing.T) {
	m := testFSMap()
	got, err := DecodeFSMapUser(m, 4096)
	if err != nil || got.Epoch != 17 || got.LegacyClientFSCID != 1 || !reflect.DeepEqual(got.Filesystems, []Filesystem{{1, "one"}, {7, "binary\x00name"}}) {
		t.Fatal(got, err)
	}
	if !bytes.Equal(got.RawFront, m.Front) || !bytes.Equal(got.RawMap, m.Front[4:]) {
		t.Fatal("raw FSMap envelope lost")
	}
	raw := bytes.Clone(got.RawFront)
	m.Front[0] = 0
	if !bytes.Equal(got.RawFront, raw) {
		t.Fatal("FSMap aliases caller bytes")
	}
	exact := uint32(41 + len(raw))
	m.Front = raw
	if _, err := DecodeFSMapUser(m, exact); err != nil {
		t.Fatal("exact bound rejected", err)
	}
	if _, err := DecodeFSMapUser(m, exact-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("frame header escaped budget", err)
	}
}

func TestCurrentMDSMapPreservesFieldsWithoutPolicyFixups(t *testing.T) {
	blob, _ := testMDSMap()
	got, err := DecodeMDSMap(blob, uint32(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 23 || got.Flags != 0x80000112 || got.Root != 0 || got.SessionTimeout != 61 || got.SessionAutoclose != 301 || got.MaxFileSize != 1<<42 || got.MaxMDS != 3 || got.MetadataPool != 10 || got.CASPool != -1 || !reflect.DeepEqual(got.DataPools, []int64{12, 11}) {
		t.Fatal("base map fields lost", got)
	}
	if got.Created != (Time{1700000000, 123456789}) || got.Modified != (Time{1700000001, 987654321}) || got.TableServer != 1 || got.LastFailure != 19 || got.LastFailureOSDEpoch != 31 || got.StandbyCountWanted != -1 || got.OldMaxMDS != 2 || got.Name != "native-fs" || got.Balancer != "balance-policy" || got.BalanceRankMask != "0-2" || got.MaxXattrSize != 131072 {
		t.Fatal("map metadata lost", got)
	}
	if !got.InlineDataEnabled || !got.Enabled || got.EverAllowedFeatures != 0xb2 || got.ExplicitlyAllowedFeatures != 0xa2 || got.QuiesceLeader != 50 || !reflect.DeepEqual(got.QuiesceMembers, []uint64{51, 50}) || len(got.RequiredClientFeatures) != 16 || got.RequiredClientFeatures[8] != 2 {
		t.Fatal("raw features/quiesce order lost", got)
	}
	if len(got.Compat.Incompatible.Names) != 0 || got.Compat.Incompatible.Mask != 0 {
		t.Fatal("native compatibility fixups were applied")
	}
	if len(got.Daemons) != 1 {
		t.Fatal(got.Daemons)
	}
	daemon := got.Daemons[0]
	if daemon.GID != 50 || daemon.Rank != 0 || daemon.Incarnation != 7 || daemon.State != 777 || daemon.Flags != 1<<63|1 || daemon.MDSFeatures != 1<<63|5 || daemon.JoinFSCID != 7 || len(daemon.Addresses) != 2 || daemon.Addresses[1].Endpoint != netip.MustParseAddrPort("[2001:db8::10]:6801") || daemon.StandbyForRank != -1 || daemon.LaggySince != (Time{1700000002, 1}) || !reflect.DeepEqual(daemon.ExportTargets, []int32{2, 3}) {
		t.Fatal("daemon raw state/features/addresses lost", daemon)
	}
	if !reflect.DeepEqual(got.Incarnations, []RankIncarnation{{0, -1}}) || !reflect.DeepEqual(got.Up, []RankGID{{0, 50}, {1, 51}}) || !reflect.DeepEqual(got.Damaged, []int32{4}) {
		t.Fatal("rank metadata lost")
	}
	expected := bytes.Clone(got.Raw)
	blob[0] = 0
	if !bytes.Equal(got.Raw, expected) {
		t.Fatal("map aliases caller bytes")
	}
}

func TestMDSMapMessageFSIDEpochAndRawEnvelope(t *testing.T) {
	blob, _ := testMDSMap()
	m := testMDSMessage(blob)
	got, err := DecodeMDSMapMessage(m, 4096)
	if err != nil || got.Epoch != 23 || got.Map.Epoch != 23 || got.MapFSName != "wire-map-name" || !bytes.Equal(got.FSID[:], m.Front[:16]) || !bytes.Equal(got.Map.Raw, blob) {
		t.Fatal(got, err)
	}
	expected := bytes.Clone(got.RawFront)
	m.Front[16] = 0
	if !bytes.Equal(got.RawFront, expected) {
		t.Fatal("message aliases caller bytes")
	}
	if _, err := DecodeMDSMapMessage(m, 4096); err == nil {
		t.Fatal("outer/inner epoch mismatch accepted")
	}
	m.Front = expected
	limit := uint32(41 + len(expected))
	if _, err := DecodeMDSMapMessage(m, limit); err != nil {
		t.Fatal("exact bound rejected", err)
	}
	if _, err := DecodeMDSMapMessage(m, limit-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("outer frame unbounded", err)
	}
}

func TestMapsRejectMalformedEnvelopesCountsAndNestedFields(t *testing.T) {
	blob, offset := testMDSMap()
	for cut := 0; cut < len(blob); cut++ {
		if _, err := DecodeMDSMap(blob[:cut], 4096); err == nil {
			t.Fatalf("truncated MDSMap accepted %d", cut)
		}
	}
	changes := []func([]byte){func(b []byte) { b[0] = 4 }, func(b []byte) { b[1] = 3 }, func(b []byte) { binary.LittleEndian.PutUint32(b[2:6], math.MaxUint32) }, func(b []byte) { binary.LittleEndian.PutUint32(b[offset["daemon_count"]:], MaxMapEntries+1) }, func(b []byte) { b[offset["gid"]] = 51 }, func(b []byte) { b[offset["daemon_struct"]] = 9 }, func(b []byte) { b[offset["daemon_struct"]+1] = 3 }, func(b []byte) { b[offset["extension"]] = 18 }, func(b []byte) { b[offset["inline_bool"]] = 2 }, func(b []byte) { b[offset["enabled_bool"]] = 2 }, func(b []byte) { binary.LittleEndian.PutUint32(b[offset["feature_bytes"]:], 15) }, func(b []byte) { binary.LittleEndian.PutUint32(b[offset["feature_bytes"]:], MaxClientFeatureBytes+8) }, func(b []byte) { binary.LittleEndian.PutUint32(b[offset["compat"]+8:], MaxFeatureNames+1) }, func(b []byte) { binary.LittleEndian.PutUint64(b[offset["last_member"]:], 51) }}
	for i, change := range changes {
		bad := bytes.Clone(blob)
		change(bad)
		if _, err := DecodeMDSMap(bad, 4096); err == nil {
			t.Fatalf("malformed MDSMap accepted %d", i)
		}
	}
	if _, err := DecodeMDSMap(append(bytes.Clone(blob), 0), 4096); err == nil {
		t.Fatal("trailing map bytes accepted")
	}
	for _, m := range []msgr.MessageData{testFSMap(), testMDSMessage(blob)} {
		decode := func(m msgr.MessageData) error {
			if m.Type == FSMapUserMessage {
				_, err := DecodeFSMapUser(m, 4096)
				return err
			}
			_, err := DecodeMDSMapMessage(m, 4096)
			return err
		}
		for cut := 0; cut < len(m.Front); cut++ {
			bad := m
			bad.Front = m.Front[:cut]
			if err := decode(bad); err == nil {
				t.Fatal("truncated message accepted", m.Type, cut)
			}
		}
		for _, change := range []func(*msgr.MessageData){func(m *msgr.MessageData) { m.Version++ }, func(m *msgr.MessageData) { m.CompatVersion++ }, func(m *msgr.MessageData) { m.Middle = []byte{0} }, func(m *msgr.MessageData) { m.Data = []byte{0} }, func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }} {
			bad := m
			bad.Front = bytes.Clone(m.Front)
			change(&bad)
			if err := decode(bad); err == nil {
				t.Fatal("malformed map message accepted")
			}
		}
	}
	fs := testFSMap()
	for _, edit := range []func([]byte){func(b []byte) { b[0] = 0 }, func(b []byte) { b[4] = 2 }, func(b []byte) { b[5] = 0 }, func(b []byte) { binary.LittleEndian.PutUint32(b[18:22], MaxMapEntries+1) }} {
		bad := fs
		bad.Front = bytes.Clone(fs.Front)
		edit(bad.Front)
		if _, err := DecodeFSMapUser(bad, 4096); err == nil {
			t.Fatal("malformed filesystem list accepted")
		}
	}
}

func FuzzDecodeCurrentMaps(f *testing.F) {
	blob, _ := testMDSMap()
	f.Add(uint8(0), blob)
	f.Add(uint8(1), testFSMap().Front)
	f.Add(uint8(2), testMDSMessage(blob).Front)
	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) > 65536 {
			t.Skip()
		}
		switch kind % 3 {
		case 0:
			DecodeMDSMap(data, 65536)
		case 1:
			DecodeFSMapUser(msgr.MessageData{Type: FSMapUserMessage, Version: 1, CompatVersion: 1, Front: data}, 65536)
		case 2:
			DecodeMDSMapMessage(msgr.MessageData{Type: MDSMapMessageType, Version: 2, CompatVersion: 1, Front: data}, 65536)
		}
	})
}

// A fresh MFSMapUser constructor starts with compat0, but Message::encode
// normalizes it to version1 in its empty-payload path before transmission.
func TestFSMapUserRejectsNoncanonicalCompatZero(t *testing.T) {
	m := testFSMap()
	m.CompatVersion = 0
	if _, err := DecodeFSMapUser(m, 4096); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("noncanonical compat0 accepted", err)
	}
}
