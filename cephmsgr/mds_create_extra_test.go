package cephmsgr

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// The reply method decodes both native forms of a created-inode extra and
// keeps the raw ranges; the vectors are the native ones bound in
// internal/mds/native_create_extra_test.go plus a bare inode.
func TestMDSReplyDecodeCreateExtra(t *testing.T) {
	for name, tc := range map[string]struct {
		hex  string
		want MDSCreateExtra
	}{
		"bare inode":       {"0100000000010000", MDSCreateExtra{Ino: 0x10000000001}},
		"native empty set": {"01010c000000000000000000000000000000", MDSCreateExtra{Delegation: true}},
		"native one range": {"01011c00000001000000000000000100000001000000000000000a00000000000000",
			MDSCreateExtra{Ino: 1, Delegation: true, Delegated: []MDSInodeRange{{Start: 1, Length: 10}}}},
	} {
		raw, err := hex.DecodeString(tc.hex)
		if err != nil {
			t.Fatal(err)
		}
		got, err := MDSReply{Extra: raw}.DecodeCreateExtra()
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v, %v", name, got, err)
		}
	}
	// A reply without a created inode (for example a first-time MKDIR) has no
	// extra and does not decode as inode 0.
	if got, err := (MDSReply{}).DecodeCreateExtra(); err == nil {
		t.Fatal("empty extra decoded", got)
	}
}

// TestCephFSDelegInoIsDecodeOnly binds what the DELEG_INO declaration means
// here: the CephFS profile (and only it) declares the bit, a reply carrying a
// delegated range reaches the caller raw, and the client still refuses the
// ASYNC flag and never presets an inode, so no delegated inode is ever used.
func TestCephFSDelegInoIsDecodeOnly(t *testing.T) {
	c, _, s, peer, ctx := cephfsOpen(t)
	if !MDSFeatureBitsetHas(c.MDSClientFeatures(), mds.FeatureDelegIno) || MDSFeatureBitsetHas(MDSSessionClientFeatures(), mds.FeatureDelegIno) {
		t.Fatal("DELEG_INO must be declared by the CephFS profile only")
	}
	for _, flags := range []uint32{mds.RequestFlagAsync, mds.RequestFlagAsync | MDSRequestFlagWantDentry, mds.RequestFlagAsync | mds.RequestFlagReplay} {
		if call, err := s.Submit(ctx, MDSRequest{Op: MDSOpCreate, Flags: flags, Path: MDSFilePath{Ino: 1, Path: "async"}}); err == nil || call != nil {
			t.Fatal("ASYNC request admitted", flags)
		}
	}
	call, err := s.Submit(ctx, MDSRequest{Op: MDSOpCreate, Flags: MDSRequestFlagWantDentry, Path: MDSFilePath{Ino: 1, Path: "file"}})
	if err != nil {
		t.Fatal(err)
	}
	// The refused submissions sent nothing: the next frame is this CREATE.
	frame := peer.next(t)
	sent, err := mds.DecodeClientRequest(frame, 0)
	if err != nil || sent.Op != MDSOpCreate || frame.Transaction != call.Transaction() || sent.Flags != MDSRequestFlagWantDentry || sent.Ino != 0 {
		t.Fatalf("CREATE on the wire differs: %+v, %v", sent, err)
	}
	extra, _ := hex.DecodeString("01011c00000001000000000000000100000001000000000000000a00000000000000")
	reply := cephfsReply(MDSOpCreate, call.Transaction(), 0, true, nil)
	// cephfsReply ends with empty trace, extra and snap blobs.
	reply.Front = append(reply.Front[:len(reply.Front)-8], append(binary.LittleEndian.AppendUint32(nil, uint32(len(extra))), append(extra, 0, 0, 0, 0)...)...)
	peer.send(t, reply)
	got, err := call.WaitSafe(ctx)
	if err != nil || !bytes.Equal(got.Extra, extra) {
		t.Fatal("reply extra was not kept raw", got.Extra, err)
	}
	created, err := got.DecodeCreateExtra()
	if err != nil || !reflect.DeepEqual(created, MDSCreateExtra{Ino: 1, Delegation: true, Delegated: []MDSInodeRange{{Start: 1, Length: 10}}}) {
		t.Fatal("delegated range lost", created, err)
	}
}
