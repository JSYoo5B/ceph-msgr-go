package cephmsgr

import (
	"encoding/hex"
	"reflect"
	"testing"
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
