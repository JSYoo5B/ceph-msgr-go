package session

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// TestAcceptFramesMatchNativeAssembly compares every control frame the
// acceptor writes, assembled by the Go CRC-mode msgr2.1 writer, with the
// same frame assembled by native Ceph v20.2.4 FrameAssembler (rev1, data
// CRC) from frames_v2.h, byte for byte. The vectors are the "frames" set of
// internal/cephx/testdata/mgr-acceptor-native-provenance.json, whose
// producer hashes the cephx package test binds. Secure-mode frames are
// covered by the frame codec's own tests, not here.
func TestAcceptFramesMatchNativeAssembly(t *testing.T) {
	var proof struct {
		Fixtures struct {
			Frames map[string]string `json:"frames"`
		} `json:"fixtures"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "cephx", "testdata", "mgr-acceptor-native-provenance.json"))
	if err != nil || json.Unmarshal(raw, &proof) != nil || len(proof.Fixtures.Frames) != 8 {
		t.Fatal("native frames", err)
	}
	a := &acceptor{
		policy: AcceptPolicy{Self: 0x10, ServerID: 9001, Mode: SecureMode, Supported: MgrOSDPeerFeatures, Peers: map[uint8]uint64{4: MgrOSDPeerRequiredFeatures},
			Addresses: []msgr.Address{{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.10:6800")}}},
		socketPeer: msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.20:51234")},
		peer:       AcceptedPeer{ServerGlobalSeq: 1},
		required:   MgrOSDPeerRequiredFeatures,
	}
	payload := []byte{1, 2, 3}
	for name, frame := range map[string]msgr.Frame{
		"hello":                  {Tag: msgr.Hello, Segments: [][]byte{a.helloPayload()}},
		"auth_reply_more":        {Tag: msgr.AuthReplyMore, Segments: [][]byte{authReplyMorePayload(payload)}},
		"auth_done":              {Tag: msgr.AuthDone, Segments: [][]byte{authDonePayload(4242, SecureMode, payload)}},
		"auth_bad_method":        {Tag: msgr.AuthBadMethod, Segments: [][]byte{a.badMethodPayload(2, errnoEACCES)}},
		"ident_missing_features": {Tag: msgr.IdentMissingFeatures, Segments: [][]byte{identMissingFeaturesPayload(1 << 21)}},
		"server_ident":           {Tag: msgr.ServerIdent, Segments: [][]byte{a.serverIdentPayload()}},
		"session_reset":          {Tag: msgr.SessionReset, Segments: [][]byte{sessionResetPayload()}},
		"compression_done":       {Tag: msgr.CompressionDone, Segments: [][]byte{compressionDonePayload()}},
	} {
		var out bytes.Buffer
		if err := msgr.NewWriter(&out, 1<<20).Write(frame); err != nil {
			t.Fatal(name, err)
		}
		native, err := hex.DecodeString(proof.Fixtures.Frames[name])
		if err != nil || !bytes.Equal(out.Bytes(), native) {
			t.Errorf("%s: Go %x, native %x", name, out.Bytes(), native)
		}
	}
}
