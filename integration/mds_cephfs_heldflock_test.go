package integration_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
)

// Held whole-file FLOCK across an owned MDS restart (issue #10). The blob
// builder and probe checks below are test-only consumer code: the product
// carries MDSCapReconnect.Flock as opaque bytes and implements no lock policy.

// cephfsFilelockBlob builds the Client::_encode_filelocks layout that
// Server::recover_filelocks decodes: a u32 fcntl count and that many
// ceph_filelock records, then a u32 flock count and records. Each record is
// start, length, client, owner and pid as little-endian u64 plus the type byte.
func cephfsFilelockBlob(fcntl, flock []cephfsFilelock) []byte {
	var blob []byte
	for _, locks := range [][]cephfsFilelock{fcntl, flock} {
		blob = binary.LittleEndian.AppendUint32(blob, uint32(len(locks)))
		for _, l := range locks {
			for _, v := range []uint64{l.Start, l.Length, l.Client, l.Owner, l.PID} {
				blob = binary.LittleEndian.AppendUint64(blob, v)
			}
			blob = append(blob, l.Type)
		}
	}
	return blob
}

// TestCephFSHeldFlockBlobMatchesNativeLayout compares the builder with the
// native-encoded blob retained in the reconnect provenance (one byte-range
// fcntl lock and one whole-file flock written by Client::_encode_filelocks
// code) and checks the held-lock blob this acceptance sends.
func TestCephFSHeldFlockBlobMatchesNativeLayout(t *testing.T) {
	data, err := os.ReadFile("../internal/mds/testdata/mds-reconnect-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Fixtures map[string]struct {
			Decoded struct {
				Caps []struct {
					FlockLen int    `json:"flock_len"`
					Hex      string `json:"flockbl_hex"`
				} `json:"caps"`
			} `json:"decoded_reconnect"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	var native [][]byte
	for _, f := range proof.Fixtures {
		for _, c := range f.Decoded.Caps {
			if c.FlockLen != 0 {
				raw, err := hex.DecodeString(c.Hex)
				if err != nil || len(raw) != c.FlockLen {
					t.Fatal("native flock blob is not valid hex of its stated length", err)
				}
				native = append(native, raw)
			}
		}
	}
	if len(native) != 1 {
		t.Fatalf("expected exactly one native flock blob, found %d", len(native))
	}
	built := cephfsFilelockBlob(
		[]cephfsFilelock{{Start: 4096, Length: 8192, Client: 4100, Owner: 0x8888, PID: 4242, Type: cephFlockShared}},
		[]cephfsFilelock{{Client: 4100, Owner: 0x9999, PID: 4243, Type: cephFlockExclusive}})
	if !slices.Equal(built, native[0]) {
		t.Fatalf("builder differs from the native layout:\n got %x\nwant %x", built, native[0])
	}

	held := cephfsFilelock{Client: 4711, Owner: cephFlockGoOwner, Type: cephFlockExclusive}
	blob := cephfsFilelockBlob(nil, []cephfsFilelock{held})
	if len(blob) != 4+4+41 || binary.LittleEndian.Uint32(blob[0:4]) != 0 || binary.LittleEndian.Uint32(blob[4:8]) != 1 {
		t.Fatalf("held-lock blob counts or size differ: %x", blob)
	}
	decoded, err := cephfsDecodeFilelock(blob[8:])
	if err != nil || decoded != held {
		t.Fatal("held-lock record does not decode to the requested lock", decoded, err)
	}
	if got := cephfsFlockArgs(cephFlockExclusive, held.Owner); got[1] != cephFlockExclusive || binary.LittleEndian.Uint64(got[2:10]) != held.Owner {
		t.Fatal("acquire arguments do not name the same owner as the reconnect record")
	}
	if len(cephfsFilelockBlob(nil, nil)) != 8 {
		t.Fatal("an empty lock blob must still carry both counts")
	}
}

// heldFlockEvidence is the retained record of one held-lock restart.
type heldFlockEvidence struct {
	Directory, File string
	Ino             uint64
	GlobalID        uint64
	CephFSNonce     uint32
	NativeAddress   string
	Lock            cephfsFilelock
	AcquireTID      uint64
	UnlockTID       uint64
	// SetFileLockSubmissions counts application Submit calls of SETFILELOCK.
	SetFileLockSubmissions int
	ReconnectBlobHex       string
	NativeBefore           *nativeMDSCephFSFlockState
	ProbeBefore            []heldFlockProbe
	ProbeAfter             []heldFlockProbe
	GetBefore, GetAfter    *cephfsFilelock
	MDSBefore, MDSAfter    nativeMDSCephFSDaemon
	AcquireReplayedByMDS   *bool
	UnlockRejected         string `json:",omitempty"`
}

type heldFlockProbe struct {
	Action         string
	NativeGlobalID uint64
	RequestID      int64
	MDSGID         uint64
	MDSNonce       uint32
	MDSIncarnation int32
	Probe          nativeMDSCephFSFlockProbe
}

// heldFlockProbeNative runs the closed nonblocking native FLOCK probe and
// checks the receipt shape. The owned file must not change: the baseline is
// fixed by the first probe.
func heldFlockProbeNative(ctx context.Context, control *mdsCephFSRecoveryControl, ino uint64, wantErrno int, baseline **nativeMDSCephFSFlockState) (heldFlockProbe, error) {
	r, err := control.request(ctx, "flock-ex-nb", "go-file")
	if err != nil {
		return heldFlockProbe{}, fmt.Errorf("native flock probe: %w", err)
	}
	if r.Native == nil || r.Native.Flock == nil {
		return heldFlockProbe{}, errors.New("native flock receipt lacks its probe")
	}
	p := *r.Native.Flock
	out := heldFlockProbe{Action: r.Action, NativeGlobalID: r.Native.NativeGlobalID, RequestID: r.RequestID, MDSGID: r.MDSGID, MDSNonce: r.MDSNonce, MDSIncarnation: r.MDSIncarnation, Probe: p}
	if r.Mutation == nil || r.Mutation.State != "applied" || p.Owner != cephFlockNativeOwner || p.Errno != wantErrno || p.Acquired != (wantErrno == 0) ||
		p.Unlocked != p.Acquired || p.ElapsedNS <= 0 || p.Before != p.After || p.Before.Ino != ino || p.Before.Size != 0 || p.Before.DataHex != "" {
		return out, fmt.Errorf("native flock probe result or owned file state differs: want errno %d, probe %+v mutation %+v", wantErrno, p, r.Mutation)
	}
	if *baseline == nil {
		first := p.Before
		*baseline = &first
	} else if **baseline != p.Before {
		return out, fmt.Errorf("flock probes changed native bytes, size, version or second-resolution mtime: %+v then %+v", **baseline, p.Before)
	}
	return out, nil
}
