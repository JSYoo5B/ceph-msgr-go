package maps

import (
	"errors"
	"net/netip"
	"os"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func monFixture(release byte) []byte {
	return monAuthFixture(release, 0)
}
func monAuthFixture(release byte, authEpoch uint32) []byte {
	e := wire.Encoder{}
	e.Raw([]byte("0123456789abcdef"))
	e.U32(7)
	e.Raw(make([]byte, 16))
	features := wire.Encoder{}
	features.U64(0)
	e.Struct(1, 1, features.Data)
	e.Struct(1, 1, features.Data)
	e.U32(1)
	e.String("a")
	info := wire.Encoder{}
	info.String("a")
	msgr.EncodeAddresses(&info, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}})
	e.Struct(6, 1, info.Data)
	e.U32(1)
	e.String("a")
	e.U8(release)
	e.U32(0) // removed ranks
	e.U8(0)
	e.U32(0) // disallowed leaders
	e.U8(0)
	e.String("")
	e.U32(0) // stretch marked-down monitors
	e.U32(authEpoch)
	e.U32(2)
	e.U32(1)
	e.U32(2)
	e.U32(2)
	mapBlob := wire.Encoder{}
	mapBlob.Struct(10, 6, e.Data)
	out := wire.Encoder{}
	out.Bytes(mapBlob.Data)
	return out.Data
}
func TestMonMapReleaseAndAddresses(t *testing.T) {
	m, err := DecodeMon(monFixture(20))
	if err != nil || m.Epoch != 7 || len(m.Addresses) != 1 {
		t.Fatal(m, err)
	}
	if _, err := DecodeMon(monFixture(19)); !errors.Is(err, ErrRelease) {
		t.Fatal("Squid accepted")
	}
	for n := 0; n < len(monFixture(20)); n++ {
		if _, err := DecodeMon(monFixture(20)[:n]); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
}

// TestRealTentacleMonMap decodes testdata/monmap-v20.2.4.bin, the unmodified
// `ceph mon getmap -o FILE` output of a disposable integration/cluster.sh
// cluster (image quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9,
// commit 7f793731f1b39eb4f465e960113d2363c311b964): one MON "a", epoch 1,
// minimum release 20, msgr2 address 127.0.0.1:33300/0, aes256k, a fixed test
// FSID and no keys or tickets. File SHA-256:
// 76ba70850239a649e2938dc18aa0a3984b55ea4dac97bb621aea4a4b20e61b79.
// It is daemon output, not a Go encoding or an upstream fixture, and a later
// harness change does not rewrite it.
func TestRealTentacleMonMap(t *testing.T) {
	blob, err := os.ReadFile("testdata/monmap-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	// mon getmap returns the map itself; MMonMap adds a bufferlist length.
	front := wire.Encoder{}
	front.Bytes(blob)
	m, err := DecodeMon(front.Data)
	if err != nil || m.Epoch != 1 || m.MinimumRelease != 20 || len(m.Addresses) != 1 || m.Addresses[0].Endpoint.String() != "127.0.0.1:33300" {
		t.Fatal(m, err)
	}
}

// TestRealTentacleIPv6Maps decodes two maps of a development cluster from the
// same image and commit as TestRealTentacleMonMap: 3 MON and 2 MGR on IPv6
// loopback, aes256k, secure Messenger,
// auth_allow_insecure_global_id_reclaim=false and an empty MGR module
// directory (so no module description catalog). monmap-3-ipv6-v20.2.4.bin is
// unmodified `ceph mon getmap -o FILE` output (MONs a, b, c at [::1]:33300 to
// 33302; SHA-256 febf0460156f42d875335c1349f270d684d33d162b29c3a1b8074cb323eb34d2).
// mgrmap-ipv6-v20.2.4.bin is the complete v14/compat 6 MgrMap read without
// re-encoding from the stopped MON store with
// `ceph-monstore-tool <store> get mgr -- --out FILE` (SHA-256
// aabc11827f6ff039104b47cf0305961a4aba916922f92fbc2f09eb0c741a5b2b); the
// expected values come from the same tool's `--readable` output.
func TestRealTentacleIPv6Maps(t *testing.T) {
	monBlob, err := os.ReadFile("testdata/monmap-3-ipv6-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	front := wire.Encoder{}
	front.Bytes(monBlob)
	mon, err := DecodeMon(front.Data)
	if err != nil || mon.Epoch != 1 || mon.MinimumRelease != 20 || len(mon.Addresses) != 3 {
		t.Fatal("independent IPv6 MonMap", mon, err)
	}
	for i, address := range mon.Addresses {
		if address.Type != 2 || address.Endpoint.Addr() != netip.IPv6Loopback() || address.Endpoint.Port() != uint16(33300+i) {
			t.Fatal("independent MonMap address", address)
		}
	}
	mgrBlob, err := os.ReadFile("testdata/mgrmap-ipv6-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := DecodeMgr(mgrBlob)
	// Expected fields come from ceph-monstore-tool's independent readable
	// decoding of this same daemon-generated map, not from a Go encoder.
	if err != nil || mgr.Epoch != 4 || mgr.GlobalID != 4111 || !mgr.Available || mgr.Name != "b" || len(mgr.Addresses) != 1 {
		t.Fatal("independent IPv6 MgrMap", mgr, err)
	}
	if address := mgr.Addresses[0]; address.Type != 2 || address.Endpoint.String() != "[::1]:36801" || address.Nonce != 3962530023 {
		t.Fatal("independent MgrMap address", address)
	}
	if len(mgr.Standbys) != 1 || mgr.Standbys[0].Name != "a" || mgr.Standbys[0].GlobalID != 4114 {
		t.Fatal("independent MgrMap standbys", mgr.Standbys)
	}
	if !reflect.DeepEqual(mgr.EnabledModules, []string{"iostat", "nfs", "nvmeof"}) || mgr.AvailableModules != nil {
		t.Fatal("independent MgrMap module metadata", mgr.EnabledModules, mgr.AvailableModules)
	}
}
func TestMgrUnavailableAndActive(t *testing.T) {
	for _, available := range []bool{false, true} {
		e := wire.Encoder{}
		e.U32(8)
		msgr.EncodeAddresses(&e, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("[2001:db8::1]:6800")}})
		e.U64(99)
		if available {
			e.U8(1)
		} else {
			e.U8(0)
		}
		e.String("a")
		e.U32(0)                // standby map
		e.U32(0)                // explicit enabled modules
		e.U32(0)                // services
		e.U32(0)                // active available modules
		e.Raw(make([]byte, 44)) // empty v7–v14 MgrMap tail
		out := wire.Encoder{}
		out.Struct(14, 6, e.Data)
		m, err := DecodeMgr(out.Data)
		if err != nil || m.Available != available || m.GlobalID != 99 {
			t.Fatal(m, err)
		}
	}
}
func FuzzMaps(f *testing.F) {
	f.Add(monFixture(20))
	for _, name := range []string{"monmap-v20.2.4.bin", "monmap-3-ipv6-v20.2.4.bin", "mgrmap-ipv6-v20.2.4.bin"} {
		blob, err := os.ReadFile("testdata/" + name)
		if err != nil {
			f.Fatal(err)
		}
		if name != "mgrmap-ipv6-v20.2.4.bin" {
			front := wire.Encoder{}
			front.Bytes(blob)
			blob = front.Data
		}
		f.Add(blob)
	}
	f.Fuzz(func(t *testing.T, p []byte) { DecodeMon(p); DecodeMgr(p) })
}
