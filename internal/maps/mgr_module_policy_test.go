package maps

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func mgrModulePolicyLiteralFront(t *testing.T, version uint8, tail string) []byte {
	t.Helper()
	data, err := hex.DecodeString(tail)
	if err != nil {
		t.Fatal(err)
	}
	body := wire.Encoder{}
	body.U32(8)
	msgr.EncodeAddresses(&body, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("[2001:db8::1]:6800")}})
	body.U64(99)
	body.U8(1)
	body.String("active")
	body.U32(0) // standbys
	body.U32(0) // enabled modules
	body.U32(0) // services
	body.U32(0) // available modules
	body.Raw(data)
	front := wire.Encoder{}
	front.Struct(version, 6, body.Data)
	return front.Data
}

func TestMgrModulePolicyNativeLayoutAndVersions(t *testing.T) {
	// Independent literals follow the fixed Tentacle MgrMap tail. These older
	// envelope versions exercise its current wire contract, not older releases.
	fields := []string{
		"0100000002000000", // v7 active_change
		"03000000" + // v8 always_on_modules: three release codes
			"14000000020000000500000063726173680c0000006465766963656865616c7468" + // 20: crash, devicehealth
			"1500000000000000" + // 21: empty set
			"ffffffff0100000006000000667574757265", // unknown release: future
		"efcdab8967452301", // v9 active_mgr_features
		"63000000",         // v10 last_failure_osd_epoch
		"02000000" + // v11 two client address vectors
			"0200000000" + // first empty
			"0201000000" + // second has one address
			"0101011c00000002000000090000001000000002000ce4c00002070000000000000000",
		"0200000008000000636c69656e742e6108000000636c69656e742e7a", // v12 client.a, client.z
		"0807060504030201", // v13 flags
		"020000000800000062616c616e6365720900000064617368626f617264", // v14 balancer, dashboard
	}
	want := map[uint32][]string{20: {"crash", "devicehealth"}, 21: nil, 0xffffffff: {"future"}}
	forced := []string{"balancer", "dashboard"}
	for version := uint8(6); version <= 14; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			front := mgrModulePolicyLiteralFront(t, version, strings.Join(fields[:int(version)-6], ""))
			mgr, err := DecodeMgr(front)
			if err != nil || mgr.Epoch != 8 || mgr.GlobalID != 99 || !mgr.Available || mgr.Name != "active" || len(mgr.Addresses) != 1 || mgr.Addresses[0].Endpoint.String() != "[2001:db8::1]:6800" {
				t.Fatal("policy tail lost map fields or client-address alignment", mgr, err)
			}
			var expected map[uint32][]string
			if version >= 8 {
				expected = want
			}
			var expectedForced []string
			if version >= 14 {
				expectedForced = forced
			}
			if !reflect.DeepEqual(mgr.AlwaysOnModules, expected) || !reflect.DeepEqual(mgr.ForceDisabledModules, expectedForced) {
				t.Fatal("policy raw codes, empty sets or ordered strings changed", mgr)
			}
			other, err := DecodeMgr(front)
			if err != nil {
				t.Fatal(err)
			}
			clear(front)
			if !reflect.DeepEqual(mgr.AlwaysOnModules, expected) || !reflect.DeepEqual(mgr.ForceDisabledModules, expectedForced) {
				t.Fatal("policy strings retained receive-buffer storage", mgr)
			}
			if version >= 8 {
				mgr.AlwaysOnModules[20][0] = "caller changed"
				delete(mgr.AlwaysOnModules, 21)
			}
			if version >= 14 {
				mgr.ForceDisabledModules[0] = "caller changed"
			}
			if !reflect.DeepEqual(other.AlwaysOnModules, expected) || !reflect.DeepEqual(other.ForceDisabledModules, expectedForced) {
				t.Fatal("decoded policy maps or sets shared storage", other)
			}
		})
	}
	// Complete empty policies are distinct from truncating their tail fields.
	empty := "0000000000000000" + "00000000" + "0000000000000000" + "00000000" +
		"00000000" + "00000000" + "0000000000000000" + "00000000"
	mgr, err := DecodeMgr(mgrModulePolicyLiteralFront(t, 14, empty))
	if err != nil || mgr.AlwaysOnModules != nil || mgr.ForceDisabledModules != nil {
		t.Fatal("empty full policy retained values", mgr, err)
	}
}

func TestMgrModulePolicyRealTentacleFixture(t *testing.T) {
	blob, err := os.ReadFile("testdata/mgrmap-ipv6-v20.2.4.bin")
	if err != nil {
		t.Fatal(err)
	}
	// These six identical policy sets were inspected in the stored daemon
	// output's binary tail; no Go encoder generated this complete v14 map.
	modules := []string{"balancer", "crash", "devicehealth", "orchestrator", "pg_autoscaler", "progress", "rbd_support", "status", "telemetry", "volumes"}
	want := make(map[uint32][]string, 6)
	for release := uint32(15); release <= 20; release++ {
		want[release] = modules
	}
	mgr, err := DecodeMgr(blob)
	if err != nil || mgr.Epoch != 4 || mgr.Name != "b" || mgr.GlobalID != 4111 || !reflect.DeepEqual(mgr.AlwaysOnModules, want) || mgr.ForceDisabledModules != nil {
		t.Fatal("recorded native MgrMap policy changed", mgr, err)
	}
}
