package maps

import (
	"encoding/hex"
	"net/netip"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMgrModulesNativeLayout(t *testing.T) {
	// These literal collections follow the fixed Tentacle MgrMap.h encoding,
	// independently of a Go ModuleInfo encoder. The first v2 entry includes
	// a nonempty module_options map containing an interval ModuleOption v1;
	// the following v1 entry proves that the child tail preserves alignment.
	const modules = "02000000" +
		"06000000696f73746174" + // iostat
		"030000006e6673" // nfs
	const services = "01000000" +
		"0900000064617368626f617264" + // dashboard
		"1700000068747470733a2f2f6578616d706c652e696e76616c6964" // https://example.invalid
	const available = "02000000" +
		"020164000000" + // ModuleInfo v2/compat1, 100-byte body
		"040000007a65746100" + // zeta, can_run=false
		"0f0000006d697373696e67207061636b616765" + // missing package
		"01000000" + // module_options map count
		"08000000696e74657276616c" + // interval map key
		"010132000000" + // ModuleOption v1/compat1, 50-byte body
		"08000000696e74657276616c" + // option name
		"000000000000" + // type, level, flags
		"000000000000000000000000" + // default_value, min, max
		"00000000" + // enum_allowed
		"0000000000000000" + // desc, long_desc
		"0000000000000000" + // tags, see_also
		"01010e000000" + // ModuleInfo v1/compat1, 14-byte body
		"05000000616c7068610100000000" // alpha, can_run=true, empty error
	for _, test := range []struct {
		name      string
		tail      string
		enabled   []string
		services  map[string]string
		available []ModuleInfo
	}{
		{name: "empty collections", tail: "000000000000000000000000", services: map[string]string{}},
		{name: "enabled only", tail: modules + "0000000000000000", enabled: []string{"iostat", "nfs"}, services: map[string]string{}},
		{name: "services only", tail: "00000000" + services + "00000000", services: map[string]string{"dashboard": "https://example.invalid"}},
		{
			name:     "module prefixes and options tail",
			tail:     modules + services + available,
			enabled:  []string{"iostat", "nfs"},
			services: map[string]string{"dashboard": "https://example.invalid"},
			available: []ModuleInfo{
				{Name: "zeta", CanRun: false, ErrorString: "missing package", Options: map[string]ModuleOption{"interval": {Name: "interval"}}},
				{Name: "alpha", CanRun: true},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tail, err := hex.DecodeString(test.tail)
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
			body.Raw(tail)
			body.Raw([]byte{1, 2, 3, 4, 5, 6, 7, 8}) // v7 active_change timestamp
			body.Raw(make([]byte, 36))               // empty v8–v14 MgrMap tail
			front := wire.Encoder{}
			front.Struct(14, 6, body.Data)
			mgr, err := DecodeMgr(front.Data)
			if err != nil || mgr.Epoch != 8 || mgr.GlobalID != 99 || !mgr.Available || mgr.Name != "active" || len(mgr.Addresses) != 1 || mgr.Standbys != nil || !reflect.DeepEqual(mgr.EnabledModules, test.enabled) || !reflect.DeepEqual(mgr.Services, test.services) || !reflect.DeepEqual(mgr.AvailableModules, test.available) {
				t.Fatal("native module metadata or map fields changed", mgr, err)
			}
			clear(front.Data)
			if !reflect.DeepEqual(mgr.EnabledModules, test.enabled) || !reflect.DeepEqual(mgr.Services, test.services) || !reflect.DeepEqual(mgr.AvailableModules, test.available) {
				t.Fatal("module metadata retained receive-buffer storage", mgr)
			}
		})
	}
}
