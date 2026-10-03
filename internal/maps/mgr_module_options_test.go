package maps

import (
	"encoding/hex"
	"net/netip"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMgrModuleOptionsNativeLayout(t *testing.T) {
	// Independent literals follow fixed Tentacle ModuleOption v1 field order.
	// The second option uses a compatible appended tail; the enclosing
	// ModuleInfo also has a tail before a v1 ModuleInfo with no options.
	const fullOption = "0101b1000000" + // v1/compat1, 177-byte body
		"0b000000736368656d612d6e616d650a02210000800600000020322e357320020000002d3109000000756e626f756e64" +
		"656402000000040000006661737404000000736c6f771100000073686f7274206465736372697074696f6e1600000066" +
		"69727374206c696e650a7365636f6e64206c696e650200000005000000616c706861070000006d6f6e69746f72020000" +
		"000c0000006d67722f736f757263652f610c0000006d67722f736f757263652f7a"
	const emptyOption = "02013a000000" + // v2/compat1, 58-byte body
		"0c000000656d7074792d736368656d61fffe040302010000000000000000000000000000000000000000000000000000" +
		"000000000000aabbccdd"
	const available = "02000000" + // ModuleInfo vector count
		"020120010000" + // v2/compat1, 288-byte body
		"060000006265666f72650100000000" + // before, true, empty error
		"02000000" + // option map count
		"05000000612d6b6579" + fullOption + // a-key differs from schema-name
		"050000007a2d6b6579" + emptyOption + // z-key differs from empty-schema
		"deadbeef" + // appended ModuleInfo fields
		"01010e0000000500000061667465720100000000" // v1: after, true, empty error
	tail, err := hex.DecodeString(available)
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
	body.U32(0) // explicit enabled modules
	body.U32(0) // services
	body.Raw(tail)
	body.Raw(make([]byte, 44)) // empty v7–v14 MgrMap tail
	front := wire.Encoder{}
	front.Struct(14, 6, body.Data)
	want := []ModuleInfo{
		{
			Name: "before", CanRun: true,
			Options: map[string]ModuleOption{
				"a-key": {
					Name: "schema-name", Type: 10, Level: 2, Flags: 0x80000021,
					DefaultValue: " 2.5s ", Min: "-1", Max: "unbounded",
					EnumAllowed: []string{"fast", "slow"},
					Description: "short description", LongDescription: "first line\nsecond line",
					Tags: []string{"alpha", "monitor"}, SeeAlso: []string{"mgr/source/a", "mgr/source/z"},
				},
				"z-key": {Name: "empty-schema", Type: 255, Level: 254, Flags: 0x01020304},
			},
		},
		{Name: "after", CanRun: true},
	}
	mgr, err := DecodeMgr(front.Data)
	if err != nil || mgr.Epoch != 8 || mgr.Name != "active" || !mgr.Available || !reflect.DeepEqual(mgr.AvailableModules, want) {
		t.Fatal("raw option fields, collection order or following module changed", mgr, err)
	}
	other, err := DecodeMgr(front.Data)
	if err != nil {
		t.Fatal(err)
	}
	clear(front.Data)
	if !reflect.DeepEqual(mgr.AvailableModules, want) {
		t.Fatal("option strings retained receive-buffer storage", mgr.AvailableModules)
	}
	option := mgr.AvailableModules[0].Options["a-key"]
	option.EnumAllowed[0] = "caller changed"
	option.Tags[0] = "caller changed"
	option.SeeAlso[0] = "caller changed"
	option.DefaultValue = "caller changed"
	mgr.AvailableModules[0].Options["a-key"] = option
	delete(mgr.AvailableModules[0].Options, "z-key")
	if !reflect.DeepEqual(other.AvailableModules, want) {
		t.Fatal("decoded option maps or nested collections shared storage", other.AvailableModules)
	}
}
