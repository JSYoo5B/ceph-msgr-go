package maps

import (
	"encoding/hex"
	"net/netip"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMgrStandbysNativeLayout(t *testing.T) {
	// Literal little-endian records follow the fixed MgrMap.h layout, rather
	// than using a Go StandbyInfo encoder to generate the decoder's input.
	// StandbyInfo v4/compat1 begins with gid and name. The first record's
	// module metadata is deliberately nonempty, followed by opaque features;
	// the second record proves that skipping the child tail preserves alignment.
	const twoStandbys = "02000000" + // std::map count
		"6500000000000000" + // map key 101
		"040140000000" + // StandbyInfo v4/compat1, 64-byte body
		"65000000000000000100000061" + // gid 101, name a
		"01000000060000006f7061717565" + // old module-name set: opaque
		"01000000" + // available ModuleInfo vector count
		"020113000000" + // ModuleInfo v2/compat1, 19-byte body
		"060000006f7061717565010000000000000000" + // name, can_run, empty error/options
		"efcdab8967452301" + // mgr_features
		"ca00000000000000" + // map key 202
		"04011d000000" + // StandbyInfo v4/compat1, 29-byte body
		"ca000000000000000100000062" + // gid 202, name b
		"00000000000000000807060504030201" // empty module collections, mgr_features
	for _, test := range []struct {
		name string
		tail string
		want []Standby
	}{
		{name: "empty", tail: "00000000"},
		{name: "two with module tail", tail: twoStandbys, want: []Standby{{Name: "a", GlobalID: 101}, {Name: "b", GlobalID: 202}}},
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
			body.Raw(tail)
			body.U32(0)                // explicit enabled modules
			body.U32(0)                // services
			body.U32(0)                // active available modules
			body.Raw(make([]byte, 44)) // empty v7–v14 MgrMap tail
			front := wire.Encoder{}
			front.Struct(14, 6, body.Data)
			mgr, err := DecodeMgr(front.Data)
			if err != nil || mgr.Epoch != 8 || mgr.GlobalID != 99 || !mgr.Available || mgr.Name != "active" || len(mgr.Addresses) != 1 || !reflect.DeepEqual(mgr.Standbys, test.want) {
				t.Fatal("native standby discovery changed map fields or lost child alignment", mgr, err)
			}
		})
	}
}
