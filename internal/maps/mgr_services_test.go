package maps

import (
	"encoding/hex"
	"net/netip"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func mgrServicesLiteralFront(t *testing.T, services string) []byte {
	t.Helper()
	// The raw services map is followed by two independent ModuleInfo literals.
	// Their names and v2/v1 envelopes make service-to-module alignment visible.
	const available = "02000000" +
		"020112000000050000006166746572010000000000000000" + // after, true, empty error/options
		"010111000000040000006c617374000400000073746f70" // last, false, stop
	tail, err := hex.DecodeString(services + available)
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
	body.Raw(tail)
	body.Raw([]byte{1, 2, 3, 4, 5, 6, 7, 8}) // unrelated outer metadata
	front := wire.Encoder{}
	front.Struct(14, 6, body.Data)
	return front.Data
}

func TestMgrServicesNativeLayout(t *testing.T) {
	// Literal little-endian map entries follow fixed Tentacle MgrMap.h, without
	// a Go service-map encoder. URI spelling is deliberately left opaque.
	const services = "03000000" +
		"0900000064617368626f617264" + // dashboard
		"2b00000048545450533a2f2f4578616d706c652e696e76616c69643a393434332f61253246623f583d312346726167" +
		"05000000656d70747900000000" + // empty: empty string
		"0a00000070726f6d657468657573" + // prometheus
		"1a000000687474703a2f2f5b323030313a6462383a3a315d3a393238332f"
	want := map[string]string{
		"dashboard":  "HTTPS://Example.invalid:9443/a%2Fb?X=1#Frag",
		"empty":      "",
		"prometheus": "http://[2001:db8::1]:9283/",
	}
	front := mgrServicesLiteralFront(t, services)
	mgr, err := DecodeMgr(front)
	if err != nil || mgr.Epoch != 8 || mgr.GlobalID != 99 || !mgr.Available || mgr.Name != "active" || len(mgr.Addresses) != 1 || !reflect.DeepEqual(mgr.Services, want) || !reflect.DeepEqual(mgr.AvailableModules, []ModuleInfo{{Name: "after", CanRun: true}, {Name: "last", ErrorString: "stop"}}) {
		t.Fatal("raw services or following module metadata changed", mgr, err)
	}
	other, err := DecodeMgr(front)
	if err != nil {
		t.Fatal(err)
	}
	clear(front)
	if !reflect.DeepEqual(mgr.Services, want) {
		t.Fatal("service strings retained receive-buffer storage", mgr.Services)
	}
	mgr.Services["dashboard"] = "caller changed"
	delete(mgr.Services, "empty")
	if !reflect.DeepEqual(other.Services, want) {
		t.Fatal("decoded service maps shared caller-owned storage", other.Services)
	}
	empty, err := DecodeMgr(mgrServicesLiteralFront(t, "00000000"))
	if err != nil || empty.Services == nil || len(empty.Services) != 0 || !reflect.DeepEqual(empty.AvailableModules, other.AvailableModules) {
		t.Fatal("empty full service map retained entries or lost following modules", empty, err)
	}
}
