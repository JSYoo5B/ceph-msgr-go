package msgr

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func encodeStringMap(e *wire.Encoder, m map[string]string, keys []string) {
	e.U32(uint32(len(keys)))
	for _, k := range keys {
		e.String(k)
		e.String(m[k])
	}
}

// serviceMapFront encodes the native layout independently of the decoder.
func serviceMapFront(daemonVersion uint8) []byte {
	daemon := wire.Encoder{}
	daemon.U64(4242)
	Address{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.5:0")}.Encode(&daemon)
	daemon.U32(9)
	daemon.U32(1700000000)
	daemon.U32(5)
	encodeStringMap(&daemon, map[string]string{"hostname": "h", "zone": "z"}, []string{"hostname", "zone"})
	if daemonVersion >= 2 {
		encodeStringMap(&daemon, map[string]string{"state": "ok"}, []string{"state"})
	}
	svc := wire.Encoder{}
	svc.U32(1)
	svc.String("inst-a")
	svc.Struct(daemonVersion, 1, daemon.Data)
	svc.String("summary text")
	body := wire.Encoder{}
	body.U32(12)
	body.U32(1700000100)
	body.U32(7)
	body.U32(1)
	body.String("ceph-msgr")
	body.Struct(1, 1, svc.Data)
	e := wire.Encoder{}
	e.Struct(1, 1, body.Data)
	return e.Data
}

func TestDecodeServiceMap(t *testing.T) {
	for _, version := range []uint8{1, 2} {
		got, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Version: 1, Front: serviceMapFront(version)}, 0)
		if err != nil {
			t.Fatal(version, err)
		}
		d := got.Services["ceph-msgr"].Daemons["inst-a"]
		if got.Epoch != 12 || got.ModifiedSec != 1700000100 || got.Services["ceph-msgr"].Summary != "summary text" || d.GID != 4242 || d.Address.Nonce != 77 || d.StartEpoch != 9 || d.Metadata["zone"] != "z" || len(got.Raw) == 0 {
			t.Fatalf("decoded map %+v", got)
		}
		if (version == 2) != (d.TaskStatus["state"] == "ok") {
			t.Fatal("task status presence follows the daemon struct version", version, d.TaskStatus)
		}
	}
	front := serviceMapFront(2)
	for name, m := range map[string]MessageData{
		"truncated": {Type: ServiceMapMessage, Front: front[:len(front)-3]},
		"trailing":  {Type: ServiceMapMessage, Front: append(append([]byte{}, front...), 0)},
		"type":      {Type: MgrDigestMessage, Front: front},
		"data":      {Type: ServiceMapMessage, Front: front, Data: []byte{1}},
	} {
		if _, err := DecodeServiceMap(m, 0); !errors.Is(err, ErrFrame) {
			t.Fatal("malformed map accepted", name, err)
		}
	}
}

func TestServiceMapSubscribeKeepsSortedKeys(t *testing.T) {
	m := ServiceMapSubscribe(5, "host")
	d := wire.NewDecoder(m.Front)
	if d.U32() != 3 {
		t.Fatal("subscription count")
	}
	var keys []string
	for i := 0; i < 3; i++ {
		keys = append(keys, d.String())
		next := d.U64()
		d.U8()
		if keys[i] == "servicemap" && next != 5 {
			t.Fatal("servicemap cursor", next)
		}
	}
	if keys[0] != "mgrmap" || keys[1] != "monmap" || keys[2] != "servicemap" || d.String() != "host" || d.Done() != nil {
		t.Fatal("subscription layout", keys)
	}
}

func TestUnspecifiedAddressFamilyIsNoEndpoint(t *testing.T) {
	encode := func(sock []byte) []byte {
		body := wire.Encoder{}
		body.U32(0)
		body.U32(0)
		body.Bytes(sock)
		e := wire.Encoder{}
		e.U8(1)
		e.Struct(1, 1, body.Data)
		return e.Data
	}
	d := wire.NewDecoder(encode(make([]byte, 16)))
	if a := DecodeAddress(d); d.Done() != nil || a.Endpoint.IsValid() {
		t.Fatal("zero AF_UNSPEC sockaddr not accepted as no endpoint", a, d.Err())
	}
	bad := make([]byte, 16)
	bad[5] = 1
	d = wire.NewDecoder(encode(bad))
	if DecodeAddress(d); d.Err() == nil {
		t.Fatal("nonzero AF_UNSPEC sockaddr accepted")
	}
}

func FuzzDecodeServiceMap(f *testing.F) {
	f.Add(serviceMapFront(1))
	f.Add(serviceMapFront(2))
	f.Fuzz(func(t *testing.T, front []byte) {
		m, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: front}, 1<<16)
		if err == nil && len(m.Raw) != len(front) {
			t.Fatal("raw map differs from the frame")
		}
	})
}
