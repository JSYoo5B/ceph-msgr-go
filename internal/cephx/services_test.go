package cephx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently constructed service requests and replies follow pinned Ceph
// v20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964):
// OSD/MGR/AUTH are 0x04/0x10/0x20, and Authenticate v3 ends in other_keys.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/msgr.h#L91-L96
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxProtocol.h#L176-L202
// MON returns the requested non-AUTH services under the AUTH session key:
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxServiceHandler.cc#L329-L358

func assertServiceRequest(t *testing.T, c *Client, services uint32) {
	t.Helper()
	challenge := binary.LittleEndian.AppendUint64([]byte{1}, 123)
	p, err := c.Challenge(challenge)
	if err != nil {
		t.Fatal(err)
	}
	d := wire.NewDecoder(p)
	if d.U16() != 0x100 || d.U8() != 3 {
		t.Fatal("unexpected authentication request format")
	}
	d.U64() // client nonce
	d.U64() // keyed challenge proof
	if d.U8() != 1 {
		t.Fatal("unexpected old ticket format")
	}
	d.U64()
	d.Bytes()
	if d.U32() != services || d.Done() != nil {
		t.Fatal("request did not carry the selected other_keys mask")
	}
}

func TestClientServiceMaskAndValueCopies(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		key := Key{kind: kind, secret: bytes.Repeat([]byte{0x51}, 16)}
		if kind == AES256K {
			key.secret = bytes.Repeat([]byte{0x51}, 32)
		}
		for _, services := range []uint32{0, ServiceAuth | ServiceMgr, ServiceAuth | ServiceMgr | ServiceOSD} {
			c, err := NewClient("client.services", key, services)
			if err != nil {
				t.Fatal(err)
			}
			want := services
			if want == 0 {
				want = ServiceAuth | ServiceMgr
			}
			if c.Services() != want {
				t.Fatal("constructor lost the requested services")
			}
			assertServiceRequest(t, c, want)
			copy := *c // The coordinator clones Client before reauthentication.
			copy.GlobalID = 99
			if copy.Services() != want {
				t.Fatal("value copy lost the OSD request selection")
			}
			assertServiceRequest(t, &copy, want)
		}
	}
}

func TestUnsupportedServiceMasksRejectBeforeProducingProof(t *testing.T) {
	key := rfcKey(t)
	for _, services := range []uint32{ServiceOSD, ServiceAuth, ServiceMgr, ServiceAuth | ServiceOSD, ServiceAuth | ServiceMgr | 1, ServiceAuth | ServiceMgr | 2, ServiceAuth | ServiceMgr | 8, ServiceAuth | ServiceMgr | 64, ^uint32(0)} {
		if c, err := NewClient("client.services", key, services); err == nil || c != nil {
			t.Fatalf("unsupported service mask %#x accepted", services)
		}
	}
	for _, services := range []uint32{0, ServiceOSD, ServiceAuth | ServiceMgr | 64} {
		c, _ := NewClient("client.services", key, 0)
		c.services = services // Invalid internal state must never reach the wire.
		p, err := c.Challenge(binary.LittleEndian.AppendUint64([]byte{1}, 123))
		if err == nil || p != nil {
			t.Fatalf("invalid client mask %#x produced an authentication proof", services)
		}
	}
}

func joinedTicketReplies(replies ...[]byte) []byte {
	p := binary.LittleEndian.AppendUint32([]byte{1}, uint32(len(replies)))
	for _, reply := range replies {
		p = append(p, reply[5:]...) // Each independent control has version1/count1.
	}
	return p
}

func TestOSDTicketRenewalAndServiceAuthorizer(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		for _, secure := range []bool{false, true} {
			size := 16
			if kind == AES256K {
				size = 32
			}
			key := Key{kind: kind, secret: bytes.Repeat([]byte{0x51}, size)}
			auth := Key{kind: kind, secret: bytes.Repeat([]byte{0x61}, size)}
			osd := Key{kind: kind, secret: bytes.Repeat([]byte{0x71}, size)}
			mgr := Key{kind: kind, secret: bytes.Repeat([]byte{0x81}, size)}
			secret := []byte(nil)
			if secure {
				secret = bytes.Repeat([]byte{0x39}, 40)
			}
			c, err := NewClient("client.services", key, ServiceAuth|ServiceMgr|ServiceOSD)
			if err != nil {
				t.Fatal(err)
			}
			extra := joinedTicketReplies(testTickets(t, auth, osd, ServiceOSD, nil), testTickets(t, auth, mgr, ServiceMgr, nil))
			connection := connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, secret))
			p := authReplyWithConnection(t, key, auth, nil, connection, extra)
			if _, _, err := c.Finish(42, p, secure); err != nil || len(c.Tickets) != 3 {
				t.Fatal("OSD service ticket exchange failed", err)
			}
			a, err := c.Authorizer(ServiceOSD)
			if err != nil || !reflect.DeepEqual(a.Key(), osd) {
				t.Fatal("OSD authorizer selected another service key", err)
			}
			payload, err := a.Payload(nil)
			if err != nil {
				t.Fatal(err)
			}
			d := wire.NewDecoder(payload)
			if d.U8() != 1 || d.U64() != 42 || d.U32() != ServiceOSD || d.U8() != 1 || d.U64() != 7 || !bytes.Equal(d.Bytes(), []byte("opaque ticket")) {
				t.Fatal("OSD authorizer encoded the wrong identity or ticket")
			}
			proof := d.Bytes()
			if d.Done() != nil {
				t.Fatal("malformed OSD authorizer")
			}
			if _, err := osd.Open(0x10, proof); err != nil {
				t.Fatal("OSD authorizer used another service's cipher", err)
			}
			if _, err := mgr.Open(0x10, proof); err == nil {
				t.Fatal("OSD proof also verified under the MGR key")
			}
			if a, err := c.Authorizer(ServiceMgr); err != nil || !reflect.DeepEqual(a.Key(), mgr) {
				t.Fatal("adding OSD changed the MGR authorizer", err)
			}
			before := plaintextFuzzSnapshot(c)
			nextAuth := Key{kind: kind, secret: bytes.Repeat([]byte{0x91}, size)}
			nextOSD := Key{kind: kind, secret: bytes.Repeat([]byte{0xa1}, size)}
			oldAuth, oldOSD := c.Tickets[ServiceAuth], c.Tickets[ServiceOSD]
			osdReply := testTickets(t, nextAuth, nextOSD, ServiceOSD, &oldOSD)
			badMgr := plaintextTestTicket(t, nextAuth, ServiceMgr, []byte{2}, plaintextTestBlob(), false)
			connection = connectionSecretCiphertext(t, nextAuth, plaintextTestBytes(nil, secret))
			p = authReplyWithConnection(t, key, nextAuth, &oldAuth, connection, joinedTicketReplies(osdReply, badMgr))
			if session, output, err := c.Finish(99, p, secure); !errors.Is(err, wire.ErrVersion) || session.Type() != 0 || output != nil || !reflect.DeepEqual(*c, before) {
				t.Fatal("invalid later ticket published the renewed OSD key", err)
			}
			extra = joinedTicketReplies(osdReply, testTickets(t, nextAuth, mgr, ServiceMgr, nil))
			p = authReplyWithConnection(t, key, nextAuth, &oldAuth, connection, extra)
			if _, _, err := c.Finish(42, p, secure); err != nil || !reflect.DeepEqual(c.Tickets[ServiceOSD].Key, nextOSD) || c.Services() != ServiceAuth|ServiceMgr|ServiceOSD {
				t.Fatal("OSD renewal lost its ticket or request mask", err)
			}
			assertServiceRequest(t, c, c.Services())
		}
	}
}

func TestAuthorizerRejectsUnsupportedOrUnrequestedService(t *testing.T) {
	c, _ := NewClient("client.services", rfcKey(t), 0)
	for _, service := range []uint32{0, 1, 2, 8, ServiceAuth, ServiceOSD, ServiceOSD | ServiceMgr, 64} {
		c.Tickets[service] = Ticket{Key: c.Key, Expires: time.Now().Add(time.Minute)}
		if a, err := c.Authorizer(service); !errors.Is(err, ErrTicket) || a != nil {
			t.Fatalf("unsupported or unrequested authorizer %#x accepted", service)
		}
	}
}
