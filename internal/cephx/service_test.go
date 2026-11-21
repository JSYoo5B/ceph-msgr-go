package cephx

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic checks of the service-side authorizer exchange, built from
// ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.h and
// CephxProtocol.cc. Native encodings and native refusals are compared in
// service_native_test.go; nothing here is a native observation.

type serviceTicketSpec struct {
	entityType uint32
	entityID   string
	globalID   uint64
	allowAll   bool
	caps       []byte // the caps bufferlist content
	session    Key
}

func encodeServiceTicketInfo(s serviceTicketSpec) []byte {
	e := wire.Encoder{}
	e.U8(1)
	e.U8(2)
	e.U32(s.entityType)
	e.String(s.entityID)
	e.U64(s.globalID)
	e.U64(^uint64(0)) // CEPH_AUTH_UID_DEFAULT
	e.U32(1700000000)
	e.U32(5)
	e.U32(1700003600)
	e.U32(6)
	e.U8(1)
	if s.allowAll {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.Bytes(s.caps)
	e.U32(0)
	e.Raw(encodeTestKey(s.session))
	return e.Data
}

func capsText(text string) []byte {
	e := wire.Encoder{}
	e.String(text)
	return e.Data
}

// serviceFixture is a client holding a MGR service ticket sealed with a
// rotating secret, and the verifier's lookup of that one secret.
type serviceFixture struct {
	client   *Client
	secretID uint64
	service  Key
}

func newServiceFixture(t *testing.T, kind uint16, spec serviceTicketSpec) serviceFixture {
	t.Helper()
	service := mdsTestKey(kind, 0x31)
	if spec.session.Type() == 0 {
		spec.session = mdsTestKey(kind, 0x52)
	}
	blob, err := service.Seal(usageTicketInfo, encodeServiceTicketInfo(spec))
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient("client.test", mdsTestKey(kind, 0x11), 0)
	if err != nil {
		t.Fatal(err)
	}
	c.GlobalID = spec.globalID
	c.Tickets[ServiceMgr] = Ticket{Key: spec.session, SecretID: 7, Blob: blob, Expires: time.Now().Add(time.Hour)}
	return serviceFixture{client: c, secretID: 7, service: service}
}

func (f serviceFixture) lookup(id uint64) (Key, bool) {
	if id == f.secretID {
		return f.service, true
	}
	return Key{}, false
}

func osdTicket() serviceTicketSpec {
	return serviceTicketSpec{entityType: ServiceOSD, entityID: "3", globalID: 4242, caps: capsText("allow profile osd")}
}

func refusalOf(t *testing.T, err error) AuthorizerRefusal {
	t.Helper()
	var refusal *AuthorizerError
	if !errors.As(err, &refusal) {
		t.Fatalf("not an authorizer refusal: %v", err)
	}
	return refusal.Reason
}

func TestServiceVerifierCompletesTheNativeChallengeInBothModes(t *testing.T) {
	for _, kind := range []uint16{AES, AES256K} {
		for _, secure := range []bool{false, true} {
			f := newServiceFixture(t, kind, osdTicket())
			a, err := f.client.Authorizer(ServiceMgr)
			if err != nil {
				t.Fatal(err)
			}
			v := NewServiceVerifier(ServiceMgr, f.lookup, secure)
			first, err := a.Payload(nil)
			if err != nil {
				t.Fatal(err)
			}
			challenge, done, err := v.Verify(first)
			if err != nil || done || len(challenge) == 0 {
				t.Fatal("first round must challenge", done, err)
			}
			if _, ok := v.Ticket(); ok {
				t.Fatal("ticket published before the challenge response")
			}
			second, err := a.Payload(challenge)
			if err != nil {
				t.Fatal(err)
			}
			reply, done, err := v.Verify(second)
			if err != nil || !done {
				t.Fatal("challenge response refused", err)
			}
			secret, err := a.Finish(reply, secure)
			if err != nil {
				t.Fatal("client refused the reply", err)
			}
			if secure && (len(secret) != 64 || !bytes.Equal(secret, v.ConnectionSecret())) {
				t.Fatal("secure connection secret", len(secret))
			}
			if !secure && (len(secret) != 0 || len(v.ConnectionSecret()) != 0) {
				t.Fatal("CRC reply carries a secret")
			}
			ticket, ok := v.Ticket()
			if !ok || ticket.EntityType != ServiceOSD || ticket.EntityID != "3" || ticket.GlobalID != 4242 || ticket.SecretID != 7 ||
				ticket.AllowAll || ticket.Caps != "allow profile osd" || !ticket.Expires.Equal(time.Unix(1700003600, 6)) {
				t.Fatalf("ticket %+v", ticket)
			}
			if !bytes.Equal(v.SessionKey().Signature([]byte("x")), a.Key().Signature([]byte("x"))) {
				t.Fatal("session key differs from the client's")
			}
			if _, _, err := v.Verify(second); refusalOf(t, err) != RefusedRounds {
				t.Fatal("third round accepted")
			}
		}
	}
}

func TestServiceVerifierRefusesForeignAndTamperedAuthorizers(t *testing.T) {
	type mutation struct {
		name   string
		want   AuthorizerRefusal
		mutate func(first []byte) []byte
	}
	// Plaintext offsets: version 0, global ID 1..9, service 9..13, ticket
	// version 13, secret ID 14..22, blob length 22..26.
	set := func(p []byte, at int, value []byte) []byte {
		out := bytes.Clone(p)
		copy(out[at:], value)
		return out
	}
	le := func(v uint64, n int) []byte {
		e := wire.Encoder{}
		e.U64(v)
		return e.Data[:n]
	}
	for _, m := range []mutation{
		{"truncated", RefusedMalformed, func(p []byte) []byte { return p[:20] }},
		{"version", RefusedVersion, func(p []byte) []byte { return set(p, 0, []byte{2}) }},
		{"ticket version", RefusedVersion, func(p []byte) []byte { return set(p, 13, []byte{2}) }},
		{"service OSD", RefusedService, func(p []byte) []byte { return set(p, 9, le(uint64(ServiceOSD), 4)) }},
		{"all-ones secret", RefusedGeneralSecret, func(p []byte) []byte { return set(p, 14, le(^uint64(0), 8)) }},
		{"unknown secret", RefusedUnknownSecret, func(p []byte) []byte { return set(p, 14, le(8, 8)) }},
		{"global ID", RefusedGlobalID, func(p []byte) []byte { return set(p, 1, le(4243, 8)) }},
		{"ticket byte", RefusedTicket, func(p []byte) []byte { out := bytes.Clone(p); out[30] ^= 1; return out }},
		{"authorize byte", RefusedAuthorize, func(p []byte) []byte { out := bytes.Clone(p); out[len(out)-1] ^= 1; return out }},
		{"trailing", RefusedMalformed, func(p []byte) []byte { return append(bytes.Clone(p), 0) }},
	} {
		for _, kind := range []uint16{AES, AES256K} {
			f := newServiceFixture(t, kind, osdTicket())
			a, _ := f.client.Authorizer(ServiceMgr)
			first, _ := a.Payload(nil)
			v := NewServiceVerifier(ServiceMgr, f.lookup, true)
			reply, done, err := v.Verify(m.mutate(first))
			if reply != nil || done || refusalOf(t, err) != m.want {
				t.Errorf("%s/%d: reply %v done %v err %v", m.name, kind, reply != nil, done, err)
			}
			if _, ok := v.Ticket(); ok {
				t.Errorf("%s/%d published a ticket", m.name, kind)
			}
		}
	}
}

func TestServiceVerifierBindsTheResponseToItsOwnHandshake(t *testing.T) {
	f := newServiceFixture(t, AES256K, osdTicket())
	a, _ := f.client.Authorizer(ServiceMgr)
	first, _ := a.Payload(nil)
	// A response computed for another handshake's challenge is refused.
	other := NewServiceVerifier(ServiceMgr, f.lookup, true)
	otherChallenge, _, err := other.Verify(first)
	if err != nil {
		t.Fatal(err)
	}
	replayed, _ := a.Payload(otherChallenge)
	v := NewServiceVerifier(ServiceMgr, f.lookup, true)
	v.random = bytes.NewReader(bytes.Repeat([]byte{9}, 128))
	if _, _, err := v.Verify(first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := v.Verify(replayed); refusalOf(t, err) != RefusedChallenge {
		t.Fatal("replayed response accepted", err)
	}
	// Another authorizer (new nonce) of the same ticket cannot finish this
	// handshake, nor can a first-round authorizer without the challenge.
	for name, want := range map[string]AuthorizerRefusal{"new nonce": RefusedDifferentRequest, "no challenge": RefusedChallenge} {
		v := NewServiceVerifier(ServiceMgr, f.lookup, true)
		challenge, _, err := v.Verify(first)
		if err != nil {
			t.Fatal(err)
		}
		var second []byte
		if name == "new nonce" {
			b, _ := f.client.Authorizer(ServiceMgr)
			second, _ = b.Payload(challenge)
		} else {
			second = first
		}
		if _, _, err := v.Verify(second); refusalOf(t, err) != want {
			t.Fatal(name, err)
		}
	}
	// A different ticket in the second round is refused even when its
	// response matches the challenge.
	g := newServiceFixture(t, AES256K, serviceTicketSpec{entityType: ServiceOSD, entityID: "4", globalID: 4242, session: a.Key()})
	g.client.Tickets[ServiceMgr] = Ticket{Key: a.Key(), SecretID: 7, Blob: g.client.Tickets[ServiceMgr].Blob, Expires: time.Now().Add(time.Hour)}
	b, _ := g.client.Authorizer(ServiceMgr)
	b.nonce = a.nonce
	v = NewServiceVerifier(ServiceMgr, func(uint64) (Key, bool) { return f.service, true }, true)
	challenge, _, err := v.Verify(first)
	if err != nil {
		t.Fatal(err)
	}
	swapped, _ := b.Payload(challenge)
	if _, _, err := v.Verify(swapped); refusalOf(t, err) != RefusedDifferentRequest {
		t.Fatal("second-round ticket swap accepted", err)
	}
}

func TestServiceVerifierKeepsCapsAndAllowAllApartAndFailsClosed(t *testing.T) {
	complete := func(spec serviceTicketSpec) (PeerTicket, error) {
		f := newServiceFixture(t, AES, spec)
		a, _ := f.client.Authorizer(ServiceMgr)
		v := NewServiceVerifier(ServiceMgr, f.lookup, false)
		first, _ := a.Payload(nil)
		challenge, _, err := v.Verify(first)
		if err != nil {
			return PeerTicket{}, err
		}
		second, _ := a.Payload(challenge)
		if _, _, err := v.Verify(second); err != nil {
			return PeerTicket{}, err
		}
		ticket, _ := v.Ticket()
		return ticket, nil
	}
	spec := osdTicket()
	spec.allowAll, spec.caps = true, nil
	if ticket, err := complete(spec); err != nil || !ticket.AllowAll || ticket.Caps != "" {
		t.Fatal("allow_all without text", ticket, err)
	}
	spec.allowAll, spec.caps = false, nil
	if ticket, err := complete(spec); err != nil || ticket.AllowAll || ticket.Caps != "" {
		t.Fatal("empty caps", ticket, err)
	}
	for name, caps := range map[string][]byte{
		"short":    {3, 0, 0},
		"overlong": {9, 0, 0, 0, 'a'},
		"trailing": append(capsText("allow *"), 0),
	} {
		spec.caps = caps
		if _, err := complete(spec); refusalOf(t, err) != RefusedCaps {
			t.Error(name, err)
		}
	}
	spec = osdTicket()
	spec.entityID = string(bytes.Repeat([]byte{'9'}, 1025))
	if _, err := complete(spec); refusalOf(t, err) != RefusedTicket {
		t.Error("overlong entity ID", err)
	}
}
