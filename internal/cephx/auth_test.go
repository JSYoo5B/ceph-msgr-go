package cephx

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestInitialIdentityFixture(t *testing.T) {
	c, _ := NewClient("client.admin", rfcKey(t))
	const want = "0a080000000500000061646d696e0000000000000000"
	if hex.EncodeToString(c.Initial()) != want {
		t.Fatalf("identity %x", c.Initial())
	}
	if _, err := NewClient("osd.1", rfcKey(t)); err == nil {
		t.Fatal("accepted daemon identity")
	}
}

func TestRejectedAuthReplyPreservesCodeWithoutPublishingState(t *testing.T) {
	c, _ := NewClient("client.admin", rfcKey(t))
	c.GlobalID = 42
	e := wire.Encoder{}
	e.U16(0x100)
	code := int32(-13)
	e.U32(uint32(code))
	_, _, err := c.Finish(99, e.Data, true)
	var rejected *AuthenticationError
	if !errors.As(err, &rejected) || rejected.Code != code || rejected.Method != 2 || c.GlobalID != 42 || len(c.Tickets) != 0 {
		t.Fatal("rejection lost code or changed identity", err)
	}
	_, _, err = c.Finish(99, e.Data[:len(e.Data)-1], true)
	if err == nil || errors.As(err, &rejected) {
		t.Fatal("truncated code classified as rejection", err)
	}
}

func encodeTestKey(k Key) []byte {
	e := wire.Encoder{}
	e.U16(k.Type())
	e.U32(0)
	e.U32(0)
	e.U16(uint16(len(k.secret)))
	e.Raw(k.secret)
	return e.Data
}
func testTickets(t testing.TB, encryption, session Key, typ uint32, old *Ticket) []byte {
	t.Helper()
	key := wire.Encoder{}
	key.U8(1)
	key.Raw(encodeTestKey(session))
	key.U32(60)
	key.U32(0)
	p, err := encryption.Seal(4, key.Data)
	if err != nil {
		t.Fatal(err)
	}
	blob := wire.Encoder{}
	blob.U8(1)
	blob.U64(7)
	blob.Bytes([]byte("opaque ticket"))
	e := wire.Encoder{}
	e.U8(1)
	e.U32(1)
	e.U32(typ)
	e.U8(1)
	e.Bytes(p)
	if old == nil {
		e.U8(0)
		e.Bytes(blob.Data)
	} else {
		container := wire.Encoder{}
		container.Bytes(blob.Data)
		encrypted, err := old.Key.Seal(5, container.Data)
		if err != nil {
			t.Fatal(err)
		}
		e.U8(1)
		e.Bytes(encrypted)
	}
	return e.Data
}
func testAuthReply(t testing.TB, key, auth, mgr Key, old *Ticket) []byte {
	t.Helper()
	e := wire.Encoder{}
	e.U16(0x100)
	e.U32(0)
	e.Raw(testTickets(t, key, auth, ServiceAuth, old))
	secret := wire.Encoder{}
	secret.Bytes(bytes.Repeat([]byte{0x17}, 40))
	enc, err := auth.Seal(3, secret.Data)
	if err != nil {
		t.Fatal(err)
	}
	container := wire.Encoder{}
	container.Bytes(enc)
	e.Bytes(container.Data)
	e.Bytes(testTickets(t, auth, mgr, ServiceMgr, nil))
	return e.Data
}

func FuzzAuthenticationPublication(f *testing.F) {
	for _, aes := range []bool{false, true} {
		key := rfcKey(f)
		if aes {
			key = Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		}
		old := Ticket{Key: key, SecretID: 3, Blob: []byte("previous proof")}
		// Valid cryptographic seeds reach fields beyond the outer header.
		// This checks atomic state publication, not wire interoperability.
		f.Add(aes, testAuthReply(f, key, key, key, nil))
		f.Add(aes, testAuthReply(f, key, key, key, &old))
	}
	f.Fuzz(func(t *testing.T, aes bool, p []byte) {
		key := rfcKey(t)
		if aes {
			key = Key{kind: AES, secret: bytes.Repeat([]byte{0x51}, 16)}
		}
		c, err := NewClient("client.fuzz", key)
		if err != nil {
			t.Fatal(err)
		}
		c.GlobalID = 42
		for _, service := range []uint32{ServiceAuth, ServiceMgr} {
			c.Tickets[service] = Ticket{Key: key, SecretID: 3, Blob: []byte("previous proof"), Expires: time.Now().Add(time.Minute)}
		}
		before := make(map[uint32]Ticket, len(c.Tickets))
		for service, ticket := range c.Tickets {
			ticket.Blob = append([]byte(nil), ticket.Blob...)
			ticket.Key.secret = append([]byte(nil), ticket.Key.secret...)
			before[service] = ticket
		}
		_, _, err = c.Finish(99, p, true)
		if err != nil && (c.GlobalID != 42 || !reflect.DeepEqual(c.Tickets, before)) {
			t.Fatal("failed authentication published identity or ticket state")
		}
	})
}

func TestTicketRenewalAndAtomicFailure(t *testing.T) {
	key := rfcKey(t)
	auth := Key{kind: AES256K, secret: bytes.Repeat([]byte{0x21}, 32)}
	mgr := Key{kind: AES256K, secret: bytes.Repeat([]byte{0x31}, 32)}
	c, _ := NewClient("client.admin", key)
	reply := testAuthReply(t, key, auth, mgr, nil)
	session, secret, err := c.Finish(42, reply, true)
	if err != nil || session.Type() != AES256K || len(secret) != 40 {
		t.Fatal(err)
	}
	if c.GlobalID != 42 || !c.Tickets[ServiceMgr].Expires.After(time.Now()) {
		t.Fatal("ticket state")
	}
	old := c.Tickets[ServiceAuth]
	next := Key{kind: AES256K, secret: bytes.Repeat([]byte{0x41}, 32)}
	renew := testAuthReply(t, key, next, mgr, &old)
	if _, _, err := c.Finish(42, renew, true); err != nil {
		t.Fatal("renewal", err)
	}
	previous := c.Tickets[ServiceAuth].Key.Signature(nil)
	renew[len(renew)-1] ^= 1
	if _, _, err := c.Finish(99, renew, true); err == nil {
		t.Fatal("damaged renewal accepted")
	}
	if c.GlobalID != 42 || !bytes.Equal(previous, c.Tickets[ServiceAuth].Key.Signature(nil)) {
		t.Fatal("partial ticket update")
	}
}

func TestAuthorizerChallengeAndReply(t *testing.T) {
	k := rfcKey(t)
	c, _ := NewClient("client.admin", k)
	c.GlobalID = 42
	c.Tickets[ServiceMgr] = Ticket{Key: k, Blob: []byte("opaque"), Expires: time.Now().Add(time.Minute)}
	a, err := c.Authorizer(ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	challenge := wire.Encoder{}
	challenge.U8(1)
	challenge.U64(57)
	enc, err := k.Seal(0x11, challenge.Data)
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Payload(enc)
	if err != nil {
		t.Fatal(err)
	}
	d := wire.NewDecoder(p)
	d.U8()
	d.U64()
	d.U32()
	d.U8()
	d.U64()
	d.Bytes()
	plain, err := k.Open(0x10, d.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	r := wire.NewDecoder(plain)
	if r.U8() != 2 || r.U64() != a.nonce || !r.Bool() || r.U64() != 58 || r.Done() != nil {
		t.Fatal("challenge proof")
	}
	for _, valid := range []bool{true, false} {
		reply := wire.Encoder{}
		reply.U8(2)
		reply.U64(a.nonce + 1)
		if !valid {
			reply.Data[1] ^= 1
		}
		reply.Bytes(make([]byte, 40))
		enc, err := k.Seal(0x12, reply.Data)
		if err != nil {
			t.Fatal(err)
		}
		e := wire.Encoder{}
		e.Bytes(enc)
		_, err = a.Finish(e.Data, true)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid && !errors.Is(err, ErrIntegrity) {
			t.Fatal("bad nonce accepted")
		}
	}
	c.Tickets[ServiceMgr] = Ticket{Key: k, Expires: time.Now().Add(-time.Second)}
	if _, err := c.Authorizer(ServiceMgr); !errors.Is(err, ErrTicket) {
		t.Fatal("expired ticket accepted")
	}
}
