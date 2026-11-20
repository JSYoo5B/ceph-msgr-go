package cephx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic, source-shaped checks of the MGR daemon identity. The request
// layouts follow ceph/ceph/blob/v20.2.4/src/auth/cephx/CephxProtocol.h and
// CephxClientHandler.cc; native encodings of the rotating reply are checked
// in internal/msgr against mgr-daemon-native-provenance.json.

func TestMgrDaemonIdentityNamesTheMgrEntity(t *testing.T) {
	key := rfcKey(t)
	for _, name := range []string{"client.x", "mgr.", "mgr", "osd.1", "mgr.a\nb"} {
		if c, err := NewMgrDaemon(name, key); err == nil || c != nil {
			t.Fatal("accepted MGR identity", name)
		}
	}
	if _, err := NewMgrDaemon("mgr.x", Key{}); !errors.Is(err, ErrKey) {
		t.Fatal("accepted an empty key", err)
	}
	c, err := NewMgrDaemon("mgr.x", key)
	if err != nil {
		t.Fatal(err)
	}
	if c.EntityType() != 0x10 || c.ID() != "x" || c.Services() != ServiceMon|ServiceOSD|ServiceMDS|ServiceMgr|ServiceAuth {
		t.Fatal("daemon identity", c.EntityType(), c.ID(), c.Services())
	}
	d := wire.NewDecoder(c.Initial())
	if d.U8() != 10 || d.U32() != 0x10 || d.String() != "x" || d.U64() != 0 || d.Done() != nil {
		t.Fatal("initial request does not name MGR x")
	}
	assertServiceRequest(t, c, MgrDaemonServices)
	client, err := NewClient("client.x", key, 0)
	if err != nil || client.EntityType() != 8 || client.ID() != "x" {
		t.Fatal("client identity changed", err)
	}
	// The CLIENT constructor still refuses the MON ticket a daemon requests.
	if _, err := NewClient("client.x", key, MgrDaemonServices); err == nil {
		t.Fatal("CLIENT accepted the MGR daemon mask")
	}
	if _, err := c.Authorizer(ServiceMon); !errors.Is(err, ErrTicket) {
		t.Fatal("MON authorizer built", err)
	}
}

func TestRenewalReusesTheHandshakeChallengeAndKeepsGlobalID(t *testing.T) {
	key := rfcKey(t)
	c, err := NewMgrDaemon("mgr.x", key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RenewalRequest(); !errors.Is(err, ErrTicket) {
		t.Fatal("renewal before a handshake challenge", err)
	}
	if _, err := c.Challenge(binary.LittleEndian.AppendUint64([]byte{1}, 0x5150)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Finish(4242, testAuthReply(t, key, key, key, nil), true); err != nil {
		t.Fatal(err)
	}
	p, err := c.RenewalRequest()
	if err != nil {
		t.Fatal(err)
	}
	d := wire.NewDecoder(p)
	if d.U16() != 0x100 || d.U8() != 3 {
		t.Fatal("renewal header")
	}
	nonce, proof := d.U64(), d.U64()
	challenge := wire.Encoder{}
	challenge.U64(0x5150)
	challenge.U64(nonce)
	hash := key.Signature(challenge.Data)
	var want uint64
	for i := 0; i < len(hash); i += 8 {
		want ^= binary.LittleEndian.Uint64(hash[i:])
	}
	if proof != want {
		t.Fatal("renewal proof is not keyed with the handshake challenge")
	}
	if d.U8() != 1 || d.U64() != 7 || !bytes.Equal(d.Bytes(), []byte("opaque ticket")) || d.U32() != MgrDaemonServices || d.Done() != nil {
		t.Fatal("renewal does not present the AUTH ticket and daemon mask")
	}
	// In session the MON encodes an empty connection-secret field.
	in := wire.Encoder{}
	in.U16(0x100)
	in.U32(0)
	in.Raw(testTickets(t, key, key, ServiceAuth, nil))
	in.Bytes(nil)
	in.Bytes(testTickets(t, key, key, ServiceMon, nil))
	before := c.Tickets[ServiceAuth].ReceiptIdentity()
	if err := c.FinishRenewal(in.Data); err != nil {
		t.Fatal(err)
	}
	if c.GlobalID != 4242 || c.Tickets[ServiceAuth].ReceiptIdentity() == before || c.Tickets[ServiceMon].ReceiptIdentity() == nil {
		t.Fatal("renewal lost the identity or tickets")
	}
	// A failed renewal publishes nothing.
	snapshot := c.Tickets[ServiceAuth].ReceiptIdentity()
	if err := c.FinishRenewal(in.Data[:len(in.Data)-1]); err == nil || c.Tickets[ServiceAuth].ReceiptIdentity() != snapshot {
		t.Fatal("truncated renewal accepted or partially published", err)
	}
}

func rotatingReply(t *testing.T, principal Key, status int32, ids ...uint64) []byte {
	t.Helper()
	plain := wire.Encoder{}
	plain.U8(1)
	plain.U32(uint32(len(ids)))
	for i, id := range ids {
		plain.U64(id)
		plain.U8(1)
		plain.U16(AES256K)
		plain.U32(0)
		plain.U32(0)
		plain.U16(32)
		plain.Raw(bytes.Repeat([]byte{byte(id)}, 32))
		plain.U32(uint32(1700000000 + 12*i))
		plain.U32(250000000)
	}
	var max uint64
	if len(ids) > 0 {
		max = ids[len(ids)-1]
	}
	plain.U64(max)
	ciphertext, err := principal.Seal(0x20, plain.Data)
	if err != nil {
		t.Fatal(err)
	}
	e := wire.Encoder{}
	e.U16(0x0400)
	e.U32(uint32(status))
	e.Bytes(ciphertext)
	return e.Data
}

func TestRotatingReplyDecodesAtomicallyAndLooksUpExactIDs(t *testing.T) {
	principal := rfcKey(t)
	ring, err := DecodeRotatingReply(rotatingReply(t, principal, 0, 5, 6, 7), principal)
	if err != nil || len(ring.Keys) != 3 || ring.MaxVersion != 7 {
		t.Fatal(ring, err)
	}
	if !ring.Keys[1].Expires.Equal(time.Unix(1700000012, 250000000)) {
		t.Fatal("expiration", ring.Keys[1].Expires)
	}
	if k, ok := ring.Lookup(6); !ok || !bytes.Equal(k.secret, bytes.Repeat([]byte{6}, 32)) {
		t.Fatal("lookup of a held ID")
	}
	if _, ok := ring.Lookup(4); ok {
		t.Fatal("lookup of an evicted ID")
	}
	if typ, err := ResponseType(rotatingReply(t, principal, 0, 5)); err != nil || typ != 0x0400 {
		t.Fatal("response type", typ, err)
	}
	other := Key{kind: AES256K, secret: bytes.Repeat([]byte{9}, 32)}
	if _, err := DecodeRotatingReply(rotatingReply(t, principal, 0, 5, 6, 7), other); err == nil {
		t.Fatal("decrypted with another principal")
	}
	var refused *AuthenticationError
	if _, err := DecodeRotatingReply(rotatingReply(t, principal, -13, 5), principal); !errors.As(err, &refused) || refused.Code != -13 {
		t.Fatal("CephX refusal", err)
	}
	if _, err := DecodeRotatingReply(rotatingReply(t, principal, 0, 6, 5, 7), principal); !errors.Is(err, ErrRotating) {
		t.Fatal("unordered IDs accepted", err)
	}
	valid := rotatingReply(t, principal, 0, 5, 6, 7)
	for n := range len(valid) {
		if _, err := DecodeRotatingReply(valid[:n], principal); err == nil {
			t.Fatal("truncated reply accepted", n)
		}
	}
	wrongType := append([]byte(nil), valid...)
	wrongType[1] = 0x01
	if _, err := DecodeRotatingReply(wrongType, principal); !errors.Is(err, ErrRotating) {
		t.Fatal("ticket reply accepted as rotating", err)
	}
}

func TestRotatingRefreshFollowsTheNativeMiddleKeyRule(t *testing.T) {
	ttl := 12 * time.Second
	now := time.Unix(1000, 0)
	ring := func(middle time.Duration) RotatingSecrets {
		return RotatingSecrets{Keys: []RotatingKey{{ID: 1, Expires: now.Add(middle - ttl)}, {ID: 2, Expires: now.Add(middle)}, {ID: 3, Expires: now.Add(middle + ttl)}}, MaxVersion: 3}
	}
	if !(RotatingSecrets{}).NeedsRefresh(now, ttl) || !(RotatingSecrets{Keys: ring(time.Hour).Keys[:2]}).NeedsRefresh(now, ttl) {
		t.Fatal("an undersized set needs a refresh")
	}
	// cutoff = now - min(30 s, ttl/4) = now - 3 s: a middle key that has
	// just expired is not yet due; one that expired 3 s ago is.
	for _, tc := range []struct {
		middle time.Duration
		want   bool
	}{{time.Second, false}, {0, false}, {-2 * time.Second, false}, {-3 * time.Second, true}, {-time.Minute, true}} {
		if got := ring(tc.middle).NeedsRefresh(now, ttl); got != tc.want {
			t.Fatal("middle key expiring at", tc.middle, got)
		}
	}
	if at := ring(time.Second).RefreshAt(ttl); !at.Equal(now.Add(4 * time.Second)) {
		t.Fatal("refresh time", at)
	}
	// The 30 s cap applies for long service tickets.
	if ring(-29*time.Second).NeedsRefresh(now, time.Hour) || !ring(-30*time.Second).NeedsRefresh(now, time.Hour) {
		t.Fatal("30 s cutoff cap")
	}
}
