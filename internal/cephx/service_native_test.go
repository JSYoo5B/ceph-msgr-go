package cephx

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type nativeExchangeMode struct {
	ServerChallenge  uint64 `json:"server_challenge"`
	Challenge        string `json:"challenge"`
	Response         string `json:"response"`
	Reply            string `json:"reply"`
	ConnectionSecret string `json:"connection_secret"`
	ReplayAccepted   bool   `json:"replay_accepted"`
}

type nativeExchange struct {
	KeyType    uint16             `json:"key_type"`
	ServiceKey string             `json:"service_key"`
	SessionKey string             `json:"session_key"`
	TicketBlob string             `json:"ticket_blob"`
	Nonce      uint64             `json:"nonce"`
	Authorizer string             `json:"authorizer"`
	Secure     nativeExchangeMode `json:"secure"`
	CRC        nativeExchangeMode `json:"crc"`
	Refusals   map[string]struct {
		Authorizer string `json:"authorizer"`
		Accepted   bool   `json:"accepted"`
		Challenged bool   `json:"challenged"`
	} `json:"refusals"`
}

// MgrAcceptorVectors loads testdata/mgr-acceptor-native-provenance.json:
// the service side of the CephX authorizer exchange run by the native
// cephx_verify_authorizer of Ceph v20.2.4 (commit
// 7f793731f1b39eb4f465e960113d2363c311b964) against a native
// CephXTicketHandler authorizer, native refusals of altered authorizers,
// native plaintext and aes encodings, and natively assembled CRC-mode
// msgr2.1 control frames (used by internal/session). Regenerate on a Linux
// amd64 Docker host with copies of tools/mgr_acceptor_native_encoder.{py,cc}
// and tools/mon_ping_native_encoder.{py,cc} in a writable directory mounted
// at /tools and a cache or empty directory at /work:
//
//	docker run --rm -v <tools>:/tools -v <work>:/work \
//	  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
//	  python3 /tools/mgr_acceptor_native_encoder.py --work /work --wrapper /tools/mgr_acceptor_native_encoder.cc
//
// The last JSON line of its output is the provenance file (stored
// indented). All keys in it are synthetic constants; the random server
// challenges and connection secrets come from the native verifier.
func mgrAcceptorVectors(t *testing.T) (exchanges map[string]nativeExchange, plaintexts, frames map[string]string) {
	t.Helper()
	var proof struct {
		Commit       string            `json:"ceph_commit"`
		Image        string            `json:"image"`
		Architecture string            `json:"build_architecture"`
		Wrapper      string            `json:"wrapper_sha256"`
		Driver       string            `json:"build_driver_sha256"`
		Bootstrap    map[string]string `json:"bootstrap_sources_sha256"`
		Verifier     bool              `json:"native_verifier_checked"`
		Loader       bool              `json:"matching_native_loader_verified"`
		Fixtures     map[string]json.RawMessage
	}
	raw, err := os.ReadFile(filepath.Join("..", "cephx", "testdata", "mgr-acceptor-native-provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" ||
		proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" ||
		proof.Architecture != "x86_64" || !proof.Verifier || !proof.Loader || len(proof.Fixtures) != 4 {
		t.Fatal("pinned native provenance missing")
	}
	producers := map[string]string{"mgr_acceptor_native_encoder.cc": proof.Wrapper, "mgr_acceptor_native_encoder.py": proof.Driver}
	for _, path := range []string{"mon_ping_native_encoder.py", "mon_ping_native_encoder.cc"} {
		producers[path] = proof.Bootstrap[path]
	}
	for path, want := range producers {
		b, err := os.ReadFile(filepath.Join("..", "..", "tools", path))
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != want {
			t.Fatal("producer differs from retained proof", path)
		}
	}
	exchanges = map[string]nativeExchange{}
	for _, name := range []string{"exchange-aes256k", "exchange-aes"} {
		var e nativeExchange
		if err := json.Unmarshal(proof.Fixtures[name], &e); err != nil {
			t.Fatal(name, err)
		}
		exchanges[name] = e
	}
	if json.Unmarshal(proof.Fixtures["plaintexts"], &plaintexts) != nil || json.Unmarshal(proof.Fixtures["frames"], &frames) != nil {
		t.Fatal("native plaintexts or frames")
	}
	return exchanges, plaintexts, frames
}

func nativeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func nativeKey(t *testing.T, s string) Key {
	t.Helper()
	d := wire.NewDecoder(nativeHex(t, s))
	k := DecodeKey(d)
	if d.Done() != nil {
		t.Fatal("native key encoding")
	}
	return k
}

// TestServiceVerifierMatchesTheNativeVerifier repeats each native exchange
// with the Go verifier: it decodes the native ticket and authorizer,
// issues the same challenge when given the native verifier's random
// challenge (byte-equal ciphertext for aes, equal plaintext for aes256k,
// whose confounder is random), accepts the native client's response, and
// produces the native final reply when given the native connection secret
// (byte-equal for aes). A native client verifies the Go reply's structure
// through Authorizer.Finish, the same nonce and secret checks. The
// response replayed after another handshake's challenge, refused natively,
// is refused here too.
func TestServiceVerifierMatchesTheNativeVerifier(t *testing.T) {
	exchanges, _, _ := mgrAcceptorVectors(t)
	for name, e := range exchanges {
		service, session := nativeKey(t, e.ServiceKey), nativeKey(t, e.SessionKey)
		if service.Type() != e.KeyType || session.Type() != e.KeyType {
			t.Fatal(name, "key types")
		}
		lookup := func(id uint64) (Key, bool) { return service, id == 7 }
		first := nativeHex(t, e.Authorizer)
		for mode, m := range map[string]nativeExchangeMode{"secure": e.Secure, "crc": e.CRC} {
			secure := mode == "secure"
			if m.ReplayAccepted {
				t.Fatal(name, mode, "native verifier accepted a replay")
			}
			random := binary.LittleEndian.AppendUint64(nil, m.ServerChallenge)
			random = append(random, nativeHex(t, m.ConnectionSecret)...)
			v := NewServiceVerifier(ServiceMgr, lookup, secure)
			v.random = bytes.NewReader(random)
			challenge, done, err := v.Verify(first)
			if err != nil || done {
				t.Fatal(name, mode, "first round", err)
			}
			native := nativeHex(t, m.Challenge)
			if e.KeyType == AES && !bytes.Equal(challenge, native) {
				t.Fatal(name, mode, "challenge ciphertext differs")
			}
			goPlain, err := session.Open(usageAuthorizeChallenge, challenge)
			if err != nil {
				t.Fatal(err)
			}
			nativePlain, err := session.Open(usageAuthorizeChallenge, native)
			if err != nil || !bytes.Equal(goPlain, nativePlain) {
				t.Fatal(name, mode, "challenge plaintext differs", err)
			}
			reply, done, err := v.Verify(nativeHex(t, m.Response))
			if err != nil || !done {
				t.Fatal(name, mode, "native response refused", err)
			}
			nativeReply := nativeHex(t, m.Reply)
			if e.KeyType == AES && !bytes.Equal(reply, nativeReply) {
				t.Fatal(name, mode, "reply ciphertext differs")
			}
			for _, r := range [][]byte{reply, nativeReply} {
				d := wire.NewDecoder(r)
				plain, err := session.Open(usageAuthorizeReply, d.Bytes())
				want := wire.Encoder{}
				if secure {
					want.U8(2)
					want.U64(e.Nonce + 1)
					want.Bytes(nativeHex(t, m.ConnectionSecret))
				} else {
					want.U8(1)
					want.U64(e.Nonce + 1)
				}
				if err != nil || d.Done() != nil || !bytes.Equal(plain, want.Data) {
					t.Fatal(name, mode, "reply plaintext", err)
				}
			}
			ticket, _ := v.Ticket()
			if ticket.EntityType != ServiceOSD || ticket.EntityID != "3" || ticket.GlobalID != 4242 || ticket.SecretID != 7 ||
				ticket.AllowAll || ticket.Caps != "allow profile osd" || ticket.Created.Unix() != 1700000000 || ticket.Expires.Nanosecond() != 6 {
				t.Fatalf("%s %s: ticket %+v", name, mode, ticket)
			}
			if !bytes.Equal(v.ConnectionSecret(), nativeHex(t, m.ConnectionSecret)) {
				t.Fatal(name, mode, "connection secret")
			}
			// The native response, replayed into a handshake with another
			// challenge, is refused as natively.
			replay := NewServiceVerifier(ServiceMgr, lookup, secure)
			replay.random = bytes.NewReader(binary.LittleEndian.AppendUint64(nil, m.ServerChallenge+2))
			if _, _, err := replay.Verify(first); err != nil {
				t.Fatal(err)
			}
			if _, _, err := replay.Verify(nativeHex(t, m.Response)); refusalOf(t, err) != RefusedChallenge {
				t.Fatal(name, mode, "replayed native response", err)
			}
		}
	}
}

// TestServiceVerifierRefusesWhatTheNativeVerifierRefuses runs the Go
// verifier on each authorizer the native verifier refused without issuing
// a challenge.
func TestServiceVerifierRefusesWhatTheNativeVerifierRefuses(t *testing.T) {
	exchanges, _, _ := mgrAcceptorVectors(t)
	want := map[string]AuthorizerRefusal{
		"service_osd": RefusedService, "unknown_secret": RefusedUnknownSecret, "all_ones_secret": RefusedGeneralSecret,
		"global_id": RefusedGlobalID, "ticket_byte": RefusedTicket, "authorize_byte": RefusedAuthorize,
	}
	for name, e := range exchanges {
		if len(e.Refusals) != len(want) {
			t.Fatal(name, "native refusal set")
		}
		service := nativeKey(t, e.ServiceKey)
		for c, r := range e.Refusals {
			if r.Accepted || r.Challenged {
				t.Fatal(name, c, "native verifier admitted it")
			}
			v := NewServiceVerifier(ServiceMgr, func(id uint64) (Key, bool) { return service, id == 7 }, true)
			if _, _, err := v.Verify(nativeHex(t, r.Authorizer)); refusalOf(t, err) != want[c] {
				t.Errorf("%s %s: %v", name, c, err)
			}
		}
	}
}

// TestServiceStructuresMatchNativeEncodings compares the encodings the
// verifier writes and reads with native ones: the challenge and both reply
// versions byte for byte, plaintext and aes, the authorize request and the
// ticket info (with caps, and allow_all without caps) through the decoder.
func TestServiceStructuresMatchNativeEncodings(t *testing.T) {
	_, plaintexts, _ := mgrAcceptorVectors(t)
	aes := mdsTestKey(AES, 0x52)
	challenge := wire.Encoder{}
	challenge.U8(1)
	challenge.U64(0x1122334455667788)
	sealedChallenge, _ := aes.Seal(usageAuthorizeChallenge, challenge.Data)
	if !bytes.Equal(challenge.Data, nativeHex(t, plaintexts["challenge_plain"])) || !bytes.Equal(sealedChallenge, nativeHex(t, plaintexts["challenge_aes"])) {
		t.Fatal("challenge encoding differs")
	}
	for name, secret := range map[string][]byte{"reply_v1": nil, "reply_v2": bytes.Repeat([]byte{0x5a}, 64)} {
		e := wire.Encoder{}
		if secret == nil {
			e.U8(1)
			e.U64(0x0102030405060709)
		} else {
			e.U8(2)
			e.U64(0x0102030405060709)
			e.Bytes(secret)
		}
		sealed, _ := aes.Seal(usageAuthorizeReply, e.Data)
		framed := wire.Encoder{}
		framed.Bytes(sealed)
		if !bytes.Equal(e.Data, nativeHex(t, plaintexts[name+"_plain"])) || !bytes.Equal(framed.Data, nativeHex(t, plaintexts[name+"_aes"])) {
			t.Fatal(name, "encoding differs")
		}
	}
	authorize := wire.Encoder{}
	authorize.U8(2)
	authorize.U64(0x0102030405060708)
	authorize.U8(1)
	authorize.U64(0x1122334455667789)
	if !bytes.Equal(authorize.Data, nativeHex(t, plaintexts["authorize_plain"])) {
		t.Fatal("authorize encoding differs")
	}
	for name, want := range map[string]PeerTicket{
		"ticket_info_caps_plain":      {EntityType: ServiceOSD, EntityID: "3", GlobalID: 4242, Caps: "allow profile osd"},
		"ticket_info_allow_all_plain": {EntityType: ServiceOSD, EntityID: "3", GlobalID: 4242, AllowAll: true},
	} {
		blob, _ := aes.Seal(usageTicketInfo, nativeHex(t, plaintexts[name]))
		ticket, key, err := decodeTicketInfo(aes, blob)
		if err != nil || key.Type() != AES || ticket.EntityType != want.EntityType || ticket.EntityID != want.EntityID ||
			ticket.GlobalID != want.GlobalID || ticket.AllowAll != want.AllowAll || ticket.Caps != want.Caps {
			t.Fatalf("%s: %+v %v", name, ticket, err)
		}
	}
	spec := osdTicket()
	spec.session = aes
	if !bytes.Equal(encodeServiceTicketInfo(spec), nativeHex(t, plaintexts["ticket_info_caps_plain"])) {
		t.Fatal("the synthetic ticket encoder of these tests differs from native")
	}
}
