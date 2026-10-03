package cephx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These independently assembled replies follow Ceph v20.2.4 at
// 7f793731f1b39eb4f465e960113d2363c311b964; they test parsing and publication,
// not live interoperability. CRC requests a zero-length connection secret:
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/Auth.h#L147-L154
// MON encrypts that empty string and sends extra service tickets separately:
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxServiceHandler.cc#L309-L345
// MGR replies encode version 1/nonce only when the connection secret is empty:
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxProtocol.h#L324-L346

func connectionSecretCiphertext(t *testing.T, key Key, plaintext []byte) []byte {
	t.Helper()
	p, err := key.Seal(3, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	return plaintextTestBytes(nil, p)
}

func authReplyWithConnection(t *testing.T, key, auth Key, old *Ticket, connection, extra []byte) []byte {
	t.Helper()
	p := binary.LittleEndian.AppendUint16(nil, 0x100)
	p = binary.LittleEndian.AppendUint32(p, 0)
	p = append(p, testTickets(t, key, auth, ServiceAuth, old)...)
	p = plaintextTestBytes(p, connection)
	return plaintextTestBytes(p, extra)
}

func TestMONConnectionSecretModeRequirements(t *testing.T) {
	for _, cipher := range []struct {
		name string
		kind uint16
		size int
	}{{"aes", AES, 16}, {"aes256k", AES256K, 32}} {
		t.Run(cipher.name, func(t *testing.T) {
			key := Key{kind: cipher.kind, secret: bytes.Repeat([]byte{0x51}, cipher.size)}
			auth := Key{kind: cipher.kind, secret: bytes.Repeat([]byte{0x61}, cipher.size)}
			full := bytes.Repeat([]byte{0x39}, 40)
			oversized := binary.LittleEndian.AppendUint32(nil, wire.DefaultLimit+1)
			for _, control := range []struct {
				name       string
				connection []byte
				secret     []byte
				want       error
				valid      bool
			}{
				{"absent", nil, nil, nil, true},
				{"encrypted-empty", connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, nil)), nil, nil, true},
				{"complete", connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, full)), full, nil, true},
				{"short-secret", connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, full[:39])), nil, ErrIntegrity, false},
				{"empty-ciphertext", plaintextTestBytes(nil, nil), nil, ErrIntegrity, false},
				{"missing-inner-length", connectionSecretCiphertext(t, auth, nil), nil, io.ErrUnexpectedEOF, false},
				{"truncated-inner-length", connectionSecretCiphertext(t, auth, []byte{0, 0}), nil, io.ErrUnexpectedEOF, false},
				{"oversized-inner-length", connectionSecretCiphertext(t, auth, oversized), nil, wire.ErrLimit, false},
				{"trailing-inner-data", connectionSecretCiphertext(t, auth, []byte{0, 0, 0, 0, 1}), nil, nil, false},
			} {
				for _, requireSecret := range []bool{false, true} {
					mode := "crc"
					if requireSecret {
						mode = "secure"
					}
					t.Run(control.name+"/"+mode, func(t *testing.T) {
						c := plaintextTestClient(t, key)
						before := plaintextFuzzSnapshot(c)
						extra := testTickets(t, auth, key, ServiceMgr, nil)
						p := authReplyWithConnection(t, key, auth, nil, control.connection, extra)
						session, secret, err := c.Finish(99, p, requireSecret)
						valid, want := control.valid, control.want
						if valid && requireSecret && len(control.secret) == 0 {
							valid, want = false, ErrIntegrity
							if control.name == "absent" {
								want = io.ErrUnexpectedEOF
							}
						}
						if valid {
							if err != nil || !reflect.DeepEqual(session, auth) || !bytes.Equal(secret, control.secret) || c.GlobalID != 99 || !reflect.DeepEqual(c.Tickets[ServiceMgr].Key, key) || c.Tickets[ServiceMgr].SecretID != 7 {
								t.Fatal("valid authentication did not publish complete credentials", err)
							}
						} else if err == nil || want != nil && !errors.Is(err, want) || session.Type() != 0 || secret != nil || !reflect.DeepEqual(*c, before) {
							t.Fatal("invalid authentication changed state or lost its cause", err)
						}
					})
				}
			}
		})
	}
}

func TestCRCAuthenticationRenewalVerifiesExtraTicketsAtomically(t *testing.T) {
	for _, size := range []int{16, 32} {
		kind := uint16(AES)
		if size == 32 {
			kind = AES256K
		}
		key := Key{kind: kind, secret: bytes.Repeat([]byte{0x51}, size)}
		c := plaintextTestClient(t, key)
		for _, invalidExtra := range []bool{false, true} {
			before := plaintextFuzzSnapshot(c)
			old := c.Tickets[ServiceAuth]
			auth := Key{kind: kind, secret: bytes.Repeat([]byte{byte(old.SecretID + 60)}, size)}
			connection := connectionSecretCiphertext(t, auth, plaintextTestBytes(nil, nil))
			extra := testTickets(t, auth, key, ServiceMgr, nil)
			if invalidExtra {
				extra = []byte{2} // Authenticated MON reply, invalid extra-ticket version.
			}
			p := authReplyWithConnection(t, key, auth, &old, connection, extra)
			session, secret, err := c.Finish(99, p, false)
			if invalidExtra {
				if !errors.Is(err, wire.ErrVersion) || session.Type() != 0 || secret != nil || !reflect.DeepEqual(*c, before) {
					t.Fatal("empty CRC secret bypassed atomic extra-ticket verification", err)
				}
			} else if err != nil || len(secret) != 0 || c.GlobalID != 99 || !reflect.DeepEqual(session, auth) || !reflect.DeepEqual(c.Tickets[ServiceAuth].Key, auth) || c.Tickets[ServiceMgr].SecretID != 7 {
				t.Fatal("CRC renewal failed to publish verified tickets", err)
			}
		}
	}
}

func TestAuthorizerConnectionSecretModeRequirements(t *testing.T) {
	for _, cipher := range []struct {
		name string
		kind uint16
		size int
	}{{"aes", AES, 16}, {"aes256k", AES256K, 32}} {
		t.Run(cipher.name, func(t *testing.T) {
			key := Key{kind: cipher.kind, secret: bytes.Repeat([]byte{0x51}, cipher.size)}
			v1 := binary.LittleEndian.AppendUint64([]byte{1}, 124)
			v2 := binary.LittleEndian.AppendUint64([]byte{2}, 124)
			full := bytes.Repeat([]byte{0x39}, 40)
			for _, control := range []struct {
				name      string
				plaintext []byte
				secret    []byte
				valid     bool
			}{
				{"nonce-only-v1", v1, nil, true},
				{"empty-v2", plaintextTestBytes(append([]byte(nil), v2...), nil), nil, true},
				{"complete-v2", plaintextTestBytes(append([]byte(nil), v2...), full), full, true},
				{"short-v2", plaintextTestBytes(append([]byte(nil), v2...), full[:39]), nil, false},
				{"truncated-v1", v1[:8], nil, false},
				{"trailing-v1", append(append([]byte(nil), v1...), 0), nil, false},
				{"missing-v2-length", v2, nil, false},
				{"wrong-nonce-v1", binary.LittleEndian.AppendUint64([]byte{1}, 125), nil, false},
				{"unsupported-version", []byte{3}, nil, false},
			} {
				for _, requireSecret := range []bool{false, true} {
					mode := "crc"
					if requireSecret {
						mode = "secure"
					}
					t.Run(control.name+"/"+mode, func(t *testing.T) {
						a := &Authorizer{ticket: Ticket{Key: key}, nonce: 123}
						before := *a
						ciphertext, err := key.Seal(0x12, control.plaintext)
						if err != nil {
							t.Fatal(err)
						}
						secret, err := a.Finish(plaintextTestBytes(nil, ciphertext), requireSecret)
						valid := control.valid && (!requireSecret || len(control.secret) >= 40)
						if valid && (err != nil || !bytes.Equal(secret, control.secret)) || !valid && (err == nil || secret != nil) || !reflect.DeepEqual(*a, before) {
							t.Fatal("authorizer bypassed mode, nonce or complete-payload verification", err)
						}
					})
				}
			}
		})
	}
}
