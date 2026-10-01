package cephx

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	p, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func rfcKey(t testing.TB) Key {
	return Key{kind: AES256K, secret: unhex(t, "6d404d37faf79f9df0d33568d320669800eb4836472ea8a026d16b7182460c52")}
}

func TestRFC8009Derivation(t *testing.T) {
	// RFC 8009 Appendix A, aes256-cts-hmac-sha384-192, usage 2.
	k := rfcKey(t)
	for _, tc := range []struct {
		kind byte
		n    int
		want string
	}{
		{0xaa, 32, "56ab22bee63d82d7bc5227f6773f8ea7a5eb1c825160c38312980c442e5c7e49"},
		{0x55, 24, "69b16514e3cd8e56b82010d5c73012b622c4d00ffc23ed1f"},
	} {
		if got := k.derive(2, tc.kind, tc.n); !bytes.Equal(got, unhex(t, tc.want)) {
			t.Fatalf("derived %x", got)
		}
	}
}

func TestRFC8009Encryption(t *testing.T) {
	// Official independent vectors cover empty, partial, full and multiple
	// blocks. Ceph's aes256k uses the same cryptosystem with different usages.
	k := rfcKey(t)
	for _, tc := range []struct {
		n          int
		conf, want string
	}{
		{0, "f764e9fa15c276478b2c7d0c4e5f58e4", "41f53fa5bfe7026d91faf9be959195a058707273a96a40f0a01960621ac612748b9bbfbe7eb4ce3c"},
		{6, "b80d3251c1f6471494256ffe712d0b9a", "4ed7b37c2bcac8f74f23c1cf07e62bc7b75fb3f637b9f559c7f664f69eab7b6092237526ea0d1f61cb20d69d10f2"},
		{16, "53bf8a0d105265d4e276428624ce5e63", "bc47ffec7998eb91e8115cf8d19dac4bbbe2e163e87dd37f49beca92027764f68cf51f14d798c2273f35df574d1f932e40c4ff255b36a266"},
		{21, "763e65367e864f02f55153c7e3b58af1", "40013e2df58e8751957d2878bcd2d6fe101ccfd556cb1eae79db3c3ee86429f2b2a602ac86fef6ecb647d6295fae077a1feb517508d2c16b4192e01f62"},
	} {
		p := make([]byte, tc.n)
		for i := range p {
			p[i] = byte(i)
		}
		got, err := k.encrypt(2, p, unhex(t, tc.conf))
		if err != nil {
			t.Fatal(err)
		}
		want := unhex(t, tc.want)
		if !bytes.Equal(got, want) {
			t.Fatalf("length %d: %x", tc.n, got)
		}
		plain, err := k.Decrypt(2, want)
		if err != nil || !bytes.Equal(plain, p) {
			t.Fatal("decryption", err)
		}
		if _, err := k.Decrypt(3, want); !errors.Is(err, ErrIntegrity) {
			t.Fatal("wrong usage accepted")
		}
		for _, i := range []int{0, len(want) - 1} {
			bad := append([]byte(nil), want...)
			bad[i] ^= 1
			if _, err := k.Decrypt(2, bad); !errors.Is(err, ErrIntegrity) {
				t.Fatal("tampering accepted")
			}
		}
	}
}

func TestCredentialEncodingAndRedaction(t *testing.T) {
	e := wire.Encoder{}
	e.U16(AES256K)
	e.U32(1)
	e.U32(2)
	e.U16(32)
	e.Raw(bytes.Repeat([]byte{0xab}, 32))
	encoded := base64.StdEncoding.EncodeToString(e.Data)
	k, err := ParseKey(encoded)
	if err != nil || k.Type() != AES256K {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%v %#v", k, k), "171") || strings.Contains(k.String(), encoded) {
		t.Fatal("key exposed")
	}
	for _, bad := range []string{encoded + "AA==", "invalid", base64.StdEncoding.EncodeToString(e.Data[:16])} {
		if _, err := ParseKey(bad); !errors.Is(err, ErrKey) {
			t.Fatal("invalid credential accepted")
		}
	}
}

func TestEnvelopeAndConfounder(t *testing.T) {
	k := rfcKey(t)
	payload := []byte("ticket")
	a, err := k.Seal(4, payload)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.Seal(4, payload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("confounder reused")
	}
	plain, err := k.Open(4, a)
	if err != nil || !bytes.Equal(plain, payload) {
		t.Fatal(err)
	}
	wrong, _ := k.Encrypt(4, payload)
	if _, err := k.Open(4, wrong); !errors.Is(err, ErrIntegrity) {
		t.Fatal("missing envelope accepted")
	}
}

func TestLegacyAESFixture(t *testing.T) {
	// Independent Python cryptography AES-CBC result for the Ceph IV and PKCS7.
	k := Key{kind: AES, secret: []byte("0123456789abcdef")}
	p := []byte("ticket")
	got, err := k.Encrypt(4, p)
	if err != nil {
		t.Fatal(err)
	}
	want := unhex(t, "9ae02965a46178c11a03c808b9d7e4d9")
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy fixture %x", got)
	}
	plain, err := k.Decrypt(4, want)
	if err != nil || !bytes.Equal(plain, p) {
		t.Fatal(err)
	}
}

func FuzzDecrypt(f *testing.F) {
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, p []byte) {
		for _, k := range []Key{{kind: AES, secret: make([]byte, 16)}, {kind: AES256K, secret: make([]byte, 32)}} {
			k.Decrypt(4, p)
			k.Open(4, p)
		}
	})
}
