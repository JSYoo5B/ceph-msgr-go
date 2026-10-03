package cephmsgr

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// Literal Ceph key headers: type U16, zero creation utime_t, key length U16.
// The bodies are the existing AES fixture and public RFC 8009 AES256K vector.
// The canonical text layout follows fixed Tentacle KeyRing::print; actual
// daemon-generated keyrings are independently exercised by integration tests.
const (
	keyringAES     = "AQAAAAAAAAAAABAAMDEyMzQ1Njc4OWFiY2RlZg=="
	keyringAES256K = "AgAAAAAAAAAAACAAbUBNN/r3n53w0zVo0yBmmADrSDZHLqigJtFrcYJGDFI="
)

func TestParseKeyringCanonicalKeysAndInputOwnership(t *testing.T) {
	text := "[client.aes]\n\tkey = " + keyringAES + "\n\tcaps mon = \"allow r\"\n" +
		"[client.aes256k]\n\tkey = " + keyringAES256K + "\n\tpending key = " + keyringAES + "\n\tcaps mgr = \"allow command \\\"key = ignored\\\"\"\n" +
		"[unrelated]\n\tkey = not-a-supported-key\n"
	for _, tc := range []struct {
		name, text string
	}{
		{"canonical LF", text},
		{"CRLF", strings.ReplaceAll(text, "\n", "\r\n")},
		{"no final newline", strings.TrimSuffix(text, "\n")},
		{"comments and whitespace", " \t# comment\n; comment\n\n" + strings.ReplaceAll(text, "\tkey = ", " \tkey\t=\t")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.text)
			before := bytes.Clone(data)
			for _, selected := range []struct {
				identity, encoded string
				kind              uint16
			}{{"client.aes", keyringAES, 1}, {"client.aes256k", keyringAES256K, 2}} {
				want, err := ParseKey(selected.encoded)
				if err != nil {
					t.Fatal("literal credential is invalid", err)
				}
				got, err := ParseKeyring(data, selected.identity)
				if err != nil || !reflect.DeepEqual(got, want) || got.value.Type() != selected.kind {
					t.Fatal("selected the wrong active key or key family", selected.identity, err)
				}
				if !bytes.Equal(data, before) {
					t.Fatal("parser modified caller input")
				}
				owned := bytes.Clone(data)
				independent, err := ParseKeyring(owned, selected.identity)
				clear(owned)
				if err != nil || !reflect.DeepEqual(independent, want) {
					t.Fatal("returned key aliases caller input", err)
				}
			}
		})
	}
}

func TestParseKeyringRequiresExactFullIdentity(t *testing.T) {
	data := []byte("[client.management.extra]\n\tkey = " + keyringAES)
	for _, identity := range []string{"client.management", "client.management.extra ", "client.other"} {
		got, err := ParseKeyring(data, identity)
		if !reflect.DeepEqual(got, Key{}) || !errors.Is(err, ErrKeyringIdentityNotFound) {
			t.Fatal("identity selection inferred a default or prefix", identity, err)
		}
	}
	for _, identity := range []string{"", "management.extra", "client.", "mon.a"} {
		got, err := ParseKeyring(data, identity)
		if !reflect.DeepEqual(got, Key{}) || err == nil {
			t.Fatal("non-client or incomplete identity accepted", identity, err)
		}
	}
}

func TestParseKeyringErrorsReturnZeroAndPreserveKeyCause(t *testing.T) {
	valid := "[client.selected]\n\tkey = " + keyringAES + "\n"
	_, keyCause := ParseKey("invalid-active-key")
	for _, tc := range []struct {
		name, text string
		notFound   bool
		keyCause   bool
	}{
		{"empty keyring", "", true, false},
		{"other identity only", "[client.other]\n\tkey = " + keyringAES, true, false},
		{"caps without active key", "[client.selected]\n\tcaps mon = \"allow *\"", false, false},
		{"pending is not active", "[client.selected]\n\tpending key = " + keyringAES256K, false, false},
		{"empty active key", "[client.selected]\n\tkey =", false, true},
		{"invalid active key", "[client.selected]\n\tkey = invalid-active-key", false, true},
		{"quoted key is not canonical", "[client.selected]\n\tkey = \"" + keyringAES + "\"", false, true},
		{"duplicate section", valid + valid, false, false},
		{"duplicate active key", valid + "\tkey = " + keyringAES256K, false, false},
		{"duplicate unrelated section", valid + "[other]\nkey = invalid\n[other]\nkey = invalid", false, false},
		{"duplicate unrelated key", valid + "[other]\nkey = invalid\nkey = invalid", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseKeyring([]byte(tc.text), "client.selected")
			if !reflect.DeepEqual(got, Key{}) || err == nil || errors.Is(err, ErrKeyringIdentityNotFound) != tc.notFound {
				t.Fatal("keyring failure returned a key or wrong selection cause", err)
			}
			if tc.keyCause && !errors.Is(err, keyCause) {
				t.Fatal("active-key parsing lost the existing ParseKey cause", err)
			}
		})
	}
}
