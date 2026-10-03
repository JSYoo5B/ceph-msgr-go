package integration_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestFixtureKeyringSelection(t *testing.T) {
	// Literal Ceph key headers and public test bodies: AES and RFC 8009 AES256K.
	const aes = "AQAAAAAAAAAAABAAMDEyMzQ1Njc4OWFiY2RlZg=="
	const aes256k = "AgAAAAAAAAAAACAAbUBNN/r3n53w0zVo0yBmmADrSDZHLqigJtFrcYJGDFI="
	text := "[client.aes]\n\tkey = " + aes + "\n\tcaps mon = \"allow r\"\n" +
		"[client.aes256k]\n\tkey = " + aes256k + "\n\tpending key = " + aes + "\n" +
		"[client.unrelated]\n\tkey = unused-key-material\n"
	for _, layout := range []string{text, strings.ReplaceAll(text, "\n", "\r\n")} {
		for _, selected := range []struct{ identity, encoded string }{{"client.aes", aes}, {"client.aes256k", aes256k}} {
			data := []byte(layout)
			want, err := cephmsgr.ParseKey(selected.encoded)
			if err != nil {
				t.Fatal("literal credential", err)
			}
			got, err := parseTestKeyring(data, selected.identity)
			clear(data)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("fixture selected the wrong active credential", selected.identity, err)
			}
		}
	}
	for _, selected := range []struct{ text, identity string }{
		{text, "client.aes256k.extra"},
		{"[client.pending]\npending key = " + aes, "client.pending"},
		{"[client.duplicate]\nkey = " + aes + "\nkey = " + aes256k, "client.duplicate"},
	} {
		if got, err := parseTestKeyring([]byte(selected.text), selected.identity); err == nil || !reflect.DeepEqual(got, cephmsgr.Key{}) {
			t.Fatal("fixture selected an absent or ambiguous active credential", selected.identity)
		}
	}
}
