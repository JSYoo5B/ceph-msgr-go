package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// newCommand is a fixture consumer's JSON construction, outside the Messenger
// product. Bulk input is supplied separately through cephmsgr.Command.Input.
func newCommand(prefix string, arguments map[string]any) (cephmsgr.Command, error) {
	object := make(map[string]any, len(arguments)+1)
	for name, value := range arguments {
		object[name] = value
	}
	object["prefix"] = prefix
	data, err := json.Marshal(object)
	if err != nil {
		return cephmsgr.Command{}, err
	}
	return cephmsgr.Command{JSON: data}, nil
}

// parseTestKeyring selects the active credential from a native fixture's text
// keyring. File handling and identity selection belong to this consumer; only
// Ceph's encoded key is passed to the product's ParseKey.
func parseTestKeyring(data []byte, identity string) (cephmsgr.Key, error) {
	section, encoded := "", ""
	found := false
	for _, raw := range bytes.Split(data, []byte{'\n'}) {
		line := strings.TrimSpace(string(raw))
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			continue
		}
		if section != identity {
			continue
		}
		name, value, assignment := strings.Cut(line, "=")
		if !assignment || strings.TrimSpace(name) != "key" {
			continue
		}
		if found {
			return cephmsgr.Key{}, fmt.Errorf("fixture keyring has multiple active keys for %s", identity)
		}
		encoded, found = strings.TrimSpace(value), true
	}
	if !found {
		return cephmsgr.Key{}, fmt.Errorf("fixture keyring has no active key for %s", identity)
	}
	return cephmsgr.ParseKey(encoded)
}
