package cephmsgr

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// ErrKeyringIdentityNotFound means the requested exact entity section is absent.
var ErrKeyringIdentityNotFound = errors.New("ceph: keyring identity not found")

// ParseKeyring selects the active key of an exact full client.<id> identity from
// canonical Ceph text keyrings. It accepts blank lines, whole-line #/; comments,
// spaces, tabs, CRLF and a missing final newline. Duplicate sections or active
// key assignments are rejected. Caps, pending keys and other attributes are
// ignored; keys belonging to other sections are not decoded.
//
// This is not a full Ceph configuration parser or binary-keyring reader. It
// reads no files and does not infer an identity or authorization from caps.
// The input is unchanged, and the returned Key owns its credential independently.
// Any error returns a zero Key; active-key decoding uses ParseKey.
func ParseKeyring(data []byte, identity string) (Key, error) {
	if !strings.HasPrefix(identity, "client.") || len(identity) <= 7 || len(identity) > 1024 || strings.ContainsAny(identity, "\x00\r\n") {
		return Key{}, errors.New("ceph: keyring identity must be client.<id>")
	}
	sections, keys := make(map[string]bool), make(map[string]bool)
	section, encoded := "", ""
	for index, raw := range bytes.Split(data, []byte{'\n'}) {
		line := strings.Trim(string(raw), " \t\r")
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") || len(line) <= 2 {
				return Key{}, fmt.Errorf("ceph: invalid keyring section at line %d", index+1)
			}
			section = line[1 : len(line)-1]
			if sections[section] {
				return Key{}, fmt.Errorf("ceph: duplicate keyring section at line %d", index+1)
			}
			sections[section] = true
			continue
		}
		name, value, assignment := strings.Cut(line, "=")
		if section == "" || !assignment {
			return Key{}, fmt.Errorf("ceph: invalid keyring attribute at line %d", index+1)
		}
		if strings.Trim(name, " \t") != "key" {
			continue
		}
		if keys[section] {
			return Key{}, fmt.Errorf("ceph: duplicate active key at line %d", index+1)
		}
		keys[section] = true
		if section == identity {
			encoded = strings.Trim(value, " \t")
		}
	}
	if !sections[identity] {
		return Key{}, ErrKeyringIdentityNotFound
	}
	if !keys[identity] {
		return Key{}, errors.New("ceph: keyring identity has no active key")
	}
	key, err := ParseKey(encoded)
	if err != nil {
		return Key{}, fmt.Errorf("ceph: parse active key in keyring: %w", err)
	}
	return key, nil
}
