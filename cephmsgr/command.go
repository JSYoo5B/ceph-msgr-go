package cephmsgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// NewCommand encodes a command prefix and arbitrary Ceph JSON arguments.
// prefix must contain a non-whitespace character; its original text is kept.
// arguments may be nil, but must not contain the reserved "prefix" key. The
// caller's map is not changed. Values follow encoding/json's marshaling rules.
//
// This helper does not select a daemon route or validate a command schema or
// permission. Set the returned Command.Input separately for bulk input, then
// call the desired context-based command method.
func NewCommand(prefix string, arguments map[string]any) (Command, error) {
	if strings.TrimSpace(prefix) == "" {
		return Command{}, errors.New("ceph: command JSON requires a string prefix")
	}
	if _, reserved := arguments["prefix"]; reserved {
		return Command{}, errors.New("ceph: command arguments contain reserved prefix key")
	}
	object := make(map[string]any, len(arguments)+1)
	for name, value := range arguments {
		object[name] = value
	}
	object["prefix"] = prefix
	data, err := json.Marshal(object)
	if err != nil {
		return Command{}, fmt.Errorf("ceph: encode command JSON: %w", err)
	}
	return Command{JSON: data}, nil
}
