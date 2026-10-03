package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Catalog interpretation belongs to the command consumer. These private test
// types let the native oracle exercise raw commands without a product schema API.
type catalogFlags uint64

const (
	catalogFlagManager catalogFlags = 1 << 3
	catalogFlagPoll    catalogFlags = 1 << 4
)

type commandCatalog struct {
	Result   cephmsgr.Result
	Commands []catalogEntry
}

type catalogEntry struct {
	ID, Prefix, Help, Module, Permission string
	Signature                            []catalogSignaturePart
	Flags                                catalogFlags
	Raw                                  json.RawMessage
}

type catalogSignaturePart struct {
	Literal  string
	Argument *catalogArgument
}

type catalogArgument struct {
	Name, Type string
	Attributes map[string]json.RawMessage
}

func fetchCatalog(ctx context.Context, call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)) (commandCatalog, error) {
	result, err := call(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`)})
	catalog := commandCatalog{Result: result}
	if err != nil {
		return catalog, err
	}
	catalog.Commands, err = decodeCatalog(result.Data)
	if err != nil {
		return catalog, fmt.Errorf("test catalog decode: %w", err)
	}
	return catalog, nil
}

func decodeCatalog(data []byte) ([]catalogEntry, error) {
	var descriptors map[string]json.RawMessage
	if err := json.Unmarshal(data, &descriptors); err != nil {
		return nil, err
	}
	if descriptors == nil {
		return nil, errors.New("expected command description object")
	}
	ids := make([]string, 0, len(descriptors))
	for id := range descriptors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	commands := make([]catalogEntry, 0, len(ids))
	for _, id := range ids {
		var encoded struct {
			Signature  []json.RawMessage `json:"sig"`
			Help       string            `json:"help"`
			Module     string            `json:"module"`
			Permission string            `json:"perm"`
			Flags      catalogFlags      `json:"flags"`
		}
		raw := descriptors[id]
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return nil, fmt.Errorf("descriptor %q: %w", id, err)
		}
		command := catalogEntry{ID: id, Help: encoded.Help, Module: encoded.Module, Permission: encoded.Permission, Flags: encoded.Flags, Raw: raw}
		var literals []string
		argumentSeen := false
		for _, part := range encoded.Signature {
			part = bytes.TrimSpace(part)
			var decoded catalogSignaturePart
			if len(part) > 0 && part[0] == '"' {
				if err := json.Unmarshal(part, &decoded.Literal); err != nil {
					return nil, err
				}
				if !argumentSeen {
					literals = append(literals, decoded.Literal)
				}
			} else if len(part) > 0 && part[0] == '{' {
				argument := &catalogArgument{}
				if err := json.Unmarshal(part, &argument.Attributes); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(argument.Attributes["name"], &argument.Name); err != nil {
					return nil, fmt.Errorf("argument name: %w", err)
				}
				if err := json.Unmarshal(argument.Attributes["type"], &argument.Type); err != nil {
					return nil, fmt.Errorf("argument type: %w", err)
				}
				if argument.Name == "" || argument.Type == "" {
					return nil, errors.New("argument name and type must be nonempty strings")
				}
				decoded.Argument = argument
				argumentSeen = true
			} else {
				return nil, errors.New("signature part must be a literal string or argument object")
			}
			command.Signature = append(command.Signature, decoded)
		}
		command.Prefix = strings.Join(literals, " ")
		if command.Prefix == "" {
			return nil, fmt.Errorf("descriptor %q has no command prefix", id)
		}
		commands = append(commands, command)
	}
	return commands, nil
}
