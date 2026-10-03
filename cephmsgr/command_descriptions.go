package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalidCommandDescriptions identifies a local schema decoding failure
// after a successful command reply. The raw Result remains available. This
// does not retire the session or make the completed command outcome unknown.
var ErrInvalidCommandDescriptions = errors.New("ceph: invalid command descriptions")

// CommandFlags preserves the server's command metadata flags, including
// unknown bits. Flags and permission strings do not grant execution rights.
type CommandFlags uint64

// Values follow Tentacle's mon_command_flags in src/mon/MonCommand.h.
const (
	CommandFlagNoForward CommandFlags = 1 << iota
	CommandFlagObsolete
	CommandFlagDeprecated
	CommandFlagManager
	CommandFlagPoll
	CommandFlagHidden
)

// CommandDescriptions is one server reply and its decoded advertised commands.
// Commands are sorted lexically by ID within this reply. IDs are opaque and
// may change on another lookup. Multiple entries can have the same Prefix.
type CommandDescriptions struct {
	Result   Result
	Commands []CommandDescription
}

// CommandDescription preserves one advertised command and its original JSON.
// The returned signature, Raw and attribute values are owned by the caller.
type CommandDescription struct {
	ID         string
	Prefix     string
	Signature  []CommandSignaturePart
	Help       string
	Module     string
	Permission string
	Flags      CommandFlags
	// Raw retains the complete descriptor JSON, including unknown metadata.
	Raw json.RawMessage
}

// CommandSignaturePart is either a literal or an argument, in server order.
// Prefix uses the space-joined literals before the first argument; later
// literals remain in Signature. Argument is nil for a literal part.
type CommandSignaturePart struct {
	Literal  string
	Argument *CommandArgument
}

// CommandArgument is a named argument with its server-advertised type.
type CommandArgument struct {
	Name string
	Type string
	// Attributes owns every original attribute value, including name/type.
	// Values such as req, n and strings are not normalized or interpreted.
	Attributes map[string]json.RawMessage
}

// MonCommandDescriptions fetches the current MON's advertised management
// command schema. A fresh command is sent on every call, using ctx and the
// normal command slots. Advertised commands can require other daemon routes
// or capabilities; this method does not validate or execute them.
func (c *Client) MonCommandDescriptions(ctx context.Context) (CommandDescriptions, error) {
	return fetchCommandDescriptions(ctx, c.MonCommand)
}

// MgrCommandDescriptions fetches the active MGR's management command schema.
// It uses the normal MGR command path, including setup, context cancellation
// and unknown outcomes, and does not cache across calls or MGR changes.
func (c *Client) MgrCommandDescriptions(ctx context.Context) (CommandDescriptions, error) {
	return fetchCommandDescriptions(ctx, c.MgrCommand)
}

func fetchCommandDescriptions(ctx context.Context, call func(context.Context, Command) (Result, error)) (CommandDescriptions, error) {
	result, err := call(ctx, Command{JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`)})
	response := CommandDescriptions{Result: result}
	if err != nil {
		return response, err
	}
	response.Commands, err = decodeCommandDescriptions(result.Data)
	if err != nil {
		return response, fmt.Errorf("%w: %w", ErrInvalidCommandDescriptions, err)
	}
	return response, nil
}

func decodeCommandDescriptions(data []byte) ([]CommandDescription, error) {
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
	commands := make([]CommandDescription, 0, len(ids))
	for _, id := range ids {
		var encoded struct {
			Signature  []json.RawMessage `json:"sig"`
			Help       string            `json:"help"`
			Module     string            `json:"module"`
			Permission string            `json:"perm"`
			Flags      CommandFlags      `json:"flags"`
		}
		raw := descriptors[id]
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return nil, fmt.Errorf("descriptor %q: %w", id, err)
		}
		command := CommandDescription{ID: id, Help: encoded.Help, Module: encoded.Module, Permission: encoded.Permission, Flags: encoded.Flags, Raw: raw}
		var literals []string
		argumentSeen := false
		for _, part := range encoded.Signature {
			part = bytes.TrimSpace(part)
			var decoded CommandSignaturePart
			if len(part) > 0 && part[0] == '"' {
				if err := json.Unmarshal(part, &decoded.Literal); err != nil {
					return nil, err
				}
				if !argumentSeen {
					literals = append(literals, decoded.Literal)
				}
			} else if len(part) > 0 && part[0] == '{' {
				argument := &CommandArgument{}
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
