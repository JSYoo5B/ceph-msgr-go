package cephmsgr

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestNewCommandPreservesTypedArgumentsAndOwnership(t *testing.T) {
	values := []string{"雪", "a\n\"b"}
	arguments := map[string]any{"format": "json", "input": "JSON argument", "enabled": false, "ids": []uint64{1, math.MaxUint64}, "large": int64(math.MaxInt64), "labels": values}
	before := map[string]any{"format": "json", "input": "JSON argument", "enabled": false, "ids": []uint64{1, math.MaxUint64}, "large": int64(math.MaxInt64), "labels": []string{"雪", "a\n\"b"}}
	prefix := " \tpg stat\n "
	command, err := NewCommand(prefix, arguments)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(arguments, before) || command.Input != nil {
		t.Fatal("constructor changed arguments or created bulk input", arguments)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(command.JSON, &decoded); err != nil {
		t.Fatal(err)
	}
	var gotPrefix string
	if err := json.Unmarshal(decoded["prefix"], &gotPrefix); err != nil || gotPrefix != prefix {
		t.Fatal("prefix was normalized", gotPrefix, err)
	}
	if string(decoded["enabled"]) != "false" || string(decoded["ids"]) != "[1,18446744073709551615]" || string(decoded["large"]) != "9223372036854775807" || string(decoded["input"]) != `"JSON argument"` {
		t.Fatal("typed values lost precision or type", string(command.JSON))
	}
	var labels []string
	if err := json.Unmarshal(decoded["labels"], &labels); err != nil || !reflect.DeepEqual(labels, values) {
		t.Fatal("Unicode or escaped array values changed", labels, err)
	}
	encoded := append([]byte(nil), command.JSON...)
	arguments["format"], values[0] = "plain", "changed"
	delete(arguments, "enabled")
	if !bytes.Equal(command.JSON, encoded) {
		t.Fatal("command JSON aliases caller arguments")
	}
	command.JSON[0] = '!'
	if arguments["format"] != "plain" || values[0] != "changed" {
		t.Fatal("command mutation changed caller arguments")
	}
}

type commandMarshalFailure struct{ cause error }

func (v commandMarshalFailure) MarshalJSON() ([]byte, error) { return nil, v.cause }

func TestNewCommandValidationAndMarshalCause(t *testing.T) {
	for _, prefix := range []string{"", " \t\n", "\u2003"} {
		command, err := NewCommand(prefix, nil)
		if err == nil || command.JSON != nil || command.Input != nil {
			t.Fatal("blank prefix returned a command", prefix, err)
		}
	}
	arguments := map[string]any{"prefix": nil, "format": "json"}
	command, err := NewCommand("status", arguments)
	if err == nil || command.JSON != nil || command.Input != nil || len(arguments) != 2 || arguments["format"] != "json" {
		t.Fatal("reserved prefix was accepted or arguments changed", arguments, err)
	}
	cause := errors.New("custom argument cannot marshal")
	command, err = NewCommand("status", map[string]any{"option": commandMarshalFailure{cause: cause}})
	var marshal *json.MarshalerError
	if !errors.Is(err, cause) || !errors.As(err, &marshal) || command.JSON != nil || command.Input != nil {
		t.Fatal("marshal failure lost its cause or returned partial command", err)
	}
	command, err = NewCommand("status", map[string]any{"unsupported": make(chan int)})
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) || command.JSON != nil || command.Input != nil {
		t.Fatal("unsupported JSON type lost its cause", err)
	}
	command, err = NewCommand("status", nil)
	if err != nil || strings.TrimSpace(string(command.JSON)) != `{"prefix":"status"}` || command.Input != nil {
		t.Fatal("nil arguments failed", command, err)
	}
}
