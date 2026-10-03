package integration_test

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestFixtureCommandArguments(t *testing.T) {
	labels := []string{"雪", "a\n\"b"}
	arguments := map[string]any{"labels": labels, "ids": []uint64{1, math.MaxUint64}, "enabled": false, "input": "JSON argument"}
	command, err := newCommand("pg dump", arguments)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(command.JSON, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["prefix"]) != `"pg dump"` || string(fields["ids"]) != "[1,18446744073709551615]" || string(fields["enabled"]) != "false" || string(fields["input"]) != `"JSON argument"` || command.Input != nil {
		t.Fatal("fixture JSON changed argument types or created bulk input")
	}
	var decoded []string
	if err := json.Unmarshal(fields["labels"], &decoded); err != nil || !reflect.DeepEqual(decoded, labels) {
		t.Fatal("fixture JSON changed Unicode or escaped arguments", err)
	}
	before := bytes.Clone(command.JSON)
	arguments["enabled"], labels[0] = true, "changed"
	if !bytes.Equal(command.JSON, before) {
		t.Fatal("fixture command retains caller-owned arguments")
	}
}
