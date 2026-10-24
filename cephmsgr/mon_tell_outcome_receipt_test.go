package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestMonTellOutcomeValuesRefuseCandidatesOutsideTheBound(t *testing.T) {
	for _, baseline := range []int64{-1, 0, 19999, 20000, 20001, 1 << 40} {
		if a, b, err := monTellOutcomeValues(baseline); err == nil || a != 0 || b != 0 {
			t.Fatal("baseline outside the bound produced candidates", baseline, a, b)
		}
	}
	for _, baseline := range []int64{1, 500, 19998} {
		a, b, err := monTellOutcomeValues(baseline)
		if err != nil || a != baseline+1 || b != baseline+2 || a == b || b > monTellCeiling {
			t.Fatal("in-bound baseline did not give distinct raised candidates under the ceiling", baseline, a, b, err)
		}
	}
}

func TestMonTellOutcomeCommandsAreClosed(t *testing.T) {
	set := monTellSetCommand(501, 0)
	var decoded struct {
		Prefix string   `json:"prefix"`
		Var    string   `json:"var"`
		Val    []string `json:"val"`
	}
	if err := json.Unmarshal(set.JSON, &decoded); err != nil || decoded.Prefix != "config set" || decoded.Var != "log_max_recent" || len(decoded.Val) != 1 || decoded.Val[0] != "501" {
		t.Fatal("candidate command is not the single-option daemon-local set", string(set.JSON), err)
	}
	padded := monTellSetCommand(501, 40<<10)
	if len(padded.JSON) < 40<<10 || json.Unmarshal(padded.JSON, &decoded) != nil {
		t.Fatal("padded candidate is not valid JSON of the padded size")
	}
}

func TestParseMonTellEffectiveAcceptsOnlyTheOwnedOption(t *testing.T) {
	for data, want := range map[string]int64{`{"log_max_recent":"500"}`: 500, `{"log_max_recent":501}`: 501} {
		if got, ok := parseMonTellEffective([]byte(data)); !ok || got != want {
			t.Fatal("owned option reply was not decoded", data, got, ok)
		}
	}
	for _, data := range []string{``, `500`, `{}`, `{"debug_mon":"1/5"}`, `{"log_max_recent":"x"}`,
		`{"log_max_recent":"500","debug_mon":"1/5"}`, `{"log_max_recent":null}`} {
		if got, ok := parseMonTellEffective([]byte(data)); ok {
			t.Fatal("unexpected reply accepted", data, got)
		}
	}
}

func TestObserveNativeMonTellRejectsOpenRequestsBeforePublishing(t *testing.T) {
	control := t.TempDir()
	token := strings.Repeat("a", 32)
	for _, request := range [][3]string{
		{"short", "initial", "b"}, {strings.ToUpper(token), "initial", "b"}, {token + " x", "initial", "b"},
		{token, "cleanup", "b"}, {token, "", "b"}, {token, "initial", "mon.b"}, {token, "initial", "d"},
		{token, "initial", "b extra"}, {token, "initial", ""},
	} {
		if _, err := observeNativeMonTell(context.Background(), control, request[0], request[1], request[2]); err == nil {
			t.Fatal("open request was delegated", request)
		}
	}
	if entries, err := os.ReadDir(control); err != nil || len(entries) != 0 {
		t.Fatal("rejected requests published control files", entries, err)
	}
}

// Receipt shape published by tools/mon_tell_outcome_oracle.py for an absent
// override, effective value 500 and one bracketed daemon.
const nativeMonTellReceiptSample = `{"baseline":{"effective":"500","present":false},"phase":"restored","request_id":3,"target":"b","token":"cccccccccccccccccccccccccccccccc","views":{"config_diff":{"code":0,"data_bytes":12,"data_sha256":"00","entry":null,"status":"","valid":true},"config_get":{"code":0,"data":"eyJsb2dfbWF4X3JlY2VudCI6IjUwMCJ9","status":""},"daemon_after":{"code":0,"data_bytes":900,"data_sha256":"00","name":"b","rank":1,"state":"peon","status":""},"daemon_before":{"code":0,"data_bytes":900,"data_sha256":"00","name":"b","rank":1,"state":"peon","status":""}}}`

func TestNativeMonTellReceiptSeparatesEffectiveValueFromOverride(t *testing.T) {
	var receipt nativeMonTellReceipt
	if err := json.Unmarshal([]byte(nativeMonTellReceiptSample), &receipt); err != nil {
		t.Fatal(err)
	}
	views := receipt.Views
	if value, ok := parseMonTellEffective(views.ConfigGet.Data); !ok || value != 500 {
		t.Fatal("raw effective bytes were not retained", value, ok)
	}
	if !nativeMonTellMatches(views, "b", nativeMonTellState{value: 500}) {
		t.Fatal("absent override with effective 500 was not recognized")
	}
	override := int64(500)
	// The same effective value does not establish an override.
	if nativeMonTellMatches(views, "b", nativeMonTellState{500, &override}) {
		t.Fatal("effective value alone was accepted as a runtime override")
	}
	if nativeMonTellMatches(views, "b", nativeMonTellState{value: 501}) || nativeMonTellMatches(views, "a", nativeMonTellState{value: 500}) {
		t.Fatal("wrong value or target matched")
	}
	text := "500"
	views.ConfigDiff.Entry = &nativeMonTellEntry{OverridePresent: true, Override: &text}
	if !nativeMonTellMatches(views, "b", nativeMonTellState{500, &override}) || nativeMonTellMatches(views, "b", nativeMonTellState{value: 500}) {
		t.Fatal("override presence was not compared exactly")
	}
	other := int64(501)
	if nativeMonTellMatches(views, "b", nativeMonTellState{500, &other}) {
		t.Fatal("a different override value matched")
	}
	failed := receipt.Views
	failed.ConfigDiff.Code = -13
	if nativeMonTellMatches(failed, "b", nativeMonTellState{value: 500}) {
		t.Fatal("a refused diff established override absence")
	}
	failed = receipt.Views
	failed.ConfigDiff.Valid = false
	if nativeMonTellMatches(failed, "b", nativeMonTellState{value: 500}) {
		t.Fatal("an unparsed diff established override absence")
	}
	changed := receipt.Views
	rank := int64(2)
	changed.DaemonAfter.Rank = &rank
	if _, stable := nativeMonTellStable(changed, "b"); stable {
		t.Fatal("a changed daemon rank was treated as one bracket")
	}
}

func tellFrame(t *testing.T, message msgr.MessageData) []byte {
	t.Helper()
	var encoded bytes.Buffer
	if err := msgr.NewWriter(&encoded, 128<<10).Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestMonOutcomeProbeBindsTellRequestIdentity(t *testing.T) {
	message := msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config set","var":"log_max_recent","val":["501"]}`}, nil)
	message.Transaction, message.Sequence = 23, 9
	probe := &monOutcomeWrites{}
	probe.enabled.Store(true)
	conn := &monOutcomeConn{Conn: monOutcomeSink{}, record: probe, connection: 4, peer: "mon.b"}
	_, _ = conn.Write(tellFrame(t, message))
	writes, err := probe.matching(message.Front, message.Data)
	if err != nil || len(writes) != 1 || writes[0].Type != msgr.TellCommandMessage || writes[0].Transaction != 23 || writes[0].Sequence != 9 || writes[0].Peer != "mon.b" || !writes[0].Complete {
		t.Fatal("Tell identity observation", writes, err)
	}
}

func TestMonOutcomeGateArmsOnlyTheExactTargetAndMessageType(t *testing.T) {
	tell := tellFrame(t, msgr.TellCommand([16]byte{1}, []string{`{"prefix":"config get"}`}, nil))
	monCommand := tellFrame(t, msgr.CommandMessage(false, [16]byte{1}, []string{`{"prefix":"status"}`}, nil))
	cases := []struct {
		name  string
		peer  string
		frame []byte
		armed int
	}{
		{"target tell", "mon.b", tell, 1},
		{"other mon tell", "mon.a", tell, 0},
		{"mgr peer tell", "mgr.a", tell, 0},
		{"target mon command", "mon.b", monCommand, 0},
		{"target small non-frame", "mon.b", []byte("ping"), 0},
	}
	for _, tc := range cases {
		armed := 0
		conn := &monOutcomeConn{Conn: monOutcomeSink{}, record: &monOutcomeWrites{}, peer: tc.peer,
			gate: &monOutcomeGate{Peer: "mon.b", Type: msgr.TellCommandMessage, Arm: func() { armed++ }}}
		_, _ = conn.Write(tc.frame)
		if armed != tc.armed {
			t.Fatal(tc.name, "armed", armed, "want", tc.armed)
		}
	}
	// Frames that cannot be decoded arm only through the explicit size fallback,
	// and only for the exact target.
	for _, tc := range []struct {
		peer     string
		size     int
		fallback int
		armed    int
	}{{"mon.b", 8192, 4096, 1}, {"mon.b", 100, 4096, 0}, {"mon.a", 8192, 4096, 0}, {"mon.b", 8192, 0, 0}} {
		armed := 0
		conn := &monOutcomeConn{Conn: monOutcomeSink{}, record: &monOutcomeWrites{}, peer: tc.peer,
			gate: &monOutcomeGate{Peer: "mon.b", Type: msgr.TellCommandMessage, FallbackBytes: tc.fallback, Arm: func() { armed++ }}}
		_, _ = conn.Write(bytes.Repeat([]byte{0xee}, tc.size))
		if armed != tc.armed {
			t.Fatal("fallback armed", tc, armed)
		}
	}
}
