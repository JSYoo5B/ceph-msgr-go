package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestObserveNativeDaemonCommandRejectsOpenRequestsBeforePublishing(t *testing.T) {
	control := t.TempDir()
	token := strings.Repeat("a", 32)
	osd := nativeDaemonCommandTarget{Kind: "osd", ID: "0"}
	for _, request := range []struct {
		token, phase string
		target       nativeDaemonCommandTarget
	}{
		{"short", "initial", osd}, {strings.ToUpper(token), "initial", osd}, {token + " x", "initial", osd},
		{token, "cleanup", osd}, {token, "overwritten", osd}, {token, "", osd}, {token, "overwrite 10002", osd},
		{token, "initial", nativeDaemonCommandTarget{Kind: "osd", ID: "0 1"}}, {token, "initial", nativeDaemonCommandTarget{Kind: "osd", ID: "01"}},
		{token, "initial", nativeDaemonCommandTarget{Kind: "osd", ID: "0", PGID: "2.0"}}, {token, "initial", nativeDaemonCommandTarget{Kind: "pg", ID: "0"}},
		{token, "initial", nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.0 x"}}, {token, "initial", nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.G"}},
		{token, "initial", nativeDaemonCommandTarget{Kind: "mds", ID: "a b"}}, {token, "initial", nativeDaemonCommandTarget{Kind: "mds", ID: "../a"}},
		{token, "initial", nativeDaemonCommandTarget{Kind: "mds", ID: "a", PGID: "2.0"}}, {token, "initial", nativeDaemonCommandTarget{Kind: "mgr", ID: "a"}},
		{token, "initial", nativeDaemonCommandTarget{}},
	} {
		if _, err := observeNativeDaemonCommand(context.Background(), control, request.token, request.phase, request.target); err == nil {
			t.Fatal("open request was delegated", request)
		}
	}
	if entries, err := os.ReadDir(control); err != nil || len(entries) != 0 {
		t.Fatal("rejected requests published control files", entries, err)
	}
}

// Receipt shapes published by integration/fixture/daemon_command_outcome.go.
const (
	nativeDaemonCommandOptionSample = `{"request_id":3,"token":"cccccccccccccccccccccccccccccccc","phase":"applied","target":{"kind":"osd","id":"0"},"restorable":true,"baseline":{"present":false,"value":"","effective":"500"},"views":{"daemon_before":{"ok":true,"error":"","daemon":"osd.0","up":true,"incarnation":5,"address":"192.0.2.7:6800/9"},"config_get":{"ok":true,"error":"","data":"eyJsb2dfbWF4X3JlY2VudCI6IjUwMCJ9"},"config_diff":{"ok":true,"error":"","data_bytes":12,"valid":true,"entry":null,"top_level_keys":["diff"],"diff_entries":3},"daemon_after":{"ok":true,"error":"","daemon":"osd.0","up":true,"incarnation":5,"address":"192.0.2.7:6800/9"}}}`
	nativeDaemonCommandPGSample     = `{"request_id":4,"token":"cccccccccccccccccccccccccccccccc","phase":"applied","target":{"kind":"pg","id":"0","pgid":"2.0"},"restorable":true,"views":{"daemon_before":{"ok":true,"error":"","daemon":"osd.0","up":true,"incarnation":5,"address":"192.0.2.7:6800/9"},"pg_query":{"ok":true,"error":"","data_bytes":6755,"valid":true,"pgid":"2.0","state":"active+clean","scrubber_present":true,"flag_present":true},"daemon_after":{"ok":true,"error":"","daemon":"osd.0","up":true,"incarnation":5,"address":"192.0.2.7:6800/9"}}}`
)

func TestNativeDaemonCommandReceiptBindsStateToTheTargetIncarnation(t *testing.T) {
	var receipt nativeDaemonCommandReceipt
	if err := json.Unmarshal([]byte(nativeDaemonCommandOptionSample), &receipt); err != nil {
		t.Fatal(err)
	}
	target := nativeDaemonCommandTarget{Kind: "osd", ID: "0"}
	daemon := nativeDaemonIncarnation{"osd.0", 5, "192.0.2.7:6800/9"}
	absent := nativeMonTellState{value: 500}
	if !nativeDaemonCommandMatches(receipt, target, daemon, absent) {
		t.Fatal("absent override with effective 500 on the bracketed daemon was not recognized")
	}
	override := int64(500)
	// The same effective value does not establish an override, and a lost
	// application (another value) never matches.
	if nativeDaemonCommandMatches(receipt, target, daemon, nativeMonTellState{500, &override}) || nativeDaemonCommandMatches(receipt, target, daemon, nativeMonTellState{value: 501}) {
		t.Fatal("effective value alone was accepted as an override, or another value matched")
	}
	// Wrong target: another daemon, a restarted incarnation, another address
	// or nonce, a receipt of another target, or another kind for the same OSD.
	for _, other := range []nativeDaemonIncarnation{{"osd.1", 5, daemon.address}, {"osd.0", 6, daemon.address}, {"osd.0", 5, "192.0.2.7:6800/10"}, {"mds.0", 5, daemon.address}} {
		if nativeDaemonCommandMatches(receipt, target, other, absent) {
			t.Fatal("another daemon, incarnation or address matched", other)
		}
	}
	if nativeDaemonCommandMatches(receipt, nativeDaemonCommandTarget{Kind: "osd", ID: "1"}, nativeDaemonIncarnation{"osd.1", 5, daemon.address}, absent) ||
		nativeDaemonCommandMatches(receipt, nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.0"}, daemon, absent) {
		t.Fatal("a receipt of another target matched")
	}
	text := "500"
	present := receipt
	diff := *receipt.Views.ConfigDiff
	diff.Entry = &nativeMonTellEntry{OverridePresent: true, Override: &text}
	present.Views.ConfigDiff = &diff
	if !nativeDaemonCommandMatches(present, target, daemon, nativeMonTellState{500, &override}) || nativeDaemonCommandMatches(present, target, daemon, absent) {
		t.Fatal("override presence was not compared exactly")
	}
	for name, change := range map[string]func(*nativeDaemonCommandViews){
		"failed map read":   func(v *nativeDaemonCommandViews) { v.DaemonAfter.OK = false },
		"down before":       func(v *nativeDaemonCommandViews) { v.DaemonBefore.Up = false },
		"restarted":         func(v *nativeDaemonCommandViews) { v.DaemonAfter.Incarnation = 6 },
		"nonce changed":     func(v *nativeDaemonCommandViews) { v.DaemonAfter.Address = "192.0.2.7:6800/10" },
		"no address":        func(v *nativeDaemonCommandViews) { v.DaemonBefore.Address, v.DaemonAfter.Address = "", "" },
		"missing get":       func(v *nativeDaemonCommandViews) { v.ConfigGet = nil },
		"missing diff":      func(v *nativeDaemonCommandViews) { v.ConfigDiff = nil },
		"failed get":        func(v *nativeDaemonCommandViews) { get := *v.ConfigGet; get.OK = false; v.ConfigGet = &get },
		"unparsed diff":     func(v *nativeDaemonCommandViews) { diff := *v.ConfigDiff; diff.Valid = false; v.ConfigDiff = &diff },
		"mixed query views": func(v *nativeDaemonCommandViews) { v.PGQuery = &nativeDaemonCommandPG{OK: true, Valid: true} },
	} {
		changed := receipt
		change(&changed.Views)
		if nativeDaemonCommandMatches(changed, target, daemon, absent) {
			t.Fatal("an unbracketed or unreadable observation established the state:", name)
		}
	}
}

func TestNativeDaemonCommandReceiptReadsThePGScrubberFlag(t *testing.T) {
	var receipt nativeDaemonCommandReceipt
	if err := json.Unmarshal([]byte(nativeDaemonCommandPGSample), &receipt); err != nil {
		t.Fatal(err)
	}
	target := nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.0"}
	daemon := nativeDaemonIncarnation{"osd.0", 5, "192.0.2.7:6800/9"}
	listed, unlisted := nativeMonTellState{value: 1}, nativeMonTellState{}
	if !nativeDaemonCommandMatches(receipt, target, daemon, listed) || nativeDaemonCommandMatches(receipt, target, daemon, unlisted) {
		t.Fatal("a listed counter was not read as the set flag")
	}
	cleared := receipt
	query := *receipt.Views.PGQuery
	query.FlagPresent = false
	cleared.Views.PGQuery = &query
	if !nativeDaemonCommandMatches(cleared, target, daemon, unlisted) || nativeDaemonCommandMatches(cleared, target, daemon, listed) {
		t.Fatal("an unlisted counter was not read as the unset flag")
	}
	for name, change := range map[string]func(*nativeDaemonCommandPG){
		"failed query":    func(q *nativeDaemonCommandPG) { q.OK = false },
		"unparsed query":  func(q *nativeDaemonCommandPG) { q.Valid = false },
		"another PG":      func(q *nativeDaemonCommandPG) { q.PGID = "2.1" },
		"not the primary": func(q *nativeDaemonCommandPG) { q.ScrubberPresent = false },
	} {
		changed := receipt
		query := *receipt.Views.PGQuery
		change(&query)
		changed.Views.PGQuery = &query
		if nativeDaemonCommandMatches(changed, target, daemon, listed) || nativeDaemonCommandMatches(changed, target, daemon, unlisted) {
			t.Fatal("an unreadable PG observation established a flag state:", name)
		}
	}
	if nativeDaemonCommandMatches(receipt, nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.1"}, daemon, listed) {
		t.Fatal("a receipt of another PG matched")
	}
}

func TestPGScrubSessionsCommandAndQueryProjection(t *testing.T) {
	command := pgScrubSessionsCommand("2.0", 64)
	var decoded map[string]string
	if err := json.Unmarshal(command.JSON, &decoded); err != nil || len(decoded) != 4 || decoded["prefix"] != "scrubdebug" || decoded["pgid"] != "2.0" ||
		decoded["cmd"] != "set" || decoded["value"] != "sessions" || !strings.HasSuffix(string(command.JSON), strings.Repeat(" ", 64)) {
		t.Fatal("the padded PG command is not the one closed scrubber flag command", string(command.JSON), err)
	}
	for data, want := range map[string]int64{
		`{"state":"active+clean","info":{"pgid":"2.0"},"scrubber":{"active":false}}`:                   0,
		`{"state":"active+clean","info":{"pgid":"2.0"},"scrubber":{"active":false,"test_sequence":0}}`: 1,
	} {
		if got, ok := parsePGScrubSessions("2.0", []byte(data)); !ok || got != want {
			t.Fatal("query projection", data, got, ok)
		}
	}
	for _, data := range []string{``, `[]`, `{"info":{"pgid":"2.0"}}`, `{"info":{"pgid":"2.1"},"scrubber":{}}`, `{"scrubber":{"test_sequence":1}}`} {
		if _, ok := parsePGScrubSessions("2.0", []byte(data)); ok {
			t.Fatal("a reply of another PG or without a scrubber section was readable", data)
		}
	}
}

// The secure cells wait on the native state instead of a decoded reply. Only
// a state of the change from the previous to the wanted one may be read again.
func TestNativeDaemonCommandBetweenAdmitsOnlyTheChangeFromPreviousToWanted(t *testing.T) {
	var receipt nativeDaemonCommandReceipt
	if err := json.Unmarshal([]byte(nativeDaemonCommandOptionSample), &receipt); err != nil {
		t.Fatal(err)
	}
	target := nativeDaemonCommandTarget{Kind: "osd", ID: "0"}
	daemon := nativeDaemonIncarnation{"osd.0", 5, "192.0.2.7:6800/9"}
	a := int64(501)
	previous, want := nativeMonTellState{value: 500}, nativeMonTellState{501, &a}
	state := func(value string, override *string) nativeDaemonCommandReceipt {
		changed := receipt
		get, diff := *receipt.Views.ConfigGet, *receipt.Views.ConfigDiff
		get.Data, diff.Entry = []byte(`{"log_max_recent":"`+value+`"}`), nil
		if override != nil {
			diff.Entry = &nativeMonTellEntry{OverridePresent: true, Override: override}
		}
		changed.Views.ConfigGet, changed.Views.ConfigDiff = &get, &diff
		return changed
	}
	wanted, baseline, other := "501", "500", "502"
	// The previous state, the wanted state and both torn two-call reads.
	for _, admitted := range []nativeDaemonCommandReceipt{state("500", nil), state("501", &wanted), state("500", &wanted), state("501", nil)} {
		if !nativeDaemonCommandBetween(admitted, target, daemon, previous, want) {
			t.Fatal("a state of the change from previous to wanted was refused", nativeDaemonCommandSummary(admitted))
		}
	}
	if !nativeDaemonCommandMatches(state("501", &wanted), target, daemon, want) || nativeDaemonCommandMatches(state("500", &wanted), target, daemon, want) ||
		nativeDaemonCommandMatches(state("501", nil), target, daemon, want) {
		t.Fatal("a torn read ended the wait, or the wanted state did not")
	}
	// A lost application that only adds an override of the baseline value, a
	// third value and a third override are not part of that change.
	for _, refused := range []nativeDaemonCommandReceipt{state("500", &baseline), state("502", &other), state("502", nil), state("501", &other)} {
		if nativeDaemonCommandBetween(refused, target, daemon, previous, want) {
			t.Fatal("a state outside the change from previous to wanted was admitted", nativeDaemonCommandSummary(refused))
		}
	}
	restarted := state("500", nil)
	restarted.Views.DaemonAfter.Incarnation = 6
	renonced := state("500", nil)
	renonced.Views.DaemonBefore.Address = "192.0.2.7:6800/10"
	unread := state("500", nil)
	failed := *unread.Views.ConfigGet
	failed.OK = false
	unread.Views.ConfigGet = &failed
	elsewhere := state("500", nil)
	elsewhere.Target = nativeDaemonCommandTarget{Kind: "osd", ID: "1"}
	for name, refused := range map[string]nativeDaemonCommandReceipt{"restarted": restarted, "another nonce": renonced, "failed read": unread, "another target": elsewhere} {
		if nativeDaemonCommandBetween(refused, target, daemon, previous, want) {
			t.Fatal("an unbracketed, unreadable or foreign observation was admitted:", name)
		}
	}
	for _, other := range []nativeDaemonIncarnation{{"osd.1", 5, daemon.address}, {"osd.0", 6, daemon.address}, {"osd.0", 5, "192.0.2.7:6800/10"}} {
		if nativeDaemonCommandBetween(state("500", nil), target, other, previous, want) {
			t.Fatal("an observation bracketed by another daemon, incarnation or address was admitted", other)
		}
	}

	// The PG flag has two states and no override: only those two are admitted,
	// and only from the primary's scrubber section of the requested PG.
	var query nativeDaemonCommandReceipt
	if err := json.Unmarshal([]byte(nativeDaemonCommandPGSample), &query); err != nil {
		t.Fatal(err)
	}
	pg := nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.0"}
	unset, set := nativeMonTellState{}, nativeMonTellState{value: 1}
	cleared := query
	projection := *query.Views.PGQuery
	projection.FlagPresent = false
	cleared.Views.PGQuery = &projection
	if !nativeDaemonCommandBetween(query, pg, daemon, unset, set) || !nativeDaemonCommandBetween(cleared, pg, daemon, unset, set) ||
		nativeDaemonCommandMatches(cleared, pg, daemon, set) {
		t.Fatal("the unset and set PG flag are not the two states of the change, or the unset flag ended the wait")
	}
	replica := query
	projection = *query.Views.PGQuery
	projection.ScrubberPresent = false
	replica.Views.PGQuery = &projection
	if nativeDaemonCommandBetween(replica, pg, daemon, unset, set) || nativeDaemonCommandBetween(query, nativeDaemonCommandTarget{Kind: "pg", ID: "0", PGID: "2.1"}, daemon, unset, set) ||
		nativeDaemonCommandBetween(query, target, daemon, unset, set) {
		t.Fatal("a PG observation without a scrubber section, of another PG or of another target kind was admitted")
	}
}
