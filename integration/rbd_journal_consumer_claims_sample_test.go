package integration_test

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdJournalConsumerSample is a consistent record of one consumer run with
// synthetic values: the setup's tag is 1 and the producer's 2.
func rbdJournalConsumerSample() *rbdJournalLostAppendRecord {
	hash := func(c string) string { return strings.Repeat(c, 64) }
	const tag = 2
	token := strings.Repeat("a1", 16)
	goClient, oracle := "ceph-msgr-mirror-a1a1a1a1", "ceph-msgr-oracle-a1a1a1a1"
	prefix := "journal_data.3.ab."
	goData := []byte{1, 2, 3}
	setupCommit := []rbdJournalPosition{{3, 1, 3}, {2, 1, 2}, {1, 1, 1}, {0, 1, 0}}
	sizes := func(exists []bool) []uint64 {
		var s []uint64
		for _, e := range exists {
			s = append(s, map[bool]uint64{true: 3149}[e])
		}
		return s
	}
	digests := func(exists []bool) []string {
		var s []string
		for i, e := range exists {
			s = append(s, map[bool]string{true: hash(fmt.Sprint(i % 10)), false: ""}[e])
		}
		return s
	}
	client := func(id, state string, positions []rbdJournalPosition) rbdJournalClientView {
		if positions == nil {
			positions = []rbdJournalPosition{}
		}
		return rbdJournalClientView{ID: id, State: state, Positions: slices.Clone(positions)}
	}
	view := func(action string, exists []bool, minimum, active uint64, image, goPositions []rbdJournalPosition, goState, oracleState string) *rbdJournalLostAppendView {
		// A native observation digests an absent object as empty.
		observed := digests(exists)
		for i, e := range exists {
			if !e {
				observed[i] = rbdJournalDigest(nil)
			}
		}
		return &rbdJournalLostAppendView{Action: action, ObjectSizes: sizes(exists), ObjectSHA256: observed, ObjectExists: slices.Clone(exists), ImageHeadSHA256: hash("a"),
			ImageCommit: slices.Clone(image), MinimumSet: minimum, ActiveSet: active, LockFree: true, NextTag: tag, LastTag: rbdJournalLostAppendTag{TID: 1},
			Clients: []rbdJournalClientView{client("", "connected", image), client(goClient, goState, goPositions), client(oracle, oracleState, nil)}}
	}
	first, last := rbdJournalConsumerPosition(tag, 7), rbdJournalConsumerPosition(tag, 15)
	setup := view("setup", rbdJournalConsumerSets(0, 1), 0, 0, setupCommit, nil, "", "connected")
	setup.Clients = []rbdJournalClientView{client("", "connected", setupCommit), client(oracle, "connected", nil)}
	final := view("produce", rbdJournalConsumerSets(3, 4), 3, 3, last, last, "disconnected", "disconnected")
	final.ImageHeadSHA256, final.NextTag, final.LastTag = hash("f"), tag+1, rbdJournalLostAppendTag{TID: tag}
	r := &rbdJournalLostAppendRecord{Token: token, PoolID: 3, SubjectGlobalID: 4866, ObserverGlobalID: 4800, ImageID: "ab", JournalHeader: "journal.ab",
		Order: slices.Clone(rbdJournalConsumerOrder), Timings: map[string]string{},
		Views: map[string]*rbdJournalLostAppendView{"setup": setup, "observe-registered": view("observe-registered", rbdJournalConsumerSets(0, 1), 0, 0, setupCommit, nil, "connected", "connected"),
			"disconnect-oracle": view("disconnect-oracle", rbdJournalConsumerSets(0, 1), 0, 0, setupCommit, nil, "connected", "disconnected"), "produce": final}}
	attempt := func(label string) *rbdLockOwnerLossAttempt {
		return &rbdLockOwnerLossAttempt{Label: label, Invocations: 1, Class: "applied", Submission: cephmsgr.OSDSubmission{Transaction: 9, GlobalID: 4866}, ReplyTransaction: 9}
	}
	header := func(minimum, active uint64, image, goPositions []rbdJournalPosition, goState uint8) rbdJournalConsumerHeader {
		if goPositions == nil {
			goPositions = []rbdJournalPosition{}
		}
		return rbdJournalConsumerHeader{MinimumSet: minimum, ActiveSet: active, Image: rbdJournalClient{Positions: slices.Clone(image)},
			Go: rbdJournalClient{ID: goClient, Data: goData, Positions: slices.Clone(goPositions), State: goState}, Oracle: rbdJournalClient{ID: oracle, Positions: []rbdJournalPosition{}, State: 1}}
	}
	// withSetup puts the setup's entry of tag 1 before the producer's in each
	// object of set 0, as the journal objects hold them.
	withSetup := func(entries []rbdJournalPosition) []rbdJournalPosition {
		var all []rbdJournalPosition
		for _, e := range entries {
			if e.Object < rbdJournalSplay {
				all = append(all, rbdJournalPosition{e.Object, 1, e.Object})
			}
			all = append(all, e)
		}
		return all
	}
	phase := func(name string, h rbdJournalConsumerHeader, exists []bool, entries []rbdJournalPosition, events int) *rbdJournalConsumerPhase {
		return &rbdJournalConsumerPhase{Phase: name, IsOwner: true, Header: h, ObjectExists: slices.Clone(exists), ObjectSizes: sizes(exists), ObjectSHA256: digests(exists),
			Entries: entries, WaitMet: true, WaitPolls: 3, Events: events}
	}
	x := &rbdJournalConsumer{GoDataHex: hex.EncodeToString(goData), Register: attempt("client_register"),
		Registered:       &rbdJournalClient{ID: goClient, Data: goData, Positions: []rbdJournalPosition{}},
		DisconnectOutput: []string{"client " + oracle + " disconnected"}, SetupTag: 1, ProducerTag: tag,
		Phases: map[string]*rbdJournalConsumerPhase{
			"produced":     phase("produced", header(0, 1, first, nil, 0), rbdJournalConsumerSets(0, 2), withSetup(rbdJournalConsumerEntries(tag, 0, 7)), 8),
			"trimmed":      phase("trimmed", header(1, 1, first, first, 0), rbdJournalConsumerSets(1, 2), rbdJournalConsumerEntries(tag, 4, 7), 0),
			"disconnected": phase("disconnected", header(3, 3, last, first, 1), rbdJournalConsumerSets(3, 4), rbdJournalConsumerEntries(tag, 12, 15), 8)},
		Reads: map[string][]rbdJournalTrimRead{}, CommitPositions: slices.Clone(first), Commit: attempt("client_commit"), Notified: true,
		LatePositions: slices.Clone(last), LateCommit: attempt("client_commit_disconnected"), LateRecord: &rbdJournalClient{ID: goClient, Positions: slices.Clone(last), State: 1},
		LaggyLog:     []string{"schedule_laggy_clients_disconnect: " + goClient + ": scheduling disconnect", "operator(): " + goClient + ": flagging disconnected"},
		ExpectedHead: hash("f")}
	x.TrimLog = append([]string{"handle_metadata_updated"}, rbdJournalConsumerTrimMarks(prefix, goClient)...)
	for stage, exists := range map[string][]bool{"produced": rbdJournalConsumerSets(0, 2), "trimmed": rbdJournalConsumerSets(1, 2), "disconnected": rbdJournalConsumerSets(3, 4),
		"produce": rbdJournalConsumerSets(3, 4)} {
		for _, h := range rbdJournalConsumerHeaderReads(goClient, oracle) {
			x.Reads[stage] = append(x.Reads[stage], rbdJournalTrimRead{Label: stage + "_" + h.Method, Object: "journal.ab", Method: h.Method, InputHex: h.InputHex, Number: -1, NativeExists: true, Equal: true})
		}
		for n, e := range exists {
			rd := rbdJournalTrimRead{Label: fmt.Sprintf("%s_read_journal_object_%d", stage, n), Object: prefix + fmt.Sprint(n), Number: n, NativeExists: e, Equal: true}
			if !e {
				rd.Code = -2
			}
			x.Reads[stage] = append(x.Reads[stage], rd)
		}
	}
	x.GoEntries = map[string][]rbdJournalPosition{}
	for name, p := range x.Phases {
		x.GoEntries[name] = slices.Clone(p.Entries)
	}
	r.Consumer = x
	return r
}

// TestRBDJournalConsumerClaims feeds the consumer claims contradictory
// records: the consistent sample passes at every stage, and each change
// fails at the stage named and every later one.
func TestRBDJournalConsumerClaims(t *testing.T) {
	judge := func(r *rbdJournalLostAppendRecord, full []string, stage string) []string {
		r.Order = slices.Clone(full[:min(len(full), slices.Index(rbdJournalConsumerOrder, stage)+1)])
		return rbdJournalLostAppendClaims(r, stage)
	}
	for _, stage := range rbdJournalConsumerStages {
		r := rbdJournalConsumerSample()
		if problems := judge(r, r.Order, stage); len(problems) != 0 {
			t.Fatalf("consistent consumer record refused at %s: %v", stage, problems)
		}
	}
	phase := func(r *rbdJournalLostAppendRecord, name string) *rbdJournalConsumerPhase {
		return r.Consumer.Phases[name]
	}
	read := func(r *rbdJournalLostAppendRecord, stage string, label string) *rbdJournalTrimRead {
		for i := range r.Consumer.Reads[stage] {
			if strings.HasSuffix(r.Consumer.Reads[stage][i].Label, label) {
				return &r.Consumer.Reads[stage][i]
			}
		}
		panic(label)
	}
	for name, c := range map[string]struct {
		stage  string
		change func(r *rbdJournalLostAppendRecord)
	}{
		"registration refused":        {"observe-registered", func(r *rbdJournalLostAppendRecord) { r.Consumer.Register.Class = "refused" }},
		"registered with a position":  {"observe-registered", func(r *rbdJournalLostAppendRecord) { r.Consumer.Registered.Positions = []rbdJournalPosition{{0, 1, 0}} }},
		"registered with other data":  {"observe-registered", func(r *rbdJournalLostAppendRecord) { r.Consumer.Registered.Data = []byte{9} }},
		"Go client missing natively":  {"observe-registered", func(r *rbdJournalLostAppendRecord) { r.Views["observe-registered"].Clients = r.Views["setup"].Clients }},
		"steps out of order":          {"observe-registered", func(r *rbdJournalLostAppendRecord) { r.Order[1], r.Order[2] = r.Order[2], r.Order[1] }},
		"oracle still connected":      {"disconnect-oracle", func(r *rbdJournalLostAppendRecord) { r.Views["disconnect-oracle"].Clients[2].State = "connected" }},
		"another client disconnected": {"disconnect-oracle", func(r *rbdJournalLostAppendRecord) { r.Consumer.DisconnectOutput = []string{"client x disconnected"} }},
		"producer's tag not the next": {"produced", func(r *rbdJournalLostAppendRecord) { r.Consumer.ProducerTag = 3 }},
		"producer not the lock owner": {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").IsOwner = false }},
		"image client still in set 0": {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").Header.Image.Positions[3].Object = 3 }},
		"Go client already committed": {"produced", func(r *rbdJournalLostAppendRecord) {
			phase(r, "produced").Header.Go.Positions = rbdJournalConsumerPosition(2, 7)
		}},
		"set 0 trimmed too early":    {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").Header.MinimumSet = 1 }},
		"an entry in another object": {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").Entries[5].Object = 9 }},
		"an entry twice":             {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").Entries[5] = phase(r, "produced").Entries[4] }},
		"producer wait not met":      {"produced", func(r *rbdJournalLostAppendRecord) { phase(r, "produced").WaitMet = false }},
		"a read differs":             {"produced", func(r *rbdJournalLostAppendRecord) { read(r, "produced", "object_5").Equal = false }},
		"an absent object answered":  {"produced", func(r *rbdJournalLostAppendRecord) { read(r, "produced", "object_9").Code = 0 }},
		"a header read twice": {"produced", func(r *rbdJournalLostAppendRecord) {
			*read(r, "produced", "_get_active_set") = *read(r, "produced", "_get_minimum_set")
		}},
		"Go decoded another entry": {"produced", func(r *rbdJournalLostAppendRecord) { r.Consumer.GoEntries["produced"][3].Entry = 9 }},
		"Go read no setup entry": {"produced", func(r *rbdJournalLostAppendRecord) {
			r.Consumer.GoEntries["produced"] = r.Consumer.GoEntries["produced"][1:]
		}},
		"commit not from the Go reads": {"trimmed", func(r *rbdJournalLostAppendRecord) {
			// The Go reads miss entry 7; a commit that names it came from elsewhere.
			g := r.Consumer.GoEntries["produced"]
			r.Consumer.GoEntries["produced"] = g[:len(g)-1]
			r.Consumer.Phases["produced"].Entries = slices.Clone(r.Consumer.GoEntries["produced"])
		}},
		"late commit not from the Go reads": {"produce", func(r *rbdJournalLostAppendRecord) {
			g := r.Consumer.GoEntries["disconnected"]
			r.Consumer.GoEntries["disconnected"] = g[:len(g)-1]
			r.Consumer.Phases["disconnected"].Entries = slices.Clone(r.Consumer.GoEntries["disconnected"])
		}},
		"commit past another position": {"trimmed", func(r *rbdJournalLostAppendRecord) { r.Consumer.CommitPositions = rbdJournalConsumerPosition(2, 6) }},
		"commit refused":               {"trimmed", func(r *rbdJournalLostAppendRecord) { r.Consumer.Commit.Class = "refused" }},
		"header not notified":          {"trimmed", func(r *rbdJournalLostAppendRecord) { r.Consumer.Notified = false }},
		"set 0 kept":                   {"trimmed", func(r *rbdJournalLostAppendRecord) { phase(r, "trimmed").ObjectExists[0] = true }},
		"minimum set not raised":       {"trimmed", func(r *rbdJournalLostAppendRecord) { phase(r, "trimmed").Header.MinimumSet = 0 }},
		"set 1 changed by the trim":    {"trimmed", func(r *rbdJournalLostAppendRecord) { phase(r, "trimmed").ObjectSHA256[5] = "x" }},
		"trimmed object read":          {"trimmed", func(r *rbdJournalLostAppendRecord) { read(r, "trimmed", "object_2").Code = 0 }},
		"Go client still connected":    {"disconnected", func(r *rbdJournalLostAppendRecord) { phase(r, "disconnected").Header.Go.State = 0 }},
		"Go position moved natively": {"disconnected", func(r *rbdJournalLostAppendRecord) {
			phase(r, "disconnected").Header.Go.Positions = rbdJournalConsumerPosition(2, 15)
		}},
		"set 2 kept":                  {"disconnected", func(r *rbdJournalLostAppendRecord) { phase(r, "disconnected").ObjectExists[8] = true }},
		"trimmed past the active set": {"disconnected", func(r *rbdJournalLostAppendRecord) { phase(r, "disconnected").Header.MinimumSet = 4 }},
		"late commit refused":         {"produce", func(r *rbdJournalLostAppendRecord) { r.Consumer.LateCommit.Class = "refused" }},
		"late commit reconnected":     {"produce", func(r *rbdJournalLostAppendRecord) { r.Consumer.LateRecord.State = 0 }},
		"late commit of another place": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Consumer.LatePositions = slices.Clone(r.Consumer.CommitPositions)
		}},
		"image differs at the end": {"produce", func(r *rbdJournalLostAppendRecord) { r.Views["produce"].ImageHeadSHA256 = strings.Repeat("a", 64) }},
		"lock kept at the end":     {"produce", func(r *rbdJournalLostAppendRecord) { r.Views["produce"].LockFree = false }},
		"final set 3 changed":      {"produce", func(r *rbdJournalLostAppendRecord) { r.Views["produce"].ObjectSHA256[13] = "x" }},
		"trimmer never named Go":   {"produce", func(r *rbdJournalLostAppendRecord) { r.Consumer.TrimLog = r.Consumer.TrimLog[2:] }},
		"set 3 removed": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Consumer.TrimLog = append(r.Consumer.TrimLog, "remove_set: removing object set 3")
		}},
		"set 0 kept after its removal": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Consumer.TrimLog = append(r.Consumer.TrimLog, "object set 0 still in-use by client x")
		}},
		"trim log truncated":  {"produce", func(r *rbdJournalLostAppendRecord) { r.Consumer.TrimLogTruncated = true }},
		"no laggy disconnect": {"produce", func(r *rbdJournalLostAppendRecord) { r.Consumer.LaggyLog = r.Consumer.LaggyLog[:1] }},
		"oracle disconnected too": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Consumer.LaggyLog = append(r.Consumer.LaggyLog, "operator(): x: flagging disconnected")
		}},
		"Go client trimmed": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Requests = append(r.Requests, rbdWriteRequestEvidence{Label: "remove", Inputs: []rbdOperationInputEvidence{{Code: uint16(cephmsgr.OSDDelete)}}})
		}},
		"Go client set its state": {"produce", func(r *rbdJournalLostAppendRecord) {
			r.Requests = append(r.Requests, rbdWriteRequestEvidence{Label: "update", Inputs: []rbdOperationInputEvidence{{Code: uint16(cephmsgr.OSDCall), Class: "journal", Method: "client_update_state"}}})
		}},
	} {
		r := rbdJournalConsumerSample()
		c.change(r)
		full := slices.Clone(r.Order)
		at := slices.Index(rbdJournalConsumerStages, c.stage)
		for i, stage := range rbdJournalConsumerStages {
			if problems := judge(r, full, stage); i >= at && len(problems) == 0 {
				t.Errorf("%s: accepted at %s", name, stage)
			}
		}
	}
}
