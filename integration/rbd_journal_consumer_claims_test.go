package integration_test

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// rbdJournalConsumerHeaderReads are the journal header reads of a compared
// consumer stage: the mutable metadata, the client list and the three
// registered clients, with the inputs the producer's exec used
// (rbd_journal.go journal_calls).
func rbdJournalConsumerHeaderReads(goClient, oracle string) []rbdJournalTrimHeaderRead {
	return []rbdJournalTrimHeaderRead{{"get_minimum_set", ""}, {"get_active_set", ""}, {"client_list", hex.EncodeToString(rbdJournalClientListInput())},
		{"get_client", hex.EncodeToString(rbdString(""))}, {"get_client", hex.EncodeToString(rbdString(goClient))}, {"get_client", hex.EncodeToString(rbdString(oracle))}}
}

// rbdJournalConsumerReadsClaims checks one compared stage: exactly one read
// of each header method and input and of each journal object 0 to 15,
// nothing else, each on its own object and matching native. The header
// reads answer 0; an object answers 0 when it exists and ENOENT otherwise.
func rbdJournalConsumerReadsClaims(name string, reads []rbdJournalTrimRead, header, prefix, goClient, oracle string, exists []bool, fail func(string, ...any)) {
	text := rbdJournalHeldText
	want := rbdJournalConsumerHeaderReads(goClient, oracle)
	seenHeader, seenObject := map[rbdJournalTrimHeaderRead]int{}, map[int]int{}
	for _, rd := range reads {
		if rd.Number < 0 {
			key := rbdJournalTrimHeaderRead{rd.Method, rd.InputHex}
			seenHeader[key]++
			if !slices.Contains(want, key) || rd.Object != header || rd.Code != 0 || !rd.Equal {
				fail("%s: header read %s is not an expected method answered 0 with native's output: %s", name, rd.Label, text(rd))
			}
			continue
		}
		seenObject[rd.Number]++
		if rd.Number >= len(exists) {
			fail("%s: read of an unexpected journal object: %s", name, text(rd))
			continue
		}
		code := int32(-2)
		if exists[rd.Number] {
			code = 0
		}
		if rd.Object != prefix+strconv.Itoa(rd.Number) || rd.NativeExists != exists[rd.Number] || rd.Code != code || !rd.Equal {
			fail("%s: read of journal object %d is not answered %d in agreement with native librados: %s", name, rd.Number, code, text(rd))
		}
	}
	for _, key := range want {
		if seenHeader[key] != 1 {
			fail("%s: header read %s with input %s observed %d times, want once", name, key.Method, key.InputHex, seenHeader[key])
		}
	}
	for n := range exists {
		if seenObject[n] != 1 {
			fail("%s: journal object %d read %d times, want once", name, n, seenObject[n])
		}
	}
}

// rbdJournalConsumerSets says which of the observed journal objects exist:
// those of the object sets from..to-1.
func rbdJournalConsumerSets(from, to int) []bool {
	exists := make([]bool, rbdJournalConsumerObjects)
	for n := from * rbdJournalSplay; n < to*rbdJournalSplay; n++ {
		exists[n] = true
	}
	return exists
}

// rbdJournalConsumerEntries are the producer tag's entries first..last, entry
// n in journal object n.
func rbdJournalConsumerEntries(tag uint64, first, last uint64) []rbdJournalPosition {
	var entries []rbdJournalPosition
	for n := first; n <= last; n++ {
		entries = append(entries, rbdJournalPosition{n, tag, n})
	}
	return entries
}

// rbdJournalConsumerPosition is the commit position after entries
// last-3..last of the producer's tag: one position per splay offset, most
// recent first.
func rbdJournalConsumerPosition(tag, last uint64) []rbdJournalPosition {
	var positions []rbdJournalPosition
	for n := last; n+rbdJournalSplay > last; n-- {
		positions = append(positions, rbdJournalPosition{n, tag, n})
	}
	return positions
}

// rbdJournalConsumerTrimMarks are the producer trimmer's debug lines the
// claims require, in this order (journal/JournalTrimmer.cc
// handle_metadata_updated, remove_set at ceph/ceph/blob/v20.2.4/src/): the
// Go client keeping object set 0, then the removal of sets 0, 1 and 2, each
// object by name.
func rbdJournalConsumerTrimMarks(prefix, goClient string) []string {
	marks := []string{"object set 0 still in-use by client " + goClient}
	for set := range 3 {
		marks = append(marks, fmt.Sprintf("remove_set: removing object set %d", set))
		for n := set * rbdJournalSplay; n < (set+1)*rbdJournalSplay; n++ {
			marks = append(marks, "removing journal object "+prefix+strconv.Itoa(n))
		}
	}
	return marks
}

// rbdJournalConsumerClaims checks the consumer record up to one stage. It
// keeps apart what the Go client did (its registration and commits, with
// their answers), what the producer's observations showed in each phase,
// how the Go client's reads compare with them, and the producer's own
// account of its trimming and laggy-client disconnect.
func rbdJournalConsumerClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := rbdJournalHeldText
	x := r.Consumer
	reached := slices.Index(rbdJournalConsumerStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if x == nil || r.Held != nil || r.OwnerLoss != nil || r.Trim != nil || r.PrimaryChange != nil {
		return []string{"record: not a consumer record"}
	}
	if want := rbdJournalConsumerOrder[:slices.Index(rbdJournalConsumerOrder, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if len(r.Token) != 32 || r.SubjectGlobalID == 0 || r.ImageID == "" || r.JournalHeader != "journal."+r.ImageID {
		fail("identity: token, client or journal identity incomplete")
		return problems
	}
	goClient, oracle := "ceph-msgr-mirror-"+r.Token[:8], "ceph-msgr-oracle-"+r.Token[:8]
	prefix := fmt.Sprintf("journal_data.%d.%s.", r.PoolID, r.ImageID)
	view := func(name string, at int) *rbdJournalLostAppendView {
		if reached < at {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || len(v.ObjectSizes) != rbdJournalConsumerObjects || len(v.ObjectExists) != rbdJournalConsumerObjects {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, registered, disconnected, final := view("setup", 0), view("observe-registered", 0), view("disconnect-oracle", 1), view("produce", 5)
	if len(problems) != 0 {
		return problems
	}
	// clients checks a native observation's registered clients, in the
	// order listed: the image client at image, the Go client at its
	// position in goState, the oracle at its empty position in oracleState.
	clients := func(name string, v *rbdJournalLostAppendView, image, goPositions []rbdJournalPosition, goState, oracleState string) {
		if goPositions == nil {
			goPositions = []rbdJournalPosition{}
		}
		want := []rbdJournalClientView{{"", "connected", image}, {goClient, goState, goPositions}, {oracle, oracleState, []rbdJournalPosition{}}}
		if !reflect.DeepEqual(v.Clients, want) {
			fail("%s: the registered clients are %s, want %s", name, text(v.Clients), text(want))
		}
	}
	reads := func(name string, exists []bool) {
		rbdJournalConsumerReadsClaims(name, x.Reads[name], r.JournalHeader, prefix, goClient, oracle, exists, fail)
	}
	// phase checks one producer phase's header, objects and the producer
	// tag's entries.
	phase := func(name string, minimum, active uint64, image, goPositions []rbdJournalPosition, goState uint8, from, to int, entries []rbdJournalPosition) *rbdJournalConsumerPhase {
		p := x.Phases[name]
		if p == nil {
			fail("%s: the producer's phase is missing", name)
			return nil
		}
		if goPositions == nil {
			goPositions = []rbdJournalPosition{}
		}
		h := p.Header
		if !p.IsOwner || !p.WaitMet || p.WaitPolls < 1 || h.MinimumSet != minimum || h.ActiveSet != active {
			fail("%s: the producer does not own the lock, its wait was not met or the sets are %d and %d, want %d and %d (owner=%v met=%v)", name, h.MinimumSet, h.ActiveSet, minimum, active, p.IsOwner, p.WaitMet)
		}
		if h.Image.ID != "" || !reflect.DeepEqual(h.Image.Positions, image) || h.Image.State != 0 {
			fail("%s: the image client is at %v, want %v", name, h.Image.Positions, image)
		}
		if h.Go.ID != goClient || !reflect.DeepEqual(h.Go.Positions, goPositions) || h.Go.State != goState || hex.EncodeToString(h.Go.Data) != x.GoDataHex {
			fail("%s: the Go client is %s, want position %v in state %d", name, text(h.Go), goPositions, goState)
		}
		if h.Oracle.ID != oracle || len(h.Oracle.Positions) != 0 || h.Oracle.State != 1 {
			fail("%s: the oracle client is not disconnected at its empty position: %s", name, text(h.Oracle))
		}
		if want := rbdJournalConsumerSets(from, to); !slices.Equal(p.ObjectExists, want) {
			fail("%s: the journal objects that exist are %v, want %v", name, p.ObjectExists, want)
		}
		if !reflect.DeepEqual(p.Entries, entries) {
			fail("%s: the producer's entries are %v, want %v", name, p.Entries, entries)
		}
		return p
	}

	// The Go client's registration and the oracle's disconnect.
	if a := x.Register; a == nil || a.Class != "applied" || a.Code != 0 || a.Invocations != 1 {
		fail("register: the Go client's client_register was not answered 0 once: %s", text(a))
	}
	if c := x.Registered; c == nil || c.ID != goClient || hex.EncodeToString(c.Data) != x.GoDataHex || x.GoDataHex == "" || len(c.Positions) != 0 || c.State != 0 {
		fail("register: the Go client's record is not connected at an empty position with its data: %s", text(c))
	}
	if setup.MinimumSet != 0 || setup.ActiveSet != 0 || !slices.Equal(setup.ObjectExists, rbdJournalConsumerSets(0, 1)) || len(setup.ImageCommit) != rbdJournalSplay {
		fail("setup: not one native entry per splay offset of object set 0 (objects %v, commit %v)", setup.ObjectExists, setup.ImageCommit)
	}
	clients("observe-registered", registered, setup.ImageCommit, nil, "connected", "connected")
	if !slices.Equal(registered.ObjectSHA256, setup.ObjectSHA256) || registered.MinimumSet != 0 || registered.ActiveSet != 0 {
		fail("observe-registered: the journal changed since the setup")
	}
	if reached < 1 {
		return problems
	}
	if !slices.Equal(x.DisconnectOutput, []string{"client " + oracle + " disconnected"}) || x.DisconnectCode != 0 {
		fail("disconnect-oracle: rbd journal client disconnect did not report the oracle client alone: %v (exit %d)", x.DisconnectOutput, x.DisconnectCode)
	}
	clients("disconnect-oracle", disconnected, setup.ImageCommit, nil, "connected", "disconnected")
	if !slices.Equal(disconnected.ObjectSHA256, setup.ObjectSHA256) || disconnected.ImageHeadSHA256 != setup.ImageHeadSHA256 {
		fail("disconnect-oracle: the journal or the image changed")
	}
	if reached < 2 {
		return problems
	}
	t := x.ProducerTag
	if x.SetupTag == 0 || t != x.SetupTag+1 {
		fail("identity: the producer's tag %d does not follow the setup's %d", t, x.SetupTag)
	}

	// The first batch: entries 0 to 3 after the setup in set 0, 4 to 7 in
	// set 1; the image client past set 0, the Go client still empty.
	first := rbdJournalConsumerPosition(t, 7)
	produced := phase("produced", 0, 1, first, nil, 0, 0, 2, rbdJournalConsumerEntries(t, 0, 7))
	if produced != nil && produced.Events != 8 {
		fail("produced: the producer's first batch is %d writes, want 8", produced.Events)
	}
	reads("produced", rbdJournalConsumerSets(0, 2))
	if reached < 3 {
		return problems
	}

	// The Go client's commit and the trim of set 0.
	if !reflect.DeepEqual(x.CommitPositions, first) {
		fail("commit: the Go client's position is %v, want %v", x.CommitPositions, first)
	}
	if a := x.Commit; a == nil || a.Class != "applied" || a.Code != 0 || a.Invocations != 1 || !x.Notified {
		fail("commit: the Go client's commit was not answered 0 once and the header notified: %s", text(a))
	}
	trimmed := phase("trimmed", 1, 1, first, first, 0, 1, 2, rbdJournalConsumerEntries(t, 4, 7))
	if trimmed != nil && produced != nil && !slices.Equal(trimmed.ObjectSHA256[4:8], produced.ObjectSHA256[4:8]) {
		fail("trimmed: journal objects 4 to 7 changed")
	}
	reads("trimmed", rbdJournalConsumerSets(1, 2))
	if reached < 4 {
		return problems
	}

	// The second batch, the laggy-client disconnect and the trim past the Go
	// client's position.
	last := rbdJournalConsumerPosition(t, 15)
	laggy := phase("disconnected", 3, 3, last, first, 1, 3, 4, rbdJournalConsumerEntries(t, 12, 15))
	if laggy != nil && laggy.Events != 8 {
		fail("disconnected: the producer's second batch is %d writes, want 8", laggy.Events)
	}
	reads("disconnected", rbdJournalConsumerSets(3, 4))
	if reached < 5 {
		return problems
	}

	// The disconnected Go client's next commit.
	if !reflect.DeepEqual(x.LatePositions, last) {
		fail("late commit: the disconnected Go client's position is %v, want %v", x.LatePositions, last)
	}
	if a := x.LateCommit; a == nil || a.Class != "applied" || a.Code != 0 || a.Invocations != 1 {
		fail("late commit: the disconnected Go client's commit was not answered 0 once: %s", text(a))
	}
	if c := x.LateRecord; c == nil || c.ID != goClient || !reflect.DeepEqual(c.Positions, last) || c.State != 1 {
		fail("late commit: the Go client's record is not disconnected at its new position: %s", text(c))
	}

	// The producer's close and final observation, and its own account.
	clients("produce", final, last, last, "disconnected", "disconnected")
	// Set 3, the one that remains, is byte for byte what the last phase read.
	remains := 3 * rbdJournalSplay
	if final.MinimumSet != 3 || final.ActiveSet != 3 || !slices.Equal(final.ObjectExists, rbdJournalConsumerSets(3, 4)) ||
		laggy != nil && (!slices.Equal(final.ObjectSHA256[remains:], laggy.ObjectSHA256[remains:]) || !slices.Equal(final.ObjectSizes[remains:], laggy.ObjectSizes[remains:])) {
		fail("produce: the journal after the close is not object set 3 alone as the last phase showed it (sets %d %d, objects %v)", final.MinimumSet, final.ActiveSet, final.ObjectExists)
	}
	if final.ImageHeadSHA256 != x.ExpectedHead || x.ExpectedHead == "" || !final.LockFree || final.Watchers != 0 {
		fail("produce: the image is not the setup and the producer's writes, or the lock or a watch remains (free=%v watchers=%d)", final.LockFree, final.Watchers)
	}
	reads("produce", rbdJournalConsumerSets(3, 4))
	if x.TrimLogTruncated || !rbdJournalSubsequence(x.TrimLog, rbdJournalConsumerTrimMarks(prefix, goClient)) || slices.Contains(x.TrimLog, "remove_set: removing object set 3") {
		fail("trim: the producer's trimmer log does not show the Go client keeping set 0, then sets 0, 1 and 2 removed: %s", text(x.TrimLog))
	}
	if i := slices.Index(x.TrimLog, "remove_set: removing object set 0"); i >= 0 && slices.ContainsFunc(x.TrimLog[i:], func(line string) bool {
		return strings.HasPrefix(line, "object set 0 still in-use")
	}) {
		fail("trim: the trimmer still kept object set 0 after removing it: %s", text(x.TrimLog))
	}
	scheduled, flagged := 0, 0
	for _, line := range x.LaggyLog {
		switch line {
		case "schedule_laggy_clients_disconnect: " + goClient + ": scheduling disconnect":
			scheduled++
		case "operator(): " + goClient + ": flagging disconnected":
			flagged++
		default:
			fail("laggy: the producer's laggy-client log names another client or step: %q", line)
		}
	}
	if x.LaggyLogTruncated || scheduled < 1 || flagged < 1 {
		fail("laggy: the producer did not schedule and flag the Go client's disconnect: %s", text(x.LaggyLog))
	}
	// The trimming and the disconnect are native.
	for _, req := range r.Requests {
		for _, in := range req.Inputs {
			if in.Method == "set_minimum_set" || in.Method == "client_update_state" || in.Method == "client_unregister" || in.Code == uint16(cephmsgr.OSDDelete) {
				fail("consumer: the Go client sent %s (%s %d)", req.Label, in.Method, in.Code)
			}
		}
	}
	return problems
}
