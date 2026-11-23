package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDJournalConsumerIntegration has the Go client consume an image
// journal that native librbd writes, as a mirroring consumer would, and lets
// native librbd trim the journal past the Go client's commit position and
// then disconnect the Go client as laggy. The fixture creates the journaling
// image with the trim plan's 4 KiB journal data objects and the default
// splay width of four; the setup journals one small entry at each splay
// offset of object set 0, and the native oracle journal client is registered
// at an empty commit position. The journal data objects 0 to 15 (object sets
// 0 to 3) are observed throughout.
//
// The Go client registers its own journal client with librbd mirroring's
// peer client data (journal.client_register; cls_journal records the minimum
// commit position of the connected clients, the oracle's empty one), and
// native `rbd journal client disconnect` flags the oracle client
// disconnected. A native librbd instance (the producer) then opens the image
// read-write with rbd_journal_max_concurrent_object_sets 1 and
// rbd_journal_commit_age 1 s (so that its own commits follow its writes
// within a second) and keeps it open for the rest of the sequence; it owns
// the exclusive lock throughout.
//
//   - Its first eight 3 KiB writes are entries 0 to 7 of its tag. Entries 0 to
//     3 follow the setup entries in objects 0 to 3; entry 4 finds object 0
//     beyond the 4 KiB soft limit, the journal recorder closes object set 0
//     and entries 4 to 7 go to objects 4 to 7 (journal/JournalRecorder.cc,
//     journal/ObjectRecorder.cc send_appends, cls/journal/cls_journal.cc
//     journal_object_guard_append at ceph/ceph/blob/v20.2.4/src/). Once the
//     image client's position names entries 7 to 4, object set 0 is kept
//     only by the Go client's empty position (journal/JournalTrimmer.cc
//     handle_metadata_updated).
//   - The Go client reads the journal header and every journal object and
//     compares each answer with the producer's own exec and reads of that
//     phase. It takes the producer's tag from its own read of the image
//     client's position, decodes the entries of its own object reads (they
//     must equal what the test decodes from the producer's view), commits
//     the position a consumer that replayed those entries would commit (one
//     position per splay offset, entries 7 to 4) and notifies the journal
//     header. The producer's
//     trimmer then removes object set 0 and raises the minimum set to 1.
//   - The producer's next eight writes are entries 8 to 15, in objects 8 to
//     11 and 12 to 15. The active set 3 is then more than one object set
//     past the Go client's position in set 1, and when the producer next
//     commits its own position its journal metadata flags the Go client
//     disconnected (journal/JournalMetadata.cc
//     schedule_laggy_clients_disconnect); its trimmer, which skips
//     disconnected clients, removes object sets 1 and 2.
//   - The Go client finds its client record disconnected with its committed
//     position; its next client_commit, of the position after the entries
//     its own reads of the second batch found, is answered 0 and leaves the record
//     disconnected (cls_journal.cc journal_client_commit does not look at
//     the state).
//
// Every Go read of a stage is judged by its identity: each header method and
// input, and each journal object number, read exactly once and answered as
// native librados answered it in the same phase (an object the producer did
// not find answers ENOENT). The producer's debug log keeps the trimmer's and
// the laggy-client lines; the claims require the trimmer to name the Go
// client as the one keeping object set 0 before its commit, the removal of
// sets 0, 1 and 2 in that order, and the laggy-client disconnect of the Go
// client alone. The final native observation after the producer closed
// shows the image as the setup and the sixteen writes, the image client at
// entries 15 to 12, the Go client disconnected at its last position and the
// minimum set 3.
//
// Consumer choices kept explicit here: the Go client commits without
// applying the entries anywhere, commits once per stage, and commits again
// after it was disconnected instead of re-registering; the oracle's
// disconnect makes trimming reachable, and the lag bound and the commit age
// are the producer's settings for this test. The Go client sends no remove, no
// set_minimum_set and no client_update_state: the trimming and the
// disconnect are native. Not covered: a Go consumer that replays the entries
// into another image, a Go client that reconnects, and trimming on several
// OSDs.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalconsumer key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-consumer
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournalconsumer key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-consumer
//scenario:covers RBD underlying I/O
//fixture:isolated disconnects the journal oracle client, registers a Go journal client that native librbd disconnects, leaves an image
func TestCephRBDJournalConsumerIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantConsumer)
}

// rbdJournalVariantConsumer is the variant of runRBDJournalLostAppend this
// file adds.
const rbdJournalVariantConsumer = "consumer"

// rbdJournalConsumerObjects is the number of journal data objects the
// consumer plan observes (four object sets).
const rbdJournalConsumerObjects = 16

// The consumer variant's order and the stages at which it is judged.
var (
	rbdJournalConsumerOrder = []string{"setup", "register", "observe-registered", "disconnect-oracle", "produced", "committed", "trimmed", "disconnected",
		"committed_disconnected", "produce"}
	rbdJournalConsumerStages = []string{"observe-registered", "disconnect-oracle", "produced", "trimmed", "disconnected", "produce"}
)

// nativeRBDJournalProducePhase is one published phase of the producer
// (integration/fixture/rbd_journal_consumer.go).
type nativeRBDJournalProducePhase struct {
	Phase          string                    `json:"phase"`
	TimeNS         int64                     `json:"time_ns"`
	IsOwner        bool                      `json:"is_owner"`
	ClassCalls     []nativeRBDWriteClassCall `json:"class_calls"`
	JournalObjects []nativeRBDJournalObject  `json:"journal_objects"`
	Blob           nativeRBDBlob             `json:"blob"`
	Events         []nativeRBDJournalApplied `json:"events"`
	Wait           *struct {
		Polls    int   `json:"polls"`
		WaitedNS int64 `json:"waited_ns"`
		Met      bool  `json:"met"`
	} `json:"wait"`
	ProceedNS int64 `json:"proceed_ns"`
}

// rbdJournalConsumerHeader is the journal header as one producer phase's exec
// showed it.
type rbdJournalConsumerHeader struct {
	MinimumSet, ActiveSet uint64
	Image, Go, Oracle     rbdJournalClient
}

// rbdJournalConsumerPhase is what the record keeps of one producer phase:
// its header, which journal objects existed with their sizes and SHA-256,
// the entries the producer's tag holds in them (decoded by the test) and
// its wait.
type rbdJournalConsumerPhase struct {
	Phase        string
	IsOwner      bool
	Header       rbdJournalConsumerHeader
	ObjectExists []bool
	ObjectSizes  []uint64
	ObjectSHA256 []string
	// Entries are the entries of every tag the producer's binary view
	// holds, by object, in object order (decoded by the test).
	Entries   []rbdJournalPosition
	WaitMet   bool
	WaitPolls int
	Events    int
}

// rbdJournalConsumer is the consumer variant's own record.
type rbdJournalConsumer struct {
	GoDataHex string
	// Register is the Go client's client_register, Registered its record
	// read back right after.
	Register   *rbdLockOwnerLossAttempt
	Registered *rbdJournalClient
	// DisconnectOutput is `rbd journal client disconnect`'s stdout lines.
	DisconnectOutput []string
	DisconnectCode   int
	// SetupTag is the tag of the setup's entries as native librbd observed
	// it; ProducerTag the tag the Go client's read of the image client's
	// position names after the first batch.
	SetupTag, ProducerTag uint64
	Phases                map[string]*rbdJournalConsumerPhase
	// Reads are the Go reads compared with the producer's, by stage, and
	// GoEntries the entries the Go client decoded from its own object reads
	// of each stage, every tag, by object.
	Reads     map[string][]rbdJournalTrimRead
	GoEntries map[string][]rbdJournalPosition
	// Commit is the Go client's commit after the first batch with
	// CommitPositions, Notify the header notification after it;
	// LateCommit the commit after it was disconnected with LatePositions,
	// LateRecord its record read back after that commit.
	CommitPositions []rbdJournalPosition
	Commit          *rbdLockOwnerLossAttempt
	Notified        bool
	LatePositions   []rbdJournalPosition
	LateCommit      *rbdLockOwnerLossAttempt
	LateRecord      *rbdJournalClient
	// The producer's debug lines.
	TrimLog, LaggyLog                   []string
	TrimLogTruncated, LaggyLogTruncated bool
	ExpectedHead                        string
}

// rbdJournalConsumerSteps hands the consumer variant what it needs from the
// shared sequence after the setup.
type rbdJournalConsumerSteps struct {
	ctx           context.Context
	record        *rbdJournalLostAppendRecord
	plan          nativeRBDJournalPlan
	token, dir    string
	gid           uint64
	setup         *nativeRBDJournal
	journalHeader string
	prefix        string
	before        []byte
	route         func(*testing.T, string) rbdRoute
	request       func(rbdRoute, ...cephmsgr.OSDOperation) cephmsgr.OSDRequest
	keep          func(string, rbdRoute, cephmsgr.OSDRequest, cephmsgr.OSDReply, error)
	now           func() cephmsgr.OSDTime
	connect       func(*testing.T, rbdRoute) *cephmsgr.OSDConnection
	native        func(*testing.T, string) *nativeRBDJournal
	nativeRead    func(*testing.T, string) *nativeRBDJournal
	judge         func(*testing.T, string)
	done          func(string)
}

// rbdJournalConsumerPhaseOf projects one producer phase: the header from its
// exec outputs (rbd_journal.go journal_calls order), the objects and the
// entries decoded from its binary view.
func rbdJournalConsumerPhaseOf(dir string, p nativeRBDJournalProducePhase) (*rbdJournalConsumerPhase, []byte, error) {
	v := &rbdJournalConsumerPhase{Phase: p.Phase, IsOwner: p.IsOwner, Entries: []rbdJournalPosition{}, Events: len(p.Events)}
	if p.Wait != nil {
		v.WaitMet, v.WaitPolls = p.Wait.Met, p.Wait.Polls
	}
	if len(p.ClassCalls) != 11 {
		return nil, nil, errors.New("producer phase exec list is incomplete")
	}
	out := func(i int) ([]byte, error) {
		c := p.ClassCalls[i]
		if c.Code != 0 {
			return nil, fmt.Errorf("producer phase exec %s answered %d", c.Method, c.Code)
		}
		return hex.DecodeString(c.OutputHex)
	}
	minimum, err1 := out(3)
	active, err2 := out(4)
	if err := errors.Join(err1, err2); err != nil || len(minimum) != 8 || len(active) != 8 {
		return nil, nil, fmt.Errorf("producer phase object sets do not decode: %v", err)
	}
	v.Header.MinimumSet, v.Header.ActiveSet = binary.LittleEndian.Uint64(minimum), binary.LittleEndian.Uint64(active)
	for i, c := range []*rbdJournalClient{&v.Header.Image, &v.Header.Go, &v.Header.Oracle} {
		data, err := out(6 + i)
		if err != nil {
			return nil, nil, err
		}
		if *c, err = decodeRBDJournalClient(data); err != nil {
			return nil, nil, err
		}
	}
	blob, err := os.ReadFile(filepath.Join(dir, filepath.Base(p.Blob.Name)))
	if err != nil || uint64(len(blob)) != p.Blob.Length || rbdJournalDigest(blob) != p.Blob.SHA256 {
		return nil, nil, fmt.Errorf("producer phase binary view %s differs from its receipt: %v", p.Blob.Name, err)
	}
	for i, o := range p.JournalObjects {
		if o.Number != uint64(i) || o.Offset+o.Size > uint64(len(blob)) {
			return nil, nil, errors.New("producer phase object list differs from its binary view")
		}
		v.ObjectExists, v.ObjectSizes, v.ObjectSHA256 = append(v.ObjectExists, o.Exists), append(v.ObjectSizes, o.Size), append(v.ObjectSHA256, o.SHA256)
		entries, err := decodeRBDJournalObject(o.Number, blob[o.Offset:o.Offset+o.Size])
		if err != nil {
			return nil, nil, err
		}
		for _, en := range entries {
			v.Entries = append(v.Entries, rbdJournalPosition{en.Object, en.Tag, en.EntryID})
		}
	}
	return v, blob, nil
}

// runRBDJournalConsumer is the consumer variant after the setup.
func runRBDJournalConsumer(t *testing.T, s rbdJournalConsumerSteps) {
	r, x := s.record, s.record.Consumer
	ctx := s.ctx
	x.Phases, x.Reads, x.GoEntries = map[string]*rbdJournalConsumerPhase{}, map[string][]rbdJournalTrimRead{}, map[string][]rbdJournalPosition{}
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	headerRoute := s.route(t, s.journalHeader)
	conn := s.connect(t, headerRoute)
	call := func(t *testing.T, label, method string, mode cephmsgr.OSDCallMode, input []byte) (cephmsgr.OSDReply, rbdLockOwnerLossAttempt) {
		t.Helper()
		route := s.route(t, s.journalHeader)
		req := s.request(route, classOp("journal", method, mode, input))
		if mode == cephmsgr.OSDCallWrite {
			req.ModificationTime = s.now()
		}
		reply, err := conn.Request(ctx, req)
		s.keep(label, route, req, reply, err)
		return reply, rbdLockOwnerLossOutcome(label, reply, err)
	}
	readClient := func(t *testing.T, label string) *rbdJournalClient {
		t.Helper()
		reply, got := call(t, label, "get_client", cephmsgr.OSDCallRead, rbdString(s.plan.GoClient))
		if got.Class != "applied" || len(reply.Operations) != 1 {
			t.Fatal("Go journal client read", label, got.Error)
		}
		c, err := decodeRBDJournalClient(reply.Operations[0].Data)
		if err != nil {
			t.Fatal("Go journal client record", label, err)
		}
		return &c
	}

	// The Go client registers as a mirroring peer would.
	goData := rbdJournalMirrorPeerData("ceph-msgr-go-"+s.token[:8], rbdJournalMirrorReplaying)
	x.GoDataHex = hex.EncodeToString(goData)
	_, reg := call(t, "client_register", "client_register", cephmsgr.OSDCallWrite, append(rbdString(s.plan.GoClient), rbdJournalBL(goData)...))
	x.Register = &reg
	if reg.Class != "applied" || reg.Code != 0 {
		t.Fatal("Go journal client registration", reg.Error)
	}
	x.Registered = readClient(t, "get_client_registered")
	s.done("register")
	s.native(t, "observe-registered")
	s.judge(t, "observe-registered")
	n := s.native(t, "disconnect-oracle")
	if n.Disconnect != nil {
		x.DisconnectOutput = splitRBDLines(n.Disconnect.Stdout)
		x.DisconnectCode = n.Disconnect.ReturnCode
	}
	s.judge(t, "disconnect-oracle")

	// The setup's tag as native librbd's observation of the setup shows it,
	// kept for comparison only: the producer's tag is what the Go client's
	// own read of the image client's position names.
	x.SetupTag = r.Views["setup"].LastTag.TID

	// The producer, one native action that waits for three proceed signals.
	statePath := filepath.Join(s.dir, "rbd-journal-state.json")
	for i := 0; i <= 3; i++ {
		name := statePath
		if i > 0 {
			name = filepath.Join(s.dir, fmt.Sprintf("rbd-journal-proceed-%d", i))
		}
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(s.dir, "rbd-journal-produce.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	begun := time.Now()
	var controlErr error
	controlDone := make(chan struct{})
	go func() {
		controlErr = testcluster.RequestControl(ctx, s.dir, "rbd-journal", "rbd-journal-completed", s.token, "produce")
		close(controlDone)
	}()
	defer func() {
		// After a failure the producer gives up at its own 30 s proceed
		// deadline; do not leave the control request behind.
		select {
		case <-controlDone:
		case <-time.After(45 * time.Second):
		}
	}()
	var state struct {
		Token          string                         `json:"token"`
		RequestID      int64                          `json:"request_id"`
		NativeGlobalID uint64                         `json:"native_global_id"`
		Phases         []nativeRBDJournalProducePhase `json:"phases"`
	}
	waitPhase := func(t *testing.T, phase string) ([]nativeRBDJournalObject, []nativeRBDWriteClassCall, []byte) {
		t.Helper()
		for {
			data, err := os.ReadFile(statePath)
			if err == nil && json.Unmarshal(data, &state) == nil {
				if i := slices.IndexFunc(state.Phases, func(p nativeRBDJournalProducePhase) bool { return p.Phase == phase }); i >= 0 {
					p := state.Phases[i]
					if state.Token != s.token || state.NativeGlobalID == 0 || state.NativeGlobalID == s.gid {
						t.Fatal("native producer scope differs", state.Token, state.NativeGlobalID)
					}
					v, blob, err := rbdJournalConsumerPhaseOf(s.dir, p)
					if err != nil {
						t.Fatal(phase, err)
					}
					x.Phases[phase] = v
					s.done(phase)
					return p.JournalObjects, p.ClassCalls, blob
				}
			}
			select {
			case <-controlDone:
				t.Fatalf("native producer ended before phase %s: %v", phase, controlErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	proceed := func(t *testing.T, step int) {
		t.Helper()
		signal := fmt.Sprintf("%s %d %d", s.token, state.RequestID, step)
		if err := testcluster.WritePublicEvidence(filepath.Join(s.dir, fmt.Sprintf("rbd-journal-proceed-%d", step)), []byte(signal)); err != nil {
			t.Fatal(err)
		}
	}
	// reads compares the journal header calls and the journal objects, read
	// by the Go client, with what the producer read in the same phase, and
	// returns what the Go client decoded from its own answers: the entries of
	// every journal object it read and the image client's record. Its commits
	// are computed from these; the producer's reads are the comparison.
	reads := func(t *testing.T, stage string, objects []nativeRBDJournalObject, calls []nativeRBDWriteClassCall, blob []byte) ([]rbdJournalPosition, rbdJournalClient) {
		t.Helper()
		entries, image := []rbdJournalPosition{}, rbdJournalClient{}
		route := s.route(t, s.journalHeader)
		for _, c := range []struct {
			index  int
			method string
			input  []byte
		}{{3, "get_minimum_set", nil}, {4, "get_active_set", nil}, {5, "client_list", rbdJournalClientListInput()}, {6, "get_client", rbdString("")},
			{7, "get_client", rbdString(s.plan.GoClient)}, {8, "get_client", rbdString(s.plan.OracleClient)}} {
			req := s.request(route, classOp("journal", c.method, cephmsgr.OSDCallRead, c.input))
			reply, err := conn.Request(ctx, req)
			label := fmt.Sprintf("%s_%s_%d", stage, c.method, c.index)
			s.keep(label, route, req, reply, err)
			nc := calls[c.index]
			read := rbdJournalTrimRead{Label: label, Object: s.journalHeader, Method: c.method, InputHex: hex.EncodeToString(c.input), Number: -1, NativeExists: true, Code: reply.Code}
			var server *cephmsgr.OSDError
			if errors.As(err, &server) {
				read.Code = server.Code
			}
			if err != nil || len(reply.Operations) != 1 || nc.Method != c.method || nc.InputHex != hex.EncodeToString(c.input) || nc.Code != 0 {
				read.Detail = fmt.Sprintf("go %v, native %s %d", err, nc.Method, nc.Code)
			} else {
				read.Equal = hex.EncodeToString(reply.Operations[0].Data) == nc.OutputHex
				if c.index == 6 {
					if image, err = decodeRBDJournalClient(reply.Operations[0].Data); err != nil {
						t.Fatal("Go read of the image journal client", stage, err)
					}
				}
			}
			x.Reads[stage] = append(x.Reads[stage], read)
		}
		for _, no := range objects {
			route := s.route(t, no.Object)
			req := s.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 256 << 10})
			reply, err := conn.Request(ctx, req)
			label := fmt.Sprintf("%s_read_journal_object_%d", stage, no.Number)
			s.keep(label, route, req, reply, err)
			read := rbdJournalTrimRead{Label: label, Object: no.Object, Number: int(no.Number), NativeExists: no.Exists, Code: reply.Code}
			var server *cephmsgr.OSDError
			if errors.As(err, &server) {
				read.Code = server.Code
			}
			switch {
			case !no.Exists:
				read.Equal = read.Code == -2
			case err == nil && len(reply.Operations) == 2:
				read.Equal = reply.Code == 0 && bytes.Equal(reply.Operations[1].Data, blob[no.Offset:no.Offset+no.Size])
			default:
				read.Detail = fmt.Sprint(err)
			}
			x.Reads[stage] = append(x.Reads[stage], read)
			if err == nil && reply.Code == 0 && len(reply.Operations) == 2 {
				decoded, err := decodeRBDJournalObject(no.Number, reply.Operations[1].Data)
				if err != nil {
					t.Fatal("Go read of journal object", no.Number, err)
				}
				for _, en := range decoded {
					entries = append(entries, rbdJournalPosition{en.Object, en.Tag, en.EntryID})
				}
			}
		}
		x.GoEntries[stage] = entries
		return entries, image
	}
	// producerEntries are the entries of the producer's tag among entries,
	// in entry order.
	producerEntries := func(entries []rbdJournalPosition) []rbdJournalEntry {
		var mine []rbdJournalEntry
		for _, p := range entries {
			if p.Tag == x.ProducerTag {
				mine = append(mine, rbdJournalEntry{Object: p.Object, Tag: p.Tag, EntryID: p.Entry})
			}
		}
		slices.SortStableFunc(mine, func(a, b rbdJournalEntry) int { return int(a.EntryID) - int(b.EntryID) })
		return mine
	}
	notify := func(t *testing.T, label string) {
		t.Helper()
		route := s.route(t, s.journalHeader)
		req := s.request(route)
		req.Operations = nil
		nt, reply, err := conn.NotifyObject(ctx, req, nil, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
		s.keep(label, route, req, reply, err)
		if err != nil {
			t.Fatal("Go journal header notify", label, err)
		}
		defer nt.Close()
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		completion, err := nt.Next(wait)
		if err != nil || completion.Opcode != cephmsgr.OSDWatchCompletion || completion.ReturnCode != 0 || len(completion.Missed) != 0 {
			t.Fatal("Go journal header notify completion", label, err)
		}
		x.Notified = true
	}

	// The first batch: the Go client reads what the producer read, decodes
	// the producer's entries and commits past object set 0.
	objects, calls, blob := waitPhase(t, "produced")
	goEntries, imageClient := reads(t, "produced", objects, calls, blob)
	if len(imageClient.Positions) == 0 {
		t.Fatal("the Go client's read of the image client names no position")
	}
	x.ProducerTag = imageClient.Positions[0].Tag
	s.judge(t, "produced")
	x.CommitPositions = rbdJournalCommitPosition(nil, producerEntries(goEntries), rbdJournalSplay)
	_, commit := call(t, "client_commit", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(s.plan.GoClient), rbdJournalPositionsEncode(x.CommitPositions)...))
	x.Commit = &commit
	if commit.Class != "applied" || commit.Code != 0 {
		t.Fatal("Go journal client commit", commit.Error)
	}
	notify(t, "journal_notify_commit")
	s.done("committed")
	proceed(t, 1)

	objects, calls, blob = waitPhase(t, "trimmed")
	reads(t, "trimmed", objects, calls, blob)
	s.judge(t, "trimmed")
	proceed(t, 2)

	objects, calls, blob = waitPhase(t, "disconnected")
	goEntries, _ = reads(t, "disconnected", objects, calls, blob)
	s.judge(t, "disconnected")
	// The disconnected client commits the position after the entries its own
	// reads of the second batch found.
	x.LatePositions = rbdJournalCommitPosition(x.CommitPositions, producerEntries(goEntries), rbdJournalSplay)
	_, late := call(t, "client_commit_disconnected", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(s.plan.GoClient), rbdJournalPositionsEncode(x.LatePositions)...))
	x.LateCommit = &late
	x.LateRecord = readClient(t, "get_client_disconnected")
	s.done("committed_disconnected")
	proceed(t, 3)

	select {
	case <-controlDone:
	case <-ctx.Done():
		t.Fatal("native producer did not complete", ctx.Err())
	}
	if controlErr != nil {
		t.Fatal("native producer", controlErr)
	}
	r.Timings["native_produce"] = time.Since(begun).String()
	produced := s.nativeRead(t, "produce")
	if l := produced.TrimLog; l != nil {
		x.TrimLog, x.TrimLogTruncated = []string{}, l.Truncated
		for _, line := range l.Lines {
			x.TrimLog = append(x.TrimLog, line.Text)
		}
	}
	if l := produced.LaggyLog; l != nil {
		x.LaggyLog, x.LaggyLogTruncated = []string{}, l.Truncated
		for _, line := range l.Lines {
			x.LaggyLog = append(x.LaggyLog, line.Text)
		}
	}
	// The image: the setup, then the producer's sixteen writes.
	image := slices.Clone(s.before)
	for _, e := range s.plan.ProduceEvents {
		copy(image[e.Offset:], rbdJournalPattern(s.token, "produce", e.Offset, e.Length))
	}
	for _, e := range s.plan.LaggyEvents {
		copy(image[e.Offset:], rbdJournalPattern(s.token, "laggy", e.Offset, e.Length))
	}
	x.ExpectedHead = rbdJournalDigest(image)
	final := readRBDBlob(t, s.dir, produced.Blobs["journal_objects"])
	reads(t, "produce", produced.JournalObjects, produced.ClassCalls, final)
	s.judge(t, "produce")
	t.Logf("go journal consumer producer_tag=%d committed=%v late=%v trim_lines=%d laggy=%v", x.ProducerTag, x.CommitPositions, x.LatePositions, len(x.TrimLog), x.LaggyLog)
}

// splitRBDLines splits command output into its lines.
func splitRBDLines(out string) []string {
	var lines []string
	for _, line := range bytes.Split(bytes.TrimRight([]byte(out), "\n"), []byte("\n")) {
		lines = append(lines, string(line))
	}
	return lines
}
