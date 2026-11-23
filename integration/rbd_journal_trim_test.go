package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// TestCephRBDJournalTrimIntegration fills an object set of an image journal
// with Go appends and lets native librbd trim it after a lock-owner loss. The
// fixture creates the journaling image with 4 KiB journal data objects
// (rbd_journal_order 12, the smallest librbd accepts) and the default splay
// width of four; the setup journals one small entry at each splay offset of
// object set 0, and the native oracle journal client stays registered at an
// empty commit position.
//
// The Go subject takes the image's exclusive lock in librbd's managed-lock
// format and allocates its tag t, then appends entries 0 to 7 of t, each a
// 3 KiB AioWrite, in librbd's placement (entry n at splay offset n mod 4 of
// the active object set) with the journal's object size as the soft limit
// of journal.append. Entries 0 to 3 fit in object set 0. Entry 4 finds
// journal object 0 full and the OSD refuses it with -EOVERFLOW, a known
// refusal that changes nothing (cls/journal/cls_journal.cc
// journal_object_append at ceph/ceph/blob/v20.2.4/src/). As
// journal/JournalRecorder.cc does on an overflow, the subject then raises the
// journal header's active set to 1 (journal.set_active_set), notifies the
// header and appends entry 4 again, now to journal object 4: the refusal was
// known, so this is the protocol's redirect, not the resend of an uncertain
// request. Entries 5 to 7 follow in objects 5 to 7. The subject applies
// entries 0 to 6 to the image's data objects as librbd's ObjectWriteRequest
// does and commits the image client's position after entry 6
// (JournalMetadata::committed keeps one position per splay offset), then
// notifies the header. Entry 7 is journaled and neither applied nor
// committed.
//
// A native observation (observe-committed) shows the journal objects as the
// setup bytes followed by exactly those entries, the minimum set 0 and the
// active set 1, the image client's position after entry 6 (its position at
// splay offset 3 is entry 3, still in object set 0) and the oracle client at
// its empty position. The subject's own class reads of the journal header
// (get_minimum_set, get_active_set, client_list, get_client) and of every
// journal object return the bytes native librados exec and reads returned;
// each method and input and each object number is read exactly once per
// compared stage and judged by that identity and its result code.
// Native `rbd journal client disconnect` then flags the oracle client
// disconnected (tools/rbd/action/Journal.cc), so that only the image client's
// position can keep an object set: JournalTrimmer::handle_metadata_updated
// skips disconnected clients and keeps the lowest object set any other
// client's position names (journal/JournalTrimmer.cc). The next observation
// shows nothing trimmed: no native journal is open, and the image client's
// position still names object set 0. Its `rbd journal inspect` replays
// entry 7 alone: the inspector registers its client at the lowest commit
// position of the connected clients (cls_journal.cc client_register and
// find_min_commit_position skip disconnected ones), now the image client's,
// so it replays exactly what remains after that position. The subject renews its watch and native
// librbd breaks its lock with lock_break, as in
// TestCephRBDJournalForcedBreakIntegration (the subject's address is
// blocklisted, the lock free). The subject sends nothing more.
//
// Finally a native librbd write takes the free lock. Its journal replays
// entry 7, the one entry after the image client's position, once; it
// allocates tag t+1 and journals its own write at splay offset 0 of object
// set 1; its commit of entry 7 moves the image client's last position out of
// object set 0. Its own journal trimmer then removes journal objects 0 to 3
// and raises the minimum set to 1 (JournalTrimmer::trim_objects, remove_set
// and handle_set_removed); the driver keeps the image open until it reads
// the minimum set moved and keeps the trimmer's debug lines, which name the
// four objects. The observation after the takeover shows the minimum set 1,
// objects 0 to 3 gone, objects 5 to 7 unchanged and object 4 as entry 4
// followed by the takeover's entry of tag t+1 (decoded by the test).
// Its `rbd journal inspect` registers at the image client's new position
// and replays nothing: no entry remains after it. The image
// is the setup, the eight Go extents (entry 7 by the replay) and then the
// takeover write over the start of entry 7's extent, and the image client's
// position is the takeover's entry before entries 7, 6 and 5. The observer's
// reads after the takeover agree with native exec on the header and with
// native reads on objects 4 to 7, and the OSD answers ENOENT for objects 0
// to 3. The Go client sent no remove and no set_minimum_set: the trimming
// is native.
//
// Consumer choices kept explicit here: the Go appender's object set
// advance, its commit positions and its not trimming are consumer
// decisions; disconnecting the oracle client is the test's decision to make
// trimming reachable (an oracle at an empty position keeps every object set,
// see TestCephRBDJournalForcedBreakPendingIntegration). Not covered: a
// trimming Go consumer, a laggy-client disconnect
// (rbd_journal_max_concurrent_object_sets), trimming while another client
// replays, and more than one trimmed set.
//
// Everything journal-specific is the test-only consumer code of
// rbd_journal_test.go. The product is unchanged.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournaltrim key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-trim
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdjournaltrim key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-journal-trim
//scenario:covers RBD underlying I/O
//fixture:isolated disconnects the journal oracle client, blocklists the Go lock owner (removed at teardown), leaves an image
func TestCephRBDJournalTrimIntegration(t *testing.T) {
	runRBDJournalLostAppend(t, rbdJournalVariantTrim)
}

// rbdJournalVariantTrim is the variant of runRBDJournalLostAppend this file
// adds.
const rbdJournalVariantTrim = "trim"

// rbdJournalTrimJournalOrder is the journal object order of the fixture's
// trim plan.
const rbdJournalTrimJournalOrder = 12

// rbdJournalOverflow is -EOVERFLOW, cls_journal's answer to an append to a
// full journal object.
const rbdJournalOverflow = -75

// The trimming variant's order and the stages at which it is judged.
var (
	rbdJournalTrimOrder = []string{"setup", "lock", "tag_create", "observe-locked", "appended", "applied", "committed", "observe-committed", "disconnect-oracle", "ping",
		"force-break", "takeover"}
	rbdJournalTrimStages = []string{"observe-committed", "disconnect-oracle", "force-break", "takeover"}
)

// rbdJournalTrimAppend is one journal.append the subject sent: the entry, the
// journal object it went to and the raw outcome.
type rbdJournalTrimAppend struct {
	Entry   uint64
	Object  uint64
	Offset  uint64
	Outcome rbdLockOwnerLossAttempt
}

// rbdJournalTrimRead is one subject or observer read compared with what
// native librados returned in the same observation. A journal header read
// is named by its class method and input (Number -1), a journal object
// read by the object's number. Code is the OSD's answer (0, or the
// OSDError code), NativeExists whether native librados found the object,
// and Equal whether the answer matched native: the same output or bytes,
// or ENOENT for an object native librados did not find.
type rbdJournalTrimRead struct {
	Label, Object    string
	Method, InputHex string
	Number           int
	NativeExists     bool
	Equal            bool
	Code             int32
	Detail           string
}

// rbdJournalTrimHeaderRead is one journal header read every compared stage
// carries exactly once.
type rbdJournalTrimHeaderRead struct{ Method, InputHex string }

// rbdJournalTrimHeaderReads are the header reads of a compared stage: the
// mutable metadata, the client list and the two registered clients, with
// the inputs native exec used (rbd_journal.go journal_calls).
func rbdJournalTrimHeaderReads(oracle string) []rbdJournalTrimHeaderRead {
	return []rbdJournalTrimHeaderRead{{"get_minimum_set", ""}, {"get_active_set", ""}, {"client_list", hex.EncodeToString(rbdJournalClientListInput())},
		{"get_client", hex.EncodeToString(rbdString(""))}, {"get_client", hex.EncodeToString(rbdString(oracle))}}
}

// rbdJournalTrimReadsClaims checks one compared stage: exactly one read of
// each header method and input and of each journal object 0 to
// 2*splay-1, nothing else, each on its own object and matching native. The
// header reads answer 0; an object answers 0 unless gone[n], when native
// librados did not find it and the OSD answered ENOENT.
func rbdJournalTrimReadsClaims(name string, reads []rbdJournalTrimRead, header, prefix, oracle string, gone []bool, fail func(string, ...any)) {
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	seenHeader, seenObject := map[rbdJournalTrimHeaderRead]int{}, map[int]int{}
	for _, rd := range reads {
		if rd.Number < 0 {
			key := rbdJournalTrimHeaderRead{rd.Method, rd.InputHex}
			seenHeader[key]++
			if !slices.Contains(rbdJournalTrimHeaderReads(oracle), key) || rd.Object != header || rd.Code != 0 || !rd.Equal {
				fail("%s: header read %s is not an expected method answered 0 with native's output: %s", name, rd.Label, text(rd))
			}
			continue
		}
		seenObject[rd.Number]++
		if rd.Number >= len(gone) {
			fail("%s: read of an unexpected journal object: %s", name, text(rd))
			continue
		}
		code := int32(0)
		if gone[rd.Number] {
			code = -2
		}
		if rd.Object != prefix+strconv.Itoa(rd.Number) || rd.NativeExists == gone[rd.Number] || rd.Code != code || !rd.Equal {
			fail("%s: read of journal object %d is not answered %d in agreement with native librados: %s", name, rd.Number, code, text(rd))
		}
	}
	for _, key := range rbdJournalTrimHeaderReads(oracle) {
		if seenHeader[key] != 1 {
			fail("%s: header read %s with input %s observed %d times, want once", name, key.Method, key.InputHex, seenHeader[key])
		}
	}
	for n := range gone {
		if seenObject[n] != 1 {
			fail("%s: journal object %d read %d times, want once", name, n, seenObject[n])
		}
	}
}

// rbdJournalTrim is the trimming variant's own record. The lock record
// address, the ping, the break and the takeover are in the record's
// OwnerLoss.
type rbdJournalTrim struct {
	SoftMax uint64
	// Entries are the subject's entries 0 to 7 of its tag where they were
	// finally appended.
	Entries []rbdJournalLostAppendEntry
	// Appends are the appends in the order sent, the refused one included,
	// and ActiveSet the set_active_set after the refusal.
	Appends   []rbdJournalTrimAppend
	ActiveSet *rbdLockOwnerLossAttempt
	// Applied are the entries whose image data write was answered 0, in the
	// order written; Commit is the client_commit of the image client's
	// position after entry 6, CommitPositions that position.
	Applied         []uint64
	Commit          *rbdLockOwnerLossAttempt
	CommitPositions []rbdJournalPosition
	Notifies        []string
	// The journal objects 0 to 7 as they must be after the appends: sizes
	// and SHA-256 of the setup bytes followed by the subject's entries.
	SizesAppended  []uint64
	SHA256Appended []string
	// The image after entries 0 to 6 and after the takeover.
	ExpectedHeadCommitted, ExpectedHeadTakeover string
	ExpectedCommitTakeover                      []rbdJournalPosition
	TakeoverEvents                              []rbdJournalLostAppendEvent
	// DisconnectOutput is `rbd journal client disconnect`'s stdout lines,
	// DisconnectCode and DisconnectStderr its exit code and stderr.
	DisconnectOutput []string
	DisconnectCode   int
	DisconnectStderr string
	// Reads are the Go reads compared with native ones, by stage.
	Reads map[string][]rbdJournalTrimRead
	// The takeover: its wait for the trimmer, the trimmer's debug lines and
	// the entries journal object 4 holds after the subject's entry 4.
	TrimWaitPolls     int
	TrimWaitNS        int64
	TrimWaitMinimum   uint64
	TrimWaitMoved     bool
	TrimLog           []string
	TrimLogTruncated  bool
	Object4Tail       []rbdJournalEntry
	Object4TailEvents []rbdJournalLostAppendEvent
}

// rbdJournalTrimSteps hands the trimming variant the forced-break steps and
// what it needs beyond them: the observer, the setup and the image before
// the subject's writes, the journal header and data prefix, the soft limit,
// the image client's position at the setup and the control directory.
type rbdJournalTrimSteps struct {
	rbdJournalForcedBreakSteps
	observer       *cephmsgr.Client
	setup          *nativeRBDJournal
	journalHeader  string
	prefix         string
	softMax        uint64
	imagePositions []rbdJournalPosition
	before         []byte
	imageOutput    []byte
	dir            string
}

// runRBDJournalTrim is the trimming variant after the observation of the
// locked image.
func runRBDJournalTrim(t *testing.T, s rbdJournalTrimSteps) {
	r, ol, x := s.record, s.record.OwnerLoss, s.record.Trim
	ctx := s.ctx
	x.SoftMax, x.Reads = s.softMax, map[string][]rbdJournalTrimRead{}
	classOp := func(class, method string, mode cephmsgr.OSDCallMode, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 65536}}
	}
	object := func(n uint64) string { return s.prefix + strconv.FormatUint(n, 10) }
	tag := s.entry.Tag
	// The journal objects as the subject builds them, from the bytes native
	// librados read at observe-locked.
	blob := readRBDBlob(t, s.dir, s.locked.Blobs["journal_objects"])
	objects := make([][]byte, len(s.locked.JournalObjects))
	for i, o := range s.locked.JournalObjects {
		objects[i] = slices.Clone(blob[o.Offset : o.Offset+o.Size])
	}
	notify := func(t *testing.T, label string) {
		t.Helper()
		route := s.route(t, s.journalHeader)
		req := s.request(route)
		req.Operations = nil
		n, reply, err := s.conn.NotifyObject(ctx, req, nil, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
		s.keep(label, route, req, reply, err)
		if err != nil {
			t.Fatal("Go journal header notify", label, err)
		}
		defer n.Close()
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		completion, err := n.Next(wait)
		if err != nil || completion.Opcode != cephmsgr.OSDWatchCompletion || completion.ReturnCode != 0 || len(completion.Missed) != 0 {
			t.Fatal("Go journal header notify completion", label, err)
		}
		x.Notifies = append(x.Notifies, label)
	}

	// Entries 0 to 7 of the subject's tag, librbd's placement: entry n at
	// splay offset n mod 4 of the active set. A known -EOVERFLOW closes the
	// set: the active set is raised and the entry goes to the next set.
	active := uint64(0)
	for i, extent := range s.plan.GoWrites {
		at := s.now()
		data := rbdJournalEntryEncode(tag, uint64(i), rbdJournalWriteEvent(extent.Offset, rbdJournalPattern(s.token, "go-trim", extent.Offset, extent.Length), at))
		for {
			n := active*s.splay + uint64(i)%s.splay
			if n >= uint64(len(objects)) {
				t.Fatal("a Go entry falls outside the observed journal objects", i, n)
			}
			label := fmt.Sprintf("journal_append_%d_object_%d", i, n)
			route := s.route(t, object(n))
			req := s.request(route, classOp("journal", "append", cephmsgr.OSDCallWrite, append(rbdLE64(s.softMax), rbdJournalBL(data)...)))
			req.ModificationTime = s.now()
			reply, err := s.conn.Request(ctx, req)
			s.keep(label, route, req, reply, err)
			outcome := rbdLockOwnerLossOutcome(label, reply, err)
			x.Appends = append(x.Appends, rbdJournalTrimAppend{Entry: uint64(i), Object: n, Offset: uint64(len(objects[n])), Outcome: outcome})
			if outcome.Class == "applied" && outcome.Code == 0 {
				x.Entries = append(x.Entries, rbdJournalLostAppendEntry{Tag: tag, EntryTID: uint64(i), Object: n, ObjectName: object(n), Offset: len(objects[n]), Length: len(data),
					SHA256: rbdJournalDigest(data), ImageOffset: extent.Offset, ImageLength: extent.Length, Timestamp: at})
				objects[n] = append(objects[n], data...)
				break
			}
			if outcome.Class != "refused" || outcome.ErrorCode != rbdJournalOverflow || active != 0 {
				t.Fatalf("Go %s: not applied and not the one known overflow of object set 0; nothing is sent again: %+v", label, outcome)
			}
			// JournalRecorder closes the full set and moves to the next one.
			reply = s.send(t, s.conn, s.gid, "journal_set_active_set_1", s.journalHeader, classOp("journal", "set_active_set", cephmsgr.OSDCallWrite, rbdLE64(1)))
			attempt := rbdLockOwnerLossOutcome("journal_set_active_set_1", reply, nil)
			x.ActiveSet = &attempt
			notify(t, "journal_notify_active_set")
			active = 1
		}
	}
	*s.entry = x.Entries[0]
	r.Entry = x.Entries[0]
	for _, o := range objects {
		x.SizesAppended, x.SHA256Appended = append(x.SizesAppended, uint64(len(o))), append(x.SHA256Appended, rbdJournalDigest(o))
	}
	s.done("appended")

	// Entries 0 to 6 applied as librbd's ObjectWriteRequest does: [SETALLOCHINT
	// FAILOK, WRITE] without SnapContext, on the image's data objects.
	image := slices.Clone(s.before)
	objectSize := uint64(1) << s.plan.Order
	for i, e := range x.Entries[:len(x.Entries)-1] {
		data := rbdJournalPattern(s.token, "go-trim", e.ImageOffset, e.ImageLength)
		name := fmt.Sprintf("%s.%016x", s.setup.Stat.Prefix, e.ImageOffset/objectSize)
		route := s.route(t, name)
		req := s.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: e.ImageOffset % objectSize, Length: e.ImageLength, Data: data})
		req.ModificationTime = s.now()
		label := fmt.Sprintf("data_write_%d", i)
		reply, err := s.conn.Request(ctx, req)
		s.keep(label, route, req, reply, err)
		if err != nil || reply.Code != 0 {
			t.Fatal("Go image data write", label, reply.Code, err)
		}
		copy(image[e.ImageOffset:], data)
		x.Applied = append(x.Applied, e.EntryTID)
	}
	x.ExpectedHeadCommitted = rbdJournalDigest(image)
	s.done("applied")

	// The image client's position after entry 6, then the header update.
	var committed []rbdJournalEntry
	for _, e := range x.Entries[:len(x.Entries)-1] {
		committed = append(committed, rbdJournalEntry{Object: e.Object, Tag: e.Tag, EntryID: e.EntryTID})
	}
	x.CommitPositions = rbdJournalCommitPosition(s.imagePositions, committed, s.splay)
	reply := s.send(t, s.conn, s.gid, "client_commit_image", s.journalHeader, classOp("journal", "client_commit", cephmsgr.OSDCallWrite, append(rbdString(""), rbdJournalPositionsEncode(x.CommitPositions)...)))
	attempt := rbdLockOwnerLossOutcome("client_commit_image", reply, nil)
	x.Commit = &attempt
	notify(t, "journal_notify_commit")
	s.done("committed")

	// reads compares the journal header calls and the journal objects, read
	// on o, with what the native observation of stage returned.
	reads := func(t *testing.T, stage string, o *cephmsgr.OSDConnection, n *nativeRBDJournal) {
		t.Helper()
		route := s.route(t, s.journalHeader)
		for _, c := range []struct {
			index  int
			method string
			input  []byte
		}{{3, "get_minimum_set", nil}, {4, "get_active_set", nil}, {5, "client_list", rbdJournalClientListInput()}, {6, "get_client", rbdString("")}, {8, "get_client", rbdString(s.plan.OracleClient)}} {
			req := s.request(route, classOp("journal", c.method, cephmsgr.OSDCallRead, c.input))
			reply, err := o.Request(ctx, req)
			label := fmt.Sprintf("%s_%s_%d", stage, c.method, c.index)
			s.keep(label, route, req, reply, err)
			native := n.ClassCalls[c.index]
			read := rbdJournalTrimRead{Label: label, Object: s.journalHeader, Method: c.method, InputHex: hex.EncodeToString(c.input), Number: -1, NativeExists: true, Code: reply.Code}
			var server *cephmsgr.OSDError
			if errors.As(err, &server) {
				read.Code = server.Code
			}
			if err != nil || len(reply.Operations) != 1 || native.Method != c.method || native.InputHex != hex.EncodeToString(c.input) || native.Code != 0 {
				read.Detail = fmt.Sprintf("go %v, native %s %d", err, native.Method, native.Code)
			} else {
				read.Equal = hex.EncodeToString(reply.Operations[0].Data) == native.OutputHex
			}
			x.Reads[stage] = append(x.Reads[stage], read)
		}
		nativeBlob := readRBDBlob(t, s.dir, n.Blobs["journal_objects"])
		for _, no := range n.JournalObjects {
			route := s.route(t, no.Object)
			req := s.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 256 << 10})
			reply, err := o.Request(ctx, req)
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
				read.Equal = reply.Code == 0 && bytes.Equal(reply.Operations[1].Data, nativeBlob[no.Offset:no.Offset+no.Size])
			default:
				read.Detail = fmt.Sprint(err)
			}
			x.Reads[stage] = append(x.Reads[stage], read)
		}
	}

	n := s.native(t, "observe-committed")
	reads(t, "observe-committed", s.conn, n)
	s.judge(t, "observe-committed")

	n = s.native(t, "disconnect-oracle")
	if n.Disconnect != nil {
		x.DisconnectOutput = strings.Split(strings.TrimRight(n.Disconnect.Stdout, "\n"), "\n")
		x.DisconnectCode, x.DisconnectStderr = n.Disconnect.ReturnCode, n.Disconnect.Stderr
	}
	s.judge(t, "disconnect-oracle")

	// The subject's watch is live: renewed right before the break.
	headerRoute := s.route(t, s.header)
	watchRequest := s.request(headerRoute)
	watchRequest.Operations = nil
	ping, err := s.w.Ping(ctx, headerRoute.Epoch)
	s.keep("watch_ping", headerRoute, watchRequest, ping, err)
	pinged := rbdLockOwnerLossOutcome("watch_ping", ping, err)
	ol.PingBefore = &pinged
	if err != nil {
		t.Fatal("subject watch ping before the break", err)
	}
	s.done("ping")
	s.released()
	b, err := rbdJournalBreakOf(s.native(t, "force-break"), r.Views["force-break"])
	if err != nil {
		t.Fatal(err)
	}
	ol.Break = b
	s.judge(t, "force-break")

	// The takeover's expectations: entry 7 replayed, then its writes as
	// entries 0, 1, ... of the next tag at splay offsets 0, 1, ... of set 1.
	last := x.Entries[len(x.Entries)-1]
	copy(image[last.ImageOffset:], rbdJournalPattern(s.token, "go-trim", last.ImageOffset, last.ImageLength))
	after := []rbdJournalEntry{{Object: last.Object, Tag: tag, EntryID: last.EntryTID}}
	for i, w := range s.plan.TakeoverEvents {
		copy(image[w.Offset:], rbdJournalPattern(s.token, "takeover", w.Offset, w.Length))
		x.TakeoverEvents = append(x.TakeoverEvents, rbdJournalLostAppendEvent{Type: "AioWrite", Offset: w.Offset, Length: w.Length})
		after = append(after, rbdJournalEntry{Object: s.splay + uint64(i)%s.splay, Tag: tag + 1, EntryID: uint64(i)})
	}
	x.ExpectedHeadTakeover = rbdJournalDigest(image)
	x.ExpectedCommitTakeover = rbdJournalCommitPosition(x.CommitPositions, after, s.splay)
	taken := s.native(t, "takeover")
	ol.Takeover = rbdJournalTakeoverOf(taken)
	if w := taken.TrimWait; w != nil {
		x.TrimWaitPolls, x.TrimWaitNS, x.TrimWaitMinimum, x.TrimWaitMoved = w.Polls, w.WaitedNS, w.MinimumSet, w.Moved
	}
	if l := taken.TrimLog; l != nil {
		x.TrimLog, x.TrimLogTruncated = []string{}, l.Truncated
		for _, line := range l.Lines {
			x.TrimLog = append(x.TrimLog, line.Text)
		}
	}
	// What journal object 4 holds after the subject's entry 4, decoded.
	takenBlob := readRBDBlob(t, s.dir, taken.Blobs["journal_objects"])
	if o4 := taken.JournalObjects[s.splay]; o4.Exists && o4.Size >= x.SizesAppended[s.splay] {
		data := takenBlob[o4.Offset : o4.Offset+o4.Size]
		if bytes.Equal(data[:x.SizesAppended[s.splay]], objects[s.splay]) {
			tail, err := decodeRBDJournalObject(s.splay, data[x.SizesAppended[s.splay]:])
			if err != nil {
				t.Fatal("journal object 4 after the takeover", err)
			}
			x.Object4Tail = tail
			for _, e := range tail {
				ev, err := decodeRBDJournalEvent(e.Payload)
				if err != nil {
					t.Fatal("journal object 4 takeover event", err)
				}
				x.Object4TailEvents = append(x.Object4TailEvents, rbdJournalLostAppendEvent{Type: map[uint32]string{rbdJournalEventWrite: "AioWrite"}[ev.Type], Offset: ev.Offset, Length: ev.Length})
			}
		}
	}
	// The observer reads what the takeover left: the subject is fenced.
	hr := s.route(t, s.journalHeader)
	oc, err := s.observer.OpenOSD(ctx, cephmsgr.OSDTarget{ID: hr.Primary, Address: hr.Address})
	if err != nil {
		t.Fatal("observer journal connection", err)
	}
	reads(t, "takeover", oc, taken)
	s.judge(t, "takeover")
	t.Logf("go journal trim tag=%d overflow_at=%d active_set=1 committed=%v trim_wait=%v polls=%d minimum_set=%d takeover_write=%v",
		tag, slices.IndexFunc(x.Appends, func(a rbdJournalTrimAppend) bool { return a.Outcome.Class == "refused" }), x.CommitPositions,
		time.Duration(x.TrimWaitNS), x.TrimWaitPolls, x.TrimWaitMinimum, time.Duration(taken.WriteNS))
}

// rbdJournalEntryEvent is the event `rbd journal inspect` prints for a Go
// AioWrite entry.
func rbdJournalEntryEvent(en rbdJournalLostAppendEntry) rbdJournalLostAppendEvent {
	return rbdJournalLostAppendEvent{Type: "AioWrite", Offset: en.ImageOffset, Length: en.ImageLength,
		Timestamp: time.Unix(int64(en.Timestamp.Seconds), int64(en.Timestamp.Nanoseconds)/1000*1000).UTC().Format(time.RFC3339Nano)}
}

// rbdJournalTrimMarks are the trimmer's debug lines the takeover must show,
// in this order (journal/JournalTrimmer.cc trim_objects, remove_set and
// handle_set_removed at ceph/ceph/blob/v20.2.4/src/); prefix is the
// journal's data object prefix.
func rbdJournalTrimMarks(prefix string, splay uint64) []string {
	marks := []string{"trim_objects: min_set=1", "remove_set: removing object set 0"}
	for n := range splay {
		marks = append(marks, "removing journal object "+prefix+strconv.FormatUint(n, 10))
	}
	return append(marks, "handle_set_removed: r=0, set=0, trim=1")
}

// rbdJournalSubsequence says whether want occurs in lines in order.
func rbdJournalSubsequence(lines, want []string) bool {
	i := 0
	for _, line := range lines {
		if i < len(want) && line == want[i] {
			i++
		}
	}
	return i == len(want)
}

// rbdJournalTrimClaims checks the trimming record up to one stage. It keeps
// apart what the subject did and what native clients saw of it
// ("observe-committed"), the oracle client's disconnect
// ("disconnect-oracle"), the break of the live owner ("force-break") and the
// native takeover with its trim ("takeover").
func rbdJournalTrimClaims(r *rbdJournalLostAppendRecord, stage string) []string {
	var problems []string
	fail := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	text := func(v any) string { data, _ := json.Marshal(v); return string(data) }
	ol, x, e := r.OwnerLoss, r.Trim, r.Entry
	reached := slices.Index(rbdJournalTrimStages, stage)
	if reached < 0 {
		return []string{"unknown stage " + stage}
	}
	if ol == nil || !ol.Forced || x == nil {
		return []string{"record: not a trimming record"}
	}
	const splay = rbdJournalSplay
	view := func(name string, from int) *rbdJournalLostAppendView {
		if reached < from {
			return nil
		}
		v := r.Views[name]
		if v == nil || v.Action != name || len(v.ObjectSizes) != 2*splay || len(v.ObjectSHA256) != 2*splay || len(v.ObjectExists) != 2*splay {
			fail("%s: native observation missing or incomplete", name)
			return nil
		}
		return v
	}
	setup, locked, committed := view("setup", 0), view("observe-locked", 0), view("observe-committed", 0)
	disconnected, broke, takeover := view("disconnect-oracle", 1), view("force-break", 2), view("takeover", 3)
	if len(problems) != 0 {
		return problems
	}
	if want := rbdJournalTrimOrder[:slices.Index(rbdJournalTrimOrder, stage)+1]; !reflect.DeepEqual(r.Order, want) {
		fail("order: steps completed as %v, want %v", r.Order, want)
	}
	if e.Tag == 0 || e.EntryTID != 0 || e.Object != 0 || r.SubjectGlobalID == 0 || r.SubjectGlobalID == r.ObserverGlobalID || r.WatchCookie == 0 || ol.LockerAddress == "" ||
		len(r.Token) != 32 || x.SoftMax != 1<<rbdJournalTrimJournalOrder {
		fail("identity: entry, client identity, lock record address or soft limit incomplete")
	}
	owner := fmt.Sprintf("client.%d", r.SubjectGlobalID)
	oracle := "ceph-msgr-oracle-" + r.Token[:min(8, len(r.Token))]
	held := func(name string, v *rbdJournalLostAppendView) {
		if v.LockFree || v.LockOwner != owner || v.LockCookie != r.LockCookie || !v.SubjectWatcher {
			fail("%s: the subject does not hold the lock with its watch (free=%v owner=%q cookie=%q watcher=%v)", name, v.LockFree, v.LockOwner, v.LockCookie, v.SubjectWatcher)
		}
	}
	// clients checks the registered clients: the image client at position
	// image and the oracle client at its empty position in state.
	clients := func(name string, v *rbdJournalLostAppendView, image []rbdJournalPosition, state string) {
		if len(v.Clients) != 2 || len(setup.Clients) != 2 || v.Clients[0].ID != "" || !reflect.DeepEqual(v.Clients[0].Positions, image) || v.Clients[0].State != setup.Clients[0].State ||
			v.Clients[1].ID != oracle || len(v.Clients[1].Positions) != 0 || v.Clients[1].State != state {
			fail("%s: the registered clients are not the image client at %v and the oracle client at its empty position %s: %s", name, image, state, text(v.Clients))
		}
	}
	sets := func(name string, v *rbdJournalLostAppendView, minimum, active uint64) {
		if v.MinimumSet != minimum || v.ActiveSet != active {
			fail("%s: the journal's minimum and active sets are %d and %d, want %d and %d", name, v.MinimumSet, v.ActiveSet, minimum, active)
		}
	}
	journal := func(name string, v *rbdJournalLostAppendView) {
		if !slices.Equal(v.ObjectSizes, x.SizesAppended) || !slices.Equal(v.ObjectSHA256, x.SHA256Appended) || slices.Contains(v.ObjectExists, false) {
			fail("%s: the journal objects are not the setup bytes followed by exactly the subject's entries (sizes %v, want %v)", name, v.ObjectSizes, x.SizesAppended)
		}
	}
	prefix := e.ObjectName[:len(e.ObjectName)-1]
	reads := func(name string, gone []bool) {
		rbdJournalTrimReadsClaims(name, x.Reads[name], r.JournalHeader, prefix, oracle, gone, fail)
	}
	blocked := func(name string, blocks []rbdLockOwnerLossBlock, want bool) {
		if blocks == nil || rbdJournalBlocked(blocks, ol.LockerAddress) != want {
			fail("%s: the subject's lock record address %q blocklisted is not %v: %s", name, ol.LockerAddress, want, text(blocks))
		}
	}
	setupCommit := setup.ImageCommit

	// The setup: one small entry per splay offset of set 0, nothing in set 1.
	sets("setup", setup, 0, 0)
	clients("setup", setup, setupCommit, setup.Clients[1].State)
	if setup.Clients[1].State != "connected" || len(setupCommit) != splay || !slices.Equal(locked.ObjectSHA256, setup.ObjectSHA256) ||
		!slices.Equal(setup.ObjectExists, []bool{true, true, true, true, false, false, false, false}) {
		fail("setup: not one native entry per splay offset of object set 0 with a connected oracle client (commit %v, objects %v)", setupCommit, setup.ObjectExists)
	}
	for i := range splay {
		if setup.ObjectSizes[i] == 0 || setup.ObjectSizes[i] >= x.SoftMax {
			fail("setup: journal object %d is empty or already full (%d bytes)", i, setup.ObjectSizes[i])
		}
	}
	held("observe-locked", locked)

	// The appends: entries 0 to 3 in set 0, entry 4 refused at the full
	// object 0, the active set raised, entries 4 to 7 in set 1.
	type sent struct {
		entry, object uint64
		applied       bool
	}
	want := []sent{{0, 0, true}, {1, 1, true}, {2, 2, true}, {3, 3, true}, {4, 0, false}, {4, 4, true}, {5, 5, true}, {6, 6, true}, {7, 7, true}}
	got := []sent{}
	for _, a := range x.Appends {
		ok := a.Outcome.Invocations == 1 && !a.Outcome.UnknownOutcome && a.Outcome.Submission.GlobalID == r.SubjectGlobalID && a.Outcome.Submission.Transaction != 0
		if a.Outcome.Class == "applied" {
			ok = ok && a.Outcome.Code == 0 && a.Outcome.ReplyTransaction == a.Outcome.Submission.Transaction && a.Offset < x.SoftMax
		} else {
			ok = ok && a.Outcome.Class == "refused" && a.Outcome.ErrorCode == rbdJournalOverflow && a.Offset >= x.SoftMax
		}
		if !ok {
			fail("append: entry %d to object %d is not one known answer 0 below the soft limit or one known -EOVERFLOW at it: %s", a.Entry, a.Object, text(a))
		}
		got = append(got, sent{a.Entry, a.Object, a.Outcome.Class == "applied"})
	}
	if !reflect.DeepEqual(got, want) {
		fail("append: the subject's appends are %v, want %v", got, want)
	}
	if a := x.ActiveSet; a == nil || a.Class != "applied" || a.Code != 0 || !slices.Equal(x.Notifies, []string{"journal_notify_active_set", "journal_notify_commit"}) {
		fail("append: the active set was not raised once after the overflow and the header notified: %s, notifies %v", text(a), x.Notifies)
	}
	if len(x.Entries) != 8 || !slices.Equal(x.Applied, []uint64{0, 1, 2, 3, 4, 5, 6}) {
		fail("append: not eight entries journaled and entries 0 to 6 applied once each in order (%d, %v)", len(x.Entries), x.Applied)
	} else {
		for i, en := range x.Entries {
			if en.Tag != e.Tag || en.EntryTID != uint64(i) || en.Object != uint64(i) || en.Offset >= int(x.SoftMax) || en.Length <= 0 {
				fail("append: entry %d is not where its append was answered: %s", i, text(en))
			}
		}
	}
	committedPositions := []rbdJournalPosition{{6, e.Tag, 6}, {5, e.Tag, 5}, {4, e.Tag, 4}, {3, e.Tag, 3}}
	if c := x.Commit; c == nil || c.Class != "applied" || c.Code != 0 || !reflect.DeepEqual(x.CommitPositions, committedPositions) {
		fail("commit: the image client's position after entry 6 was not committed once (%v): %s", x.CommitPositions, text(c))
	}

	// What native clients saw before the oracle was disconnected.
	journal("observe-committed", committed)
	sets("observe-committed", committed, 0, 1)
	clients("observe-committed", committed, committedPositions, "connected")
	held("observe-committed", committed)
	if committed.ImageHeadSHA256 != x.ExpectedHeadCommitted || x.ExpectedHeadCommitted == r.ExpectedHeadBefore || committed.NextTag != e.Tag+1 || committed.LastTag.TID != e.Tag {
		fail("observe-committed: image or tags differ from entries 0 to 6 applied (head=%s next tag=%d)", committed.ImageHeadSHA256, committed.NextTag)
	}
	reads("observe-committed", make([]bool, 2*splay))
	if reached < 1 {
		return problems
	}

	// The oracle client disconnected; nothing trimmed.
	if !slices.Equal(x.DisconnectOutput, []string{"client " + oracle + " disconnected"}) || x.DisconnectCode != 0 || x.DisconnectStderr != "" {
		fail("disconnect-oracle: rbd journal client disconnect did not report the oracle client alone: %v (exit %d, stderr %q)", x.DisconnectOutput, x.DisconnectCode, x.DisconnectStderr)
	}
	// `rbd journal inspect` registers its client at the lowest commit
	// position of the connected clients (cls_journal.cc client_register,
	// find_min_commit_position), now the image client's: it replays exactly
	// what remains after that position, entry 7.
	if len(x.Entries) == 8 && (disconnected.InspectTotal == nil || disconnected.InspectErrors == nil || *disconnected.InspectTotal != 1 || *disconnected.InspectErrors != 0 ||
		!reflect.DeepEqual(disconnected.InspectOwned, []rbdJournalLostAppendEvent{rbdJournalEntryEvent(x.Entries[7])}) || len(disconnected.InspectNext) != 0) {
		fail("disconnect-oracle: rbd journal inspect does not replay entry 7 alone (total %s, owned %s, next %s)",
			text(disconnected.InspectTotal), text(disconnected.InspectOwned), text(disconnected.InspectNext))
	}
	journal("disconnect-oracle", disconnected)
	sets("disconnect-oracle", disconnected, 0, 1)
	clients("disconnect-oracle", disconnected, committedPositions, "disconnected")
	held("disconnect-oracle", disconnected)
	if disconnected.ImageHeadSHA256 != x.ExpectedHeadCommitted {
		fail("disconnect-oracle: image moved")
	}
	if reached < 2 {
		return problems
	}

	// The break of the live owner.
	b := ol.Break
	if p := ol.PingBefore; p == nil || p.Class != "applied" || p.Code != 0 || p.Invocations != 1 {
		fail("ping: the subject's watch was not renewed before the break: %s", text(p))
	}
	if b == nil || !slices.Equal(b.Owners, []string{ol.LockerAddress}) || b.LockerBefore != owner || b.LockerCookieBefore != r.LockCookie || b.LockerAddressBefore != ol.LockerAddress ||
		!slices.Contains(b.WatchersBefore, rbdJournalWatch{r.SubjectGlobalID, r.WatchCookie, ol.LockerAddress}) || len(b.OwnersAfter) != 0 || b.IsOwner || b.BreakNS <= 0 {
		fail("force-break: native librbd did not break the lock of the one live owner, the subject: %s", text(b))
	} else {
		blocked("force-break (before)", b.BlocklistBefore, false)
		blocked("force-break (after)", b.Blocklist, true)
		rbdJournalLockPathClaims("force-break lock path", b.LockPath, rbdJournalForcedBreakLockPath, fail)
	}
	journal("force-break", broke)
	sets("force-break", broke, 0, 1)
	clients("force-break", broke, committedPositions, "disconnected")
	if !broke.LockFree || broke.ImageHeadSHA256 != x.ExpectedHeadCommitted {
		fail("force-break: the lock is still held or the image moved")
	}
	blocked("force-break", broke.Blocklist, true)
	if reached < 3 {
		return problems
	}

	// The takeover: a free lock, entry 7 replayed once, the trim of set 0.
	tk := ol.Takeover
	if tk == nil {
		return append(problems, "takeover: native output missing")
	}
	if !tk.Owner || tk.WriteNS <= 0 || !takeover.LockFree || takeover.SubjectWatcher {
		fail("takeover: native librbd did not own the lock for its writes and release it (owner=%v free=%v subject watcher=%v)", tk.Owner, takeover.LockFree, takeover.SubjectWatcher)
	}
	rbdJournalTakeoverBoundaryClaims(r, fail)
	blocked("takeover (before)", tk.BlocklistBefore, true)
	blocked("takeover (after)", tk.Blocklist, true)
	writes := []rbdJournalLostAppendEvent{}
	for _, w := range tk.Events {
		writes = append(writes, rbdJournalLostAppendEvent{Type: w.Type, Offset: w.Offset, Length: w.Length})
	}
	if len(x.TakeoverEvents) == 0 || !reflect.DeepEqual(writes, x.TakeoverEvents) {
		fail("takeover: the native writes differ from the plan: %s, want %s", text(tk.Events), text(x.TakeoverEvents))
	}
	if !x.TrimWaitMoved || x.TrimWaitMinimum != 1 || x.TrimWaitPolls < 1 {
		fail("trim: the takeover did not see the journal's minimum set move to 1 while its image was open (moved=%v minimum=%d polls=%d)", x.TrimWaitMoved, x.TrimWaitMinimum, x.TrimWaitPolls)
	}
	if x.TrimLogTruncated || !rbdJournalSubsequence(x.TrimLog, rbdJournalTrimMarks(prefix, splay)) || slices.Contains(x.TrimLog, "remove_set: removing object set 1") {
		fail("trim: the takeover's journal trimmer log does not show set 0 alone removed and the minimum set raised: %s", text(x.TrimLog))
	}
	sets("takeover", takeover, 1, 1)
	clients("takeover", takeover, x.ExpectedCommitTakeover, "disconnected")
	if !slices.Equal(takeover.ObjectExists, []bool{false, false, false, false, true, true, true, true}) {
		fail("takeover: object set 0 is not removed while set 1 stays: %v", takeover.ObjectExists)
	}
	for i := splay + 1; i < 2*splay; i++ {
		if takeover.ObjectSHA256[i] != x.SHA256Appended[i] || takeover.ObjectSizes[i] != x.SizesAppended[i] {
			fail("takeover: journal object %d changed", i)
		}
	}
	if takeover.ObjectSizes[splay] <= x.SizesAppended[splay] || len(x.Object4Tail) != len(x.TakeoverEvents) || !reflect.DeepEqual(x.Object4TailEvents, x.TakeoverEvents) {
		fail("takeover: journal object %d is not entry 4 followed by the takeover's entries (%d bytes): %s", splay, takeover.ObjectSizes[splay], text(x.Object4Tail))
	}
	for i, te := range x.Object4Tail {
		if te.Tag != e.Tag+1 || te.EntryID != uint64(i) {
			fail("takeover: the entry after entry 4 in journal object %d is not entry %d of tag %d: %s", splay, i, e.Tag+1, text(te))
		}
	}
	// After the takeover the image client's position is the takeover's last
	// entry, and inspect registers there: nothing remains to replay.
	if takeover.InspectTotal == nil || takeover.InspectErrors == nil || *takeover.InspectTotal != 0 || *takeover.InspectErrors != 0 || len(takeover.InspectOwned) != 0 ||
		len(takeover.InspectNext) != 0 {
		fail("takeover: rbd journal inspect replays entries after the image client's position (total %s, owned %s, next %s)",
			text(takeover.InspectTotal), text(takeover.InspectOwned), text(takeover.InspectNext))
	}
	if x.ExpectedHeadTakeover == "" || x.ExpectedHeadTakeover == x.ExpectedHeadCommitted || takeover.ImageHeadSHA256 != x.ExpectedHeadTakeover {
		fail("takeover: image differs from the replay of entry 7 followed by the takeover writes (head=%s)", takeover.ImageHeadSHA256)
	}
	if len(x.ExpectedCommitTakeover) == 0 || x.ExpectedCommitTakeover[0] != (rbdJournalPosition{splay, e.Tag + 1, 0}) || !reflect.DeepEqual(takeover.ImageCommit, x.ExpectedCommitTakeover) {
		fail("takeover: image client commit position differs from the takeover's entries after entry 7 (commit=%v)", takeover.ImageCommit)
	}
	if takeover.LastTag != (rbdJournalLostAppendTag{e.Tag + 1, true, e.Tag, 7}) || takeover.NextTag != e.Tag+2 {
		fail("takeover: native tag differs (tag=%+v next=%d)", takeover.LastTag, takeover.NextTag)
	}
	reads("takeover", []bool{true, true, true, true, false, false, false, false})
	// The trimming is native: the subject never removed an object or moved
	// the minimum set.
	for _, req := range r.Requests {
		for _, in := range req.Inputs {
			if in.Method == "set_minimum_set" || in.Code == uint16(cephmsgr.OSDDelete) {
				fail("trim: the subject sent %s (%s %d)", req.Label, in.Method, in.Code)
			}
		}
	}
	return problems
}
