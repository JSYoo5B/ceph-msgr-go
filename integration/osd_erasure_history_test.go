package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/bits"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// pgHistoryLocation is one position of an exact request identity in the
// native primary PG-log dump (tools/osd_pg_history_oracle.py).
type pgHistoryLocation struct {
	Tier       string `json:"tier"`
	Index      int    `json:"index"`
	ExtraIndex *int   `json:"extra_index"`
	Version    string `json:"version"`
	Op         string `json:"op"`
	Object     string `json:"object"`
	ReturnCode *int   `json:"return_code"`
	// UserVersion is reported only for an extra_reqids position.
	UserVersion *uint64 `json:"user_version"`
}

// pgHistorySnapshot is the fail-closed receipt of one bracketed primary
// PG-history snapshot. Complete is false on a refused, oversized, truncated
// or unparsable reply and whenever the placement brackets differ.
type pgHistorySnapshot struct {
	Complete           bool             `json:"complete"`
	Failures           []string         `json:"failures"`
	Code               int              `json:"code"`
	Bytes              int              `json:"bytes"`
	LimitBytes         int              `json:"limit_bytes"`
	SHA256             string           `json:"sha256"`
	Raw                string           `json:"raw"`
	Head               string           `json:"head"`
	Tail               string           `json:"tail"`
	LogEntries         int              `json:"log_entries"`
	DupEntries         int              `json:"dup_entries"`
	ExtraReqidEntries  int              `json:"extra_reqid_entries"`
	Before             osdFixtureOracle `json:"before"`
	After              osdFixtureOracle `json:"after"`
	OSD                int32            `json:"osd"`
	PGID               string           `json:"pgid"`
	PrimaryOnly        bool             `json:"primary_only"`
	AbsenceEstablished bool             `json:"absence_established"`
	Targets            []struct {
		Reqid     string              `json:"reqid"`
		Locations []pgHistoryLocation `json:"locations"`
	} `json:"targets"`
}

// pgHistoryTarget is one full osd_reqid_t key: entity type and number,
// incarnation and TID.
type pgHistoryTarget struct {
	Type string `json:"type"`
	Num  uint64 `json:"num"`
	Inc  int32  `json:"inc"`
	TID  uint64 `json:"tid"`
}

func (r pgHistoryTarget) String() string {
	return fmt.Sprintf("%s.%d.%d:%d", r.Type, r.Num, r.Inc, r.TID)
}

// primaryPGHistory takes one bracketed primary snapshot for an object of the
// owned pool in slot and requires it to be complete. It establishes presence
// on that primary at that epoch only, never absence.
func primaryPGHistory(t *testing.T, ctx context.Context, slot int, object string, targets []pgHistoryTarget) pgHistorySnapshot {
	t.Helper()
	s := nativePool(t, ctx, "pghistory", map[string]any{"slot": slot, "object": object, "targets": targets}).History
	if !s.Complete || len(s.Failures) != 0 {
		t.Fatalf("primary PG-history snapshot is incomplete: %q (code %d, %d bytes)", s.Failures, s.Code, s.Bytes)
	}
	if !reflect.DeepEqual(s.Before, s.After) || s.OSD != s.Before.PrimaryID || s.OSD != s.Before.ID || s.PGID != s.Before.PGID {
		t.Fatalf("snapshot brackets or attribution differ: before %+v after %+v osd %d pgid %s", s.Before, s.After, s.OSD, s.PGID)
	}
	if !s.PrimaryOnly || s.AbsenceEstablished || len(s.Targets) != len(targets) {
		t.Fatal("snapshot scope differs", s.PrimaryOnly, s.AbsenceEstablished, len(s.Targets))
	}
	// The complete reply travels in the receipt; recheck its length and
	// digest, and count every exact quoted identity independently of the
	// Python parser.
	raw, err := base64.StdEncoding.DecodeString(s.Raw)
	digest := sha256.Sum256(raw)
	if err != nil || len(raw) != s.Bytes || s.Bytes > s.LimitBytes || hex.EncodeToString(digest[:]) != s.SHA256 {
		t.Fatal("snapshot raw bytes differ from their receipt", err, len(raw), s.Bytes, s.LimitBytes)
	}
	for i, target := range targets {
		if s.Targets[i].Reqid != target.String() {
			t.Fatal("snapshot target order differs", s.Targets[i].Reqid, target)
		}
		if n := bytes.Count(raw, []byte(`"reqid":"`+target.String()+`"`)); n != len(s.Targets[i].Locations) {
			t.Fatalf("parser found %d positions of %s; the raw reply holds %d", len(s.Targets[i].Locations), target, n)
		}
	}
	t.Logf("primary PG-history snapshot: osd.%d pg %s epoch %d acting %v, %d bytes sha256 %s, head %s tail %s, %d log entries, %d extra reqids, %d dups",
		s.OSD, s.PGID, s.Before.Epoch, s.Before.Acting, s.Bytes, s.SHA256, s.Head, s.Tail, s.LogEntries, s.ExtraReqidEntries, s.DupEntries)
	if len(raw) <= 64<<10 {
		t.Logf("complete native pg log reply: %s", raw)
	}
	return s
}

// requirePGHistoryOnRoute requires the snapshot's primary, sets, PG, shard
// and pg_temp to equal the placement the requests used; the epoch may be newer.
func requirePGHistoryOnRoute(t *testing.T, s pgHistorySnapshot, route osdFixtureOracle) {
	t.Helper()
	if s.Before.ID != route.ID || s.Before.PrimaryID != route.PrimaryID || !slices.Equal(s.Before.Acting, route.Acting) || !slices.Equal(s.Before.Up, route.Up) ||
		s.Before.PGID != route.PGID || s.Before.Shard != route.Shard || s.Before.PGTemp != route.PGTemp || s.Before.Epoch < route.Epoch {
		t.Fatalf("snapshot primary differs from the placement the requests used: %+v vs %+v", s.Before, route)
	}
}

// hobjectHead is the pinned hobject_t print form (src/common/hobject.cc:247-270)
// of a head object without namespace or locator key: the hash is printed in
// its bit-reversed sort order.
func hobjectHead(route osdFixtureOracle) string {
	return fmt.Sprintf("%d:%08x:::%s:head", route.Pool, bits.Reverse32(route.Hash), route.Name)
}

// The Go client issues a seed WRITEFULL and a known APPEND under client
// incarnation 7 to an optimized isa k=2,m=2 pool. One bracketed snapshot of
// the primary's native PG log must contain each request exactly once, as an
// ordinary log entry for that object at the reply's replay version, keyed by
// the full entity/incarnation/TID identity. Near-miss identities, including
// a real TID under the other request's incarnation, must not match. This
// calibrates presence detection only: it claims nothing about absence,
// replica or shard-local history, trimming or replay.
func TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration(t *testing.T) {
	width := erasureK2M2
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("pool is not the requested erasure-coded shape", created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, true)

	const (
		object      = "ec-history-calibration"
		incarnation = 7
	)
	route := poolPlace(t, ctx, 1, object, "")
	facts.checkPlacement(t, route)
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	seed := bytes.Repeat([]byte{0x5a, 0, 0xff, 'h'}, 1024)
	seedRequest := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(seed)), Data: seed})
	seedRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006000}
	seeded, err := o.Request(ctx, seedRequest)
	if err != nil || seeded.Code != 0 || seeded.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("seed WRITEFULL", err, seeded.Code, seeded.Flags)
	}
	tail := []byte("history\x00\xff" + object)
	appendRequest := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	appendRequest.ClientIncarnation = incarnation
	appendRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006001}
	appended, err := o.Request(ctx, appendRequest)
	if err != nil || appended.Code != 0 || appended.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("known APPEND", err, appended.Code, appended.Flags)
	}
	gid := c.Snapshot().GlobalID
	for _, reply := range []cephmsgr.OSDReply{seeded, appended} {
		if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != gid || reply.Submission.Attempt != 0 || reply.RetryAttempt != 0 {
			t.Fatal("reply identity differs from its submission", reply.Transaction, reply.Submission)
		}
	}
	if appended.Submission.ClientIncarnation != incarnation || seeded.Submission.ClientIncarnation == incarnation || appended.Transaction == seeded.Transaction {
		t.Fatal("seed and APPEND identities are not distinct", seeded.Submission, appended.Submission)
	}
	native := poolObserve(t, ctx, 1, object, "", "")
	if native.Code != 0 || !bytes.Equal(native.Data, append(bytes.Clone(seed), tail...)) || native.Version != appended.UserVersion || native.Seconds != 1700006001 {
		t.Fatal("native readback differs from one seed and one APPEND", native.Code, len(native.Data), native.Version, appended.UserVersion)
	}

	known := pgHistoryTarget{Type: "client", Num: gid, Inc: incarnation, TID: appended.Transaction}
	seedID := pgHistoryTarget{Type: "client", Num: gid, Inc: int32(seeded.Submission.ClientIncarnation), TID: seeded.Transaction}
	misses := []pgHistoryTarget{
		{Type: "client", Num: gid, Inc: incarnation + 1, TID: appended.Transaction},
		{Type: "client", Num: gid, Inc: incarnation, TID: seeded.Transaction},
		{Type: "client", Num: gid, Inc: seedID.Inc, TID: appended.Transaction},
		{Type: "client", Num: gid + 1, Inc: incarnation, TID: appended.Transaction},
		{Type: "osd", Num: gid, Inc: incarnation, TID: appended.Transaction},
		{Type: "client", Num: gid, Inc: incarnation, TID: appended.Transaction + 1<<20},
	}
	s := primaryPGHistory(t, ctx, 1, object, append([]pgHistoryTarget{known, seedID}, misses...))
	requirePGHistoryOnRoute(t, s, route)
	want := hobjectHead(route)
	for i, reply := range []cephmsgr.OSDReply{appended, seeded} {
		identity, locations := s.Targets[i].Reqid, s.Targets[i].Locations
		version := fmt.Sprintf("%d'%d", reply.ReplayVersion.Epoch, reply.ReplayVersion.Version)
		if len(locations) != 1 {
			t.Fatalf("%s occurs %d times in the primary history; want one ordinary entry: %+v", identity, len(locations), locations)
		}
		l := locations[0]
		if l.Tier != "log" || l.ExtraIndex != nil || l.Op != "modify" || l.Object != want || l.Version != version || l.ReturnCode == nil || *l.ReturnCode != 0 {
			t.Fatalf("%s entry differs: %+v (want object %s version %s)", identity, l, want, version)
		}
		t.Logf("%s: %s entry %d, object %s, version %s", identity, l.Tier, l.Index, l.Object, l.Version)
	}
	if s.Targets[1].Locations[0].Index >= s.Targets[0].Locations[0].Index {
		t.Fatal("seed entry does not precede the APPEND entry")
	}
	for i := range misses {
		if got := s.Targets[2+i]; len(got.Locations) != 0 {
			t.Fatalf("near-miss identity %s matched %+v", got.Reqid, got.Locations)
		}
	}
}
