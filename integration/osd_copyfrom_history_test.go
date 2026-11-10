package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A destination PG-log control for a successful COPY_FROM2. The pinned
// source (Ceph v20.2.4) lets the source's completed COPY_GET return the
// request identities of the source object's ordinary log entries with their
// user versions (src/osd/PrimaryLogPG.cc:9439-9446, src/osd/PGLog.h:477-506).
// The destination's finish_copyfrom moves them into the operation context
// (PrimaryLogPG.cc:10133-10137) and log construction swaps them into the
// destination entry's extra_reqids (PrimaryLogPG.cc:9145-9151). These
// consumer helpers are test-only and nothing is exported.

// copyFromHistoryWant is what one complete destination snapshot must show
// for the copy: the destination head, the copy's replay version and the
// source write's user version.
type copyFromHistoryWant struct {
	object            string
	version           string
	sourceUserVersion uint64
}

// copyFromDestinationBinding checks the positions of the copy's own request
// identity and of the source write's identity in one destination snapshot.
// The copy must be exactly one ordinary modify entry for the destination
// head at its replay version, and the source write must occur exactly once,
// as the first extra request ID of that same entry with the source user
// version. Anything else is refused with its reason.
func copyFromDestinationBinding(copyKey, sourceKey []pgHistoryLocation, want copyFromHistoryWant) error {
	if len(copyKey) != 1 {
		return fmt.Errorf("the copy's identity occurs %d times; want one ordinary entry: %+v", len(copyKey), copyKey)
	}
	entry := copyKey[0]
	if entry.Tier != "log" || entry.ExtraIndex != nil || entry.Op != "modify" || entry.ReturnCode == nil || *entry.ReturnCode != 0 {
		return fmt.Errorf("the copy's identity is not an ordinary successful modify entry: %+v", entry)
	}
	if entry.Object != want.object || entry.Version != want.version {
		return fmt.Errorf("the copy's entry is %s at %s; want %s at %s", entry.Object, entry.Version, want.object, want.version)
	}
	if len(sourceKey) != 1 {
		return fmt.Errorf("the source write's identity occurs %d times at the destination; want one extra request ID: %+v", len(sourceKey), sourceKey)
	}
	extra := sourceKey[0]
	if extra.Tier != "extra_reqids" || extra.ExtraIndex == nil || *extra.ExtraIndex != 0 {
		return fmt.Errorf("the source write's identity is not the first extra request ID of an entry: %+v", extra)
	}
	if extra.Index != entry.Index || extra.Object != entry.Object || extra.Version != entry.Version || extra.Op != entry.Op {
		return fmt.Errorf("the source write's identity is on entry %d (%s at %s), not on the copy's entry %d", extra.Index, extra.Object, extra.Version, entry.Index)
	}
	if extra.UserVersion == nil || *extra.UserVersion != want.sourceUserVersion {
		return fmt.Errorf("the extra request ID does not carry source user version %d: %+v", want.sourceUserVersion, extra)
	}
	return nil
}

// The Go client writes a fresh source head once and copies it once, pinned to
// that write's user version, to a fresh destination head in another owned
// pool. One bracketed primary snapshot per PG must then show the source
// write as an ordinary entry of the source object, and at the destination the
// copy's own identity on the destination entry with the source write's
// identity and user version among that entry's extra request IDs. Native
// readback checks the copied bytes and xattr independently. The copy-error
// injector stays off. This is a positive control past the destination commit:
// it times no crash and claims nothing about absence, replicas or replay.
func TestCephOSDCopyFromDestinationHistoryIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1, 2)
	const (
		source           = "copy-history-source"
		destination      = "copy-history-destination"
		writeIncarnation = 7
		copyIncarnation  = 9
	)
	if poolObserve(t, ctx, 1, source, "", "").Code != -2 || poolObserve(t, ctx, 2, destination, "", "").Code != -2 {
		t.Fatal("source or destination head already exists")
	}
	sourceRoute, destinationRoute := poolPlace(t, ctx, 1, source, ""), poolPlace(t, ctx, 2, destination, "")
	if sourceRoute.Pool != pools.ids[1] || destinationRoute.Pool != pools.ids[2] || sourceRoute.PGID == destinationRoute.PGID || sourceRoute.PGID == "" || destinationRoute.PGID == "" {
		t.Fatal("source and destination are not in distinct owned PGs", sourceRoute.PGID, destinationRoute.PGID)
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	gid := c.Snapshot().GlobalID
	// send issues one Go request exactly once under its own incarnation.
	send := func(label string, route osdFixtureOracle, incarnation uint32, seconds uint32, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		request := pools.request(route, ops...)
		request.ClientIncarnation = incarnation
		request.ModificationTime = cephmsgr.OSDTime{Seconds: seconds}
		reply, err := connections[route.ID].Request(ctx, request)
		if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatalf("%s: err=%v code=%d flags=%#x; not resent", label, err, reply.Code, reply.Flags)
		}
		if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != incarnation ||
			reply.Submission.Attempt != 0 || reply.RetryAttempt != 0 || reply.Object != route.Name || reply.PG.Pool != uint64(route.Pool) || len(reply.Operations) != len(ops) {
			t.Fatal("reply identity differs from its submission", label, reply.Transaction, reply.Submission)
		}
		return reply
	}

	data := []byte("copy-history\x00\xff source bytes 0123456789")
	attrName, attrValue := "copy-history-attr", []byte{0xff, 0, 'H'}
	written := send("source write", sourceRoute, writeIncarnation, 1700008000,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attrName}, Data: attrValue})
	nativeSource := poolObserve(t, ctx, 1, source, "", "")
	if nativeSource.Code != 0 || !bytes.Equal(nativeSource.Data, data) || attrNames(nativeSource)[attrName] != string(attrValue) || nativeSource.Version != written.UserVersion || written.UserVersion == 0 {
		t.Fatal("native source differs from the one Go write", nativeSource.Code, nativeSource.Version, written.UserVersion)
	}
	copied := send("version-pinned COPY_FROM2", destinationRoute, copyIncarnation, 1700008001,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: pools.ids[1], SourceName: source, SourceVersion: written.UserVersion}})
	if got := copied.Operations[0]; got.Result != 0 || got.CopyFromVersion != written.UserVersion || got.PayloadLength != 0 {
		t.Fatal("copy operation result differs", got.Result, got.CopyFromVersion, got.PayloadLength)
	}
	// Independent native readback: the destination holds the source's bytes
	// and xattr at the copy reply's user version; the source did not change.
	nativeDestination := poolObserve(t, ctx, 2, destination, "", "")
	if nativeDestination.Code != 0 || !bytes.Equal(nativeDestination.Data, data) || !reflect.DeepEqual(attrNames(nativeDestination), attrNames(nativeSource)) || nativeDestination.Version != copied.UserVersion {
		t.Fatalf("native destination differs from the source: code=%d data=%x attrs=%v version=%d reply=%d",
			nativeDestination.Code, nativeDestination.Data, attrNames(nativeDestination), nativeDestination.Version, copied.UserVersion)
	}
	if after := poolObserve(t, ctx, 1, source, "", ""); after.Version != nativeSource.Version || !bytes.Equal(after.Data, data) {
		t.Fatal("copy altered the source")
	}

	sourceKey := pgHistoryTarget{Type: "client", Num: gid, Inc: writeIncarnation, TID: written.Transaction}
	copyKey := pgHistoryTarget{Type: "client", Num: gid, Inc: copyIncarnation, TID: copied.Transaction}
	if sourceKey == copyKey || written.Transaction == copied.Transaction {
		t.Fatal("write and copy identities are not distinct", sourceKey, copyKey)
	}
	// Near misses: each request's real TID under the other request's
	// incarnation, then one changed field of the source write's key.
	misses := []pgHistoryTarget{
		{Type: "client", Num: gid, Inc: writeIncarnation, TID: copied.Transaction},
		{Type: "client", Num: gid, Inc: copyIncarnation, TID: written.Transaction},
		{Type: "client", Num: gid, Inc: writeIncarnation + 1, TID: written.Transaction},
		{Type: "client", Num: gid + 1, Inc: writeIncarnation, TID: written.Transaction},
		{Type: "osd", Num: gid, Inc: writeIncarnation, TID: written.Transaction},
		{Type: "client", Num: gid, Inc: writeIncarnation, TID: written.Transaction + 1<<20},
	}
	version := func(reply cephmsgr.OSDReply) string {
		return fmt.Sprintf("%d'%d", reply.ReplayVersion.Epoch, reply.ReplayVersion.Version)
	}
	noMisses := func(s pgHistorySnapshot) {
		t.Helper()
		for i := range misses {
			if got := s.Targets[2+i]; len(got.Locations) != 0 {
				t.Fatalf("near-miss identity %s matched %+v", got.Reqid, got.Locations)
			}
		}
	}

	// The source object's ordinary log holds the write. An ordinary write
	// carries no extra request ID, and the copy left no entry in this PG.
	sourceSnapshot := primaryPGHistory(t, ctx, 1, source, append([]pgHistoryTarget{sourceKey, copyKey}, misses...))
	requirePGHistoryOnRoute(t, sourceSnapshot, sourceRoute)
	locations := sourceSnapshot.Targets[0].Locations
	if len(locations) != 1 {
		t.Fatalf("%s occurs %d times in the source history; want one ordinary entry: %+v", sourceKey, len(locations), locations)
	}
	if l := locations[0]; l.Tier != "log" || l.ExtraIndex != nil || l.Op != "modify" || l.Object != hobjectHead(sourceRoute) || l.Version != version(written) || l.ReturnCode == nil || *l.ReturnCode != 0 {
		t.Fatalf("source write entry differs: %+v (want object %s version %s)", l, hobjectHead(sourceRoute), version(written))
	}
	if got := sourceSnapshot.Targets[1].Locations; len(got) != 0 || sourceSnapshot.ExtraReqidEntries != 0 {
		t.Fatalf("source PG holds the copy's identity %+v or %d extra request IDs", got, sourceSnapshot.ExtraReqidEntries)
	}
	noMisses(sourceSnapshot)

	// The destination entry binds the source history to the committed copy.
	destinationSnapshot := primaryPGHistory(t, ctx, 2, destination, append([]pgHistoryTarget{copyKey, sourceKey}, misses...))
	requirePGHistoryOnRoute(t, destinationSnapshot, destinationRoute)
	want := copyFromHistoryWant{object: hobjectHead(destinationRoute), version: version(copied), sourceUserVersion: written.UserVersion}
	if err := copyFromDestinationBinding(destinationSnapshot.Targets[0].Locations, destinationSnapshot.Targets[1].Locations, want); err != nil {
		t.Fatalf("destination PG log does not bind the copy to the source write: %v", err)
	}
	if destinationSnapshot.ExtraReqidEntries != 1 {
		t.Fatalf("destination PG holds %d extra request IDs; the one source write gives one", destinationSnapshot.ExtraReqidEntries)
	}
	noMisses(destinationSnapshot)
	t.Logf("source write %s user version %d: source pg %s entry %s at %s; copy %s: destination pg %s entry %s at %s with extra request ID %s user version %d",
		sourceKey, written.UserVersion, sourceRoute.PGID, hobjectHead(sourceRoute), version(written),
		copyKey, destinationRoute.PGID, want.object, want.version, sourceKey, written.UserVersion)
}

func TestCopyFromDestinationHistoryBinding(t *testing.T) {
	zero, one, seven := 0, 1, uint64(7)
	want := copyFromHistoryWant{object: "3:0000002a:::dst:head", version: "20'1", sourceUserVersion: 7}
	entry := pgHistoryLocation{Tier: "log", Index: 0, Version: "20'1", Op: "modify", Object: want.object, ReturnCode: &zero}
	extra := pgHistoryLocation{Tier: "extra_reqids", Index: 0, ExtraIndex: &zero, Version: "20'1", Op: "modify", Object: want.object, UserVersion: &seven}
	if err := copyFromDestinationBinding([]pgHistoryLocation{entry}, []pgHistoryLocation{extra}, want); err != nil {
		t.Fatal("bound destination entry refused:", err)
	}
	change := func(l pgHistoryLocation, edit func(*pgHistoryLocation)) []pgHistoryLocation {
		edit(&l)
		return []pgHistoryLocation{l}
	}
	six, failed := uint64(6), -5
	for name, c := range map[string]struct {
		copyKey, sourceKey []pgHistoryLocation
	}{
		"copy missing":                {nil, []pgHistoryLocation{extra}},
		"copy twice":                  {[]pgHistoryLocation{entry, entry}, []pgHistoryLocation{extra}},
		"copy only a duplicate":       {change(entry, func(l *pgHistoryLocation) { l.Tier, l.Op, l.Object, l.ReturnCode = "dups", "", "", nil }), []pgHistoryLocation{extra}},
		"copy is an extra request ID": {change(extra, func(*pgHistoryLocation) {}), []pgHistoryLocation{extra}},
		"copy entry failed":           {change(entry, func(l *pgHistoryLocation) { l.ReturnCode = &failed }), []pgHistoryLocation{extra}},
		"copy entry is a delete":      {change(entry, func(l *pgHistoryLocation) { l.Op = "delete" }), change(extra, func(l *pgHistoryLocation) { l.Op = "delete" })},
		"copy on another object":      {change(entry, func(l *pgHistoryLocation) { l.Object = "3:0000002a:::other:head" }), change(extra, func(l *pgHistoryLocation) { l.Object = "3:0000002a:::other:head" })},
		"copy at another version":     {change(entry, func(l *pgHistoryLocation) { l.Version = "20'2" }), change(extra, func(l *pgHistoryLocation) { l.Version = "20'2" })},
		"source missing":              {[]pgHistoryLocation{entry}, nil},
		"source twice":                {[]pgHistoryLocation{entry}, []pgHistoryLocation{extra, extra}},
		"source as its own entry":     {[]pgHistoryLocation{entry}, change(entry, func(l *pgHistoryLocation) { l.Index, l.Version = 1, "20'2" })},
		"source only a duplicate":     {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.Tier, l.ExtraIndex, l.UserVersion = "dups", nil, nil })},
		"source on another entry":     {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.Index, l.Version = 1, "20'2" })},
		"source on another object":    {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.Object = "3:0000002a:::other:head" })},
		"source not the first extra":  {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.ExtraIndex = &one })},
		"other source user version":   {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.UserVersion = &six })},
		"source user version missing": {[]pgHistoryLocation{entry}, change(extra, func(l *pgHistoryLocation) { l.UserVersion = nil })},
	} {
		if err := copyFromDestinationBinding(c.copyKey, c.sourceKey, want); err == nil {
			t.Error("unbound destination history accepted:", name)
		}
	}
}
