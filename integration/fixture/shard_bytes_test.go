package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeShardBytes answers the store-copy steps as fakeShardHistory does and
// the listing and get-bytes of one shard as a store holding listing and data
// would.
type fakeShardBytes struct {
	*fakeShardHistory
	listing   string
	data      string
	bytesCode int
	// byPath answers whoami and the listing for the OSD and shard the
	// arguments name.
	byPath bool
}

func newFakeShardBytes() *fakeShardBytes {
	return &fakeShardBytes{fakeShardHistory: newFakeShardHistory(), listing: shardBytesListing, data: "chunk-bytes"}
}

// shardBytesListing is the shape the pinned tool prints for one object of
// shard 1 of PG 4.2: [pgid, collection, ghobject], the ghobject with the
// hobject's max and its own.
const shardBytesListing = `[["4.2s1","4.2s1_head",{"oid":"ec-bytes","key":"","snapid":-2,"hash":2,"max":0,"pool":4,"namespace":"","shard_id":1,"max":0}]]`

func (f *fakeShardBytes) reader(context.Context) (shardHistoryReader, bool, error) {
	err := f.step("reader")
	if f.noReader {
		return nil, false, err
	}
	return f, f.isolated, err
}

func (f *fakeShardBytes) run(_ context.Context, argv []string, _ int) (int, []byte, []byte, error) {
	switch {
	case argv[0] == "cat" && argv[1] == shardBytesPath:
		return 0, []byte(f.data), nil, f.step("cat bytes")
	case argv[0] == "cat":
		// Every copy is the store of the OSD it was made of, unless the
		// case names another whoami.
		whoami := f.whoami
		if f.byPath {
			whoami = strings.TrimSuffix(strings.TrimPrefix(argv[1], "/shard/ceph-"), "/whoami")
		}
		return 0, []byte(whoami), nil, f.step("whoami " + argv[1])
	case argv[0] == "sh":
		return 0, []byte(f.layout), nil, f.step("layout " + argv[len(argv)-1])
	}
	if slices.Contains(argv, "get-bytes") {
		return f.bytesCode, nil, nil, f.step("get-bytes " + argv[len(argv)-3])
	}
	listing := f.listing
	if f.byPath {
		// The listing of the shard the argv names.
		spg := argv[slices.Index(argv, "--pgid")+1]
		shard := spg[strings.Index(spg, "s")+1:]
		listing = strings.ReplaceAll(shardBytesListing, "4.2s1", spg)
		listing = strings.Replace(listing, `"shard_id":1`, `"shard_id":`+shard, 1)
	}
	return 0, []byte(listing), nil, f.step("list " + argv[len(argv)-1])
}

func TestCaptureShardBytesReadsTheSelectedHeadOfTheCopy(t *testing.T) {
	out := t.TempDir()
	engine := newFakeShardBytes()
	target := shardBytesTarget{PGID: "4.2", Pool: 4, Object: "ec-bytes"}
	shard := captureShardBytes(context.Background(), engine, out, target, 1, 2)
	selected := `["4.2s1",{"oid":"ec-bytes","key":"","snapid":-2,"hash":2,"max":0,"pool":4,"namespace":"","shard_id":1,"max":0}]`
	want := []string{"reader", "pause", "copy", "resume", "whoami /shard/ceph-2/whoami", "layout /shard/ceph-2", "list ec-bytes", "get-bytes " + selected, "cat bytes", "remove"}
	if !slices.Equal(engine.events, want) {
		t.Fatalf("steps = %q, want %q", engine.events, want)
	}
	if shard.Failure != "" || shard.Selected != selected || shard.BytesLength != len("chunk-bytes") || shard.BytesFile != "osd-shard-bytes-s1-data.bin" ||
		shard.ListFile != "osd-shard-bytes-s1-list.json" || !shard.ReaderRemoved || shard.Whoami != "2" || *shard.ListCode != 0 || *shard.BytesCode != 0 {
		t.Fatalf("capture record = %+v", shard)
	}
	if raw, err := os.ReadFile(filepath.Join(out, shard.BytesFile)); err != nil || string(raw) != "chunk-bytes" {
		t.Fatal("bytes file", string(raw), err)
	}
	if got := strings.Join(shardBytesToolArgv(2, "4.2s1", "--op", "list", "ec-bytes"), " "); got != "ceph-objectstore-tool --no-mon-config --data-path /shard/ceph-2 --type bluestore --pgid 4.2s1 --format json --op list ec-bytes" {
		t.Fatal("listing arguments", got)
	}
}

// A listing without exactly one head of the object on that shard, a failed
// get-bytes or a failed store copy stops before any later read; the OSD is
// resumed and the reader removed.
func TestCaptureShardBytesFailures(t *testing.T) {
	head := func(change string) string { return strings.Replace(shardBytesListing, `"shard_id":1`, change, 1) }
	for name, c := range map[string]struct {
		change func(*fakeShardBytes)
		last   string
		reason string
	}{
		"no entry":     {func(f *fakeShardBytes) { f.listing = "[]" }, "list ec-bytes", "0 listed heads"},
		"other shard":  {func(f *fakeShardBytes) { f.listing = head(`"shard_id":0`) }, "list ec-bytes", "0 listed heads"},
		"a generation": {func(f *fakeShardBytes) { f.listing = head(`"generation":3,"shard_id":1`) }, "list ec-bytes", "0 listed heads"},
		"a clone": {func(f *fakeShardBytes) {
			f.listing = strings.Replace(shardBytesListing, `"snapid":-2`, `"snapid":4`, 1)
		}, "list ec-bytes", "0 listed heads"},
		"another pool":    {func(f *fakeShardBytes) { f.listing = strings.Replace(shardBytesListing, `"pool":4`, `"pool":5`, 1) }, "list ec-bytes", "0 listed heads"},
		"temp collection": {func(f *fakeShardBytes) { f.listing = strings.Replace(shardBytesListing, `4.2s1_head`, `4.2s1_TEMP`, 1) }, "list ec-bytes", "0 listed heads"},
		"two heads": {func(f *fakeShardBytes) {
			f.listing = "[" + shardBytesListing[1:len(shardBytesListing)-1] + "," + shardBytesListing[1:]
		}, "list ec-bytes", "2 listed heads"},
		"no identity":       {func(f *fakeShardBytes) { f.listing = `[["4.2s1","4.2s1_head",{"oid":"ec-bytes"}]]` }, "list ec-bytes", "lacks its identity"},
		"not a listing":     {func(f *fakeShardBytes) { f.listing = `{"oid":"ec-bytes"}` }, "list ec-bytes", "not an array of entries"},
		"get-bytes refused": {func(f *fakeShardBytes) { f.bytesCode = 1 }, "get-bytes", "offline get-bytes of 4.2s1: exit 1"},
		"copy fails":        {func(f *fakeShardBytes) { f.fail = "copy" }, "resume", "copy the paused OSD's data directory"},
	} {
		engine := newFakeShardBytes()
		c.change(engine)
		shard := captureShardBytes(context.Background(), engine, t.TempDir(), shardBytesTarget{PGID: "4.2", Pool: 4, Object: "ec-bytes"}, 1, 2)
		events := engine.events
		if !strings.Contains(shard.Failure, c.reason) || !shard.ReaderRemoved || shard.BytesFile != "" || events[len(events)-1] != "remove" ||
			!strings.HasPrefix(events[len(events)-2], c.last) || !slices.Contains(events, "resume") {
			t.Errorf("%s: record = %+v, steps %q", name, shard, events)
		}
	}
}

func TestShardBytesPlanReadsTheUpVector(t *testing.T) {
	owned := []int{0, 1, 2, 3}
	bracket := func(up string) []byte {
		return []byte(`{"epoch":28,"pgid":"4.2","pool_id":4,"object":"ec-bytes","namespace":"","acting":[1,2147483647,2,0],"up":` + up + `}`)
	}
	target, err := shardBytesPlan(bracket("[1,3,2,0]"), owned)
	if err != nil || target.PGID != "4.2" || !slices.Equal(target.Up, []int{1, 3, 2, 0}) || target.Object != "ec-bytes" {
		t.Fatal("plan", target, err)
	}
	if target, err = shardBytesPlan(bracket("[1,2147483647,2,0]"), owned); err != nil || target.Up[1] != crushItemNone {
		t.Fatal("an empty position is a plan", target, err)
	}
	for name, raw := range map[string][]byte{
		"unowned member":    bracket("[1,7,2,0]"),
		"repeated member":   bracket("[1,1,2,0]"),
		"one present":       bracket("[1,2147483647,2147483647,2147483647]"),
		"one position":      bracket("[1]"),
		"namespace":         []byte(`{"pgid":"4.2","pool_id":4,"object":"ec-bytes","namespace":"n","up":[1,3,2,0]}`),
		"another pool's PG": []byte(`{"pgid":"5.2","pool_id":4,"object":"ec-bytes","namespace":"","up":[1,3,2,0]}`),
		"no object":         []byte(`{"pgid":"4.2","pool_id":4,"object":"","namespace":"","up":[1,3,2,0]}`),
		"empty":             nil,
	} {
		if _, err := shardBytesPlan(raw, owned); err == nil {
			t.Error("plan accepted", name)
		}
	}
}

func TestCaptureShardBytesAllRecordsAbsentPositions(t *testing.T) {
	out := t.TempDir()
	engine := newFakeShardBytes()
	engine.byPath = true
	before := []byte(`{"pgid":"4.2","pool_id":4,"object":"ec-bytes","namespace":"","up":[2,2147483647,3]}`)
	receipt, err := captureShardBytesAll(context.Background(), engine, out, before, []int{2, 3})
	if err != nil || !receipt.Complete || len(receipt.Shards) != 3 || !receipt.Shards[1].Absent || receipt.Shards[1].SPG != "4.2s1" || receipt.Shards[1].Selected != "" ||
		receipt.Shards[0].Absent || receipt.Shards[2].Absent || receipt.Shards[2].OSD != 3 || !strings.HasPrefix(receipt.Shards[2].Selected, `["4.2s2",`) ||
		receipt.Object != "ec-bytes" || receipt.Pool != 4 || !slices.Equal(receipt.Up, []int{2, crushItemNone, 3}) {
		raw, _ := json.Marshal(receipt)
		t.Fatalf("receipt %s, %v", raw, err)
	}
	if n := strings.Count(strings.Join(engine.events, "\n"), "pause"); n != 2 {
		t.Fatalf("%d pauses for two present members: %q", n, engine.events)
	}
	// The first failed shard ends the capture.
	engine = newFakeShardBytes()
	engine.byPath, engine.bytesCode = true, 1
	if receipt, err = captureShardBytesAll(context.Background(), engine, out, before, []int{2, 3}); err == nil || receipt.Complete || len(receipt.Shards) != 1 {
		t.Fatal("a failed shard did not end the capture", receipt, err)
	}
	if receipt, err = captureShardBytesAll(context.Background(), engine, out, before, []int{2}); err == nil || len(receipt.Shards) != 0 {
		t.Fatal("an unowned member was captured", receipt, err)
	}
}

func TestRemoveShardBytesOutputs(t *testing.T) {
	out := t.TempDir()
	stale := []string{"osd-shard-bytes-s0-list.json", "osd-shard-bytes-s3-data.bin", shardBytesCaptureFile}
	kept := []string{shardBytesBeforeFile, "osd-shard-history-s0-log.json"}
	for _, name := range append(slices.Clone(stale), kept...) {
		if err := os.WriteFile(filepath.Join(out, name), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeShardBytesOutputs(out); err != nil {
		t.Fatal(err)
	}
	for _, name := range stale {
		if _, err := os.Stat(filepath.Join(out, name)); !os.IsNotExist(err) {
			t.Error("stale output kept", name)
		}
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Error("other receipt removed", name)
		}
	}
}

func TestShardBytesCaptureAdmission(t *testing.T) {
	out := t.TempDir()
	if err := writeRequestFile(out, "capture-osd-shard-bytes", "7 \n"); err != nil {
		t.Fatal(err)
	}
	c := &fixtureCluster{profile: profile{osd: "1", osdStore: "bluestore", osdReplicas: "4", testRun: "^TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration$", capabilities: "osd-erasure-width"}}
	if err := c.controlOSDShardBytesRequest(context.Background(), out); err == nil || !strings.Contains(err.Error(), "exclusive erasure-capacity BlueStore selector") {
		t.Fatal("unselected capture request:", err)
	}
	if err := c.controlOSDShardBytesRequest(context.Background(), out); err != nil {
		t.Fatal("no request must be a no-op:", err)
	}
}
