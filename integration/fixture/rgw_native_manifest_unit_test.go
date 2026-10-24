package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testMarker = "mk.1"
	testHead   = "mk.1_large-binary"
	testPool   = "zone.rgw.buckets.data"
)

func testStripe(index int64, offset, size uint64, oid string) map[string]any {
	return map[string]any{"index": index, "part_id": 0, "stripe_id": max(index, 0), "offset": offset, "size": size,
		"raw_obj": map[string]any{"pool": map[string]any{"name": testPool, "ns": ""}, "oid": oid, "loc": ""}}
}

func testManifest(size uint64, objects ...map[string]any) []byte {
	raw, err := json.Marshal(map[string]any{"size": size, "objects": objects})
	if err != nil {
		panic(err)
	}
	return raw
}

func TestRGWNativeManifestAcceptsOnlyCompleteMultiObjectLayouts(t *testing.T) {
	tail := testMarker + "__shadow_.abc_1"
	good := testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, tail))
	manifest, err := rgwNativeParseManifest(good, testPool, testMarker, testHead, 100)
	if err != nil || len(manifest.Stripes) != 2 || manifest.Sentinel != nil || len(manifest.Tails(testHead)) != 1 || manifest.Tails(testHead)[0] != tail {
		t.Fatalf("head plus tail layout: %+v %v", manifest, err)
	}
	// A pool given as a plain string is accepted too.
	plain := strings.ReplaceAll(string(good), `{"name":"`+testPool+`","ns":""}`, `"`+testPool+`"`)
	if _, err := rgwNativeParseManifest([]byte(plain), testPool, testMarker, testHead, 100); err != nil {
		t.Fatal("plain pool name rejected:", err)
	}
	// A metadata-only head sentinel is retained but never carries bytes.
	sentinel := testManifest(100, testStripe(-1, 0, 0, testHead), testStripe(0, 0, 50, testMarker+"__shadow_.abc_1"), testStripe(1, 50, 50, testMarker+"__shadow_.abc_2"))
	manifest, err = rgwNativeParseManifest(sentinel, testPool, testMarker, testHead, 100)
	if err != nil || manifest.Sentinel == nil || len(manifest.Stripes) != 2 || len(manifest.Objects()) != 3 || len(manifest.Tails(testHead)) != 2 {
		t.Fatalf("sentinel layout: %+v %v", manifest, err)
	}
	mutate := func(replace func(string) string) []byte { return []byte(replace(string(good))) }
	for name, raw := range map[string][]byte{
		"gap":                    testManifest(100, testStripe(0, 0, 50, testHead), testStripe(1, 60, 40, tail)),
		"overlap":                testManifest(100, testStripe(0, 0, 70, testHead), testStripe(1, 60, 40, tail)),
		"truncated coverage":     testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 30, tail)),
		"exceeds logical size":   testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 50, tail)),
		"single object":          testManifest(100, testStripe(0, 0, 100, testHead)),
		"out of order index":     testManifest(100, testStripe(0, 0, 60, testHead), testStripe(2, 60, 40, tail)),
		"zero-length stripe":     testManifest(100, testStripe(0, 0, 100, testHead), testStripe(1, 100, 0, tail)),
		"foreign tail":           testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, "other.2__shadow_.abc_1")),
		"unprefixed tail":        testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, testMarker+"_other")),
		"empty tail name":        testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, testMarker+"__shadow_")),
		"first not head":         testManifest(100, testStripe(0, 0, 60, tail), testStripe(1, 60, 40, tail+"x")),
		"duplicate identity":     testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, tail), testStripe(2, 100, 0, tail)),
		"logical size mismatch":  testManifest(99, testStripe(0, 0, 60, testHead), testStripe(1, 60, 39, tail)),
		"misplaced sentinel":     testManifest(100, testStripe(0, 0, 60, testHead), testStripe(-1, 0, 0, testHead), testStripe(1, 60, 40, tail)),
		"sentinel with size":     testManifest(100, testStripe(-1, 0, 5, testHead), testStripe(0, 0, 100, tail)),
		"other-pool object":      mutate(func(s string) string { return strings.Replace(s, testPool, "other.pool", 1) }),
		"namespaced object":      mutate(func(s string) string { return strings.Replace(s, `"ns":""`, `"ns":"shadow"`, 1) }),
		"locator object":         mutate(func(s string) string { return strings.Replace(s, `"loc":""`, `"loc":"key"`, 1) }),
		"missing logical size":   []byte(`{"objects":[]}`),
		"no objects":             []byte(`{"size":100,"objects":[]}`),
		"malformed":              []byte(`{"size":`),
		"unsupported pool shape": mutate(func(s string) string { return strings.Replace(s, `{"name":"`+testPool+`","ns":""}`, `7`, 1) }),
		"unexpected pool field":  mutate(func(s string) string { return strings.Replace(s, `"ns":""`, `"ns":"","zone":"x"`, 1) }),
		"oversized":              bytes.Repeat([]byte(" "), rgwNativeManifestLimit+1),
		"too many objects": func() []byte {
			var objects []map[string]any
			for i := 0; i < rgwNativeStripeLimit+2; i++ {
				objects = append(objects, testStripe(int64(i), uint64(i), 1, testMarker+"__shadow_.x_"+strconv.Itoa(i)))
			}
			return testManifest(uint64(len(objects)), objects...)
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := rgwNativeParseManifest(raw, testPool, testMarker, testHead, 100); err == nil {
				t.Fatal("unsafe manifest layout admitted")
			}
		})
	}
}

func TestRGWNativeReconstructFollowsManifestOrderAndLengths(t *testing.T) {
	payload := rgwNativeLargePayload(100)
	tail := testMarker + "__shadow_.abc_1"
	manifest, err := rgwNativeParseManifest(testManifest(100, testStripe(0, 0, 60, testHead), testStripe(1, 60, 40, tail)), testPool, testMarker, testHead, 100)
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{testHead: payload[:60], tail: payload[60:]}
	read := func(oid string) ([]byte, error) { return objects[oid], nil }
	if got, err := rgwNativeReconstruct(manifest, read); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("ordered reconstruction: %v", err)
	}
	// Equal total size with swapped stripes is an order error the hash exposes.
	swapped := map[string][]byte{testHead: payload[40:], tail: payload[:40]}
	if got, err := rgwNativeReconstruct(manifest, func(oid string) ([]byte, error) { return swapped[oid], nil }); err != nil || bytes.Equal(got, payload) {
		t.Fatal("same-length reordered stripes must reconstruct different bytes")
	}
	wrong := map[string][]byte{testHead: payload[:61], tail: payload[61:]}
	if _, err := rgwNativeReconstruct(manifest, func(oid string) ([]byte, error) { return wrong[oid], nil }); err == nil {
		t.Fatal("wrong stripe lengths admitted")
	}
	truncated := map[string][]byte{testHead: payload[:59], tail: payload[60:]}
	if _, err := rgwNativeReconstruct(manifest, func(oid string) ([]byte, error) { return truncated[oid], nil }); err == nil {
		t.Fatal("truncated stripe admitted")
	}
	if _, err := rgwNativeReconstruct(manifest, func(string) ([]byte, error) { return nil, errors.New("read failed") }); err == nil {
		t.Fatal("read error admitted")
	}
	same := rgwNativeLargePayload(100)
	if !bytes.Equal(same, payload) || bytes.Equal(payload[:32], payload[32:64]) {
		t.Fatal("large payload must be deterministic with distinct 32-byte blocks")
	}
}

func TestRGWNativeGCQueueJoinsOnlyManifestObjectsOfThisBucket(t *testing.T) {
	tail := testMarker + "__shadow_.abc_1"
	chain := func(tag string, objects ...rgwNativeGCObject) rgwNativeGCEntry {
		return rgwNativeGCEntry{Tag: tag, Objs: objects}
	}
	object := func(oid string) rgwNativeGCObject { return rgwNativeGCObject{Pool: testPool, OID: oid} }
	raw, err := json.Marshal([]rgwNativeGCEntry{chain("t1", object(tail)), chain("t2", rgwNativeGCObject{Pool: testPool, OID: "other.9__shadow_.z_1"})})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := rgwNativeParseGCList(raw)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := rgwNativeGCOwned(entries, testPool, testMarker, []string{testHead, tail})
	if err != nil || len(owned) != 1 || len(owned[tail]) != 1 || owned[tail][0] != "t1" {
		t.Fatalf("owned GC join: %v %v", owned, err)
	}
	if empty, err := rgwNativeParseGCList([]byte("[]")); err != nil || len(empty) != 0 {
		t.Fatal("empty GC queue rejected:", err)
	}
	for name, entries := range map[string][]rgwNativeGCEntry{
		"unexpected marker object": {chain("t", object(testMarker+"__shadow_.zzz_9"))},
		"wrong pool":               {chain("t", rgwNativeGCObject{Pool: "other", OID: tail})},
		"locator":                  {chain("t", rgwNativeGCObject{Pool: testPool, OID: tail, Locator: "k"})},
		"instance":                 {chain("t", rgwNativeGCObject{Pool: testPool, OID: tail, Instance: "v"})},
	} {
		if _, err := rgwNativeGCOwned(entries, testPool, testMarker, []string{testHead, tail}); err == nil {
			t.Fatalf("%s admitted", name)
		}
	}
	for _, malformed := range []string{``, `null`, `{}`, `[`, `{"entries":[]}`} {
		if _, err := rgwNativeParseGCList([]byte(malformed)); err == nil {
			t.Fatalf("malformed GC list %q admitted", malformed)
		}
	}
}

func TestRGWNativeHistoryMatchBindsNamespaceLocatorAndPool(t *testing.T) {
	make := func(target, vector string) []byte {
		raw, err := json.Marshal(map[string]any{"ops": []map[string]any{{
			"description": "osd_op(client.42.0:5 6.0 " + target + " [" + vector + "] snapc 0=[] ondisk+write e9)",
			"type_data":   map[string]any{"client_info": map[string]any{"client": "client.42", "tid": 5}, "events": []map[string]string{{"event": "reached_pg"}}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	gc := func(pool int64, namespace, locator, name string) bool {
		return pool == 6 && rgwNativeGCObjectName.MatchString(name)
	}
	operations, err := rgwNativeHistoryMatch(make("6:abcd:gc::gc.12:head", "call rgw_gc.queue_enqueue in=9b"), "client.42", gc)
	if err != nil || len(operations) != 1 || operations[0].Namespace != "gc" || operations[0].Object != "gc.12" {
		t.Fatalf("namespaced GC reception: %+v %v", operations, err)
	}
	if methods := rgwNativeCallMethods(operations[0]); len(methods) != 1 || methods[0] != "rgw_gc.queue_enqueue" {
		t.Fatalf("complete class.method token: %v", methods)
	}
	for name, raw := range map[string][]byte{
		"other pool":      make("7:abcd:gc::gc.12:head", "call rgw.gc_set_entry"),
		"other name":      make("6:abcd:gc::gc.12x:head", "call rgw.gc_set_entry"),
		"PG pool differs": []byte(strings.Replace(string(make("6:abcd:gc::gc.12:head", "call rgw.gc_set_entry")), "6.0 6:abcd", "5.0 6:abcd", 1)),
	} {
		if operations, err := rgwNativeHistoryMatch(raw, "client.42", gc); err != nil || len(operations) != 0 {
			t.Fatalf("%s admitted: %+v %v", name, operations, err)
		}
	}
	// The legacy name-keyed selector still refuses a namespaced or located twin.
	for _, target := range []string{"6:abcd:ns::mk.1_o:head", "6:abcd::loc:mk.1_o:head"} {
		operations, err := rgwNativeHistory(make(target, "writefull 0~3"), "client.42", map[string]int64{"mk.1_o": 6})
		if err != nil || len(operations) != 0 {
			t.Fatalf("twin %s admitted: %+v %v", target, operations, err)
		}
	}
}

func TestRGWNativeGCEnqueueRequiresTheEnqueueMethodToken(t *testing.T) {
	for description, want := range map[string]bool{
		"[create,call version.check_conds in=18b,call rgw_gc.rgw_gc_queue_enqueue in=99b]": true,
		"[call rgw.gc_set_entry in=9b]":                   true,
		"[call rgw_gc.rgw_gc_queue_init in=23b]":          false,
		"[call lock.lock]":                                false,
		"[call rgw.gc_list]":                              false,
		"[call rgw_gc.rgw_gc_queue_enqueue_extra in=1b]":  false,
		"[setxattr user.rgw_gc.rgw_gc_queue_enqueue (1)]": false,
	} {
		if got := rgwNativeGCEnqueue(rgwNativeOperation{Description: description}); got != want {
			t.Fatalf("%s: got %t, want %t", description, got, want)
		}
	}
}

func TestRGWNativeMultiPutRequiresHeadTailAndBothIndexCalls(t *testing.T) {
	tail := testMarker + "__shadow_.abc_1"
	op := func(tid int, pool int, object, vector string) map[string]any {
		return map[string]any{"description": "osd_op(client.42.0:" + strconv.Itoa(tid) + " " + strconv.Itoa(pool) + ".0 " + strconv.Itoa(pool) + ":abcd:::" + object + ":head [" + vector + "] snapc 0=[] ondisk+write e7)",
			"type_data": map[string]any{"client_info": map[string]any{"client": "client.42", "tid": tid}, "events": []map[string]string{{"event": "reached_pg"}}}}
	}
	head := op(1, 7, testHead, "writefull 0~4194304")
	tailWrite := op(2, 7, tail, "writefull 0~2097152")
	prepare := op(3, 6, ".dir.bkt.9", "call rgw.bucket_prepare_op in=1b")
	complete := op(4, 6, ".dir.bkt.9", "call rgw.bucket_complete_op in=1b")
	clients := []rgwNativeClientGeneration{{Client: "client.42"}}
	for name, ops := range map[string][]map[string]any{
		"complete":        {head, tailWrite, prepare, complete},
		"no tail":         {head, prepare, complete},
		"no head":         {tailWrite, prepare, complete},
		"no prepare":      {head, tailWrite, complete},
		"no complete":     {head, tailWrite, prepare},
		"tail xattr":      {head, op(2, 7, tail, "setxattr user.not_write (1)"), prepare, complete},
		"tail call":       {head, op(2, 7, tail, "call rgw.not_write in=4b"), prepare, complete},
		"tail wrong pool": {head, op(2, 6, tail, "writefull 0~5"), prepare, complete},
		"foreign marker":  {head, op(2, 7, "other.2__shadow_.x_1", "writefull 0~5"), prepare, complete},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"ops": ops})
			if err != nil {
				t.Fatal(err)
			}
			ready, err := rgwNativeMultiPutPublished(raw, clients, testHead, testMarker, "bkt", 7, 6)
			if err != nil || ready != (name == "complete") {
				t.Fatalf("multi-object reception: ready=%t error=%v", ready, err)
			}
		})
	}
	raw, err := json.Marshal(map[string]any{"ops": []map[string]any{head, tailWrite, prepare, complete}})
	if err != nil {
		t.Fatal(err)
	}
	operations, err := rgwNativeHistory(raw, "client.42", rgwNativeMultiObjects(raw, testHead, testMarker, "bkt", 7, 6))
	if err != nil || len(operations) != 4 {
		t.Fatalf("discovered object join: %+v %v", operations, err)
	}
	if writes, bytes := rgwNativeWrittenBytes(operations, 7, tail); writes != 1 || bytes != 2097152 {
		t.Fatalf("received tail write extent: writes=%d bytes=%d", writes, bytes)
	}
	if writes, bytes := rgwNativeWrittenBytes(operations, 6, tail); writes != 0 || bytes != 0 {
		t.Fatal("write extent crossed pools")
	}
}

func TestRGWNativeS3ExplicitResponseAllowanceIsBoundedAndCancellable(t *testing.T) {
	large := bytes.Repeat([]byte{7}, 2<<20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = w.Write(large)
	}))
	defer server.Close()
	client := rgwNativeS3{endpoint: server.URL, access: "a", secret: "s", region: "us-east-1", http: &http.Client{Timeout: 5 * time.Second}}
	if _, err := client.request(context.Background(), http.MethodGet, "/bucket/key", nil, http.StatusOK); err == nil || !strings.Contains(err.Error(), "read bound") {
		t.Fatalf("default small-response bound was not kept: %v", err)
	}
	got, err := client.requestBounded(context.Background(), http.MethodGet, "/bucket/key", nil, http.StatusOK, 4<<20)
	if err != nil || !bytes.Equal(got, large) {
		t.Fatalf("explicit allowance: %v", err)
	}
	if _, err := client.requestBounded(context.Background(), http.MethodGet, "/bucket/key", nil, http.StatusOK, 1<<20); err == nil {
		t.Fatal("response above an explicit allowance admitted")
	}
	for _, limit := range []int64{0, -1, rgwNativeMaxResponseLimit + 1} {
		if _, err := client.requestBounded(context.Background(), http.MethodGet, "/bucket/key", nil, http.StatusOK, limit); err == nil {
			t.Fatalf("allowance %d admitted", limit)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.requestBounded(ctx, http.MethodGet, "/bucket/key", nil, http.StatusOK, 4<<20); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled response read did not keep cancellation: %v", err)
	}
}
