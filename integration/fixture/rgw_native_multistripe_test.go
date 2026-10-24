package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

const (
	rgwNativeMultiPayloadSize   = 6 << 20
	rgwNativeMultiResponseLimit = 8 << 20
)

// TestRGWNativeMultiStripeIntegration is the second dedicated fixture-only RGW
// checkpoint: one S3 PUT of a 6 MiB object that the pinned gateway stores as a
// head plus tail RADOS objects, its manifest, S3 DELETE and the resulting
// tail cleanup through the owned garbage-collection queue. S3 is only input.
// No product API, S3 SDK or comment-selected CI command is introduced.
func TestRGWNativeMultiStripeIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") == "" {
		t.Skip("set CEPH_MSGR_TEST_RGW_NATIVE=1 on an authorized Linux Docker runner")
	}
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") != "1" {
		t.Fatal("CEPH_MSGR_TEST_RGW_NATIVE must be 1")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("RGW native checkpoint requires an authorized Linux Docker runner")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, gateway := rgwNativeStartFixture(t, ctx)
	control := cluster.Container.Container
	version := string(rgwNativeExec(t, ctx, control, "ceph", "version"))
	if !strings.Contains(version, "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964)") {
		t.Fatal("unexpected native release/source identity")
	}
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "rgw-native-multistripe", DisplayName: "Messenger fixture"})
	if err != nil {
		t.Fatal("create owned S3 user:", err)
	}
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Timeout: 20 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer httpClient.CloseIdleConnections()
	s3 := rgwNativeS3{endpoint: endpoint, access: access, secret: secret, region: gateway.Region, http: httpClient}
	bucket := "msgr-rgw-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	key := "large-binary"
	path := "/" + bucket + "/" + key
	request := func(method, target string, payload []byte, status int, limit int64) []byte {
		t.Helper()
		result, err := s3.requestBounded(ctx, method, target, payload, status, limit)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request(http.MethodPut, "/"+bucket, nil, http.StatusOK, rgwNativeDefaultResponseLimit)
	bucketInfo := rgwNativeBucket(t, ctx, gateway, bucket)
	payload := rgwNativeLargePayload(rgwNativeMultiPayloadSize)
	payloadHash := rgwNativeHash(payload)
	head := bucketInfo.Marker + "_" + key
	// One PUT. No oracle traffic runs between it and the retained reception
	// snapshot, and an unknown S3 outcome fails the run instead of replaying.
	request(http.MethodPut, path, payload, http.StatusOK, rgwNativeDefaultResponseLimit)
	generation := rgwNativeGeneration(t, ctx, gateway)
	dataPool, indexPool, dataPoolID, indexPoolID := rgwNativePools(t, ctx, cluster)
	logPoolID := rgwNativeLogPoolID(t, ctx, cluster)
	osdName := "osd." + strconv.Itoa(cluster.OSDs()[0].ID)
	history, err := rgwNativePollHistory(ctx, func(read context.Context) []byte {
		return rgwNativeExec(t, read, control, "ceph", "tell", osdName, "dump_historic_ops")
	}, func(output []byte) (bool, error) {
		return rgwNativeMultiPutPublished(output, generation, head, bucketInfo.Marker, bucketInfo.ID, dataPoolID, indexPoolID)
	})
	if err != nil {
		t.Fatal("observe complete gateway multi-object PUT reception before oracle traffic:", err)
	}
	// The layout is captured before DELETE and before any backend byte oracle.
	manifestOutput, err := gateway.Admin(ctx, "object", "manifest", "--bucket", bucket, "--object", key)
	if err != nil {
		t.Fatal("read native object manifest:", err)
	}
	manifest, err := rgwNativeParseManifest(manifestOutput, dataPool, bucketInfo.Marker, head, uint64(len(payload)))
	if err != nil {
		t.Fatalf("native manifest is not an accepted multi-stripe layout: %v\n%s", err, rgwNativeBoundedText(manifestOutput))
	}
	tails := manifest.Tails(head)
	if len(tails) == 0 {
		t.Fatal("manifest has no tail object")
	}
	indexObjects := rgwNativeIndexObjects(t, ctx, control, indexPool, bucketInfo.ID)
	objects := map[string]int64{}
	for _, object := range manifest.Objects() {
		objects[object] = dataPoolID
	}
	for _, object := range indexObjects {
		objects[object] = indexPoolID
	}
	received := rgwNativeAttributedHistory(t, history, generation, objects)
	prepare, complete := false, false
	var putReceipt []rgwNativeOperation
	for _, op := range received {
		_, isPrepare, isComplete := rgwNativePutKinds(op, head, bucketInfo.ID, dataPoolID, indexPoolID)
		prepare, complete = prepare || isPrepare, complete || isComplete
		if isPrepare || isComplete || rgwNativeWrites(op) {
			putReceipt = append(putReceipt, op)
		}
	}
	type stripeReceipt struct {
		Index  int64  `json:"index"`
		Offset uint64 `json:"offset"`
		Size   uint64 `json:"size"`
		OID    string `json:"oid"`
		Writes int    `json:"received_writes"`
		Bytes  uint64 `json:"received_write_bytes"`
	}
	var layout []stripeReceipt
	for _, stripe := range manifest.Stripes {
		writes, written := rgwNativeWrittenBytes(received, dataPoolID, stripe.Raw.OID)
		layout = append(layout, stripeReceipt{stripe.Index, stripe.Offset, stripe.Size, stripe.Raw.OID, writes, written})
		if writes == 0 || written != stripe.Size {
			t.Fatalf("gateway-attributed WRITE reception does not join manifest stripe %d: writes=%d bytes=%d size=%d operations=%+v", stripe.Index, writes, written, stripe.Size, received)
		}
	}
	if !prepare || !complete {
		t.Fatalf("incomplete gateway-attributed index reception: prepare=%t complete=%t", prepare, complete)
	}
	// S3 readback and independent native reconstruction in manifest order.
	if read := request(http.MethodGet, path, nil, http.StatusOK, rgwNativeMultiResponseLimit); !bytes.Equal(read, payload) {
		t.Fatal("S3 readback changed the large binary object")
	}
	reconstructed, err := rgwNativeReconstruct(manifest, func(oid string) ([]byte, error) {
		return rgwNativeExecBounded(t, ctx, control, rgwNativeMultiResponseLimit, "rados", "-p", dataPool, "get", oid, "-"), nil
	})
	if err != nil {
		t.Fatal("independent native stripe reconstruction:", err)
	}
	if !bytes.Equal(reconstructed, payload) || rgwNativeHash(reconstructed) != payloadHash {
		t.Fatalf("independent native reconstruction differs from the input: sha256=%s", rgwNativeHash(reconstructed))
	}
	shard := rgwNativeIndexShard(t, ctx, control, indexPool, indexObjects, key)
	// Before DELETE the owned objects are live and no GC chain carries them.
	inventory := rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "ls"))
	for _, object := range manifest.Objects() {
		if manifest.Sentinel != nil && object == manifest.Sentinel.OID {
			continue
		}
		if !slices.Contains(inventory, object) {
			t.Fatalf("manifest object %s is absent from the native pool before DELETE", object)
		}
	}
	if owned := rgwNativeGCQueue(t, ctx, gateway, dataPool, bucketInfo.Marker, manifest.Objects()); len(owned) != 0 {
		t.Fatal("owned objects were already in the GC queue before DELETE")
	}

	var lastPutTID uint64
	for _, op := range putReceipt {
		lastPutTID = max(lastPutTID, op.TID)
	}
	request(http.MethodDelete, path, nil, http.StatusNoContent, rgwNativeDefaultResponseLimit)
	deleteGeneration := rgwNativeGeneration(t, ctx, gateway)
	removeAndEnqueue := func(output []byte) (remove, enqueue []rgwNativeOperation, err error) {
		for _, client := range deleteGeneration {
			ops, err := rgwNativeHistoryMatch(output, client.Client, func(pool int64, namespace, locator, name string) bool {
				return locator == "" && ((pool == dataPoolID && namespace == "" && name == head) ||
					(pool == logPoolID && rgwNativeGCObjectName.MatchString(name)))
			})
			if err != nil {
				return nil, nil, err
			}
			for _, op := range ops {
				switch {
				case op.PoolID == dataPoolID && rgwNativeCall(op, "obj_remove"):
					remove = append(remove, op)
				// The log pool also carries GC shard initialisation, locks and
				// list scans from before this DELETE. Only a request made after
				// the PUT phase with the enqueue method is the DELETE's chain.
				case op.PoolID == logPoolID && op.TID > lastPutTID && rgwNativeGCEnqueue(op):
					enqueue = append(enqueue, op)
				}
			}
		}
		return remove, enqueue, nil
	}
	deleteHistory, err := rgwNativePollHistory(ctx, func(read context.Context) []byte {
		return rgwNativeExec(t, read, control, "ceph", "tell", osdName, "dump_historic_ops")
	}, func(output []byte) (bool, error) {
		remove, enqueue, err := removeAndEnqueue(output)
		return len(remove) > 0 && len(enqueue) > 0, err
	})
	if err != nil {
		t.Fatal("observe gateway DELETE head removal and GC enqueue reception before oracle traffic:", err)
	}
	removeReceipt, enqueueReceipt, _ := removeAndEnqueue(deleteHistory)
	request(http.MethodGet, path, nil, http.StatusNotFound, rgwNativeDefaultResponseLimit)
	afterDelete := rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "ls"))
	if slices.Contains(afterDelete, head) {
		t.Fatal("owned head survived successful S3 delete")
	}
	for _, object := range indexObjects {
		if slices.Contains(rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", indexPool, "listomapkeys", object)), key) {
			t.Fatal("owned key survived in native bucket index")
		}
	}
	// The tails survive until the explicit GC action and are all queued.
	for _, tail := range tails {
		if !slices.Contains(afterDelete, tail) {
			t.Fatalf("tail %s disappeared before the explicit GC action", tail)
		}
	}
	queued := rgwNativeGCQueue(t, ctx, gateway, dataPool, bucketInfo.Marker, manifest.Objects())
	for _, tail := range tails {
		if len(queued[tail]) != 1 {
			t.Fatalf("tail %s is carried by %d GC chains, want exactly one", tail, len(queued[tail]))
		}
	}
	// One bounded GC action. It processes the whole store queue, which is
	// acceptable only because this fixture is wholly disposable.
	gcContext, stopGC := context.WithTimeout(ctx, 90*time.Second)
	if _, err := gateway.Admin(gcContext, "gc", "process", "--include-all"); err != nil {
		stopGC()
		t.Fatal("explicit GC process:", err)
	}
	stopGC()
	cleanupDeadline := time.Now().Add(30 * time.Second)
	for {
		remaining := rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "ls"))
		gone := true
		for _, tail := range tails {
			gone = gone && !slices.Contains(remaining, tail)
		}
		if gone && len(rgwNativeGCQueue(t, ctx, gateway, dataPool, bucketInfo.Marker, manifest.Objects())) == 0 {
			break
		}
		if time.Now().After(cleanupDeadline) {
			t.Fatal("GC action did not remove the queued tails within its observation bound")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	request(http.MethodDelete, "/"+bucket, nil, http.StatusNoContent, rgwNativeDefaultResponseLimit)
	if err := gateway.RemoveUser(ctx, user); err != nil {
		t.Fatal("remove owned user:", err)
	}
	tags := map[string]bool{}
	for _, chain := range queued {
		for _, tag := range chain {
			tags[tag] = true
		}
	}
	tagCount := len(tags)
	var sentinel *string
	if manifest.Sentinel != nil {
		sentinel = &manifest.Sentinel.OID
	}
	receipt, err := json.Marshal(map[string]any{
		"image": rgwNativeImage, "source": "7f793731f1b39eb4f465e960113d2363c311b964", "mode": "secure", "key": "aes256k",
		"bytes": len(payload), "sha256": payloadHash, "stripes": layout, "head_sentinel": sentinel,
		"native_index_shard": shard, "gc_chains": tagCount,
		"put_reception": putReceipt, "delete_head_remove": removeReceipt, "delete_gc_enqueue": enqueueReceipt,
		"gc_removed_tails": len(tails), "gateway_generation": generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_NATIVE_MULTISTRIPE %s", receipt)
}

func rgwNativeLogPoolID(t *testing.T, ctx context.Context, cluster *rgw.Container) int64 {
	t.Helper()
	pools, err := cluster.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range pools {
		if strings.HasSuffix(pool.Name, ".rgw.log") && pool.ID > 0 {
			return pool.ID
		}
	}
	t.Fatal("missing actual RGW log pool")
	return 0
}

// rgwNativeBoundedText limits a diagnostic dump of non-secret native admin
// output to a short excerpt.
func rgwNativeBoundedText(raw []byte) string {
	if len(raw) > 4096 {
		raw = raw[:4096]
	}
	return string(raw)
}

func rgwNativeExecBounded(t *testing.T, parent context.Context, container testcontainers.Container, limit int, argv ...string) []byte {
	t.Helper()
	output := rgwNativeExec(t, parent, container, argv...)
	if len(output) > limit {
		t.Fatalf("native command output exceeded %d bytes", limit)
	}
	return output
}

// rgwNativeIndexShard returns the single actual index shard that holds key.
func rgwNativeIndexShard(t *testing.T, ctx context.Context, control testcontainers.Container, indexPool string, objects []string, key string) string {
	t.Helper()
	found := ""
	for _, object := range objects {
		if slices.Contains(rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", indexPool, "listomapkeys", object)), key) {
			if found != "" {
				t.Fatal("object key appears in multiple actual index shards")
			}
			found = object
		}
	}
	if found == "" {
		t.Fatal("object key is absent from actual native index shards")
	}
	return found
}

func rgwNativeGCQueue(t *testing.T, ctx context.Context, gateway *rgw.Gateway, dataPool, marker string, manifest []string) map[string][]string {
	t.Helper()
	read, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	output, err := gateway.Admin(read, "gc", "list", "--include-all")
	if err != nil {
		t.Fatal("list native GC queue:", err)
	}
	entries, err := rgwNativeParseGCList(output)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := rgwNativeGCOwned(entries, dataPool, marker, manifest)
	if err != nil {
		t.Fatal(err)
	}
	return owned
}
