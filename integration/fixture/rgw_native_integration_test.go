package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const rgwNativeImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

// This is a dedicated fixture-only checkpoint, not a profile of the Messenger
// product suite or an available comment-selected CI scenario.
func TestRGWNativeCheckpointIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") == "" {
		t.Skip("set CEPH_MSGR_TEST_RGW_NATIVE=1 on an authorized Linux Docker runner")
	}
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") != "1" {
		t.Fatal("CEPH_MSGR_TEST_RGW_NATIVE must be 1")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	config := filepath.Join(t.TempDir(), "rgw-checkpoint.conf")
	if err := os.WriteFile(config, []byte("[global]\nosd_pool_default_pg_num = 1\n[osd]\nosd_op_history_size = 4096\nosd_op_history_duration = 600\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster, err := rgw.Run(ctx, rgwNativeImage,
		ceph.WithMonitorCount(1), ceph.WithManagerCount(1), ceph.WithOSDCount(1),
		ceph.WithPoolDefaults(1, 1), ceph.WithOSDObjectStore(ceph.OSDBlueStore),
		ceph.WithMessengerMode(ceph.MessengerV2Secure),
		ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CipherAES256K, ServiceCipher: ceph.CipherAES256K}),
		ceph.WithConfigFile(config), rgw.WithGateways(rgw.Config{SkipUserCreation: true}))
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Errorf("terminate owned RGW fixture: %v", err)
			} else {
				t.Log("RGW_NATIVE_CLEANUP owned gateway, MON, MGR, OSD, network and volume removed")
			}
		})
	}
	if err != nil {
		t.Fatal("start RGW fixture:", err)
	}
	gateways := cluster.Gateways()
	if len(gateways) != 1 || len(cluster.OSDs()) != 1 {
		t.Fatal("checkpoint requires exactly one gateway and OSD")
	}
	gateway := gateways[0]
	control := cluster.Container.Container
	version := string(rgwNativeExec(t, ctx, control, "ceph", "version"))
	if !strings.Contains(version, "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964)") {
		t.Fatal("unexpected native release/source identity")
	}
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "rgw-native-checkpoint", DisplayName: "Messenger fixture"})
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
	key := "small-binary"
	path := "/" + bucket + "/" + key
	request := func(method, target string, payload []byte, status int) []byte {
		t.Helper()
		result, err := s3.request(ctx, method, target, payload, status)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request(http.MethodPut, "/"+bucket, nil, http.StatusOK)
	bucketInfo := rgwNativeBucket(t, ctx, gateway, bucket)
	generation := rgwNativeGeneration(t, ctx, gateway)
	payload := make([]byte, 1031)
	for i := range payload {
		payload[i] = byte(i*13 + 7)
	}
	// Do not run a backend oracle between this S3 mutation and the history
	// snapshot. RGW uses client.admin too; the gateway's authenticated numeric
	// identity, not the CephX entity name, attributes these received requests.
	request(http.MethodPut, path, payload, http.StatusOK)
	history := rgwNativeExec(t, ctx, control, "ceph", "tell", "osd."+strconv.Itoa(cluster.OSDs()[0].ID), "dump_historic_ops")
	pools, err := cluster.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dataPool, indexPool := "", ""
	for _, pool := range pools {
		if strings.HasSuffix(pool.Name, ".rgw.buckets.data") {
			dataPool = pool.Name
		}
		if strings.HasSuffix(pool.Name, ".rgw.buckets.index") {
			indexPool = pool.Name
		}
	}
	if dataPool == "" || indexPool == "" {
		t.Fatal("missing actual RGW data/index pools")
	}
	head := bucketInfo.Marker + "_" + key
	indexObjects := rgwNativeIndexObjects(t, ctx, control, indexPool, bucketInfo.ID)
	objects := map[string]bool{head: true}
	for _, object := range indexObjects {
		objects[object] = true
	}
	received, err := rgwNativeHistory(history, generation.Client, objects)
	if err != nil {
		t.Fatal("parse gateway-attributed PUT receipt:", err)
	}
	write, prepare, complete := false, false, false
	for _, op := range received {
		isHead := strings.Contains(op.Description, ":"+head+":")
		write = write || isHead && (strings.Contains(op.Description, "write ") || strings.Contains(op.Description, "writefull "))
		prepare = prepare || strings.Contains(op.Description, "call rgw.bucket_prepare_op")
		complete = complete || strings.Contains(op.Description, "call rgw.bucket_complete_op")
	}
	if !write || !prepare || !complete {
		t.Fatalf("incomplete gateway-attributed OSD reception: write=%t prepare=%t complete=%t operations=%+v", write, prepare, complete, received)
	}
	if read := request(http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(read, payload) {
		t.Fatal("S3 readback changed the binary object")
	}
	before := rgwNativeOracle(t, ctx, control, dataPool, indexPool, head, indexObjects, key, payload)
	beforeState, err := gateway.State(ctx)
	if err != nil || beforeState == nil || !beforeState.Running {
		t.Fatal("gateway was not running before restart", err)
	}
	stopTimeout := 5 * time.Second
	if err := gateway.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("stop owned gateway once:", err)
	}
	stopped, err := gateway.State(ctx)
	if err != nil || stopped == nil || stopped.Running {
		t.Fatal("gateway stop was not observed", err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatal("start same owned gateway once:", err)
	}
	afterState, err := gateway.State(ctx)
	if err != nil || afterState == nil || !afterState.Running || afterState.StartedAt == beforeState.StartedAt {
		t.Fatal("gateway process generation did not change", err)
	}
	afterGeneration := rgwNativeGeneration(t, ctx, gateway)
	if afterGeneration.Client == generation.Client || afterGeneration.Nonce == generation.Nonce {
		t.Fatal("restarted gateway retained its old client identity or nonce")
	}
	// Restart is followed by read-only observations, not a replayed PUT.
	if read := request(http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(read, payload) {
		t.Fatal("S3 object bytes changed across gateway restart")
	}
	after := rgwNativeOracle(t, ctx, control, dataPool, indexPool, head, indexObjects, key, payload)
	if before != after {
		t.Fatal("native object/index postconditions changed across gateway restart")
	}
	request(http.MethodDelete, path, nil, http.StatusNoContent)
	deleted := rgwNativeReceipt(t, ctx, cluster, afterGeneration.Client, objects)
	remove := false
	for _, op := range deleted {
		remove = remove || strings.Contains(op.Description, ":"+head+":") && strings.Contains(op.Description, "delete")
	}
	if !remove {
		t.Fatal("missing gateway-attributed native head deletion")
	}
	request(http.MethodGet, path, nil, http.StatusNotFound)
	if slices.Contains(rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "ls")), head) {
		t.Fatal("owned head survived successful S3 delete")
	}
	for _, object := range indexObjects {
		if slices.Contains(rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", indexPool, "listomapkeys", object)), key) {
			t.Fatal("owned key survived in native bucket index")
		}
	}
	request(http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
	if err := gateway.RemoveUser(ctx, user); err != nil {
		t.Fatal("remove owned user:", err)
	}
	t.Logf("RGW_NATIVE_CHECKPOINT image=%s source=7f793731f1b39eb4f465e960113d2363c311b964 mode=secure key=aes256k bytes=%d sha256=%s native_index_shard=%s before=%+v after=%+v put_reception=%+v delete_reception=%+v object_index_user_cleanup=confirmed", rgwNativeImage, len(payload), rgwNativeHash(payload), before, generation, afterGeneration, received, deleted)
}

func rgwNativeExec(t *testing.T, parent context.Context, container testcontainers.Container, argv ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	code, reader, err := container.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		t.Fatal("bounded native checkpoint command:", err)
	}
	output, err := io.ReadAll(io.LimitReader(reader, (32<<20)+1))
	if err != nil || len(output) > 32<<20 || code != 0 {
		t.Fatalf("bounded native checkpoint command exit=%d read_error=%v bytes=%d", code, err, len(output))
	}
	return output
}

func rgwNativeLines(output []byte) []string {
	return strings.Fields(string(output))
}

type rgwNativeBucketInfo struct {
	Bucket string `json:"bucket"`
	ID     string `json:"id"`
	Marker string `json:"marker"`
}

func rgwNativeBucket(t *testing.T, ctx context.Context, gateway *rgw.Gateway, bucket string) rgwNativeBucketInfo {
	t.Helper()
	output, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", bucket)
	var info rgwNativeBucketInfo
	if err != nil || json.Unmarshal(output, &info) != nil || info.Bucket != bucket || info.Marker == "" || info.ID == "" {
		t.Fatal("read owned native bucket identity", err)
	}
	return info
}

func rgwNativeIndexObjects(t *testing.T, ctx context.Context, control testcontainers.Container, pool, bucketID string) []string {
	t.Helper()
	var objects []string
	for _, object := range rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", pool, "ls")) {
		if object == ".dir."+bucketID || strings.HasPrefix(object, ".dir."+bucketID+".") {
			objects = append(objects, object)
		}
	}
	if len(objects) == 0 || len(objects) > 64 {
		t.Fatal("owned native index shard count outside checkpoint bounds")
	}
	slices.Sort(objects)
	return objects
}

type rgwNativeClientGeneration struct {
	Client string `json:"client"`
	Nonce  uint64 `json:"nonce"`
}

func rgwNativeGeneration(t *testing.T, ctx context.Context, gateway *rgw.Gateway) rgwNativeClientGeneration {
	t.Helper()
	output := rgwNativeExec(t, ctx, gateway, "/bin/sh", "-c", `for socket in /var/run/ceph/*.asok; do ceph --admin-daemon "$socket" messenger dump radosclient 2>/dev/null && exit 0; done; exit 1`)
	var dump struct {
		Messenger struct {
			Nonce uint64 `json:"nonce"`
			Name  struct {
				Type string `json:"type"`
				Num  uint64 `json:"num"`
			} `json:"my_name"`
		} `json:"messenger"`
	}
	if json.Unmarshal(output, &dump) != nil || dump.Messenger.Name.Type != "client" || dump.Messenger.Name.Num == 0 || dump.Messenger.Nonce == 0 {
		t.Fatal("missing native gateway Messenger identity/nonce")
	}
	return rgwNativeClientGeneration{Client: "client." + strconv.FormatUint(dump.Messenger.Name.Num, 10), Nonce: dump.Messenger.Nonce}
}

func rgwNativeReceipt(t *testing.T, ctx context.Context, cluster *rgw.Container, client string, objects map[string]bool) []rgwNativeOperation {
	t.Helper()
	output := rgwNativeExec(t, ctx, cluster.Container.Container, "ceph", "tell", "osd."+strconv.Itoa(cluster.OSDs()[0].ID), "dump_historic_ops")
	received, err := rgwNativeHistory(output, client, objects)
	if err != nil {
		t.Fatal("parse gateway-attributed native receipt:", err)
	}
	return received
}

func rgwNativeOracle(t *testing.T, ctx context.Context, control testcontainers.Container, dataPool, indexPool, head string, objects []string, key string, payload []byte) string {
	t.Helper()
	read := rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "get", head, "-")
	if !bytes.Equal(read, payload) {
		t.Fatalf("independent native head bytes differ: len=%d sha256=%s", len(read), rgwNativeHash(read))
	}
	found := ""
	for _, object := range objects {
		keys := rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", indexPool, "listomapkeys", object))
		if slices.Contains(keys, key) {
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
