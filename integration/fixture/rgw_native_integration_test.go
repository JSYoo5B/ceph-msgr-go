package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
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
	if runtime.GOOS != "linux" {
		t.Fatal("RGW native checkpoint requires an authorized Linux Docker runner")
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
			var owned []testcontainers.Container
			for _, monitor := range cluster.Monitors() {
				owned = append(owned, monitor.Container)
			}
			for _, manager := range cluster.Managers() {
				owned = append(owned, manager.Container)
			}
			for _, osd := range cluster.OSDs() {
				owned = append(owned, osd.Container)
			}
			owned = append(owned, cluster.ServiceContainers()...)
			volumes := map[string]bool{}
			for _, container := range owned {
				inspected, err := container.Inspect(cleanup)
				if err != nil {
					t.Errorf("inspect owned resources before cleanup: %v", err)
					continue
				}
				for _, mount := range inspected.Mounts {
					if mount.Type == "volume" && mount.Name != "" {
						volumes[mount.Name] = true
					}
				}
			}
			network := cluster.NetworkName()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Errorf("terminate owned RGW fixture: %v", err)
				return
			}
			docker, err := testcontainers.NewDockerClientWithOpts(cleanup)
			if err != nil {
				t.Error("open independent cleanup inspector:", err)
				return
			}
			defer func() { _ = docker.Close() }()
			for _, container := range owned {
				if _, err := docker.ContainerInspect(cleanup, container.GetContainerID(), client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned container removal not independently confirmed: %v", err)
				}
			}
			for volume := range volumes {
				if _, err := docker.VolumeInspect(cleanup, volume, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned volume removal not independently confirmed: %v", err)
				}
			}
			if network != "" {
				if _, err := docker.NetworkInspect(cleanup, network, client.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
					t.Errorf("owned network removal not independently confirmed: %v", err)
				}
			}
			if !t.Failed() {
				t.Logf("RGW_NATIVE_CLEANUP independent inspection confirms containers=%d volumes=%d network=removed", len(owned), len(volumes))
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
	payload := make([]byte, 1031)
	for i := range payload {
		payload[i] = byte(i*13 + 7)
	}
	// Do not run a backend oracle between this S3 mutation and the history
	// snapshot. RGW uses client.admin too; the gateway's authenticated numeric
	// identity, not the CephX entity name, attributes these received requests.
	request(http.MethodPut, path, payload, http.StatusOK)
	// RGW connects additional librados handles lazily. Sample the same
	// process after PUT so its newly authenticated client IDs are included.
	// This local socket query does not submit an OSD oracle operation.
	generation := rgwNativeGeneration(t, ctx, gateway)
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
	received := rgwNativeAttributedHistory(t, history, generation, objects)
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
	// Docker can assign a new ephemeral published port to the same container
	// after Stop/Start. Resolve the owned gateway's current listener before
	// the single post-restart GET instead of retaining the stale endpoint.
	restartedEndpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal("resolve restarted owned gateway endpoint:", err)
	}
	endpointChanged := restartedEndpoint != s3.endpoint
	s3.endpoint = restartedEndpoint
	// Restart is followed by read-only observations, not a replayed PUT.
	if read := request(http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(read, payload) {
		t.Fatal("S3 object bytes changed across gateway restart")
	}
	afterGeneration := rgwNativeGeneration(t, ctx, gateway)
	for _, beforeClient := range generation {
		for _, afterClient := range afterGeneration {
			if beforeClient.Client == afterClient.Client || beforeClient.Nonce == afterClient.Nonce {
				t.Fatal("restarted gateway retained an old client identity or nonce")
			}
		}
	}
	after := rgwNativeOracle(t, ctx, control, dataPool, indexPool, head, indexObjects, key, payload)
	if before != after {
		t.Fatal("native object/index postconditions changed across gateway restart")
	}
	request(http.MethodDelete, path, nil, http.StatusNoContent)
	deleted := rgwNativeReceipt(t, ctx, cluster, rgwNativeGeneration(t, ctx, gateway), objects)
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
	t.Logf("RGW_NATIVE_CHECKPOINT image=%s source=7f793731f1b39eb4f465e960113d2363c311b964 mode=secure key=aes256k bytes=%d sha256=%s native_index_shard=%s mapped_endpoint_changed=%t before=%+v after=%+v put_reception=%+v delete_reception=%+v object_index_user_cleanup=confirmed", rgwNativeImage, len(payload), rgwNativeHash(payload), before, endpointChanged, generation, afterGeneration, received, deleted)
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

func rgwNativeGeneration(t *testing.T, ctx context.Context, gateway *rgw.Gateway) []rgwNativeClientGeneration {
	t.Helper()
	// The run volume also contains MON/MGR/OSD sockets. A zero ceph CLI exit
	// status is insufficient: an unsupported messenger can print a text error.
	// Select UNIX sockets held by the gateway's PID 1, then require decoded
	// authenticated radosclient metadata. Neither addresses nor CLI text errors
	// are emitted by this process-local inspector.
	output := rgwNativeExec(t, ctx, gateway, "python3", "-c", rgwNativeGatewayInspector)
	var dump struct {
		Candidates int                         `json:"socket_candidates"`
		Messengers []rgwNativeClientGeneration `json:"messengers"`
	}
	if err := json.Unmarshal(output, &dump); err != nil || dump.Candidates == 0 || dump.Candidates > 8 || len(dump.Messengers) == 0 || len(dump.Messengers) > 32 {
		t.Fatalf("missing bounded process-owned gateway Messenger identities/nonces: decode=%v candidates=%d messengers=%d", err, dump.Candidates, len(dump.Messengers))
	}
	return dump.Messengers
}

func rgwNativeReceipt(t *testing.T, ctx context.Context, cluster *rgw.Container, clients []rgwNativeClientGeneration, objects map[string]bool) []rgwNativeOperation {
	t.Helper()
	output := rgwNativeExec(t, ctx, cluster.Container.Container, "ceph", "tell", "osd."+strconv.Itoa(cluster.OSDs()[0].ID), "dump_historic_ops")
	return rgwNativeAttributedHistory(t, output, clients, objects)
}

func rgwNativeAttributedHistory(t *testing.T, output []byte, clients []rgwNativeClientGeneration, objects map[string]bool) []rgwNativeOperation {
	t.Helper()
	var received []rgwNativeOperation
	for _, client := range clients {
		operations, err := rgwNativeHistory(output, client.Client, objects)
		if err != nil {
			t.Fatal("parse gateway-attributed native receipt:", err)
		}
		received = append(received, operations...)
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
