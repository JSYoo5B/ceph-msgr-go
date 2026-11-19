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

// rgwNativeOverlapWindow is how long the requests issued during the OSD
// outage must stay outstanding before the OSD is started again.
const rgwNativeOverlapWindow = 5 * time.Second

// TestRGWNativeBackendFaultIntegration is the dedicated fixture-only RGW
// backend-fault checkpoint (RGW_NATIVE_VALIDATION.md). On its own owned
// one-MON, one-MGR, one-BlueStore-OSD, one-gateway cluster it stops and
// restarts the single OSD while one S3 PUT and one S3 GET are outstanding,
// then stops and restarts the manager around one S3 PUT. Each overlapping
// request is classified from its HTTP exchange alone and then checked against
// an independent native readback; reception after the OSD's return, the
// gateway's watch reconnect and its Messenger reconnections are attributed
// to the gateway's own client identities. A catalog row with rgw_native=1
// routes the trusted native workflow to this test (rgw_native_route.go).
//
//fixture:isolated starts its own owned RGW cluster and stops and restarts its OSD and manager
func TestRGWNativeBackendFaultIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") == "" {
		t.Skip("set CEPH_MSGR_TEST_RGW_NATIVE=1 on an authorized Linux Docker runner")
	}
	if os.Getenv("CEPH_MSGR_TEST_RGW_NATIVE") != "1" {
		t.Fatal("CEPH_MSGR_TEST_RGW_NATIVE must be 1")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("RGW native checkpoint requires an authorized Linux Docker runner")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, gateway := rgwNativeStartFixture(t, ctx)
	control := cluster.Container.Container
	version := string(rgwNativeExec(t, ctx, control, "ceph", "version"))
	if !strings.Contains(version, "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964)") {
		t.Fatal("unexpected native release/source identity")
	}
	osds := rgwNativeOSDs(t, ctx, cluster)
	managers, err := cluster.Managers(ctx)
	if err != nil || len(managers) != 1 {
		t.Fatal("checkpoint requires exactly one owned manager", err)
	}
	osd, manager := osds[0], managers[0]
	osdName := "osd." + strconv.Itoa(osd.ID)
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "rgw-native-backend-fault", DisplayName: "Messenger fixture"})
	if err != nil {
		t.Fatal("create owned S3 user:", err)
	}
	access, secret, err := user.CreationKey().Credentials()
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
	// The requests that overlap the OSD outage wait in the gateway until the
	// OSD returns (pinned rados_osd_op_timeout defaults to 0, no timeout).
	// Their own bound is longer than the outage, never a retry.
	faultClient := &http.Client{Timeout: 3 * time.Minute,
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer faultClient.CloseIdleConnections()
	s3Fault := s3
	s3Fault.http = faultClient
	request := func(method, target string, payload []byte, status int) []byte {
		t.Helper()
		result, err := s3.request(ctx, method, target, payload, status)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	bucket := "msgr-rgw-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	request(http.MethodPut, "/"+bucket, nil, http.StatusOK)
	bucketInfo := rgwNativeBucket(t, ctx, gateway, bucket)
	keys := []string{"before-fault", "during-osd-fault", "during-mgr-fault"}
	payloads := map[string][]byte{
		keys[0]: rgwNativeFaultPayload(13, 7),
		keys[1]: rgwNativeFaultPayload(29, 11),
		keys[2]: rgwNativeFaultPayload(31, 3),
	}
	head := func(key string) string { return bucketInfo.Marker + "_" + key }
	osdDump := func(read context.Context) (rgwNativeDaemonState, error) {
		output, err := rgwNativeTryExec(read, control, "ceph", "osd", "dump", "--format", "json")
		if err != nil {
			return rgwNativeDaemonState{}, err
		}
		return rgwNativeOSDState(output, osd.ID)
	}
	mgrDump := func(read context.Context) (rgwNativeDaemonState, error) {
		output, err := rgwNativeTryExec(read, control, "ceph", "mgr", "dump", "--format", "json")
		if err != nil {
			return rgwNativeDaemonState{}, err
		}
		return rgwNativeMgrState(output)
	}
	peers := func() []rgwNativePeer {
		t.Helper()
		parsed, err := rgwNativeParsePeers(rgwNativeExec(t, ctx, gateway, "python3", "-c", rgwNativeGatewayPeerInspector))
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	history := func(read context.Context) []byte {
		return rgwNativeExec(t, read, control, "ceph", "tell", osdName, "dump_historic_ops")
	}

	// Phase 1: one PUT before any fault, attributed as the small checkpoint
	// attributes it, before any backend oracle runs.
	request(http.MethodPut, "/"+bucket+"/"+keys[0], payloads[keys[0]], http.StatusOK)
	generation := rgwNativeGeneration(t, ctx, gateway)
	dataPool, indexPool, dataPoolID, indexPoolID := rgwNativePools(t, ctx, cluster)
	controlPoolID := rgwNativeControlPoolID(t, ctx, cluster)
	baseline, err := rgwNativePollHistory(ctx, history, func(output []byte) (bool, error) {
		return rgwNativePutPublished(output, generation, head(keys[0]), bucketInfo.ID, dataPoolID, indexPoolID)
	})
	if err != nil {
		t.Fatal("observe gateway PUT reception before the fault:", err)
	}
	indexObjects := rgwNativeIndexObjects(t, ctx, control, indexPool, bucketInfo.ID)
	owned := map[string]int64{}
	for _, key := range keys {
		owned[head(key)] = dataPoolID
	}
	for _, object := range indexObjects {
		owned[object] = indexPoolID
	}
	before := rgwNativeAttributedHistory(t, baseline, generation, owned)
	var beforeReceipt []rgwNativeOperation
	for _, op := range before {
		if w, p, c := rgwNativePutKinds(op, head(keys[0]), bucketInfo.ID, dataPoolID, indexPoolID); w || p || c {
			beforeReceipt = append(beforeReceipt, op)
		}
	}
	lastTIDs := rgwNativeMaxTIDs(rgwNativeAttributedOps(t, baseline, generation, func(int64, string, string, string) bool { return true }))
	rgwNativeOracle(t, ctx, control, dataPool, indexPool, head(keys[0]), indexObjects, keys[0], payloads[keys[0]])

	// Phase 2: stop the single OSD and observe the MON mark it down.
	osdBefore, err := osdDump(ctx)
	if err != nil || !osdBefore.Up {
		t.Fatal("OSD was not up before the fault:", err)
	}
	if connected := rgwNativeConnectedTo(peers(), "osd", osdBefore.Nonce); len(connected) == 0 {
		t.Fatal("no gateway Messenger connection to the OSD before the fault")
	}
	osdStarted, err := osd.State(ctx)
	if err != nil || osdStarted == nil || !osdStarted.Running {
		t.Fatal("OSD container was not running before the fault", err)
	}
	stopTimeout := 30 * time.Second
	if err := osd.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("stop owned OSD once:", err)
	}
	if stopped, err := osd.State(ctx); err != nil || stopped == nil || stopped.Running {
		t.Fatal("OSD stop was not observed", err)
	}
	var osdDown rgwNativeDaemonState
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "MON marking the stopped OSD down", func(read context.Context) (bool, error) {
		state, err := osdDump(read)
		if err != nil {
			return false, nil
		}
		osdDown = state
		return !state.Up && state.Epoch > osdBefore.Epoch, nil
	}); err != nil {
		t.Fatal(err)
	}

	// Phase 3: issue one PUT and one GET once each while the OSD is down. Both
	// must stay outstanding for the whole observation window, with the OSD
	// container stopped and the MON still reporting it down at its end.
	putDuring := rgwNativeIssue(ctx, s3Fault, "PUT "+keys[1], http.MethodPut, "/"+bucket+"/"+keys[1], payloads[keys[1]], http.StatusOK)
	getDuring := rgwNativeIssue(ctx, s3Fault, "GET "+keys[0], http.MethodGet, "/"+bucket+"/"+keys[0], nil, http.StatusOK)
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(rgwNativeOverlapWindow):
	}
	if !putDuring.pending() || !getDuring.pending() {
		// Classify without waiting for a request that is still outstanding.
		now, stop := context.WithTimeout(ctx, time.Millisecond)
		defer stop()
		t.Fatalf("an overlapping request returned while the OSD was down: put=%+v get=%+v", putDuring.outcome(now), getDuring.outcome(now))
	}
	if stopped, err := osd.State(ctx); err != nil || stopped == nil || stopped.Running {
		t.Fatal("OSD container was not stopped for the whole overlap window", err)
	}
	if state, err := osdDump(ctx); err != nil || state.Up {
		t.Fatal("MON did not keep the OSD down for the whole overlap window", err)
	}

	// Phase 4: start the same OSD container and observe a new process.
	pendingAtRestart := putDuring.pending() && getDuring.pending()
	if !pendingAtRestart {
		t.Fatal("an overlapping request returned before the OSD was started")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal("start same owned OSD once:", err)
	}
	if restarted, err := osd.State(ctx); err != nil || restarted == nil || !restarted.Running || restarted.StartedAt == osdStarted.StartedAt {
		t.Fatal("OSD process generation did not change", err)
	}
	var osdAfter rgwNativeDaemonState
	if err := rgwNativeWaitFor(ctx, 3*time.Minute, time.Second, "MON marking the restarted OSD up", func(read context.Context) (bool, error) {
		state, err := osdDump(read)
		if err != nil {
			return false, nil
		}
		osdAfter = state
		return state.Up && state.UpFrom > osdDown.Epoch, nil
	}); err != nil {
		t.Fatal(err)
	}
	wait, stopWait := context.WithTimeout(ctx, 3*time.Minute)
	putOutcome, getOutcome := putDuring.outcome(wait), getDuring.outcome(wait)
	stopWait()
	t.Logf("RGW_NATIVE_FAULT_OUTCOME osd %+v %+v pending_at_restart=%t", putOutcome, getOutcome, pendingAtRestart)
	if putOutcome.Class != rgwNativeSucceeded || getOutcome.Class != rgwNativeSucceeded {
		// Record the native state without replaying either request.
		t.Fatalf("an overlapping request did not succeed: put=%+v get=%+v native=%s", putOutcome, getOutcome,
			rgwNativeHeadPresence(ctx, control, dataPool, head(keys[1])))
	}
	if !bytes.Equal(getDuring.body, payloads[keys[0]]) {
		t.Fatal("the GET that overlapped the OSD outage returned other bytes")
	}
	if osdAfter.Nonce == osdBefore.Nonce {
		t.Fatal("restarted OSD kept its previous Messenger nonce")
	}

	// Phase 5: the restarted OSD process received the gateway's requests.
	afterGeneration := rgwNativeGeneration(t, ctx, gateway)
	for _, client := range generation {
		if !slices.Contains(afterGeneration, client) {
			t.Fatalf("gateway process identity %s changed during the OSD fault", client.Client)
		}
	}
	isControl := func(pool int64, namespace, locator, name string) bool {
		return pool == controlPoolID && namespace == "" && locator == "" && strings.HasPrefix(name, "notify.")
	}
	var returned, reconnects, reads []rgwNativeOperation
	recovered, err := rgwNativePollHistory(ctx, history, func(output []byte) (bool, error) {
		put, err := rgwNativePutPublished(output, afterGeneration, head(keys[1]), bucketInfo.ID, dataPoolID, indexPoolID)
		if err != nil || !put {
			return false, err
		}
		reads, reconnects = nil, nil
		for _, client := range afterGeneration {
			ops, err := rgwNativeHistory(output, client.Client, map[string]int64{head(keys[0]): dataPoolID})
			if err != nil {
				return false, err
			}
			// Only a complete READ of the whole original head is the
			// overlapping GET's backend read; other head operations are not.
			for _, op := range ops {
				if rgwNativeHeadRead(op, uint64(len(payloads[keys[0]]))) {
					reads = append(reads, op)
				}
			}
			watches, err := rgwNativeHistoryMatch(output, client.Client, isControl)
			if err != nil {
				return false, err
			}
			for _, op := range watches {
				if rgwNativeWatchReconnect(op) {
					reconnects = append(reconnects, op)
				}
			}
		}
		return len(reads) != 0 && len(reconnects) != 0, nil
	})
	if err != nil {
		t.Fatalf("observe gateway reception at the restarted OSD: %v (reads=%d watch_reconnects=%d)", err, len(reads), len(reconnects))
	}
	for _, op := range rgwNativeAttributedHistory(t, recovered, afterGeneration, owned) {
		if w, p, c := rgwNativePutKinds(op, head(keys[1]), bucketInfo.ID, dataPoolID, indexPoolID); w || p || c {
			returned = append(returned, op)
		}
	}
	if problems := rgwNativeAfterReturnProblems(slices.Concat(returned, reads, reconnects), osdAfter.UpFrom, lastTIDs); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "; "))
	}
	retries := uint64(0)
	for _, op := range slices.Concat(returned, reads) {
		retries = max(retries, rgwNativeRetryAttempt(op))
	}
	osdPeers := rgwNativeConnectedTo(peers(), "osd", osdAfter.Nonce)
	if len(osdPeers) == 0 {
		t.Fatal("no gateway Messenger connection to the restarted OSD")
	}
	if stale := rgwNativeConnectedTo(peers(), "osd", osdBefore.Nonce); len(stale) != 0 {
		t.Fatalf("gateway clients %v still connected to the stopped OSD process", stale)
	}
	// Independent native readback after recovery: both objects' bytes and
	// index keys, and the S3 view of the object written during the outage.
	rgwNativeOracle(t, ctx, control, dataPool, indexPool, head(keys[0]), indexObjects, keys[0], payloads[keys[0]])
	rgwNativeOracle(t, ctx, control, dataPool, indexPool, head(keys[1]), indexObjects, keys[1], payloads[keys[1]])
	if read := request(http.MethodGet, "/"+bucket+"/"+keys[1], nil, http.StatusOK); !bytes.Equal(read, payloads[keys[1]]) {
		t.Fatal("S3 readback of the object written during the OSD outage differs")
	}

	// Phase 6: stop the manager, put one object while it is down, restart it
	// and observe the gateway reconnect and report to the new process.
	mgrBefore, err := mgrDump(ctx)
	if err != nil || !mgrBefore.Available {
		t.Fatal("manager was not available before its fault:", err)
	}
	if connected := rgwNativeConnectedTo(peers(), "mgr", mgrBefore.Nonce); len(connected) == 0 {
		t.Fatal("no gateway Messenger connection to the active manager before its fault")
	}
	serviceBefore, err := rgwNativeServiceGIDs(rgwNativeExec(t, ctx, control, "ceph", "service", "dump", "--format", "json"))
	if err != nil || len(serviceBefore) != 1 || !slices.ContainsFunc(generation, func(c rgwNativeClientGeneration) bool {
		return c.Client == "client."+strconv.FormatUint(serviceBefore[0], 10)
	}) {
		t.Fatalf("service map rgw daemons %v are not the gateway %+v: %v", serviceBefore, generation, err)
	}
	mgrStarted, err := manager.State(ctx)
	if err != nil || mgrStarted == nil || !mgrStarted.Running {
		t.Fatal("manager container was not running before its fault", err)
	}
	if err := manager.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("stop owned manager once:", err)
	}
	if stopped, err := manager.State(ctx); err != nil || stopped == nil || stopped.Running {
		t.Fatal("manager stop was not observed", err)
	}
	status, _, err := s3.exchange(ctx, http.MethodPut, "/"+bucket+"/"+keys[2], payloads[keys[2]], rgwNativeDefaultResponseLimit)
	mgrOutcome := rgwNativeClassify("PUT "+keys[2], status, err, http.StatusOK)
	stillStopped, err := manager.State(ctx)
	if err != nil || stillStopped == nil || stillStopped.Running {
		t.Fatal("manager was not stopped for the whole PUT", err)
	}
	t.Logf("RGW_NATIVE_FAULT_OUTCOME mgr %+v", mgrOutcome)
	if mgrOutcome.Class != rgwNativeSucceeded {
		t.Fatalf("the PUT during the manager outage did not succeed: %+v native=%s", mgrOutcome, rgwNativeHeadPresence(ctx, control, dataPool, head(keys[2])))
	}
	if err := manager.Start(ctx); err != nil {
		t.Fatal("start same owned manager once:", err)
	}
	if restarted, err := manager.State(ctx); err != nil || restarted == nil || !restarted.Running || restarted.StartedAt == mgrStarted.StartedAt {
		t.Fatal("manager process generation did not change", err)
	}
	var mgrAfter rgwNativeDaemonState
	if err := rgwNativeWaitFor(ctx, 2*time.Minute, time.Second, "MON promoting the restarted manager", func(read context.Context) (bool, error) {
		state, err := mgrDump(read)
		if err != nil {
			return false, nil
		}
		mgrAfter = state
		return state.Available && state.GID != mgrBefore.GID && state.Nonce != mgrBefore.Nonce, nil
	}); err != nil {
		t.Fatal(err)
	}
	var mgrPeers []string
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "gateway Messenger connection to the restarted manager", func(context.Context) (bool, error) {
		mgrPeers = rgwNativeConnectedTo(peers(), "mgr", mgrAfter.Nonce)
		return len(mgrPeers) != 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	var stamps map[string]string
	if err := rgwNativeWaitFor(ctx, 90*time.Second, 2*time.Second, "gateway status report to the restarted manager", func(read context.Context) (bool, error) {
		output, err := rgwNativeTryExec(read, control, "ceph", "service", "status", "--format", "json")
		if err != nil {
			return false, nil
		}
		if stamps, err = rgwNativeServiceStamps(output); err != nil {
			return false, err
		}
		if len(stamps) != 1 {
			return false, nil
		}
		for _, stamp := range stamps {
			return rgwNativeReported(stamp), nil
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if mgrNow, err := mgrDump(ctx); err != nil || mgrNow.GID != mgrAfter.GID {
		t.Fatal("the manager that received the report is not the restarted one", err)
	}
	serviceAfter, err := rgwNativeServiceGIDs(rgwNativeExec(t, ctx, control, "ceph", "service", "dump", "--format", "json"))
	if err != nil || !slices.Equal(serviceAfter, serviceBefore) {
		t.Fatalf("service map rgw daemons changed across the manager restart: %v -> %v (%v)", serviceBefore, serviceAfter, err)
	}
	for _, key := range keys {
		rgwNativeOracle(t, ctx, control, dataPool, indexPool, head(key), indexObjects, key, payloads[key])
	}

	// Phase 7: delete every owned object once and require native absence.
	for _, key := range keys {
		request(http.MethodDelete, "/"+bucket+"/"+key, nil, http.StatusNoContent)
		request(http.MethodGet, "/"+bucket+"/"+key, nil, http.StatusNotFound)
	}
	inventory := rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", dataPool, "ls"))
	for _, key := range keys {
		if slices.Contains(inventory, head(key)) {
			t.Fatal("owned head survived successful S3 delete")
		}
	}
	for _, object := range indexObjects {
		for _, key := range keys {
			if slices.Contains(rgwNativeLines(rgwNativeExec(t, ctx, control, "rados", "-p", indexPool, "listomapkeys", object)), key) {
				t.Fatal("owned key survived in native bucket index")
			}
		}
	}
	request(http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
	if err := user.Remove(ctx); err != nil {
		t.Fatal("remove owned user:", err)
	}
	hashes := map[string]string{}
	for _, key := range keys {
		hashes[key] = rgwNativeHash(payloads[key])
	}
	receipt, err := json.Marshal(map[string]any{
		"image": rgwNativeImage, "source": "7f793731f1b39eb4f465e960113d2363c311b964", "mode": "secure", "key": "aes256k",
		"bytes": len(payloads[keys[0]]), "sha256": hashes, "gateway_generation": generation,
		"before_fault": map[string]any{"put_reception": beforeReceipt, "last_tids": lastTIDs},
		"osd": map[string]any{
			"before": osdBefore, "down": osdDown, "after": osdAfter, "process_restarted": true,
			"overlap_window_seconds": rgwNativeOverlapWindow.Seconds(),
			"pending_at_restart":     pendingAtRestart, "outcomes": []rgwNativeOutcome{putOutcome, getOutcome},
			"put_reception": returned, "get_reception": reads, "watch_reconnect": reconnects,
			"max_retry_attempt": retries, "gateway_connected": osdPeers,
		},
		"mgr": map[string]any{
			"before": mgrBefore, "after": mgrAfter, "process_restarted": true, "outcome": mgrOutcome,
			"gateway_connected": mgrPeers, "status_reported": true, "service_gids": serviceAfter,
		},
		"native_readback": "confirmed", "object_index_user_cleanup": "confirmed",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_NATIVE_BACKEND_FAULT %s", receipt)
}

// rgwNativeAttributedOps returns every reached_pg operation of the clients
// that accept selects, with the history joins of rgwNativeHistoryMatch.
func rgwNativeAttributedOps(t *testing.T, output []byte, clients []rgwNativeClientGeneration, accept func(int64, string, string, string) bool) []rgwNativeOperation {
	t.Helper()
	var received []rgwNativeOperation
	for _, client := range clients {
		operations, err := rgwNativeHistoryMatch(output, client.Client, accept)
		if err != nil {
			t.Fatal("parse gateway-attributed native receipt:", err)
		}
		received = append(received, operations...)
	}
	return received
}

// rgwNativeControlPoolID returns the numeric ID of the zone's control pool,
// whose notify objects the gateway watches.
func rgwNativeControlPoolID(t *testing.T, ctx context.Context, cluster *rgw.Container) int64 {
	t.Helper()
	pools, err := rgwNativePoolStates(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, pool := range pools {
		if strings.HasSuffix(pool.Name, ".rgw.control") && pool.ID > 0 {
			return pool.ID
		}
	}
	t.Fatal("missing actual RGW control pool")
	return 0
}

// rgwNativeHeadPresence describes, for a failure message only, whether a
// head object is present natively; it never changes state.
func rgwNativeHeadPresence(ctx context.Context, control testcontainers.Container, pool, object string) string {
	output, err := rgwNativeTryExec(ctx, control, "rados", "-p", pool, "stat", object)
	if err != nil {
		return "head absent or unreadable (" + err.Error() + ")"
	}
	return "head present: " + strings.TrimSpace(rgwNativeBoundedText(output))
}
