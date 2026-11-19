package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRGWNativeFaultClassifiesWithoutInferringFromAStatusAlone(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		err    error
		want   string
	}{
		"complete expected status":  {200, nil, rgwNativeSucceeded},
		"complete other status":     {503, nil, rgwNativeFailedStatus},
		"complete redirect":         {307, nil, rgwNativeFailedStatus},
		"no response":               {0, errors.New("dial"), rgwNativeUncertain},
		"expected status, cut body": {200, errors.New("read"), rgwNativeUncertain},
		"other status, cut body":    {500, errors.New("read"), rgwNativeUncertain},
	} {
		t.Run(name, func(t *testing.T) {
			outcome := rgwNativeClassify("PUT key", tc.status, tc.err, http.StatusOK)
			if outcome.Class != tc.want || outcome.Request != "PUT key" {
				t.Fatalf("outcome %+v, want %s", outcome, tc.want)
			}
			if (outcome.Class == rgwNativeUncertain) != (outcome.Detail != "") || (outcome.Class != rgwNativeUncertain && outcome.Status != tc.status) {
				t.Fatalf("outcome fields %+v", outcome)
			}
		})
	}
}

func TestRGWNativeFaultInflightRequestIsIssuedOnceAndNeverReplayed(t *testing.T) {
	release := make(chan struct{})
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("PRIVATE-S3-BODY"))
	}))
	defer server.Close()
	s3 := rgwNativeS3{endpoint: server.URL, access: "a", secret: "s", region: "r", http: &http.Client{Timeout: 5 * time.Second}}
	inflight := rgwNativeIssue(context.Background(), s3, "PUT k", http.MethodPut, "/b/k", []byte("x"), http.StatusOK)
	if !inflight.pending() {
		t.Fatal("request returned before the server answered")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if outcome := inflight.outcome(short); outcome.Class != rgwNativeUncertain {
		t.Fatalf("an unfinished local wait was classified %+v", outcome)
	}
	close(release)
	outcome := inflight.outcome(context.Background())
	if outcome.Class != rgwNativeFailedStatus || outcome.Status != http.StatusServiceUnavailable || strings.Contains(outcome.Detail, "PRIVATE") || calls != 1 {
		t.Fatalf("outcome %+v calls %d", outcome, calls)
	}
}

func TestRGWNativeFaultMapParsers(t *testing.T) {
	osd := `{"epoch":24,"osds":[{"osd":0,"up":1,"up_from":24,"public_addrs":{"addrvec":[{"type":"v2","addr":"10.0.0.1:6800","nonce":3717287850},{"type":"v1","addr":"10.0.0.1:6801","nonce":3717287850}]}}]}`
	state, err := rgwNativeOSDState([]byte(osd), 0)
	if err != nil || state != (rgwNativeDaemonState{Epoch: 24, Up: true, UpFrom: 24, Nonce: 3717287850}) {
		t.Fatalf("OSD state %+v %v", state, err)
	}
	down := strings.Replace(osd, `"up":1`, `"up":0`, 1)
	if state, err := rgwNativeOSDState([]byte(down), 0); err != nil || state.Up {
		t.Fatalf("down OSD state %+v %v", state, err)
	}
	for name, raw := range map[string]string{
		"other OSD":      strings.Replace(osd, `"osd":0`, `"osd":1`, 1),
		"no epoch":       strings.Replace(osd, `"epoch":24,`, ``, 1),
		"no up":          strings.Replace(osd, `"up":1,`, ``, 1),
		"odd up":         strings.Replace(osd, `"up":1`, `"up":2`, 1),
		"mixed nonces":   strings.Replace(osd, `"nonce":3717287850}]`, `"nonce":1}]`, 1),
		"zero nonce":     strings.ReplaceAll(osd, `3717287850`, `0`),
		"no addresses":   strings.Replace(osd, `"public_addrs":{"addrvec":[{"type":"v2","addr":"10.0.0.1:6800","nonce":3717287850},{"type":"v1","addr":"10.0.0.1:6801","nonce":3717287850}]}`, `"public_addrs":{"addrvec":[]}`, 1),
		"not JSON":       `{"epoch":`,
		"missing nonces": strings.ReplaceAll(osd, `,"nonce":3717287850`, ``),
	} {
		if _, err := rgwNativeOSDState([]byte(raw), 0); err == nil {
			t.Errorf("%s: admitted", name)
		}
	}
	mgr := `{"epoch":6,"active_gid":4337,"active_name":"a","available":true,"active_addrs":{"addrvec":[{"type":"v2","addr":"10.0.0.2:6800","nonce":303808895}]}}`
	if state, err := rgwNativeMgrState([]byte(mgr)); err != nil || state != (rgwNativeDaemonState{Epoch: 6, GID: 4337, Nonce: 303808895, Available: true}) {
		t.Fatalf("manager state %+v %v", state, err)
	}
	unavailable := `{"epoch":5,"active_gid":0,"available":false,"active_addrs":{"addrvec":[]}}`
	if state, err := rgwNativeMgrState([]byte(unavailable)); err != nil || state.Available {
		t.Fatalf("unavailable manager %+v %v", state, err)
	}
	for name, raw := range map[string]string{
		"no availability": strings.Replace(mgr, `"available":true,`, ``, 1),
		"no GID":          strings.Replace(mgr, `"active_gid":4337`, `"active_gid":0`, 1),
		"no nonce":        strings.Replace(mgr, `,"nonce":303808895`, ``, 1),
	} {
		if _, err := rgwNativeMgrState([]byte(raw)); err == nil {
			t.Errorf("%s: admitted", name)
		}
	}
	service := `{"epoch":9,"services":{"rgw":{"daemons":{"summary":"","4147":{"start_epoch":3,"gid":4147,"metadata":{}}}}}}`
	if gids, err := rgwNativeServiceGIDs([]byte(service)); err != nil || !slices.Equal(gids, []uint64{4147}) {
		t.Fatalf("service GIDs %v %v", gids, err)
	}
	if _, err := rgwNativeServiceGIDs([]byte(strings.Replace(service, `"gid":4147`, `"gid":0`, 1))); err == nil {
		t.Fatal("service daemon without a GID admitted")
	}
	status := `{"rgw":{"4147":{"status_stamp":"2026-11-19T17:25:01.123456+0000","last_beacon":"2026-11-19T17:25:01.123456+0000","status":{}}}}`
	stamps, err := rgwNativeServiceStamps([]byte(status))
	if err != nil || len(stamps) != 1 || !rgwNativeReported(stamps["4147"]) {
		t.Fatalf("service status %v %v", stamps, err)
	}
	// A restarted manager's daemon state starts with a zero stamp; a beacon
	// it assumed on its own is no report.
	zero := strings.Replace(status, `"status_stamp":"2026-11-19T17:25:01.123456+0000"`, `"status_stamp":"0.000000"`, 1)
	if stamps, err := rgwNativeServiceStamps([]byte(zero)); err != nil || rgwNativeReported(stamps["4147"]) {
		t.Fatalf("zero stamp read as a report: %v %v", stamps, err)
	}
	if _, err := rgwNativeServiceStamps([]byte(`{"rgw":{"4147":{"last_beacon":"x"}}}`)); err == nil {
		t.Fatal("status without a stamp admitted")
	}
}

func TestRGWNativeFaultPeersRequireAnEstablishedConnectionToTheNonce(t *testing.T) {
	peers := []rgwNativePeer{
		{Client: "client.2", Type: "osd", ID: 0, Nonces: []uint64{7}, State: "STATE_CONNECTION_ESTABLISHED", Connected: true},
		{Client: "client.1", Type: "osd", ID: 0, Nonces: []uint64{7}, State: "STATE_CONNECTION_ESTABLISHED", Connected: true},
		{Client: "client.3", Type: "osd", ID: 0, Nonces: []uint64{7}, State: "STATE_CONNECTING_RE", Connected: false},
		{Client: "client.4", Type: "osd", ID: 0, Nonces: []uint64{7}, State: "STATE_CONNECTION_ESTABLISHED", Connected: false},
		{Client: "client.5", Type: "mgr", ID: 9, Nonces: []uint64{7}, State: "STATE_CONNECTION_ESTABLISHED", Connected: true},
		{Client: "client.6", Type: "osd", ID: 0, Nonces: []uint64{8}, State: "STATE_CONNECTION_ESTABLISHED", Connected: true},
		{Client: "client.1", Type: "osd", ID: 0, Nonces: []uint64{7}, State: "STATE_CONNECTION_ESTABLISHED", Connected: true},
	}
	if got := rgwNativeConnectedTo(peers, "osd", 7); !slices.Equal(got, []string{"client.1", "client.2"}) {
		t.Fatalf("connected clients %v", got)
	}
	if got := rgwNativeConnectedTo(peers, "osd", 9); len(got) != 0 {
		t.Fatalf("a nonce no connection carries matched %v", got)
	}
	raw, err := json.Marshal(map[string]any{"socket_candidates": 1, "peers": peers})
	if err != nil {
		t.Fatal(err)
	}
	if parsed, err := rgwNativeParsePeers(raw); err != nil || len(parsed) != len(peers) {
		t.Fatalf("parsed %d peers: %v", len(parsed), err)
	}
	for name, raw := range map[string]string{
		"no sockets":     `{"socket_candidates":0,"peers":[]}`,
		"no peer list":   `{"socket_candidates":1}`,
		"bad client":     `{"socket_candidates":1,"peers":[{"client":"client.0","type":"osd"}]}`,
		"no peer type":   `{"socket_candidates":1,"peers":[{"client":"client.1","type":""}]}`,
		"too many socks": `{"socket_candidates":9,"peers":[]}`,
	} {
		if _, err := rgwNativeParsePeers([]byte(raw)); err == nil {
			t.Errorf("%s: admitted", name)
		}
	}
}

func TestRGWNativeFaultPeerInspectorReadsOnlyOwnedConnectionsWithoutAddresses(t *testing.T) {
	const setup = `
import glob,os,io,builtins,subprocess,json,types
glob.glob=lambda _: ['/proc/1/fd/3']
os.readlink=lambda _: 'socket:[42]'
builtins.open=lambda _: io.StringIO('0: 2 0 10000 1 1 42 /var/run/ceph/owned.asok\n0: 2 0 10000 1 1 43 /var/run/ceph/unowned.asok\n')
def native(argv,**kwargs):
    assert argv[:3]==['ceph','--admin-daemon','/var/run/ceph/owned.asok'] and argv[3:5]==['messenger','dump']
    if len(argv)==5:
        value={'status':{'messengers':['radosclient-12345','unrelated']}}
    else:
        assert argv[5:]==['radosclient-12345']
        connection={'async_connection':{'state':'STATE_CONNECTION_ESTABLISHED','status':{'connected':True},
            'peer':{'type':'osd','id':0,'global_id':0,'addr':{'addrvec':[{'type':'v2','addr':'10.0.0.9:6800','nonce':77}]}}},
            'addrvec':[{'type':'v2','addr':'10.0.0.9:6800','nonce':77}]}
        value={'status':{'messenger':{'my_name':{'type':'client','num':42},'nonce':12345,'connections':[connection]}}}
    return types.SimpleNamespace(returncode=0,stdout=json.dumps(value).encode())
subprocess.run=native
`
	output, err := exec.Command("python3", "-c", setup+rgwNativeGatewayPeerInspector).CombinedOutput()
	if err != nil {
		t.Fatalf("gateway peer inspector: %v: %s", err, output)
	}
	if strings.Contains(string(output), "10.0.0.9") {
		t.Fatalf("inspector printed a peer address: %s", output)
	}
	peers, err := rgwNativeParsePeers(output)
	if err != nil || len(peers) != 1 || peers[0].Client != "client.42" || peers[0].Type != "osd" || !slices.Equal(peers[0].Nonces, []uint64{77}) || !peers[0].Connected {
		t.Fatalf("inspector result %s: %v", output, err)
	}
}

func TestRGWNativeFaultReceptionPredicates(t *testing.T) {
	op := func(description string) rgwNativeOperation { return rgwNativeOperation{Description: description} }
	for description, want := range map[string]bool{
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [watch reconnect cookie 187651373122560 gen 1] snapc 0=[] ondisk+write e24)": true,
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [watch watch cookie 187651373122560] snapc 0=[] ondisk+write e24)":           false,
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [watch ping cookie 187651373122560 gen 2] snapc 0=[] ondisk+write e24)":      false,
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [watch reconnect cookie 1 gen 0] snapc 0=[] ondisk+write e24)":               false,
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [call x.watch reconnect cookie 1 gen 1] snapc 0=[] e24)":                     false,
		"osd_op(client.1.0:815 4.0 4:f95f44c2:::notify.0:head [stat,watch reconnect cookie 9 gen 3] snapc 0=[] e24)":                       true,
	} {
		if got := rgwNativeWatchReconnect(op(description)); got != want {
			t.Errorf("%s: reconnect %t, want %t", description, got, want)
		}
	}
	if got := rgwNativeRetryAttempt(op("osd_op(client.1.0:9 7.0 7:ab:::h:head [read 0~1] snapc 0=[] RETRY=2 ondisk+read e24)")); got != 2 {
		t.Fatalf("retry attempt %d", got)
	}
	if got := rgwNativeRetryAttempt(op("osd_op(client.1.0:9 7.0 7:ab:::h:head [read 0~1] snapc 0=[] ondisk+read e24)")); got != 0 {
		t.Fatalf("first attempt read as retry %d", got)
	}
	ops := []rgwNativeOperation{
		{Client: "client.1", TID: 812, MapEpoch: 24},
		{Client: "client.2", TID: 5, MapEpoch: 25},
	}
	before := map[string]uint64{"client.1": 811}
	if problems := rgwNativeAfterReturnProblems(ops, 24, before); len(problems) != 0 {
		t.Fatal(problems)
	}
	stale := append(slices.Clone(ops), rgwNativeOperation{Client: "client.1", TID: 811, MapEpoch: 24}, rgwNativeOperation{Client: "client.2", TID: 6, MapEpoch: 23})
	if problems := rgwNativeAfterReturnProblems(stale, 24, before); len(problems) != 2 {
		t.Fatalf("stale reception problems %v", problems)
	}
	if got := rgwNativeMaxTIDs([]rgwNativeOperation{{Client: "client.1", TID: 3}, {Client: "client.1", TID: 9}, {Client: "client.2", TID: 4}}); got["client.1"] != 9 || got["client.2"] != 4 {
		t.Fatalf("max TIDs %v", got)
	}
}

func TestRGWNativeFaultWaitObservesWithoutRepeating(t *testing.T) {
	calls := 0
	err := rgwNativeWaitFor(context.Background(), time.Second, time.Millisecond, "ready", func(context.Context) (bool, error) {
		calls++
		return calls == 3, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("wait: calls=%d err=%v", calls, err)
	}
	if err := rgwNativeWaitFor(context.Background(), 20*time.Millisecond, time.Millisecond, "never", func(context.Context) (bool, error) { return false, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded wait: %v", err)
	}
	stop := errors.New("invalid observation")
	if err := rgwNativeWaitFor(context.Background(), time.Second, time.Millisecond, "broken", func(context.Context) (bool, error) { return false, stop }); !errors.Is(err, stop) {
		t.Fatalf("observation error: %v", err)
	}
}

func TestRGWNativeFaultPayloadsAreDistinctAndMatchTheGate(t *testing.T) {
	// tools/rgw_native_gate.py FAULT_OBJECTS recomputes these hashes.
	want := map[[2]int]string{
		{13, 7}:  "f5ae8589d38de5ff5e7d3214338b062b60e477067a16e3efdf26ae28cbfa3eab",
		{29, 11}: "6b267aa088720540c41f4522c44b9d19f81010617523fc1ea9795d47c6e59e4f",
		{31, 3}:  "74e3da5fa4fec0ecffac43d0b0416a09b32dc89bf5d510a76c5bb5e46f2ddf54",
	}
	for args, hash := range want {
		if payload := rgwNativeFaultPayload(args[0], args[1]); len(payload) != 1031 || rgwNativeHash(payload) != hash {
			t.Errorf("payload %v: len=%d sha256=%s", args, len(payload), rgwNativeHash(payload))
		}
	}
}
