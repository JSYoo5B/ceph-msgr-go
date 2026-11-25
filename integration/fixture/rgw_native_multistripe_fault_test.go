package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// TestRGWNativeMultiStripeFaultIntegration holds the final MiB of one signed
// twelve-MiB PUT until an owned OSD has received a gateway stripe WRITE. It stops
// that OSD, releases the body while it is down, and recovers without replay.
// The same owned fixture then loses its active MGR to its existing standby.
// Native manifest reconstruction, index readback and explicit GC prove the
// postconditions independently of the HTTP outcome and daemon identities.
// Multipart and multisite behavior are outside this checkpoint.
//
//scenario:row rgw.aes256k.ip4.container.t-rgwmultistripefault key=aes256k service=aes256k family=4 stress= rgw_native=1 managers=2
//scenario:covers Service-induced paths
//fixture:isolated stops an owned OSD and active MGR and processes its own GC queue
func TestRGWNativeMultiStripeFaultIntegration(t *testing.T) {
	rgwNativeMultiStripeScenario(t, true)
}

// rgwNativePausedBody gates the body, not the signed request or server reply.
// Cancellation releases a blocked Read, including when an earlier assertion
// fails. A positive prefix is sent exactly once before the hold.
type rgwNativePausedBody struct {
	io.ReadCloser
	ctx       context.Context
	remaining int
	held      chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (b *rgwNativePausedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		b.once.Do(func() { close(b.held) })
		select {
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		case <-b.release:
		}
	} else if len(p) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	if b.remaining > 0 {
		b.remaining -= n
	}
	return n, err
}

type rgwNativeUploadTransport struct {
	base          http.RoundTripper
	held, release chan struct{}
}

func (r rgwNativeUploadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Body = &rgwNativePausedBody{ReadCloser: req.Body, ctx: req.Context(), remaining: 11 << 20, held: r.held, release: r.release}
	req.GetBody = nil
	return r.base.RoundTrip(req)
}

func rgwNativeInterruptStripe(t *testing.T, ctx context.Context, cluster *rgw.Container, gateway *rgw.Gateway, s3 rgwNativeS3, path string, payload []byte, marker string, dataPoolID int64, generation []rgwNativeClientGeneration) ([]byte, map[string]any) {
	t.Helper()
	control := cluster.Container.Container
	osd := rgwNativeOSDs(t, ctx, cluster)[0]
	dump := func(read context.Context) (rgwNativeDaemonState, error) {
		raw, err := rgwNativeTryExec(read, control, "ceph", "osd", "dump", "--format", "json")
		if err != nil {
			return rgwNativeDaemonState{}, err
		}
		return rgwNativeOSDState(raw, osd.ID)
	}
	before, err := dump(ctx)
	if err != nil || !before.Up {
		t.Fatal("OSD not up before upload", err)
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	held, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	defer resume()
	uploadCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	s3.http = &http.Client{Transport: rgwNativeUploadTransport{transport, held, release}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	put := rgwNativeIssue(uploadCtx, s3, "PUT multistripe", http.MethodPut, path, payload, http.StatusOK)
	select {
	case <-held:
	case <-uploadCtx.Done():
		t.Fatal("upload did not reach held body", uploadCtx.Err())
	}
	var writes []rgwNativeOperation
	history, err := rgwNativePollHistory(uploadCtx, func(read context.Context) []byte {
		return rgwNativeExec(t, read, control, "ceph", "tell", "osd."+strconv.Itoa(osd.ID), "dump_historic_ops")
	}, func(raw []byte) (bool, error) {
		writes = nil
		for _, client := range generation {
			ops, err := rgwNativeHistoryMatch(raw, client.Client, func(pool int64, namespace, locator, name string) bool {
				return pool == dataPoolID && namespace == "" && locator == "" && strings.HasPrefix(name, marker+"__shadow_")
			})
			if err != nil {
				return false, err
			}
			for _, op := range ops {
				if rgwNativeWrites(op) {
					writes = append(writes, op)
				}
			}
		}
		return len(writes) > 0, nil
	})
	if err != nil {
		t.Fatal("gateway stripe WRITE before OSD fault", err)
	}
	if !put.pending() {
		t.Fatal("upload returned before fault")
	}
	stopTimeout := 30 * time.Second
	if err := osd.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	if state, err := osd.State(ctx); err != nil || state == nil || state.Running {
		t.Fatal("owned OSD did not stop", err)
	}
	var down, after rgwNativeDaemonState
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "OSD down during held upload", func(read context.Context) (bool, error) {
		state, err := dump(read)
		down = state
		return err == nil && !state.Up && state.Epoch > before.Epoch, nil
	}); err != nil {
		t.Fatal(err)
	}
	resume()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(rgwNativeOverlapWindow):
	}
	if !put.pending() {
		t.Fatal("released upload returned while OSD remained down")
	}
	if state, err := dump(ctx); err != nil || state.Up {
		t.Fatal("OSD did not remain down", err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := rgwNativeWaitFor(ctx, 3*time.Minute, time.Second, "OSD recovery during upload", func(read context.Context) (bool, error) {
		state, err := dump(read)
		after = state
		return err == nil && state.Up && state.UpFrom > down.Epoch && state.Nonce != before.Nonce, nil
	}); err != nil {
		t.Fatal(err)
	}
	outcome := put.outcome(uploadCtx)
	t.Logf("RGW_NATIVE_FAULT_OUTCOME multistripe %+v", outcome)
	if outcome.Class != rgwNativeSucceeded {
		t.Fatalf("single upload did not complete: %+v", outcome)
	}
	if !slices.Equal(generation, rgwNativeGeneration(t, ctx, gateway)) {
		t.Fatal("gateway process changed during upload")
	}
	return history, map[string]any{"before": before, "down": down, "after": after, "prefix_bytes": 11 << 20, "pre_fault_writes": writes, "pending_at_restart": true, "overlap_window_seconds": int(rgwNativeOverlapWindow / time.Second), "outcome": outcome}
}

// Keep the first process's reception evidence: its ring disappears on restart.
// These snapshots straddle one stop, so each operation belongs to one ring.
// Retries with the same client/TID are retained once, using the later receipt.
func rgwNativeJoinHistory(t *testing.T, before, after []byte) []byte {
	t.Helper()
	var joined []json.RawMessage
	positions := map[string]int{}
	for _, raw := range [][]byte{before, after} {
		var snapshot struct {
			Ops []json.RawMessage `json:"ops"`
		}
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		for _, op := range snapshot.Ops {
			var identity struct {
				Data struct {
					Client struct {
						Client string `json:"client"`
						TID    uint64 `json:"tid"`
					} `json:"client_info"`
				} `json:"type_data"`
			}
			if err := json.Unmarshal(op, &identity); err != nil {
				t.Fatal(err)
			}
			key := identity.Data.Client.Client + ":" + strconv.FormatUint(identity.Data.Client.TID, 10)
			if identity.Data.Client.TID == 0 {
				continue
			}
			if at, ok := positions[key]; ok {
				joined[at] = op
			} else {
				positions[key] = len(joined)
				joined = append(joined, op)
			}
		}
	}
	out, err := json.Marshal(map[string]any{"ops": joined})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type rgwNativeManagerIdentity struct {
	Epoch     uint64 `json:"epoch"`
	Name      string `json:"active_name"`
	GID       uint64 `json:"active_gid"`
	Available bool   `json:"available"`
	Addrs     struct {
		Addrvec []struct {
			Addr  string `json:"addr"`
			Nonce uint64 `json:"nonce"`
		} `json:"addrvec"`
	} `json:"active_addrs"`
	Standbys []struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	} `json:"standbys"`
}

func rgwNativeManagerMap(raw []byte) (rgwNativeManagerIdentity, error) {
	var m rgwNativeManagerIdentity
	if len(raw) > 4<<20 || json.Unmarshal(raw, &m) != nil || !m.Available || m.Epoch == 0 || m.Name == "" || m.GID == 0 || len(m.Addrs.Addrvec) == 0 || len(m.Addrs.Addrvec) > 4 || len(m.Standbys) > 1 {
		return m, errors.New("invalid available manager map")
	}
	for _, addr := range m.Addrs.Addrvec {
		if addr.Addr == "" || addr.Nonce == 0 || addr.Nonce != m.Addrs.Addrvec[0].Nonce {
			return m, errors.New("manager without endpoint identity")
		}
	}
	return m, nil
}
func rgwNativeStandbyTakeover(t *testing.T, ctx context.Context, cluster *rgw.Container, gateway *rgw.Gateway, generation []rgwNativeClientGeneration) map[string]any {
	t.Helper()
	control := cluster.Container.Container
	dump := func(read context.Context) (rgwNativeManagerIdentity, error) {
		raw, err := rgwNativeTryExec(read, control, "ceph", "mgr", "dump", "--format", "json")
		if err != nil {
			return rgwNativeManagerIdentity{}, err
		}
		return rgwNativeManagerMap(raw)
	}
	var before, after rgwNativeManagerIdentity
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "existing standby manager", func(read context.Context) (bool, error) {
		state, err := dump(read)
		before = state
		return err == nil && len(state.Standbys) == 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	var beforePeers []string
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "gateway connection to original active", func(read context.Context) (bool, error) {
		raw, err := rgwNativeTryExec(read, gateway, "python3", "-c", rgwNativeGatewayPeerInspector)
		if err != nil {
			return false, nil
		}
		parsed, err := rgwNativeParsePeers(raw)
		if err != nil {
			return false, err
		}
		beforePeers = rgwNativeConnectedTo(parsed, "mgr", before.Addrs.Addrvec[0].Nonce)
		return len(beforePeers) > 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	managers, err := cluster.Managers(ctx)
	if err != nil || len(managers) != 2 {
		t.Fatal("need exactly two owned managers", err)
	}
	var active *ceph.ManagerContainer
	for _, manager := range managers {
		if manager.DaemonName == before.Name {
			active = manager
		}
	}
	if active == nil {
		t.Fatal("active manager is not owned")
	}
	stopTimeout := 30 * time.Second
	if err := active.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	if err := rgwNativeWaitFor(ctx, 2*time.Minute, time.Second, "existing standby promotion", func(read context.Context) (bool, error) {
		state, err := dump(read)
		after = state
		return err == nil && state.Epoch > before.Epoch && state.Name == before.Standbys[0].Name && state.GID == before.Standbys[0].GID && state.GID != before.GID && state.Addrs.Addrvec[0].Addr != before.Addrs.Addrvec[0].Addr && state.Addrs.Addrvec[0].Nonce != before.Addrs.Addrvec[0].Nonce, nil
	}); err != nil {
		t.Fatal(err)
	}
	var peers []string
	if err := rgwNativeWaitFor(ctx, 90*time.Second, time.Second, "gateway connection to promoted standby", func(read context.Context) (bool, error) {
		raw, err := rgwNativeTryExec(read, gateway, "python3", "-c", rgwNativeGatewayPeerInspector)
		if err != nil {
			return false, nil
		}
		parsed, err := rgwNativeParsePeers(raw)
		if err != nil {
			return false, err
		}
		peers = rgwNativeConnectedTo(parsed, "mgr", after.Addrs.Addrvec[0].Nonce)
		return len(peers) > 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if state, err := active.State(ctx); err != nil || state == nil || state.Running {
		t.Fatal("former active did not stay stopped", err)
	}
	if !slices.Equal(generation, rgwNativeGeneration(t, ctx, gateway)) {
		t.Fatal("gateway process changed during manager takeover")
	}
	return map[string]any{"before": before, "after": after, "gateway_connected_before": beforePeers, "gateway_connected": peers, "former_active_stopped": true}
}
