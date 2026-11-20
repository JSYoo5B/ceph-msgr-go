package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type namedMgrNativeMap struct {
	Epoch     uint32 `json:"epoch"`
	Name      string `json:"active_name"`
	GID       uint64 `json:"active_gid"`
	Available bool   `json:"available"`
}

type namedMgrNativeReceipt struct {
	Phase   string            `json:"phase"`
	Before  namedMgrNativeMap `json:"before"`
	After   namedMgrNativeMap `json:"after"`
	Version tellVersion       `json:"version"`
}

type namedMgrGoObservation struct {
	Action      string            `json:"action"`
	Target      string            `json:"target"`
	Result      cephmsgr.Result   `json:"result"`
	Error       string            `json:"error,omitempty"`
	Unknown     bool              `json:"outcome_unknown"`
	ClientID    uint64            `json:"client_id"`
	FSID        string            `json:"fsid"`
	Manager     namedMgrNativeMap `json:"manager"`
	Endpoint    string            `json:"endpoint"`
	AuthExpires time.Time         `json:"auth_expires"`
	MgrExpires  time.Time         `json:"mgr_expires"`
	Closed      bool              `json:"closed"`
}

func namedMgrNativeObserve(t *testing.T, ctx context.Context, control, phase string) namedMgrNativeReceipt {
	t.Helper()
	if err := testcluster.RequestControl(ctx, control, "observe-named-mgr", "named-mgr-observed", phase); err != nil {
		t.Fatal("native named MGR observation", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "named-mgr-oracle-"+phase+".json"))
	var receipt namedMgrNativeReceipt
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &receipt) != nil || receipt.Phase != phase || !receipt.Before.Available || !receipt.After.Available || receipt.Before.GID == 0 || receipt.Before.GID != receipt.After.GID || receipt.Before.Name != receipt.After.Name || receipt.Before.Epoch == 0 || receipt.After.Epoch < receipt.Before.Epoch || receipt.Version.Release != "tentacle" || !strings.HasPrefix(receipt.Version.Version, "20.2.") {
		t.Fatal("invalid independent native named MGR receipt", err, receipt)
	}
	return receipt
}

// MgrTellTo against the exact active manager name, checked against a fresh
// native `ceph tell mgr.<name>` bracketed by native MgrMap observations of one
// stable active identity. Covered natively: local refusal of absent, prefixed,
// wildcard and numeric names and of the standby; raw negative status and text
// (including an allow-all denial for a read-only identity) as CommandError;
// concurrent ordinary command, current tell and named tell on one session;
// ticket renewal with a stable GlobalID and binding; and, after one explicit
// mgr fail, local refusal of the old name and success with the new one.
// The race between a queued unstarted call and a binding change, and MON
// admission loss, are covered by synthetic peers only.
//
//scenario:covers CLIENT to MON/MGR
//fixture:isolated runs mgr fail on the named MGR; the active MGR is not restored
func TestCephNamedManagerTellIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prepareNativeManagerFailover(t, ctx, control)
	before := namedMgrNativeObserve(t, ctx, control, "before")
	c, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	var observations []namedMgrGoObservation
	record := func(action, target string, result cephmsgr.Result, err error, s cephmsgr.State) {
		item := namedMgrGoObservation{Action: action, Target: target, Result: result, ClientID: s.GlobalID, FSID: s.FSID, Manager: namedMgrNativeMap{s.Manager.MapEpoch, s.Manager.Name, s.Manager.GlobalID, s.Manager.Available}, Endpoint: s.Manager.Endpoint, AuthExpires: s.AuthTicket.Expires, MgrExpires: s.MgrTicket.Expires, Closed: s.Closed}
		var unknown *cephmsgr.OutcomeUnknownError
		item.Unknown = errors.As(err, &unknown)
		if err != nil {
			item.Error = err.Error()
		}
		observations = append(observations, item)
	}
	t.Cleanup(func() {
		data, err := json.MarshalIndent(observations, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "named-mgr-results.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve named MGR public results", err)
		}
	})
	state := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.MapEpoch >= before.After.Epoch && s.Manager.Name == before.After.Name && s.Manager.GlobalID == before.After.GID
	})
	name := state.Manager.Name
	record("initial-map", name, cephmsgr.Result{}, nil, state)
	version := cephmsgr.Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	check := func(t *testing.T, name string, want tellVersion) {
		t.Helper()
		result, err := c.MgrTellTo(ctx, name, version)
		record("named-version", name, result, err, c.Snapshot())
		var got tellVersion
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &got) != nil || got != want {
			t.Fatal("named MGR/native version mismatch", result, err, got, want)
		}
		s := c.Snapshot()
		_, role, err := testcluster.SplitPeer(s.Manager.Endpoint)
		if err != nil || role != "mgr."+name || s.Manager.Name != name || !s.Manager.Ready || s.GlobalID == 0 || s.AuthRejection != nil {
			t.Fatal("named MGR target/identity", s, err)
		}
	}
	t.Run("exact_active_native", func(t *testing.T) { check(t, name, before.Version) })
	t.Run("nonactive_names_refused_locally", func(t *testing.T) {
		for _, n := range []string{"__missing_named_mgr__", "mgr." + name, "*", "0"} {
			result, err := c.MgrTellTo(ctx, n, version)
			record("nonactive-name-refusal", n, result, err, c.Snapshot())
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.Is(err, cephmsgr.ErrManagerNotFound) || errors.As(err, &unknown) {
				t.Fatal(n, err)
			}
		}
		for _, standby := range state.Manager.Standbys {
			result, err := c.MgrTellTo(ctx, standby.Name, version)
			record("standby-refusal", standby.Name, result, err, c.Snapshot())
			if !errors.Is(err, cephmsgr.ErrManagerNotActive) {
				t.Fatal("standby substituted active", standby.Name, err)
			}
		}
	})
	t.Run("signed_unknown_command_and_readonly_denial", func(t *testing.T) {
		result, err := c.MgrTellTo(ctx, name, cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_tell__","format":"json"}`)})
		record("unknown-command", name, result, err, c.Snapshot())
		var server *cephmsgr.CommandError
		if result.Code != -22 || !errors.As(err, &server) || server.Message != result.Message {
			t.Fatal(result, err)
		}
		options := integrationOptions(t)
		key, err := os.ReadFile(filepath.Join(control, "readonly.key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
		if err != nil {
			t.Fatal(err)
		}
		options.Identity = "client.readonly"
		readonly, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer readonly.Close()
		result, err = readonly.MgrTellTo(ctx, name, version)
		record("readonly-command-denial", name, result, err, readonly.Snapshot())
		var auth *cephmsgr.AuthenticationError
		var unknown *cephmsgr.OutcomeUnknownError
		if result.Code != -13 || !errors.As(err, &server) || errors.As(err, &auth) || errors.As(err, &unknown) || server.Message != result.Message {
			t.Fatal(result, err)
		}
		if err := fixtureRecoveryRead(ctx, readonly, true); err != nil {
			t.Fatal("ordinary command after named denial", err)
		}
	})
	t.Run("mixed_shared_session_calls", func(t *testing.T) {
		var wg sync.WaitGroup
		type answer struct {
			result cephmsgr.Result
			err    error
			tell   bool
		}
		answers := make(chan answer, 3)
		for i, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MgrCommand, c.MgrTell, func(ctx context.Context, cmd cephmsgr.Command) (cephmsgr.Result, error) {
			return c.MgrTellTo(ctx, name, cmd)
		}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				command := version
				if i == 0 {
					command = cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
				}
				result, err := call(ctx, command)
				answers <- answer{result, err, i != 0}
			}()
		}
		wg.Wait()
		for range 3 {
			got := <-answers
			record("mixed-command-result", name, got.result, got.err, c.Snapshot())
			if got.err != nil || got.result.Code != 0 || !json.Valid(got.result.Data) {
				t.Fatal("mixed named/ordinary MGR result", got.result, got.err)
			}
			if got.tell {
				var v tellVersion
				if json.Unmarshal(got.result.Data, &v) != nil || v != before.Version {
					t.Fatal("mixed named tell/native result", v)
				}
			}
		}
	})
	t.Run("ticket_renewal_keeps_selected_daemon", func(t *testing.T) {
		baseline := c.Snapshot()
		record("before-ticket-renewal", name, cephmsgr.Result{}, nil, baseline)
		renewed := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.AuthTicket.Expires.After(baseline.AuthTicket.Expires) && s.MgrTicket.Expires.After(baseline.MgrTicket.Expires) && s.Monitor.Ready
		})
		record("after-ticket-renewal", name, cephmsgr.Result{}, nil, renewed)
		if renewed.GlobalID != baseline.GlobalID || renewed.Manager.Name != name || renewed.Manager.GlobalID != baseline.Manager.GlobalID {
			t.Fatal("ticket renewal changed selected identity", baseline, renewed)
		}
		check(t, name, before.Version)
	})
	t.Run("failover_requires_explicit_new_name", func(t *testing.T) {
		// Two further clients name the active manager across the failover,
		// one on each side of the transmission boundary.
		//
		// unsent: its dial to the named manager is held by the test until
		// its own MgrMap no longer shows that manager, and then fails. The
		// command was never written.
		//
		// sent: its padded command was written to the named manager and
		// everything received on that connection afterwards is withheld.
		isManager := func(endpoint string) bool {
			_, role, _ := testcluster.SplitPeer(endpoint)
			return role == "mgr."+name
		}
		var dialMu sync.Mutex
		var managerDials []string // manager roles dialed by the two clients
		observed := func(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
			return func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if _, role, _ := testcluster.SplitPeer(endpoint); strings.HasPrefix(role, "mgr.") {
					dialMu.Lock()
					managerDials = append(managerDials, role)
					dialMu.Unlock()
				}
				return dial(ctx, network, endpoint)
			}
		}
		errHeldDial := errors.New("named MGR dial held by the test across the failover")
		reached, release := make(chan struct{}), make(chan struct{})
		var reachedOnce sync.Once
		unsentOptions := integrationOptions(t)
		// The held dial must outlast the failover, not its setup bound.
		unsentOptions.ConnectTimeout = 2 * time.Minute
		plain := unsentOptions.DialContext
		unsentOptions.DialContext = observed(func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			if !isManager(endpoint) {
				return plain(ctx, network, endpoint)
			}
			reachedOnce.Do(func() { close(reached) })
			select {
			case <-release:
				return nil, &net.OpError{Op: "dial", Net: network, Err: errHeldDial}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		unsent, err := cephmsgr.Dial(ctx, unsentOptions)
		if err != nil {
			t.Fatal(err)
		}
		defer unsent.Close()
		hold := newCommandHold(4096, isManager)
		sentOptions := integrationOptions(t)
		sentOptions.DialContext = observed(hold.dialer(sentOptions.DialContext))
		sent, err := cephmsgr.Dial(ctx, sentOptions)
		if err != nil {
			t.Fatal(err)
		}
		defer sent.Close()
		if result, err := sent.MgrTellTo(ctx, name, version); err != nil || result.Code != 0 {
			t.Fatal("control named Tell of the sent client", result.Code, err)
		}
		if dials, large, _ := hold.counts(); dials != 1 || large != 0 {
			t.Fatal("a write before the subject command reached the padded size", dials, large)
		}
		type outcome struct {
			result cephmsgr.Result
			err    error
		}
		unsentDone, sentDone := make(chan outcome, 1), make(chan outcome, 1)
		go func() {
			result, err := unsent.MgrTellTo(ctx, name, version)
			unsentDone <- outcome{result, err}
		}()
		hold.armOnWrite()
		go func() {
			result, err := sent.MgrTellTo(ctx, name, cephmsgr.Command{JSON: append(append([]byte(nil), version.JSON...), strings.Repeat(" ", 40<<10)...)})
			sentDone <- outcome{result, err}
		}()
		for _, signal := range []struct {
			name string
			done <-chan struct{}
		}{{"the unsent client's dial to the named manager", reached}, {"the sent client's padded write", hold.bound},
			{"the return of that write", hold.wrote}, {"withheld received bytes after it", hold.received}} {
			select {
			case <-signal.done:
			case got := <-unsentDone:
				t.Fatal("the unsent named Tell ended before the failover", got.err)
			case got := <-sentDone:
				t.Fatal("the sent named Tell ended before the failover", got.err)
			case <-ctx.Done():
				t.Fatal("never observed "+signal.name, ctx.Err())
			}
		}
		mutation, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
		if result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: mutation}); err != nil || result.Code != 0 {
			t.Fatal("once-only MGR fail", result, err)
		}
		// sent: the client fails the named manager's session when its MgrMap
		// changes. The written command keeps an unknown outcome and names the
		// changed target.
		select {
		case got := <-sentDone:
			record("sent-across-failover", name, got.result, got.err, sent.Snapshot())
			if !unknownOutcome(got.result, got.err) || !errors.Is(got.err, cephmsgr.ErrManagerTargetChanged) || !errors.Is(got.err, cephmsgr.ErrManagerChanged) {
				t.Fatal("a sent named Tell across the failover lost its unknown outcome or the changed-target cause", got.err)
			}
		case <-ctx.Done():
			t.Fatal("the failover did not end the sent named Tell", ctx.Err())
		}
		// unsent: only after its own map changed does its dial fail. The
		// call is then refused locally with the changed target; the dial
		// failure itself is not the result.
		waitClientState(t, unsent, ctx, func(s cephmsgr.State) bool {
			return !s.Manager.Available || s.Manager.Name != name || s.Manager.GlobalID != before.After.GID
		})
		close(release)
		select {
		case got := <-unsentDone:
			record("unsent-across-failover", name, got.result, got.err, unsent.Snapshot())
			if !knownUnsent(got.result, got.err) || !errors.Is(got.err, cephmsgr.ErrManagerTargetChanged) || !errors.Is(got.err, cephmsgr.ErrManagerChanged) || errors.Is(got.err, errHeldDial) {
				t.Fatal("an unsent named Tell across the failover was not a known-unsent changed-target refusal", got.err)
			}
		case <-ctx.Done():
			t.Fatal("the failover did not end the unsent named Tell", ctx.Err())
		}
		// Neither call was moved to another manager or written again.
		dialMu.Lock()
		dialed := strings.Join(managerDials, ",")
		dialMu.Unlock()
		if _, large, _ := hold.counts(); dialed != "mgr."+name+",mgr."+name || large != 1 || hold.completeWrites() != 1 {
			t.Fatal("a named Tell across the failover dialed another manager, was written again or was not written in full", dialed, large, hold.completeWrites())
		}
		waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.Manager.Available && s.Manager.Name != name && s.Manager.GlobalID != before.After.GID
		})
		after := namedMgrNativeObserve(t, ctx, control, "after")
		if after.After.Name == name || after.After.GID == before.After.GID {
			t.Fatal("native map did not change named MGR", after)
		}
		waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
			return s.Manager.MapEpoch >= after.After.Epoch && s.Manager.Name == after.After.Name && s.Manager.GlobalID == after.After.GID
		})
		_, err = c.MgrTellTo(ctx, name, version)
		record("old-name-after-failover", name, cephmsgr.Result{}, err, c.Snapshot())
		var unknown *cephmsgr.OutcomeUnknownError
		if (!errors.Is(err, cephmsgr.ErrManagerNotFound) && !errors.Is(err, cephmsgr.ErrManagerNotActive)) || errors.As(err, &unknown) {
			t.Fatal("old named target substituted replacement", err)
		}
		check(t, after.After.Name, after.Version)
		if err := fixtureRecoveryRead(ctx, c, false); err != nil {
			t.Fatal(err)
		}
		if err := fixtureRecoveryRead(ctx, c, true); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("cancel_before_send_and_close", func(t *testing.T) {
		before, stop := context.WithCancel(ctx)
		stop()
		if _, err := c.MgrTellTo(before, name, version); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		record("client-closed", name, cephmsgr.Result{}, nil, c.Snapshot())
		if _, err := c.MgrTellTo(ctx, name, version); !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal(err)
		}
	})
}
