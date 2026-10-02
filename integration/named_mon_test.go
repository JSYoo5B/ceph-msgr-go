package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

type namedMonProbeKey struct{}

type namedMonStatus struct {
	Name string `json:"name"`
	Rank uint32 `json:"rank"`
	Map  struct {
		FSID  string `json:"fsid"`
		Epoch uint32 `json:"epoch"`
	} `json:"monmap"`
}

func TestCephNamedMonitorTellIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	data, err := os.ReadFile(filepath.Join(control, "named-mon-b.json"))
	var oracle namedMonStatus
	if err != nil || len(data) > 64<<10 || json.Unmarshal(data, &oracle) != nil || oracle.Name != "b" || oracle.Rank != 1 || oracle.Map.FSID == "" || oracle.Map.Epoch == 0 {
		t.Fatal("independent native mon.b status oracle", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var mu sync.Mutex
	var namedPorts []string
	var blockB bool
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			return nil, err
		}
		marked := ctx.Value(namedMonProbeKey{}) != nil
		mu.Lock()
		if marked {
			namedPorts = append(namedPorts, port)
		}
		blocked := marked && blockB && port == "33301"
		mu.Unlock()
		if blocked {
			return nil, &net.OpError{Op: "dial", Net: network, Err: io.EOF}
		}
		return dial(ctx, network, endpoint)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal("prepare primary MGR", err)
	}
	initial := c.Snapshot()
	if !initial.Monitor.Ready || !initial.Manager.Ready || initial.FSID != oracle.Map.FSID || !strings.HasSuffix(initial.Monitor.Endpoint, ":33300") {
		t.Fatal("primary sessions did not begin at MON a", initial)
	}
	checkPrimary := func() {
		t.Helper()
		state := c.Snapshot()
		if state.FSID != initial.FSID || state.GlobalID != initial.GlobalID || state.AuthRejection != nil || state.Closed || !state.Monitor.Ready || !state.Manager.Ready || state.Monitor.Endpoint != initial.Monitor.Endpoint || state.Manager.Endpoint != initial.Manager.Endpoint || state.Manager.GlobalID != initial.Manager.GlobalID || state.Manager.Name != initial.Manager.Name {
			t.Fatal("named MON changed primary authentication, endpoints or MGR", state)
		}
	}
	stream, err := c.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	probe := context.WithValue(ctx, namedMonProbeKey{}, true)
	command := cephmsgr.Command{JSON: []byte(`{"prefix":"mon_status","format":"json"}`)}
	for _, target := range []struct {
		name string
		rank uint32
	}{{"b", 1}, {"c", 2}, {"a", 0}} {
		result, err := c.MonTellTo(probe, target.name, command)
		var status namedMonStatus
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &status) != nil || status.Name != target.name || status.Rank != target.rank || status.Map.FSID != oracle.Map.FSID || status.Map.Epoch < oracle.Map.Epoch {
			t.Fatal("named MON status disagrees with native fixture identity", target.name, result.Code, err, status)
		}
		checkPrimary()
	}
	mu.Lock()
	ports := append([]string(nil), namedPorts...)
	mu.Unlock()
	if strings.Join(ports, ",") != "33301,33302,33300" {
		t.Fatal("named operations contacted the wrong MON", ports)
	}
	for _, name := range []string{"absent", "mon.b", "1", "*", "B"} {
		_, err := c.MonTellTo(probe, name, command)
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, cephmsgr.ErrMonitorNotFound) || errors.As(err, &unknown) {
			t.Fatal("unknown name lost known non-execution", name, err)
		}
	}
	mu.Lock()
	if len(namedPorts) != len(ports) {
		mu.Unlock()
		t.Fatal("unknown name opened a target connection")
	}
	blockB = true
	mu.Unlock()
	_, err = c.MonTellTo(probe, "b", command)
	var unknown *cephmsgr.OutcomeUnknownError
	if err == nil || errors.As(err, &unknown) {
		t.Fatal("unavailable named TCP target lost known non-execution", err)
	}
	mu.Lock()
	blockB = false
	blockedPorts := append([]string(nil), namedPorts[len(ports):]...)
	mu.Unlock()
	if strings.Join(blockedPorts, ",") != "33301" {
		t.Fatal("unavailable target fell back to another MON", blockedPorts)
	}
	checkPrimary()
	result, err := c.MonTellTo(probe, "b", cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_named_tell__","format":"json"}`)})
	var server *cephmsgr.CommandError
	var authentication *cephmsgr.AuthenticationError
	if !errors.As(err, &server) || errors.As(err, &unknown) || errors.As(err, &authentication) || result.Code != -22 || result.Message == "" || server.Code != result.Code || server.Message != result.Message {
		t.Fatal("named daemon rejection lost raw server result", result.Code, err)
	}
	type response struct {
		name   string
		result cephmsgr.Result
		err    error
	}
	responses := make(chan response, 4)
	for _, name := range []string{"b", "c"} {
		go func() {
			result, err := c.MonTellTo(ctx, name, command)
			responses <- response{name, result, err}
		}()
	}
	go func() {
		result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		responses <- response{"mon", result, err}
	}()
	go func() {
		result, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)})
		responses <- response{"mgr", result, err}
	}()
	for range cap(responses) {
		select {
		case got := <-responses:
			if got.err != nil || got.result.Code != 0 || !json.Valid(got.result.Data) {
				t.Fatal("concurrent named and primary commands", got.name, got.result.Code, got.err)
			}
			if got.name == "b" || got.name == "c" {
				var status namedMonStatus
				if json.Unmarshal(got.result.Data, &status) != nil || status.Name != got.name || status.Map.FSID != initial.FSID {
					t.Fatal("concurrent named command lost target identity", got.name, status)
				}
			}
		case <-ctx.Done():
			t.Fatal("concurrent named and primary commands did not finish", ctx.Err())
		}
	}
	checkPrimary()
	readonlyOptions := integrationOptions(t)
	key, err := os.ReadFile(filepath.Join(control, "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Identity = "client.readonly"
	readonly, err := cephmsgr.Dial(ctx, readonlyOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { readonly.Close() })
	readonlyID := readonly.Snapshot().GlobalID
	result, err = readonly.MonTellTo(ctx, "b", command)
	if !errors.As(err, &server) || errors.As(err, &unknown) || errors.As(err, &authentication) || result.Code != -13 || result.Message == "" || server.Code != result.Code || server.Message != result.Message || readonly.Snapshot().GlobalID != readonlyID || readonly.Snapshot().AuthRejection != nil {
		t.Fatal("named Tell permission denial changed authentication or hid server result", result.Code, err)
	}
	if err := fixtureRecoveryRead(ctx, readonly, false); err != nil {
		t.Fatal("read-only primary MON after named denial", err)
	}
	text := fmt.Sprintf("ceph-msgr-named-mon-primary-log-%d", time.Now().UnixNano())
	publishLogOnce(t, ctx, c, text)
	batch, entry := waitLogEntry(t, ctx, stream, text)
	if batch.FSID != initial.FSID || entry.NameID != "test" {
		t.Fatal("named operations disrupted primary log delivery", batch, entry)
	}
	for _, mgr := range []bool{false, true} {
		if err := fixtureRecoveryRead(ctx, c, mgr); err != nil {
			t.Fatal("ordinary command after named Tell", mgr, err)
		}
	}
	checkPrimary()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MonTellTo(ctx, "b", command); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) {
		t.Fatal("named Tell after Close lost known non-execution", err)
	}
	t.Log("native mon.b status oracle, exact named a/b/c targets, concurrent named/primary commands, local TCP target refusal without fallback, daemon errors, read-only permission denial, preserved primary MON/MGR/logs and Close passed")
}
