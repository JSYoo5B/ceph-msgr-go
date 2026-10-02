package integration_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func mappedOracleEndpoints(t *testing.T, role string, endpoints []string, ports ...uint16) map[netip.AddrPort]bool {
	t.Helper()
	if len(endpoints) != len(ports) {
		t.Fatalf("%s wire endpoint count=%d want=%d", role, len(endpoints), len(ports))
	}
	want := make(map[uint16]bool, len(ports))
	for _, port := range ports {
		want[port] = true
	}
	seen := make(map[netip.AddrPort]bool, len(endpoints))
	for _, endpoint := range endpoints {
		address, err := netip.ParseAddrPort(endpoint)
		if err != nil || !address.Addr().Is4In6() || address.Addr().Unmap() != netip.MustParseAddr("127.0.0.1") || !want[address.Port()] || seen[address] {
			t.Fatalf("%s authoritative mapped endpoint lost family/address/port: %q err=%v", role, endpoint, err)
		}
		delete(want, address.Port())
		seen[address] = true
	}
	if len(want) != 0 {
		t.Fatalf("%s wire endpoints missed fixture ports", role)
	}
	return seen
}

func TestCephMappedAddressIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MAPPED_IPV6") != "1" || os.Getenv("CEPH_MSGR_TEST_PROXY") != "" {
		t.Skip("requires the container-local mapped Ceph fixture")
	}
	metadata, err := os.ReadFile(filepath.Join(control, "mapped-oracle-summary.json"))
	if err != nil {
		t.Fatal("native Ceph mapped oracle did not complete", err)
	}
	var oracle struct {
		FSID         string   `json:"fsid"`
		MonEndpoints []string `json:"mon_endpoints"`
		MgrEndpoint  string   `json:"mgr_endpoint"`
		MgrName      string   `json:"mgr_name"`
		MgrGlobalID  uint64   `json:"mgr_global_id"`
	}
	if err := json.Unmarshal(metadata, &oracle); err != nil {
		t.Fatal("invalid native Ceph oracle metadata", err)
	}
	const fsid = "80bbab73-69c1-4a0c-a746-4271357750b8"
	if oracle.FSID != fsid || oracle.MgrGlobalID == 0 {
		t.Fatal("native oracle has unexpected cluster/MGR identity")
	}
	mgrPort := uint16(36800)
	if oracle.MgrName == "b" {
		mgrPort = 36801
	} else if oracle.MgrName != "a" {
		t.Fatal("native oracle has an unexpected MGR name")
	}
	expectedMon := mappedOracleEndpoints(t, "native MON oracle", oracle.MonEndpoints, 33300, 33301, 33302)
	expectedMgr := mappedOracleEndpoints(t, "native MGR oracle", []string{oracle.MgrEndpoint}, mgrPort)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = []string{"v2:[::ffff:7f00:1]:33300/0"}
	options.DialContext = nil
	options.ExpectedFSID = fsid
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("default Go dialer could not authenticate authoritative mapped MON", err)
	}
	t.Cleanup(func() { c.Close() })
	for _, target := range []struct {
		call   func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		prefix string
	}{{c.MonCommand, "status"}, {c.MgrCommand, "pg stat"}} {
		command, _ := json.Marshal(map[string]string{"prefix": target.prefix, "format": "json"})
		result, err := target.call(ctx, cephmsgr.Command{JSON: command})
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatalf("mapped %s: code=%d dataBytes=%d err=%q", target.prefix, result.Code, len(result.Data), err)
		}
	}
	state := c.Snapshot()
	if state.Closed || state.FSID != fsid || state.GlobalID == 0 || state.AuthRejection != nil || !state.Monitor.Ready || state.Monitor.MapEpoch == 0 || state.Monitor.MinimumRelease < 20 || !state.Manager.Ready || !state.Manager.Available || state.Manager.MapEpoch == 0 || state.Manager.Name != oracle.MgrName || state.Manager.GlobalID != oracle.MgrGlobalID {
		t.Fatal("mapped client lost authenticated cluster/MON/MGR identity", state)
	}
	actualMon := mappedOracleEndpoints(t, "authenticated MON", state.Monitor.Endpoints, 33300, 33301, 33302)
	actualMgr := mappedOracleEndpoints(t, "authenticated MGR", state.Manager.Endpoints, mgrPort)
	for endpoint := range expectedMon {
		if !actualMon[endpoint] {
			t.Fatal("authenticated MonMap disagrees with native Ceph oracle", endpoint)
		}
	}
	for endpoint := range expectedMgr {
		if !actualMgr[endpoint] {
			t.Fatal("authenticated MgrMap disagrees with native Ceph oracle", endpoint)
		}
	}
	// RemoteAddr describes the TCP peer; the authoritative mapped family comes
	// from the admitted wire maps and can differ from that socket's address form.
	t.Logf("physical MON peer=%q MGR peer=%q; mapped wire MON=%v MGR=%v; raw JSON/code0 preserved", state.Monitor.Endpoint, state.Manager.Endpoint, state.Monitor.Endpoints, state.Manager.Endpoints)
	if err := c.Close(); err != nil {
		t.Fatal("close mapped client", err)
	}
	closed := c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready {
		t.Fatal("closed mapped client retained ready sessions", closed)
	}
}
