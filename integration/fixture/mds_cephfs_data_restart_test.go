package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func dataRestartDaemon() mdsFreshDaemon {
	return mdsFreshDaemon{Name: "owned-0", GID: 100, Rank: 0, State: "up:active", Incarnation: 6, Endpoint: "172.19.0.10:6800", Nonce: 66,
		Addresses: []mdsFreshAddress{{Type: "v2", Addr: "172.19.0.10:6800", Nonce: 66}, {Type: "v1", Addr: "172.19.0.10:6801", Nonce: 66}}}
}

func TestMDSCephFSDataRestartClosedActions(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, name := range []string{"restart-arm", "restart-fire", "restart-status", "restart-adopt"} {
		id, a, err := parseMDSCephFSDataRequest([]string{"2", token, name}, token, 1)
		if err != nil || id != 2 || a.name != name || a.target != "" || a.gid != 0 {
			t.Fatal(a, err)
		}
		for _, fields := range [][]string{{"2", token, name, "100"}, {"2", strings.Repeat("b2", 16), name}, {"1", token, name}, {"02", token, name}} {
			if _, _, err := parseMDSCephFSDataRequest(fields, token, 1); err == nil {
				t.Fatal("unsafe restart request accepted", fields)
			}
		}
	}
	if _, _, err := parseMDSCephFSDataRequest([]string{"2", token, "restart-other"}, token, 1); err == nil {
		t.Fatal("unknown restart action admitted")
	}
}

func TestMDSCephFSDataRestartArmsExactOwnedIdentity(t *testing.T) {
	daemon := dataRestartDaemon()
	r, err := newMDSCephFSDataRestart(4, daemon)
	if err != nil || r.Armed.GID != 100 || r.State != "armed" || r.ArmRequestID != 4 || !slices.Equal(r.ArmedAddresses, []string{"172.19.0.10:6800/66", "172.19.0.10:6801/66"}) {
		t.Fatal(r, err)
	}
	daemon.Addresses[0].Nonce = 99
	if r.Armed.Addresses[0].Nonce != 66 {
		t.Fatal("armed address vector aliases later observations")
	}
	for _, mutate := range []func(*mdsFreshDaemon){
		func(d *mdsFreshDaemon) { d.Name = "" }, func(d *mdsFreshDaemon) { d.GID = 0 }, func(d *mdsFreshDaemon) { d.Rank = 1 },
		func(d *mdsFreshDaemon) { d.State = "up:reconnect" }, func(d *mdsFreshDaemon) { d.Nonce = 0 }, func(d *mdsFreshDaemon) { d.Incarnation = -1 },
		func(d *mdsFreshDaemon) { d.Addresses = nil }, func(d *mdsFreshDaemon) { d.Endpoint = "172.19.0.11:6800" },
		func(d *mdsFreshDaemon) { d.Addresses[0].Addr = "host:6800" },
	} {
		d := dataRestartDaemon()
		mutate(&d)
		if _, err := newMDSCephFSDataRestart(5, d); err == nil {
			t.Fatal("unproven rank0 restart identity admitted", d)
		}
	}
}

func TestMDSCephFSDataRestartFireIntentBeforeOneExactCommand(t *testing.T) {
	nativeErr := errors.New("native result not observed")
	journalErr := errors.New("journal unavailable")
	for _, tc := range []struct {
		name        string
		commandErr  error
		failJournal int
		state       string
		calls       int
	}{
		{name: "applied", state: "applied", calls: 1},
		{name: "uncertain", commandErr: nativeErr, state: "uncertain", calls: 1},
		{name: "intent-journal-failed", failJournal: 1, state: "intended", calls: 0},
		{name: "applied-journal-failed", failJournal: 2, state: "applied", calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := newMDSCephFSDataRestart(1, dataRestartDaemon())
			if err != nil {
				t.Fatal(err)
			}
			x := &mdsCephFSDataScope{restart: r}
			calls, journals := 0, 0
			journal := func() error {
				journals++
				if journals == 1 && (r.State != "intended" || r.FireRequestID != 2 || r.FireStartedUTC == "") {
					t.Fatal("intent not installed before journal", r)
				}
				if journals == tc.failJournal {
					return journalErr
				}
				return nil
			}
			command := func(ctx context.Context, args ...string) ([]byte, error) {
				calls++
				if journals != 1 || !slices.Equal(args, []string{"mds", "fail", "100"}) {
					t.Fatal("native command preceded intent or targeted another identity", args, journals)
				}
				return []byte(strings.Repeat("x", 5000)), tc.commandErr
			}
			err = x.fireRestart(context.Background(), 2, journal, command)
			if r.State != tc.state || calls != tc.calls || (err != nil) != (tc.commandErr != nil || tc.failJournal != 0) {
				t.Fatal(r, calls, err)
			}
			if tc.commandErr != nil && (!errors.Is(err, nativeErr) || r.Error == "") {
				t.Fatal("uncertain native error lost", err, r)
			}
			if calls == 1 && (r.FiredUTC == "" || len(r.Output) != 4096) {
				t.Fatal("native observation bounds differ", r)
			}
			if err := x.fireRestart(context.Background(), 3, journal, command); err == nil || calls != tc.calls {
				t.Fatal("fire repeated a consumed restart", err, calls)
			}
		})
	}
}

func TestMDSCephFSDataRestartRejectsInvalidStepsAndNativeDuringTransition(t *testing.T) {
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{}, mdsCephFSData: &mdsCephFSDataScope{}}
	for _, state := range []string{"", "armed", "intended", "applied", "uncertain", "adopted"} {
		x := c.mdsCephFSData
		x.restart = nil
		if state != "" {
			x.restart = &mdsCephFSRestart{State: state}
		}
		var result mdsCephFSDataResult
		for _, name := range []string{"restart-arm", "restart-fire", "restart-status", "restart-adopt", "restart-other"} {
			// Only invalid steps are called: the cluster is nil, so an accidental
			// native command here fails this test instead of silently succeeding.
			invalid := name == "restart-other" || name == "restart-arm" && state != "" || name == "restart-fire" && state != "armed" ||
				(name == "restart-status" || name == "restart-adopt") && state != "applied" && state != "uncertain"
			if invalid && c.mdsCephFSDataRestartAct(context.Background(), 1, mdsCephFSDataAction{name: name}, &result) == nil {
				t.Fatal("invalid restart step admitted", state, name)
			}
		}
		if x.restart.pending() {
			if err := c.mdsCephFSDataAct(context.Background(), 2, mdsCephFSDataAction{name: "layout-set", target: "dir"}, &result); err == nil || len(x.mutations) != 0 {
				t.Fatal("native mutation intent admitted during transition", state, err)
			}
			if _, _, err := c.mdsCephFSDataRunNative(context.Background(), "mksnap", "capsnap-a"); err == nil {
				t.Fatal("native mutation admitted during transition", state)
			}
		}
	}
}

func TestMDSCephFSDataRestartAdoptionRequiresTwoExactNativeProofs(t *testing.T) {
	armed := dataRestartDaemon()
	base := mdsFreshOracle{FSID: "cluster", FSCID: 7, FSName: "owned", MetadataPool: 3, DataPools: []int64{4}, MDS: armed}
	current := base
	current.MDS = dataRestartDaemon()
	current.MDS.GID = 200
	current.MDS.Nonce = 77
	current.MDS.Incarnation = 9
	current.MDS.Addresses[0].Nonce = 77
	current.MDS.Addresses[1].Nonce = 77
	rank := mdsCephFSRankDaemon{Name: current.MDS.Name, GID: 200, Rank: 0, State: "up:active", Incarnation: 9, Endpoint: current.MDS.Endpoint, Nonce: 77}
	status := mdsCephFSRankStatus{Rank0: &rank}
	if err := validateMDSCephFSDataAdoption(base, current, armed, status); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*mdsFreshOracle){
		func(o *mdsFreshOracle) { o.FSID = "other" }, func(o *mdsFreshOracle) { o.FSCID++ }, func(o *mdsFreshOracle) { o.FSName = "other" },
		func(o *mdsFreshOracle) { o.MetadataPool++ }, func(o *mdsFreshOracle) { o.DataPools = []int64{5} },
		func(o *mdsFreshOracle) { o.MDS.Name = "foreign" }, func(o *mdsFreshOracle) { o.MDS.GID = 100 }, func(o *mdsFreshOracle) { o.MDS.Nonce = 66 },
		func(o *mdsFreshOracle) { o.MDS.Incarnation = 6 }, func(o *mdsFreshOracle) { o.MDS.State = "up:replay" }, func(o *mdsFreshOracle) { o.MDS.Rank = 1 },
	} {
		bad := current
		mutate(&bad)
		if err := validateMDSCephFSDataAdoption(base, bad, armed, status); err == nil {
			t.Fatal("different FS or old/unproven incarnation adopted", bad)
		}
	}
	for _, mutate := range []func(*mdsCephFSRankStatus){
		func(s *mdsCephFSRankStatus) { s.ArmedPresent = true }, func(s *mdsCephFSRankStatus) { s.Rank0 = nil },
		func(s *mdsCephFSRankStatus) { s.Rank0.GID++ }, func(s *mdsCephFSRankStatus) { s.Rank0.Name = "foreign" },
		func(s *mdsCephFSRankStatus) { s.Rank0.State = "up:reconnect" }, func(s *mdsCephFSRankStatus) { s.Rank0.Nonce++ },
		func(s *mdsCephFSRankStatus) { s.Rank0.Incarnation++ }, func(s *mdsCephFSRankStatus) { s.Rank0.Endpoint = "172.19.0.11:6800" },
	} {
		bad := status
		copyRank := rank
		bad.Rank0 = &copyRank
		mutate(&bad)
		if err := validateMDSCephFSDataAdoption(base, current, armed, bad); err == nil {
			t.Fatal("inconsistent second native map adopted", bad)
		}
	}
}

func TestMDSCephFSDataRestartTargetAndSafeFencingAttribution(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	x := &mdsCephFSDataScope{out: out, current: dataRestartDaemon(), owned: map[string]uint64{"172.19.0.9:0/77": 4200}, baselineBlocklist: []mdsSessionsBlock{{Address: "172.19.0.10:6801/66"}}}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{token: token, name: "owned", oracle: mdsFreshOracle{FSID: "cluster", FSCID: 7}}, mdsCephFSData: x}
	r, err := newMDSCephFSDataRestart(1, x.current)
	if err != nil {
		t.Fatal(err)
	}
	x.restart = r
	for _, tc := range []struct {
		entry mdsSessionsBlock
		gone  bool
		kind  string
		err   bool
	}{
		{mdsSessionsBlock{Address: "172.19.0.10:6800/66"}, true, "mds-restart", false},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/66"}, false, "", true},
		{mdsSessionsBlock{Address: "172.19.0.10:6801/66"}, true, "", false},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/67"}, true, "", false},
		{mdsSessionsBlock{Address: "172.19.0.9:0/77"}, false, "owned-session", false},
		{mdsSessionsBlock{Address: "172.19.0.0/24", Range: true}, true, "", false},
	} {
		kind, err := x.blockOwner(tc.entry, tc.gone)
		if kind != tc.kind || (err != nil) != tc.err {
			t.Fatal(tc, kind, err)
		}
	}
	read := func() map[string]any {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(out, "mds-cephfs-data-target.json"))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if err := c.mdsCephFSDataWriteTarget(); err != nil {
		t.Fatal(err)
	}
	initial := read()
	if initial["adopted_restarts"] != float64(0) || initial["token"] != token || initial["fsid"] != "cluster" || initial["fscid"] != float64(7) {
		t.Fatal(initial)
	}
	// Fire does not publish a new target or alter data.current.
	if err := x.fireRestart(context.Background(), 2, func() error { return nil }, func(context.Context, ...string) ([]byte, error) { return nil, errors.New("unknown") }); err == nil {
		t.Fatal("expected uncertain native result")
	}
	if !reflect.DeepEqual(initial, read()) || x.current.GID != 100 {
		t.Fatal("transitional restart changed adopted target")
	}
	x.restart.State = "adopted"
	x.current.GID = 200
	if err := c.mdsCephFSDataWriteTarget(); err != nil {
		t.Fatal(err)
	}
	adopted := read()
	if adopted["adopted_restarts"] != float64(1) || adopted["mds"].(map[string]any)["gid"] != float64(200) {
		t.Fatal(adopted)
	}
}

func TestMDSCephFSDataRestartJournalsDurableIntentAndOutcome(t *testing.T) {
	out := t.TempDir()
	r, err := newMDSCephFSDataRestart(1, dataRestartDaemon())
	if err != nil {
		t.Fatal(err)
	}
	x := &mdsCephFSDataScope{out: out, current: r.Armed, restart: r}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{token: strings.Repeat("a1", 16), name: "owned", oracle: mdsFreshOracle{FSCID: 7}}, mdsCephFSData: x}
	read := func() struct {
		Current mdsFreshDaemon   `json:"current_mds"`
		Restart mdsCephFSRestart `json:"restart"`
	} {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(out, "mds-cephfs-data-owned.json"))
		if err != nil {
			t.Fatal(err)
		}
		var j struct {
			Current mdsFreshDaemon   `json:"current_mds"`
			Restart mdsCephFSRestart `json:"restart"`
		}
		if err := json.Unmarshal(data, &j); err != nil {
			t.Fatal(err)
		}
		return j
	}
	calls := 0
	err = x.fireRestart(context.Background(), 2, c.mdsCephFSDataJournal, func(context.Context, ...string) ([]byte, error) {
		calls++
		j := read()
		if j.Restart.State != "intended" || j.Restart.FireRequestID != 2 || j.Restart.Armed.GID != 100 || j.Current.GID != 100 {
			t.Fatal("native mutation reached before persisted exact intent", j)
		}
		return []byte("failed mds gid 100"), nil
	})
	if err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
	j := read()
	if j.Restart.State != "applied" || j.Restart.FiredUTC == "" || j.Current.GID != 100 {
		t.Fatal("native outcome was not retained without adoption", j)
	}
}
