package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Keep the Go map-wait assertion separate from this native control barrier.
// No MON mutation is retried and no unreachable member enters mon_host.
func waitNativeMonitorElection(t *testing.T, ctx context.Context, c *cephmsgr.Client, control, label string, state cephmsgr.MonitorState, quorumMembers []cephmsgr.MonitorMember) {
	t.Helper()
	summary, err := settleNativeMonitorElection(ctx, c, control, label, state, quorumMembers)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(summary)
}

// settleNativeMonitorElection asks the fixture for its settled-election
// barrier (the expected monmap and the same election epoch twice, at least
// 2 s apart, before fresh clients; ceph-testcontainers-go v0.3.0 "Daemon
// controls") and checks the receipt. It returns an error instead of failing
// so that a cleanup can use it.
func settleNativeMonitorElection(ctx context.Context, c *cephmsgr.Client, control, label string, state cephmsgr.MonitorState, quorumMembers []cephmsgr.MonitorMember) (string, error) {
	var quorum []string
	for _, member := range quorumMembers {
		quorum = append(quorum, member.Name)
	}
	slices.Sort(quorum)
	type expectation struct {
		FSID    string                   `json:"fsid"`
		Epoch   uint32                   `json:"epoch"`
		Members []cephmsgr.MonitorMember `json:"members"`
		Quorum  []string                 `json:"quorum"`
		Label   string                   `json:"label"`
	}
	expected := expectation{c.Snapshot().FSID, state.MapEpoch, state.Members, quorum, label}
	input, err := json.Marshal(expected)
	if err != nil {
		return "", fmt.Errorf("encode native election expectation: %w", err)
	}
	if err := testcluster.RequestControl(ctx, control, "wait-monitor-election", "monitor-election-settled", string(input)); err != nil {
		return "", fmt.Errorf("native settled-election barrier: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "monitor-election-settled.json"))
	type observation struct {
		ObservedAt    time.Time                `json:"observed_at"`
		FSID          string                   `json:"fsid"`
		MapEpoch      uint32                   `json:"map_epoch"`
		ElectionEpoch uint64                   `json:"election_epoch"`
		Members       []cephmsgr.MonitorMember `json:"members"`
		Quorum        []string                 `json:"quorum"`
	}
	var receipt struct {
		SchemaVersion int         `json:"schema_version"`
		RequestID     string      `json:"request_id"`
		Expected      expectation `json:"expected"`
		First, Second observation
	}
	if err != nil || len(data) > 16<<10 || json.Unmarshal(data, &receipt) != nil || receipt.SchemaVersion != 1 || receipt.RequestID == "" || !reflect.DeepEqual(receipt.Expected, expected) {
		return "", fmt.Errorf("native election receipt identity differs: %v", err)
	}
	for _, observed := range []observation{receipt.First, receipt.Second} {
		if observed.FSID != expected.FSID || observed.MapEpoch != expected.Epoch || observed.ElectionEpoch == 0 || !reflect.DeepEqual(observed.Members, expected.Members) || !slices.Equal(observed.Quorum, quorum) || observed.ObservedAt.IsZero() {
			return "", fmt.Errorf("native settled election metadata differs: %+v", observed)
		}
	}
	if receipt.First.ElectionEpoch != receipt.Second.ElectionEpoch || receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt) < 2*time.Second {
		return "", fmt.Errorf("native election did not satisfy two separated stable observations: %+v %+v", receipt.First, receipt.Second)
	}
	return fmt.Sprintf("%s: native election epoch %d stable over %s with MON map %d and owned quorum %v", label, receipt.Second.ElectionEpoch, receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt), state.MapEpoch, quorum), nil
}

// fixtureUnusableFile marks a disposable fixture that a cleanup could not
// return to a settled state. Later tests fail as a fixture error instead of
// timing out on admissions during an unsettled election.
const fixtureUnusableFile = "fixture-unusable.json"

func markFixtureUnusable(t *testing.T, control, reason string) {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"test": t.Name(), "reason": reason})
	if err := os.WriteFile(filepath.Join(control, fixtureUnusableFile), data, 0644); err != nil {
		t.Error("mark fixture unusable", err)
	}
}

func requireUsableFixture(t *testing.T, control string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(control, fixtureUnusableFile))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	t.Fatalf("fixture error: an earlier cleanup left this disposable fixture unusable: %s %v", data, err)
}
