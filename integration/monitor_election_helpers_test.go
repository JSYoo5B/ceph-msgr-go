package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// Keep the Go map-wait assertion separate from this native control barrier.
// No MON mutation is retried and no unreachable member enters mon_host.
func waitNativeMonitorElection(t *testing.T, ctx context.Context, c *cephmsgr.Client, control, label string, state cephmsgr.MonitorState, quorumMembers []cephmsgr.MonitorMember) {
	t.Helper()
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
		t.Fatal("encode native election expectation", err)
	}
	if err := testcluster.RequestControl(ctx, control, "wait-monitor-election", "monitor-election-settled", string(input)); err != nil {
		t.Fatal("native settled-election barrier", err)
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
		t.Fatal("native election receipt identity differs", err)
	}
	for _, observed := range []observation{receipt.First, receipt.Second} {
		if observed.FSID != expected.FSID || observed.MapEpoch != expected.Epoch || observed.ElectionEpoch == 0 || !reflect.DeepEqual(observed.Members, expected.Members) || !slices.Equal(observed.Quorum, quorum) || observed.ObservedAt.IsZero() {
			t.Fatal("native settled election metadata differs", observed)
		}
	}
	if receipt.First.ElectionEpoch != receipt.Second.ElectionEpoch || receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt) < 2*time.Second {
		t.Fatal("native election did not satisfy two separated stable observations", receipt.First, receipt.Second)
	}
	t.Logf("%s: native election epoch %d stable over %s with MON map %d and owned quorum %v", label, receipt.Second.ElectionEpoch, receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt), state.MapEpoch, quorum)
}
