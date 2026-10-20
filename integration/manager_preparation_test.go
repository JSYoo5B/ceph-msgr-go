package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// prepareNativeManagerFailover establishes the disposable active+standby
// precondition before the independent Go failover observation starts. A native
// daemon missing after a native AUTH refusal during a previous topology fault may be
// restarted once, with its original refusal retained in the public receipt.
// This never handles a tested Go authentication or replays its mutation.
func prepareNativeManagerFailover(t *testing.T, ctx context.Context, control string) {
	t.Helper()
	path := filepath.Join(control, "manager-preparation.jsonl")
	before, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("native MGR preparation receipt", err)
	}
	if err := testcluster.ControlDaemon(ctx, control, "prepare", "mgr-failover", "ready"); err != nil {
		t.Fatal("native active+standby failover preparation", err)
	}
	after, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("native MGR preparation receipt", err)
	}
	if len(after) > len(before) {
		t.Logf("native fixture preparation before independent failover: %s", after[len(before):])
	}
}
