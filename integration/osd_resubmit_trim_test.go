package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Duplicate detection covers only requests the PG still remembers. With the
// disposable cluster's PG log and duplicate tracking shortened, a completed
// APPEND whose entry has been trimmed is executed again when resubmitted under
// its original identity: the client cannot tell, which is why resubmission is
// never a replay guarantee and is never automatic.
func TestCephOSDResubmitAfterTrimIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	settings := map[string]string{
		"osd_min_pg_log_entries":  "10",
		"osd_max_pg_log_entries":  "10",
		"osd_pg_log_trim_min":     "1",
		"osd_pg_log_dups_tracked": "10",
	}
	command := func(call context.Context, fields map[string]string) error {
		encoded, _ := json.Marshal(fields)
		result, err := c.MonCommand(call, cephmsgr.Command{JSON: encoded})
		if err == nil && result.Code != 0 {
			err = fmt.Errorf("code %d: %s", result.Code, result.Message)
		}
		return err
	}
	t.Cleanup(func() {
		restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for name := range settings {
			if err := command(restore, map[string]string{"prefix": "config rm", "who": "osd", "name": name}); err != nil {
				t.Error("restore", name, err)
			}
		}
	})
	const subject = "trim-subject"
	route := poolPlace(t, ctx, 1, subject, "")
	var filler osdFixtureOracle
	for i := 0; ; i++ {
		if i == 200 {
			t.Fatal("no filler name in the subject's PG")
		}
		if r := poolPlace(t, ctx, 1, fmt.Sprintf("trim-filler-%d", i), ""); r.PGID == route.PGID {
			filler = r
			break
		}
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	appendOp := func(b []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(b)), Data: b}
	}
	original := pools.request(route, appendOp([]byte("once")))
	original.ClientIncarnation = 5
	original.ModificationTime = cephmsgr.OSDTime{Seconds: 1700004000}
	first, err := o.Request(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range settings {
		if err := command(ctx, map[string]string{"prefix": "config set", "who": "osd", "name": name, "value": value}); err != nil {
			t.Fatal("config set", name, err)
		}
	}
	time.Sleep(3 * time.Second)
	f := mutationConnection(t, c, ctx, filler)
	defer f.Close()
	for i := 0; i < 200; i++ {
		w := pools.request(filler, appendOp([]byte{byte(i)}))
		w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700004001}
		if _, err := f.Request(ctx, w); err != nil {
			t.Fatal("filler write", i, err)
		}
	}
	again := original
	again.Resubmit, again.RetryAttempt = &first.Submission, 1
	second, err := o.Request(ctx, again)
	if err != nil {
		t.Fatal("resubmission", err)
	}
	state := poolObserve(t, ctx, 1, subject, "", "")
	t.Logf("original user version %d, resubmission %d, native bytes %q version %d", first.UserVersion, second.UserVersion, state.Data, state.Version)
	if !bytes.Equal(state.Data, []byte("onceonce")) || second.UserVersion <= first.UserVersion || second.Transaction != first.Transaction {
		t.Fatalf("expected the trimmed request to execute again under its identity: %q", state.Data)
	}
}
