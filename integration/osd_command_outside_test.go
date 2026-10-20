package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A PG command sent to an OSD outside the PG's acting set (the caller names
// that OSD as primary) is refused by the OSD with a known code and is neither
// executed, retried nor redirected. A single-replica owned pool on the
// three-OSD fixture leaves two OSDs without its PG. The native client's
// command to the same OSD is the reference.
func TestCephOSDCommandOutsideActingSetIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" {
		t.Skip("needs an OSD outside a single-replica PG's acting set")
	}
	fixture := osdFixture
	hash := false
	if isReplicatedPGFixtureProfile() {
		fixture, hash = replicatedPGFixture, true
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned pools were not removed")
		}
	})
	pool := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": 1, "size": 1, "min_size": 1, "hashpspool": hash})
	if pool.PoolID <= 0 {
		t.Fatal("owned single-replica pool", pool)
	}
	place := poolPlace(t, ctx, 1, "outside-acting-set-probe", "")
	if len(place.Acting) != 1 || place.Acting[0] != place.ID {
		t.Fatalf("expected a single-OSD acting set: %+v", place)
	}
	addresses := osdDumpAddresses(t, ctx, c)
	outside := int32(-1)
	for id := range addresses {
		if !slices.Contains(place.Acting, id) && (outside < 0 || id < outside) {
			outside = id
		}
	}
	if outside < 0 {
		t.Fatal("no OSD outside the acting set", place.Acting, addresses)
	}
	pgid := fmt.Sprintf("%d.%x", place.Pool, place.PG)
	query := fmt.Sprintf(`{"prefix":"query","pgid":%q}`, pgid)
	native := nativeOSDCommand(t, ctx, osdCommandNativeInput{OSD: &outside, Command: query, Timeout: 5})

	o := mutationConnection(t, c, ctx, osdFixtureOracle{ID: outside, Address: addresses[outside]})
	defer o.Close()
	pg := cephmsgr.OSDCommandPG{Pool: uint64(place.Pool), Seed: place.PG, Primary: outside, MapEpoch: place.Epoch}
	result, err := o.PGCommand(ctx, pg, cephmsgr.Command{JSON: []byte(query)})
	var commandErr *cephmsgr.CommandError
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &commandErr) || errors.As(err, &unknown) || commandErr.Code >= 0 || result.Code != commandErr.Code || len(result.Data) != 0 {
		t.Fatalf("PG %s at osd.%d outside its acting set %v: expected a known refusal: code %d %q: %v", pgid, outside, place.Acting, result.Code, result.Message, err)
	}
	t.Logf("PG %s (acting %v) at osd.%d: go code=%d %q; native code=%d %q", pgid, place.Acting, outside, result.Code, result.Message, native.Code, native.Message)
	// The native Objecter may resend an EAGAIN until its timeout (-110); any
	// other native code must equal the single reply Go returned.
	if native.Code == result.Code && string(native.Message) != result.Message || native.Code != result.Code && !(result.Code == -11 && native.Code == -110) {
		t.Fatalf("native command to osd.%d answered %d %q, Go %d %q", outside, native.Code, native.Message, result.Code, result.Message)
	}
	if again, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || again.Submission.Transaction <= result.Submission.Transaction {
		t.Fatal("session not usable after the refusal", err)
	}
}
