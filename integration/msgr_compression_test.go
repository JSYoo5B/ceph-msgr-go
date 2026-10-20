package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Our CRC-mode client advertises snappy/zstd in CompressionRequest. Ceph has
// onwire-compression config only for OSD *peers* (ms_osd_compress_mode), not
// for client or MON peers, so a daemon never compresses toward a client: it
// declines the offer. This test confirms the offer is production-safe — the MON
// and an OSD both decline (Compression() == ""), and traffic works normally.
// Live codec interop cannot be exercised from a client because no daemon
// compresses toward one; the codec and frame paths are covered by the msgr
// unit tests (internal/msgr). Compression is offered only on CRC connections.
func TestCephMsgrCompressionOfferIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_CONNECTION_MODE") != "crc" {
		t.Skip("message compression is offered only on CRC-mode connections")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The MON declined the offer; the admitted session still runs commands.
	if comp := c.Snapshot().Monitor.Compression; comp != "" {
		t.Fatalf("MON negotiated compression %q; no client-peer compression option exists", comp)
	}
	if r, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil || len(r.Data) == 0 {
		t.Fatal("MON command over the offer-declined connection", err)
	}

	// The OSD declines too, and object reads still work.
	conn, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: base.ID, Address: base.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if comp := conn.Compression(); comp != "" {
		t.Fatalf("OSD negotiated compression %q toward a client", comp)
	}
	if r, err := conn.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || r.Code != 0 {
		t.Fatal("OSD command over the offer-declined connection", err)
	}
	t.Log("MON and OSD declined the CRC compression offer; traffic unaffected")
}
