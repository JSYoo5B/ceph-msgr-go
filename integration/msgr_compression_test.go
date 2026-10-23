package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// The CRC handshake can offer snappy/zstd when the peer banner supports
// compression. This test observes no negotiated compression on MON and OSD
// connections and successful commands on both. Those observations alone do
// not distinguish a declined CompressionRequest from a peer without the banner
// feature; no authentication transcript or negotiation frame is captured here.
// Live compressed frames remain unverified; codec/frame tests are synthetic.
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

	// The admitted MON session has no compression and still runs commands.
	if comp := c.Snapshot().Monitor.Compression; comp != "" {
		t.Fatalf("MON unexpectedly negotiated compression %q", comp)
	}
	r, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status"}`)})
	if err != nil || r.Code != 0 || len(r.Data) == 0 {
		t.Fatalf("MON status command: code=%d data_bytes=%d err=%v", r.Code, len(r.Data), err)
	}
	t.Log("MON status command succeeded: code=0, nonempty data, negotiated compression=none")

	// The admitted OSD connection also has no compression and runs a command.
	conn, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: base.ID, Address: base.Address})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if comp := conn.Compression(); comp != "" {
		t.Fatalf("OSD negotiated compression %q toward a client", comp)
	}
	osdResult, err := conn.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
	if err != nil || osdResult.Code != 0 || len(osdResult.Data) == 0 {
		t.Fatalf("OSD version command: code=%d data_bytes=%d err=%v", osdResult.Code, len(osdResult.Data), err)
	}
	t.Log("OSD version command succeeded: code=0, nonempty data, negotiated compression=none")
}
