package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// The CRC handshake offers snappy/zstd when the peer banner supports
// compression. This test reads the negotiation back from the bytes of the
// very connections that then serve a MON and an OSD command: the daemon's
// banner carries the compression bit, the client offers zstd then snappy
// right after its AUTH_SIGNATURE, the daemon refuses with CompressionDone
// (disabled, method 0), and no later frame of that connection is compressed.
// A missing bit, frame or direction on the connection fails the test; an
// empty negotiated algorithm alone would not tell a refused offer from
// negotiation that never ran.
//
// Only tags and those decoded fields are kept and logged: no authentication
// payload, key, address or message. Live compressed frames remain
// unverified; codec/frame tests are synthetic.
func TestCephMsgrCompressionOfferIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_CONNECTION_MODE") != "crc" {
		t.Skip("message compression is offered only on CRC-mode connections")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	_, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The subject client: every connection it dials is copied.
	tap := &compressionTap{}
	options := integrationOptions(t)
	options.EnableOSD = true
	options.DialContext = tap.dialer(options.DialContext)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

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

	// Both commands have returned, so each travelled on a connection that
	// is still open. Every open connection to a MON, and every open
	// connection to the owned OSD, must show the whole refused offer and
	// frames in both directions after it. A connection the client already
	// closed (a MON it tried but did not keep) is logged and decides
	// nothing.
	roles := map[string]func(string) bool{
		"MON": func(endpoint string) bool {
			for _, monitor := range options.Monitors {
				if daemonEndpoint(monitor)(endpoint) {
					return true
				}
			}
			return false
		},
		"OSD": daemonEndpoint(base.Address),
	}
	served := map[string]int{}
	for index, recorded := range tap.connections() {
		role := "other"
		for name, selects := range roles {
			if selects(recorded.endpoint) {
				role = name
			}
		}
		x := readCompressionExchange(recorded.out, recorded.in)
		t.Logf("connection %d (%s, closed=%v): %s", index+1, role, recorded.closed, x)
		if role == "other" || recorded.closed {
			continue
		}
		if problems := x.declinedOffer(); len(problems) != 0 {
			t.Errorf("open %s connection %d does not show a refused compression offer: %q", role, index+1, problems)
		}
		if x.laterOut == 0 || x.laterIn == 0 {
			t.Errorf("open %s connection %d carried no frame in both directions after the negotiation: sent=%d received=%d", role, index+1, x.laterOut, x.laterIn)
		}
		served[role]++
	}
	for role := range roles {
		if served[role] == 0 {
			t.Errorf("no open %s connection of the subject client was observed", role)
		}
	}
}
