package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// The CRC handshake offers snappy/zstd when the peer banner supports
// compression. This test reads the negotiation back from the bytes of the
// very connections that carry a MON and an OSD command and their replies
// (found by message type in the recording): the daemon's
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
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-msgrcompressionoffer key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6
//fixture:shareable reads MON status and OSD version over CRC connections only
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

	// Each command is found in the recording itself: exactly one connection
	// to a MON carries the MON command and its reply, exactly one connection
	// to the owned OSD carries the tell command and its reply, and that very
	// connection must show the whole refused offer before them. Whether the
	// connection is still open does not matter, and a connection that only
	// negotiated (a MON the client tried but did not keep, or a replacement)
	// is logged and decides nothing.
	recorded := tap.connections()
	for index, c := range recorded {
		t.Logf("connection %d (closed=%v): %s", index+1, c.closed, readCompressionExchange(c.out, c.in))
	}
	for _, role := range []struct {
		name           string
		selects        func(string) bool
		command, reply uint16
	}{
		{"MON", func(endpoint string) bool {
			for _, monitor := range options.Monitors {
				if daemonEndpoint(monitor)(endpoint) {
					return true
				}
			}
			return false
		}, msgr.MonCommandMessage, msgr.MonCommandReplyMessage},
		{"OSD", daemonEndpoint(base.Address), msgr.TellCommandMessage, msgr.TellCommandReplyMessage},
	} {
		index, x, problems := servedCommand(recorded, role.selects, role.command, role.reply)
		if len(problems) != 0 {
			t.Errorf("the %s command (connection %d) does not follow a refused compression offer on its own connection: %q", role.name, index+1, problems)
			continue
		}
		t.Logf("the %s command and its reply travelled on connection %d after its refused offer: %d sent and %d received frames after the negotiation", role.name, index+1, x.laterOut, x.laterIn)
	}
}
