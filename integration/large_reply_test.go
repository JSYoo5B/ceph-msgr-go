package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

type largeReplyConn struct {
	net.Conn
	received *atomic.Uint64
}

func (c *largeReplyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.received.Add(uint64(n))
	return n, err
}

func TestCephLargeMonitorReplyIntegration(t *testing.T) {
	options := integrationOptions(t)
	options.MaxFrameSize = 32 << 20
	dial := options.DialContext
	var dials atomic.Uint32
	var received atomic.Uint64
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		dials.Add(1)
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		return &largeReplyConn{Conn: conn, received: &received}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate MON with the larger frame limit", err)
	}
	defer c.Close()
	initial := c.Snapshot()
	query := []byte(`{"prefix":"status","format":"json"}`)
	checkStatus := func(result cephmsgr.Result, err error) {
		t.Helper()
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) {
			t.Fatal("MON status did not return a known execution outcome", err)
		}
		if err != nil {
			var server *cephmsgr.CommandError
			if errors.As(err, &server) {
				if result.Code != server.Code || result.Message != server.Message {
					t.Fatal("known MON rejection lost its server code or status", result.Code, err)
				}
				t.Fatalf("MON rejected valid status JSON with a known server error: code=%d status=%q", result.Code, result.Message)
			}
			t.Fatal("MON status failed", err)
		}
		var status struct {
			FSID string `json:"fsid"`
		}
		if result.Code != 0 || json.Unmarshal(result.Data, &status) != nil || status.FSID != initial.FSID || !bytes.HasSuffix(result.Data, []byte{'\n'}) {
			t.Fatalf("MON status did not preserve code and raw JSON: code=%d size=%d fsid=%q", result.Code, len(result.Data), status.FSID)
		}
	}
	checkStatus(c.MonCommand(ctx, cephmsgr.Command{JSON: query}))
	connections, before := dials.Load(), received.Load()

	// The large bytes belong to command.JSON, not Command.Input. Tentacle's
	// parser accepts trailing JSON whitespace, and Monitor::reply_command
	// copies the original command into MMonCommandAck's front segment. Its
	// echoed string therefore exceeds the default 16 MiB decoder limit even
	// though the status data itself remains small.
	padded := make([]byte, len(query)+(16<<20))
	copy(padded, query)
	for i := len(query); i < len(padded); i++ {
		padded[i] = ' '
	}
	if !json.Valid(padded) {
		t.Fatal("large command is not valid JSON")
	}
	checkStatus(c.MonCommand(ctx, cephmsgr.Command{JSON: padded}))
	responseBytes := received.Load() - before
	if responseBytes <= 16<<20 {
		t.Fatalf("Ceph did not send the large echoed command: received=%d", responseBytes)
	}
	checkStatus(c.MonCommand(ctx, cephmsgr.Command{JSON: query}))
	final := c.Snapshot()
	if dials.Load() != connections || !final.Monitor.Ready || final.Monitor.Endpoint != initial.Monitor.Endpoint || final.GlobalID != initial.GlobalID {
		t.Fatal("large reply replaced or disrupted the MON session", connections, dials.Load(), final)
	}
	t.Logf("one %d-byte JSON request produced %d receive bytes; server code and raw status JSON remained valid on the same MON session", len(padded), responseBytes)
}
