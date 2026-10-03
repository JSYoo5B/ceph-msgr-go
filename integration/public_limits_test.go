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

type publicLimitConn struct {
	net.Conn
	written *atomic.Uint64
}

func (conn *publicLimitConn) Write(p []byte) (int, error) {
	n, err := conn.Conn.Write(p)
	conn.written.Add(uint64(n))
	return n, err
}

func TestCephPublicLimitErrorsIntegration(t *testing.T) {
	ordinaryLogFixture(t)
	// Reserve room for bootstrap maps containing MGR module metadata.
	const limit = 256 << 10
	options := integrationOptions(t)
	options.MaxFrameSize = limit
	options.Monitors = options.Monitors[:1]
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var dials atomic.Uint32
	var written atomic.Uint64
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		dials.Add(1)
		return &publicLimitConn{Conn: conn, written: &written}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate MON with a 256 KiB logical frame bound", err)
	}
	defer c.Close()
	initial := c.Snapshot()
	if !initial.Monitor.Ready || len(initial.Monitor.Members) == 0 || initial.Manager.Ready {
		t.Fatal("limited client did not bootstrap without a MGR connection", initial)
	}
	status := []byte(`{"prefix":"status","format":"json"}`)
	checkStatus := func(result cephmsgr.Result, err error) {
		t.Helper()
		var reply struct{ FSID string }
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &reply) != nil || reply.FSID != initial.FSID {
			t.Fatal("small native status did not preserve server success and cluster identity", result.Code, len(result.Data), err)
		}
		if len(result.Data) <= 256 {
			t.Fatal("native status is too small to exceed the reply bound after command echo", len(result.Data))
		}
	}
	checkStatus(c.MonCommand(ctx, cephmsgr.Command{JSON: status}))
	connections := dials.Load()
	target := initial.Monitor.Members[0].Name
	for _, route := range []struct {
		name  string
		call  func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		query []byte
	}{
		{"MON command", c.MonCommand, status},
		{"MGR command", c.MgrCommand, []byte(`{"prefix":"pg stat","format":"json"}`)},
		{"MON tell", c.MonTell, []byte(`{"prefix":"version","format":"json"}`)},
		{"MGR tell", c.MgrTell, []byte(`{"prefix":"version","format":"json"}`)},
		{"named MON tell", func(ctx context.Context, command cephmsgr.Command) (cephmsgr.Result, error) {
			return c.MonTellTo(ctx, target, command)
		}, []byte(`{"prefix":"version","format":"json"}`)},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, input := range []bool{false, true} {
				name := "JSON"
				if input {
					name = "JSON and Input"
				}
				t.Run(name, func(t *testing.T) {
					command := cephmsgr.Command{JSON: bytes.Clone(route.query)}
					excess := limit - 256 + 1 - len(command.JSON)
					if input {
						command.Input = make([]byte, excess)
					} else {
						command.JSON = append(command.JSON, bytes.Repeat([]byte{' '}, excess)...)
					}
					if !json.Valid(command.JSON) || len(command.JSON)+len(command.Input)+256 != limit+1 {
						t.Fatal("outbound limit probe did not exceed only the local estimate")
					}
					result, err := route.call(ctx, command)
					var unknown *cephmsgr.OutcomeUnknownError
					if !errors.Is(err, cephmsgr.ErrLimitExceeded) || errors.Is(err, cephmsgr.ErrMalformedMessage) || errors.As(err, &unknown) || result.Code != 0 || result.Message != "" || len(result.Data) != 0 {
						t.Fatal("unsent oversized command lost its public limit classification", result.Code, err)
					}
					if dials.Load() != connections || c.Snapshot().Manager.Ready {
						t.Fatal("local limit rejection opened an endpoint", connections, dials.Load())
					}
				})
			}
		})
	}

	// Tentacle accepts trailing JSON whitespace and Monitor::reply_command
	// echoes the original command in MMonCommandAck. The request fits the
	// local estimate exactly, but the valid echoed command plus status data
	// exceeds this client's smaller receive bound. No mutation is submitted.
	padded := append(bytes.Clone(status), bytes.Repeat([]byte{' '}, limit-256-len(status))...)
	if !json.Valid(padded) || len(padded)+256 != limit {
		t.Fatal("incoming limit probe does not fit the outbound estimate")
	}
	before := written.Load()
	_, err = c.MonCommand(ctx, cephmsgr.Command{JSON: padded})
	var unknown *cephmsgr.OutcomeUnknownError
	var server *cephmsgr.CommandError
	if !errors.Is(err, cephmsgr.ErrLimitExceeded) || !errors.As(err, &unknown) || errors.Is(err, cephmsgr.ErrMalformedMessage) || errors.As(err, &server) {
		t.Fatal("valid oversized native reply lost its limit and uncertain outcome", err)
	}
	// Readiness recovery submits no command. The following small read is a
	// receive barrier on the replacement session; the padded read is never
	// reissued by this test. Count successful TCP writes across both sessions
	// so a replay cannot hide behind a new connection.
	if err := c.WaitMonReady(ctx); err != nil {
		t.Fatal("recover the MON after its valid reply exceeded the local bound", err)
	}
	checkStatus(c.MonCommand(ctx, cephmsgr.Command{JSON: status}))
	transmitted := written.Load() - before
	if transmitted < uint64(len(padded)) || transmitted >= 2*uint64(len(padded)) {
		t.Fatal("write budget does not match one padded request plus recovery controls", transmitted, len(padded))
	}
	final := c.Snapshot()
	if dials.Load() <= connections || final.FSID != initial.FSID || final.GlobalID != initial.GlobalID || !final.Monitor.Ready || final.Manager.Ready || final.AuthRejection != nil {
		t.Fatal("valid oversized reply did not recover its authenticated MON identity", connections, dials.Load(), final)
	}
	t.Logf("256 KiB public limit: ten outbound rejections stayed unsent; one %d-byte read-only request returned limit+unknown and recovered without replay (%d TCP bytes including controls)", len(padded), transmitted)
}
