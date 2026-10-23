package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// probe drives one disposable MON through bounded phases while recording the
// client-side identities needed to correlate a packet capture. It uses only
// the public cephmsgr API and read-only MON commands.

type probeOptions struct {
	Monitor       string
	Closed        string
	Entity        string
	KeyFile       string
	Mode          string
	TracePath     string
	RestartSignal string
	RestartDone   string
	Substitute    string
}

type recorder struct {
	mu    sync.Mutex
	trace traceFile
	phase string
}

type tracedConn struct {
	net.Conn
	rec  *recorder
	idx  int
	once sync.Once
}

func (c *tracedConn) Close() error {
	c.once.Do(func() {
		c.rec.mu.Lock()
		c.rec.trace.Connections[c.idx].Closed = time.Now().UTC()
		c.rec.mu.Unlock()
	})
	return c.Conn.Close()
}

func (r *recorder) dialer() func(context.Context, string, string) (net.Conn, error) {
	d := &net.Dialer{}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		start := time.Now().UTC()
		conn, err := d.DialContext(ctx, network, address)
		r.mu.Lock()
		defer r.mu.Unlock()
		tc := traceConn{Phase: r.phase, Network: network, Target: address, DialStart: start, DialEnd: time.Now().UTC()}
		if err != nil {
			tc.DialError = err.Error()
			r.trace.Connections = append(r.trace.Connections, tc)
			return nil, err
		}
		tc.Local, tc.Remote = conn.LocalAddr().String(), conn.RemoteAddr().String()
		r.trace.Connections = append(r.trace.Connections, tc)
		return &tracedConn{Conn: conn, rec: r, idx: len(r.trace.Connections) - 1}, nil
	}
}

func (r *recorder) begin(name string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phase = name
	return time.Now().UTC()
}

func (r *recorder) end(name string, start time.Time, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trace.Phases = append(r.trace.Phases, tracePhase{name, start, time.Now().UTC(), result})
}

func (r *recorder) command(ctx context.Context, c *cephmsgr.Client, phase, prefix string, attempt int) error {
	start := time.Now().UTC()
	res, err := c.MonCommand(ctx, cephmsgr.Command{JSON: json.RawMessage(fmt.Sprintf(`{"prefix":%q,"format":"json"}`, prefix))})
	tc := traceCommand{Phase: phase, Prefix: prefix, Attempt: attempt, Started: start, Ended: time.Now().UTC(), Code: res.Code, Bytes: len(res.Data)}
	if err != nil {
		tc.Error = err.Error()
	}
	r.mu.Lock()
	r.trace.Commands = append(r.trace.Commands, tc)
	r.mu.Unlock()
	return err
}

func runProbe(o probeOptions) error {
	raw, err := os.ReadFile(o.KeyFile)
	if err != nil {
		return err
	}
	key, err := cephmsgr.ParseKey(strings.TrimSpace(string(raw)))
	if err != nil {
		return err
	}
	mode := cephmsgr.SecureMode
	switch o.Mode {
	case "secure":
	case "crc":
		mode = cephmsgr.CRCMode
	default:
		return errors.New("mode must be crc or secure")
	}
	rec := &recorder{trace: traceFile{Tool: "msgrcap probe (development-only)", Substitute: o.Substitute,
		Mode: o.Mode, Entity: o.Entity, Monitor: o.Monitor, Started: time.Now().UTC()}}
	options := func(monitor string, timeout time.Duration) cephmsgr.Options {
		return cephmsgr.Options{Monitors: []string{monitor}, Identity: o.Entity, Key: key, ConnectionMode: mode,
			ConnectTimeout: timeout, DialContext: rec.dialer()}
	}
	write := func() error {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.trace.Finished = time.Now().UTC()
		data, err := json.MarshalIndent(rec.trace, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(o.TracePath, append(data, '\n'), 0o644)
	}
	defer func() { _ = write() }()

	// Phase 1: no TCP connection. A closed port on the MON host must fail
	// before any Messenger byte is exchanged.
	if o.Closed != "" {
		start := rec.begin("refused")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, err := cephmsgr.Dial(ctx, options(o.Closed, 2*time.Second))
		cancel()
		result := "expected failure: " + fmt.Sprint(err)
		if err == nil {
			c.Close()
			result = "unexpected success"
		}
		rec.end("refused", start, result)
	}

	// Phase 2: one authenticated MON session and one read-only command.
	start := rec.begin("command")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := cephmsgr.Dial(ctx, options(o.Monitor, 10*time.Second))
	cancel()
	if err != nil {
		rec.end("command", start, "dial failed: "+err.Error())
		return err
	}
	defer client.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	err = rec.command(ctx, client, "command", "mon_status", 1)
	cancel()
	rec.end("command", start, resultText(err))
	if err != nil {
		return err
	}

	// Phase 3: the workflow restarts the MON (peer close). Retries below are
	// explicit caller-directed attempts of a read-only command, never replay
	// of an uncertain mutation.
	if o.RestartSignal != "" {
		start = rec.begin("peer-restart")
		if err := os.WriteFile(o.RestartSignal, []byte("restart\n"), 0o644); err != nil {
			return err
		}
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if _, err := os.Stat(o.RestartDone); err == nil {
				break
			}
			if time.Now().After(deadline) {
				rec.end("peer-restart", start, "restart was not signalled")
				return errors.New("restart was not signalled")
			}
			time.Sleep(200 * time.Millisecond)
		}
		start2 := rec.begin("after-restart")
		rec.end("peer-restart", start, "MON restart signalled by the workflow")
		var last error
		for attempt := 1; attempt <= 20; attempt++ {
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			last = rec.command(ctx, client, "after-restart", "mon_status", attempt)
			cancel()
			if last == nil {
				break
			}
			time.Sleep(time.Second)
		}
		rec.end("after-restart", start2, resultText(last))
		if last != nil {
			return last
		}
	}
	start = rec.begin("close")
	err = client.Close()
	rec.end("close", start, resultText(err))
	return nil
}

func resultText(err error) string {
	if err == nil {
		return "ok"
	}
	return "error: " + err.Error()
}
