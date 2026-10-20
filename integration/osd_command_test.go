package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type osdCommandNativeInput struct {
	Role    string `json:"role,omitempty"`
	OSD     *int32 `json:"osd,omitempty"`
	PGID    string `json:"pgid,omitempty"`
	Command string `json:"command"`
	Input   []byte `json:"input,omitempty"`
	Timeout int    `json:"osd_op_timeout,omitempty"`
}

type osdCommandReceipt struct {
	Schema        int    `json:"schema_version"`
	Commit        string `json:"source_commit"`
	DaemonVersion string `json:"fixture_daemon_version"`
	Role          string `json:"role"`
	OSD           *int32 `json:"osd"`
	PGID          string `json:"pgid"`
	Code          int32  `json:"code"`
	Data          []byte `json:"data"`
	Message       []byte `json:"message"`
	Scope         struct {
		Independent bool `json:"independent_native_c_api"`
		Resend      bool `json:"native_client_may_resend"`
	} `json:"scope"`
}

func nativeOSDCommand(t *testing.T, ctx context.Context, input osdCommandNativeInput) osdCommandReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil || len(data) > 4<<20 {
		t.Fatal("bounded native OSD command input", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-command-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-command-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-command", "run"); err != nil {
		t.Fatal("native OSD command oracle", err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-command-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err = io.ReadAll(io.LimitReader(f, (6<<20)+1))
	if err != nil || len(data) > 6<<20 {
		t.Fatal("native OSD command receipt exceeds bound", err)
	}
	var result osdCommandReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if result.Schema != 1 || result.Commit != source || !strings.Contains(result.DaemonVersion, "20.2.4") || !result.Scope.Independent || result.Role != input.Role && !(input.Role == "" && result.Role == "test") {
		t.Fatal("native OSD command receipt provenance differs")
	}
	return result
}

// osdDumpAddresses interprets native JSON only in this consumer test.
func osdDumpAddresses(t *testing.T, ctx context.Context, c *cephmsgr.Client) map[int32]string {
	t.Helper()
	result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("native osd dump", err)
	}
	var dump struct {
		OSDs []struct {
			ID     int32 `json:"osd"`
			Public struct {
				Addrvec []struct {
					Type  string `json:"type"`
					Addr  string `json:"addr"`
					Nonce uint32 `json:"nonce"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(result.Data, &dump); err != nil {
		t.Fatal("decode native osd dump", err)
	}
	addresses := map[int32]string{}
	for _, osd := range dump.OSDs {
		for _, addr := range osd.Public.Addrvec {
			if addr.Type == "v2" {
				addresses[osd.ID] = fmt.Sprintf("v2:%s/%d", addr.Addr, addr.Nonce)
			}
		}
	}
	return addresses
}

func TestCephOSDCommandsIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	primary := base.ID
	o := mutationConnection(t, c, ctx, base)
	defer o.Close()
	type caseEvidence struct {
		Name   string                    `json:"name"`
		Native osdCommandReceipt         `json:"native"`
		Go     cephmsgr.OSDCommandResult `json:"go"`
		Err    string                    `json:"go_error,omitempty"`
	}
	evidence := struct {
		Schema    int              `json:"schema_version"`
		Placement osdFixtureOracle `json:"caller_placement"`
		Cases     []caseEvidence   `json:"cases"`
	}{Schema: 1, Placement: base}
	defer func() {
		for i := range evidence.Cases { // keep the bounded receipt small
			for _, data := range []*[]byte{&evidence.Cases[i].Native.Data, &evidence.Cases[i].Go.Data} {
				if len(*data) > 4096 {
					*data = (*data)[:4096]
				}
			}
		}
		data, err := json.Marshal(evidence)
		if err != nil || len(data) > 1<<20 {
			t.Error("bounded OSD command evidence", err)
			return
		}
		if err := os.WriteFile(filepath.Join(dir, "osd-command-results.json"), data, 0644); err != nil {
			t.Error(err)
		}
	}()
	osdID := func(id int32) *int32 { return &id }
	exact := func(t *testing.T, name string, connection *cephmsgr.OSDConnection, role string, target int32, command string, input []byte) {
		t.Helper()
		native := nativeOSDCommand(t, ctx, osdCommandNativeInput{Role: role, OSD: osdID(target), Command: command, Input: input})
		result, err := connection.Command(ctx, cephmsgr.Command{JSON: []byte(command), Input: input})
		var commandErr *cephmsgr.CommandError
		if native.Code < 0 {
			if !errors.As(err, &commandErr) || commandErr.Code != native.Code {
				t.Fatalf("%s: Go/native refusal differs: native=%d go=%v", name, native.Code, err)
			}
		} else if err != nil {
			t.Fatalf("%s: Go OSD command failed where native succeeded: %v", name, err)
		}
		if result.Code != native.Code || result.Message != string(native.Message) || !bytes.Equal(result.Data, native.Data) || result.Submission.OSD != target || result.Submission.Transaction == 0 || result.Submission.GlobalID == 0 {
			t.Fatalf("%s: raw Go output differs from native: code %d/%d message %q/%q data %d/%d bytes", name, result.Code, native.Code, result.Message, native.Message, len(result.Data), len(native.Data))
		}
		t.Logf("%s: code=%d message=%q data=%d bytes match native", name, result.Code, result.Message, len(result.Data))
		evidence.Cases = append(evidence.Cases, caseEvidence{Name: name, Native: native, Go: result, Err: fmt.Sprint(err)})
	}
	bulk := bytes.Repeat([]byte{0, 0xff, 'B', '\n'}, 256<<10) // 1 MiB ignored by version
	for _, tc := range []struct {
		name, command string
		input         []byte
	}{
		{"version", `{"prefix":"version"}`, nil},
		{"version with bulk input", `{"prefix":"version"}`, bulk},
		{"config get", `{"prefix":"config get","var":"osd_max_write_op_reply_len","format":"json"}`, nil},
		{"command descriptions", `{"prefix":"get_command_descriptions"}`, nil},
		{"unknown prefix", `{"prefix":"no such ceph-msgr-go command"}`, nil},
		{"invalid arguments", `{"prefix":"config get"}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) { exact(t, tc.name, o, "test", primary, tc.command, tc.input) })
		if t.Failed() {
			return
		}
	}
	t.Run("readonly capability refusal", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD, options.Identity = true, "client.readonly"
		key, err := os.ReadFile(filepath.Join(dir, "readonly.key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
		clear(key)
		if err != nil {
			t.Fatal("parse fixture readonly key", err)
		}
		reader, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("authenticate explicit readonly identity", err)
		}
		defer reader.Close()
		restricted := mutationConnection(t, reader, ctx, base)
		defer restricted.Close()
		exact(t, "readonly capability refusal", restricted, "readonly", primary, `{"prefix":"version"}`, nil)
		if reply, err := restricted.Request(ctx, osdObjectRequest(base, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})); err != nil || reply.Code != 0 {
			t.Fatal("readonly OSD session did not remain usable for object reads after the command refusal", err)
		}
	})
	pgid := fmt.Sprintf("%d.%x", base.Pool, base.PG)
	if pgid != base.PGID {
		t.Fatal("caller PG metadata differs from native mapped PG", pgid, base.PGID)
	}
	pg := cephmsgr.OSDCommandPG{Pool: uint64(base.Pool), Seed: base.PG, Primary: primary, MapEpoch: base.Epoch}
	query := fmt.Sprintf(`{"prefix":"query","pgid":%q}`, pgid)
	t.Run("PG query on primary", func(t *testing.T) {
		native := nativeOSDCommand(t, ctx, osdCommandNativeInput{PGID: pgid, Command: query})
		result, err := o.PGCommand(ctx, pg, cephmsgr.Command{JSON: []byte(query)})
		if err != nil || native.Code != 0 || result.Submission.PG == nil || *result.Submission.PG != pg {
			t.Fatal("PG query failed or lost submission metadata", native.Code, err)
		}
		// PG query output contains live timestamps; compare identity fields.
		type pgQuery struct {
			State string `json:"state"`
			Info  struct {
				PGID string `json:"pgid"`
			} `json:"info"`
			Acting []int32 `json:"acting"`
		}
		var goQuery, nativeQuery pgQuery
		if json.Unmarshal(result.Data, &goQuery) != nil || json.Unmarshal(native.Data, &nativeQuery) != nil || goQuery.Info.PGID != pgid || nativeQuery.Info.PGID != pgid || goQuery.State != nativeQuery.State || !strings.Contains(goQuery.State, "active") || fmt.Sprint(goQuery.Acting) != fmt.Sprint(nativeQuery.Acting) {
			t.Fatalf("PG query identity differs: go=%+v native=%+v", goQuery, nativeQuery)
		}
		t.Logf("PG %s query state=%s acting=%v matches native pg_command", pgid, goQuery.State, goQuery.Acting)
		evidence.Cases = append(evidence.Cases, caseEvidence{Name: "PG query on primary", Native: native, Go: result})
	})
	t.Run("PG routing refusals are not retried", func(t *testing.T) {
		addresses := osdDumpAddresses(t, ctx, c)
		var replica int32 = -1
		for _, id := range base.Acting {
			if id != primary {
				replica = id
				break
			}
		}
		if replica >= 0 {
			target := base
			target.ID, target.Address = replica, addresses[replica]
			if target.Address == "" {
				t.Fatal("native osd dump lacks the replica v2 address", replica)
			}
			connection := mutationConnection(t, c, ctx, target)
			defer connection.Close()
			// Native Objecter resends an EAGAIN command immediately until its
			// operation timeout (pinned Objecter.cc:5058-5067); Go returns the
			// server's -11 once and never resends.
			native := nativeOSDCommand(t, ctx, osdCommandNativeInput{OSD: osdID(replica), Command: query, Timeout: 5})
			asReplica := pg
			asReplica.Primary = replica
			result, err := connection.PGCommand(ctx, asReplica, cephmsgr.Command{JSON: []byte(query)})
			var commandErr *cephmsgr.CommandError
			if native.Code != -110 || !errors.As(err, &commandErr) || commandErr.Code != -11 || !strings.HasPrefix(result.Message, "not primary for pgid") {
				t.Fatalf("non-primary PG command differs: native=%d %q go=%v %q", native.Code, native.Message, err, result.Message)
			}
			if again, err := connection.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || again.Submission.Transaction <= result.Submission.Transaction {
				t.Fatal("replica session was not usable after a known PG routing refusal", err)
			}
			t.Logf("replica osd.%d: code=%d message=%q (not executed, not retried)", replica, result.Code, result.Message)
			evidence.Cases = append(evidence.Cases, caseEvidence{Name: "PG command on replica", Native: native, Go: result, Err: err.Error()})
		} else {
			t.Log("single-replica profile has no acting replica; non-primary refusal not exercised")
		}
		absent := cephmsgr.OSDCommandPG{Pool: 4095, Seed: 0, Primary: primary}
		command := `{"prefix":"query","pgid":"4095.0"}`
		native := nativeOSDCommand(t, ctx, osdCommandNativeInput{OSD: osdID(primary), Command: command})
		result, err := o.PGCommand(ctx, absent, cephmsgr.Command{JSON: []byte(command)})
		var commandErr *cephmsgr.CommandError
		if native.Code != -2 || !errors.As(err, &commandErr) || commandErr.Code != -2 || result.Message != string(native.Message) {
			t.Fatalf("absent PG command differs: native=%d %q go=%v %q", native.Code, native.Message, err, result.Message)
		}
		evidence.Cases = append(evidence.Cases, caseEvidence{Name: "absent PG", Native: native, Go: result, Err: err.Error()})
	})
	t.Run("concurrent commands and object reads share one session", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, 16)
		tids := make(chan uint64, 16)
		for i := 0; i < 8; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				result, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
				if err == nil && !bytes.Contains(result.Data, []byte("20.2.4")) {
					err = errors.New("concurrent version output lacks the pinned release")
				}
				errs <- err
				tids <- result.Submission.Transaction
			}()
			go func() {
				defer wg.Done()
				reply, err := o.Request(ctx, osdObjectRequest(base, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
				errs <- err
				tids <- reply.Transaction
			}()
		}
		wg.Wait()
		close(errs)
		close(tids)
		seen := map[uint64]bool{}
		for err := range errs {
			if err != nil {
				t.Fatal("concurrent OSD command/object request", err)
			}
		}
		for tid := range tids {
			if tid == 0 || seen[tid] {
				t.Fatal("concurrent OSD command/object TIDs are not unique", tid)
			}
			seen[tid] = true
		}
	})
	t.Run("paused OSD cancellation and Close are uncertain and never resent", func(t *testing.T) {
		name := strconv.FormatInt(int64(primary), 10)
		if err := testcluster.ControlDaemon(ctx, dir, "pause", "osd", name); err != nil {
			t.Fatal("pause primary OSD", err)
		}
		resumed := false
		defer func() {
			if !resumed {
				_ = testcluster.ControlDaemon(context.Background(), dir, "resume", "osd", name)
			}
		}()
		wait, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
		result, err := o.Command(wait, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
		stop()
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || result.Submission.Transaction == 0 || result.Code != 0 || result.Data != nil {
			t.Fatalf("cancelled command to a paused OSD lost uncertainty: %+v %v", result.Submission, err)
		}
		closing := mutationConnection(t, c, ctx, base) // same ID and address: shared handle
		if closing != o {
			t.Fatal("same OSD target did not share the explicit handle")
		}
		done := make(chan error, 1)
		go func() {
			_, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
			done <- err
		}()
		time.Sleep(200 * time.Millisecond)
		_ = o.Close()
		select {
		case err := <-done:
			if !errors.As(err, &unknown) || !errors.Is(err, cephmsgr.ErrClosed) {
				t.Fatal("Close during a sent OSD command lost uncertainty or the Close cause", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not release the pending OSD command")
		}
		if err := testcluster.ControlDaemon(ctx, dir, "resume", "osd", name); err != nil {
			t.Fatal("resume primary OSD", err)
		}
		resumed = true
		fresh := mutationConnection(t, c, ctx, base)
		defer fresh.Close()
		result, err = fresh.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
		if err != nil || !bytes.Contains(result.Data, []byte("20.2.4")) {
			t.Fatal("fresh explicit OSD handle did not serve a command after resume", err)
		}
		t.Log("cancelled and closed commands returned OutcomeUnknownError; no automatic resend or reconnect; fresh handle served version")
	})
}
