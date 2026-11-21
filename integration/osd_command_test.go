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
	if err := os.Remove(filepath.Join(dir, "osd-command-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemonInput(ctx, dir, "verify", "osd-command", "run", testcluster.ControlInput{Name: "osd-command-input.json", Data: data, Private: true}); err != nil {
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

// Raw OSD and PG commands (MCommand 97 / MCommandReply 98; a PG command is the
// same message with `pgid` in its JSON and no map epoch) are compared with
// native rados_osd_command and rados_pg_command: output bytes and status match
// for version, config get, get_command_descriptions, invalid commands and an
// `allow r` identity (-EACCES, OSD.cc:7379-7394 requires the allow-all OSD
// cap). A PG query to an acting replica gets -EAGAIN "not primary for pgid"
// and an absent pool -ENOENT without execution (OSD.cc:2789-2826, v20.2.4); Go
// does not resend on EAGAIN, while native Objecter does (Objecter.cc:5058-5067)
// so its call is bounded by rados_osd_op_timeout. A PG command naming another
// primary is refused locally with ErrOSDCommandTarget. A command cancelled,
// closed or timed out after sending, including on a paused primary, ends as an
// unknown outcome with its submission; one on a closed handle is ErrClosed.
// Not verified: counting server-side executions.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0 key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 test_run=^TestCephOSDCommand(s|Crash)Integration$
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-suite-b300c0 key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 test_run=^TestCephOSDCommand(s|Crash)Integration$
//scenario:covers CLIENT to OSD
//fixture:isolated pauses the primary OSD and resumes it without waiting for OSD up or PGs active+clean
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
		// A PG command whose explicit primary is not this connection's OSD is
		// a local refusal: nothing is submitted, unlike the daemon refusals
		// above and below, which are completed replies.
		elsewhere := pg
		elsewhere.Primary = primary + 1
		var unsent *cephmsgr.OutcomeUnknownError
		if result, err := o.PGCommand(ctx, elsewhere, cephmsgr.Command{JSON: []byte(query)}); !errors.Is(err, cephmsgr.ErrOSDCommandTarget) || errors.As(err, &unsent) ||
			result.Submission != (cephmsgr.OSDCommandSubmission{}) || result.Code != 0 || result.Data != nil {
			t.Fatalf("PG command naming another primary was not refused before transmission: %+v %v", result.Submission, err)
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
	t.Run("written commands ended by the caller are uncertain and never resent", func(t *testing.T) {
		// Each case has its own client. The command is read-only and padded,
		// so its one local write is recognizable without decoding a frame:
		// "sent" means that this write returned and that bytes then arrived
		// on its connection and were withheld. Nothing is attributed to a
		// reply and no daemon execution is counted.
		for _, form := range []string{"Command", "PGCommand"} {
			for _, ending := range []string{"cancel", "OSDConnection.Close", "Client.Close"} {
				t.Run(form+" "+ending, func(t *testing.T) {
					hold := newCommandHold(4096, daemonEndpoint(base.Address))
					options := integrationOptions(t)
					options.EnableOSD = true
					options.DialContext = hold.dialer(options.DialContext)
					subject, err := cephmsgr.Dial(ctx, options)
					if err != nil {
						t.Fatal(err)
					}
					defer subject.Close()
					handle := mutationConnection(t, subject, ctx, base)
					defer handle.Close()
					call := func(ctx context.Context, connection *cephmsgr.OSDConnection, padding int) (cephmsgr.OSDCommandResult, error) {
						if form == "PGCommand" {
							return connection.PGCommand(ctx, pg, cephmsgr.Command{JSON: []byte(query + strings.Repeat(" ", padding))})
						}
						return connection.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}` + strings.Repeat(" ", padding))})
					}
					// Control: the unpadded command is answered and no write on the
					// OSD connection reached the padded size.
					if result, err := call(ctx, handle, 0); err != nil || result.Code != 0 || len(result.Data) == 0 {
						t.Fatal("read-only control command", err, result.Code)
					}
					if dials, large, bound := hold.counts(); dials != 1 || large != 0 || bound != 0 {
						t.Fatal("a write before the subject command reached the padded size, or the OSD was dialed more than once", dials, large, bound)
					}
					hold.armOnWrite()
					wait, stop := context.WithCancel(ctx)
					defer stop()
					type outcome struct {
						result cephmsgr.OSDCommandResult
						err    error
					}
					finished := make(chan outcome, 1)
					go func() {
						result, err := call(wait, handle, 40<<10)
						finished <- outcome{result, err}
					}()
					for _, signal := range []struct {
						name string
						done <-chan struct{}
					}{{"issued", hold.bound}, {"returned from its local write", hold.wrote}, {"followed by withheld received bytes", hold.received}} {
						select {
						case <-signal.done:
						case got := <-finished:
							t.Fatal("the padded command ended before it was "+signal.name, got.err)
						case <-ctx.Done():
							t.Fatal("the padded command was never "+signal.name, ctx.Err())
						}
					}
					// Complete: every byte of the one padded write was accepted
					// without an error.
					if dials, large, bound := hold.counts(); dials != 1 || large != 1 || bound != 1 || hold.completeWrites() != 1 {
						t.Fatal("the padded command was not written exactly once and in full on the one OSD connection", dials, large, bound, hold.completeWrites())
					}
					select {
					case got := <-finished:
						t.Fatal("the caller ended while every received byte was withheld", got.err)
					default:
					}
					want := cephmsgr.ErrClosed
					switch ending {
					case "cancel":
						want = context.Canceled
						stop()
					case "OSDConnection.Close":
						if err := handle.Close(); err != nil {
							t.Fatal(err)
						}
					case "Client.Close":
						if err := subject.Close(); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case got := <-finished:
						submission := got.result.Submission
						if !unknownOutcome(got.result.Result, got.err) || !errors.Is(got.err, want) || submission.Transaction == 0 || submission.OSD != primary ||
							(form == "PGCommand") != (submission.PG != nil) || submission.PG != nil && *submission.PG != pg {
							t.Fatalf("a written %s ended by %s lost its unknown outcome, its cause or its submission: %+v %v", form, ending, submission, got.err)
						}
						t.Logf("%s ended by %s after its one padded write: transaction %d: %v", form, ending, submission.Transaction, got.err)
					case <-ctx.Done():
						t.Fatal("ending the caller did not release the written command", ctx.Err())
					}
					// A canceled call leaves the handle on a connection that can
					// deliver nothing; its owner ends it. The closed handle then
					// refuses both forms before anything is submitted.
					handle.Close()
					if result, err := call(ctx, handle, 0); !errors.Is(err, cephmsgr.ErrClosed) || !knownUnsent(result.Result, err) || result.Submission != (cephmsgr.OSDCommandSubmission{}) {
						t.Fatalf("%s on the closed handle was not a known-unsent refusal: %+v %v", form, result.Submission, err)
					}
					redials := 1
					if ending == "Client.Close" {
						if _, err := subject.OpenOSD(ctx, cephmsgr.OSDTarget{ID: base.ID, Address: base.Address}); !errors.Is(err, cephmsgr.ErrClosed) {
							t.Fatal("the closed client opened an OSD connection", err)
						}
					} else {
						// Only an explicit new handle reaches the OSD again.
						redials = 2
						fresh := mutationConnection(t, subject, ctx, base)
						defer fresh.Close()
						if result, err := call(ctx, fresh, 0); err != nil || result.Code != 0 || len(result.Data) == 0 {
							t.Fatal("an explicitly reopened handle did not serve the command", err, result.Code)
						}
					}
					// Nothing wrote the padded command again or dialed the OSD
					// without the caller.
					if dials, large, bound := hold.counts(); dials != redials || large != 1 || bound != 1 {
						t.Fatal("the padded command was written again, or the OSD was dialed without the caller", dials, large, bound)
					}
				})
			}
		}
	})
	t.Run("paused OSD cancellation and Close are uncertain and never resent", func(t *testing.T) {
		// A client of its own, so that its one OSD connection is observed.
		observed := newCommandHold(4096, daemonEndpoint(base.Address))
		options := integrationOptions(t)
		options.EnableOSD = true
		options.DialContext = observed.dialer(options.DialContext)
		subject, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer subject.Close()
		o := mutationConnection(t, subject, ctx, base)
		defer o.Close()
		if result, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || result.Code != 0 {
			t.Fatal("command before the pause", err)
		}
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
		// The commands to the paused primary are padded, so each local write
		// is counted on the connection without decoding a frame. The hold is
		// never armed: it withholds nothing here, the paused daemon answers
		// nothing by itself. Three commands of a little over 8 KiB stay
		// within the socket buffers of a peer that reads nothing.
		padding := strings.Repeat(" ", 8<<10)
		if _, large, _ := observed.counts(); large != 0 {
			t.Fatal("a write before the paused commands reached the padded size", large)
		}
		wait, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
		result, err := o.Command(wait, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}` + padding)})
		stop()
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || result.Submission.Transaction == 0 || result.Code != 0 || result.Data != nil || observed.awaitReturned(ctx, 1) != 1 {
			t.Fatalf("cancelled command to a paused OSD lost uncertainty or was not written once: %+v %v", result.Submission, err)
		}
		// The same for a PG command to the paused primary.
		wait, stop = context.WithTimeout(ctx, 1500*time.Millisecond)
		result, err = o.PGCommand(wait, pg, cephmsgr.Command{JSON: []byte(query + padding)})
		stop()
		if !errors.As(err, &unknown) || !errors.Is(err, context.DeadlineExceeded) || result.Submission.Transaction == 0 ||
			result.Submission.PG == nil || *result.Submission.PG != pg || result.Code != 0 || result.Data != nil || observed.awaitReturned(ctx, 2) != 2 {
			t.Fatalf("cancelled PG command to a paused OSD lost uncertainty, its submission or its one write: %+v %v", result.Submission, err)
		}
		closing := mutationConnection(t, subject, ctx, base) // same ID and address: shared handle
		if closing != o {
			t.Fatal("same OSD target did not share the explicit handle")
		}
		done := make(chan error, 1)
		go func() {
			_, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}` + padding)})
			done <- err
		}()
		// Close only after the third padded write returned.
		if returned := observed.awaitReturned(ctx, 3); returned != 3 || observed.completeWrites() != 3 {
			t.Fatal("the command to close was never written, or a padded write was short or failed", returned, observed.completeWrites())
		}
		select {
		case err := <-done:
			t.Fatal("the paused OSD ended a command by itself", err)
		default:
		}
		_ = o.Close()
		select {
		case err := <-done:
			if !errors.As(err, &unknown) || !errors.Is(err, cephmsgr.ErrClosed) {
				t.Fatal("Close during a sent OSD command lost uncertainty or the Close cause", err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not release the pending OSD command")
		}
		// The closed handle is an unavailable target, not an uncertain one:
		// both command forms are refused before anything is submitted.
		for name, call := range map[string]func() (cephmsgr.OSDCommandResult, error){
			"Command": func() (cephmsgr.OSDCommandResult, error) {
				return o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
			},
			"PGCommand": func() (cephmsgr.OSDCommandResult, error) {
				return o.PGCommand(ctx, pg, cephmsgr.Command{JSON: []byte(query)})
			},
		} {
			if result, err := call(); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, new(*cephmsgr.OutcomeUnknownError)) ||
				result.Submission != (cephmsgr.OSDCommandSubmission{}) {
				t.Fatalf("%s on the closed OSD handle was not a known-unsent refusal: %+v %v", name, result.Submission, err)
			}
		}
		if err := testcluster.ControlDaemon(ctx, dir, "resume", "osd", name); err != nil {
			t.Fatal("resume primary OSD", err)
		}
		resumed = true
		// Until here the client wrote each padded command once and dialed
		// the OSD once: nothing was resent and nothing reconnected.
		if dials, large, _ := observed.counts(); dials != 1 || large != 3 {
			t.Fatal("a command to the paused OSD was written again, or the OSD was dialed without the caller", dials, large)
		}
		fresh := mutationConnection(t, subject, ctx, base)
		defer fresh.Close()
		result, err = fresh.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)})
		if err != nil || !bytes.Contains(result.Data, []byte("20.2.4")) {
			t.Fatal("fresh explicit OSD handle did not serve a command after resume", err)
		}
		if dials, large, _ := observed.counts(); dials != 2 || large != 3 {
			t.Fatal("the explicit new handle did not dial once, or a paused command was written again after the resume", dials, large)
		}
		t.Log("cancelled and closed commands returned OutcomeUnknownError after their one observed padded write; no resend or reconnect; fresh handle served version")
	})
}
