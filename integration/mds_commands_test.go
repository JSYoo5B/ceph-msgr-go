package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeMDSCommands struct {
	Source           string `json:"source_commit"`
	Token            string `json:"token"`
	FSID             string `json:"fsid"`
	FSCID            int32  `json:"fscid"`
	FSName           string `json:"fs_name"`
	MDSGID           uint64 `json:"mds_gid"`
	Identity         string `json:"client_identity"`
	ReadonlyIdentity string `json:"readonly_identity"`
	CommandJSON      struct {
		Version string `json:"version"`
		Invalid string `json:"invalid"`
	} `json:"command_json"`
	InputHex string `json:"input_hex"`
}

type nativeMDSCommandReceipt struct {
	Source        string          `json:"source_commit"`
	Token         string          `json:"token"`
	RequestID     int64           `json:"request_id"`
	FSID          string          `json:"fsid"`
	FSCID         int32           `json:"fscid"`
	FSName        string          `json:"fs_name"`
	MDSGID        uint64          `json:"mds_gid"`
	Identity      string          `json:"identity"`
	CommandJSON   string          `json:"command_json"`
	InputHex      string          `json:"input_hex"`
	Code          int32           `json:"code"`
	DataHex       string          `json:"data_hex"`
	Status        string          `json:"status"`
	StatusUTF8Hex string          `json:"status_utf8_hex"`
	GlobalID      uint64          `json:"native_global_id"`
	Complete      bool            `json:"complete"`
	Process       json.RawMessage `json:"native_process,omitempty"`
	Error         string          `json:"error,omitempty"`
}

func mdsCommandNative(ctx context.Context, dir string, scope nativeMDSCommands, action string) (nativeMDSCommandReceipt, error) {
	ctx, stop := context.WithTimeout(ctx, 25*time.Second)
	defer stop()
	var result nativeMDSCommandReceipt
	path := filepath.Join(dir, "mds-commands-native-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.RequestControl(ctx, dir, "mds-commands-native", "mds-commands-native-completed", scope.Token, action); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	identity, command := scope.Identity, scope.CommandJSON.Version
	if action == "readonly-version" {
		identity = scope.ReadonlyIdentity
	}
	if action == "invalid" {
		command = scope.CommandJSON.Invalid
	}
	status, err := hex.DecodeString(result.StatusUTF8Hex)
	if err != nil || string(status) != result.Status || result.Source != mdsFreshSource || result.Token != scope.Token || result.RequestID <= 0 || result.FSID != scope.FSID || result.FSCID != scope.FSCID || result.FSName != scope.FSName || result.MDSGID != scope.MDSGID || result.Identity != identity || result.CommandJSON != command || result.InputHex != scope.InputHex || result.GlobalID == 0 || !result.Complete {
		return result, errors.New("native MDS command receipt scope, raw command or status differs")
	}
	if _, err := hex.DecodeString(result.DataHex); err != nil {
		return result, errors.New("native MDS command output is not bounded hex")
	}
	return result, nil
}

type nativeMDSCommandSessions struct {
	nativeMDSFreshSessions
	GlobalIDs  []uint64 `json:"expected_global_ids"`
	PresentIDs []uint64 `json:"present_global_ids"`
}

func mdsCommandSessions(ctx context.Context, dir string, scope nativeMDSCommands, full, readonly uint64) (nativeMDSCommandSessions, error) {
	var result nativeMDSCommandSessions
	path := filepath.Join(dir, "mds-fresh-session-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.RequestControl(ctx, dir, "mds-fresh-observe", "mds-fresh-observed", scope.Token, strconv.FormatUint(full, 10), strconv.FormatUint(readonly, 10)); err != nil {
		return result, err
	}
	if err := readMDSFreshJSON(path, &result); err != nil {
		return result, err
	}
	if result.Source != mdsFreshSource || result.Token != scope.Token || result.RequestID <= 0 || result.FSID != scope.FSID || result.FSCID != scope.FSCID || result.FSName != scope.FSName || result.MDSGID != scope.MDSGID || result.Rank != 0 || result.GlobalID != full || result.Present || !reflect.DeepEqual(result.GlobalIDs, []uint64{full, readonly}) || len(result.PresentIDs) != 0 {
		return result, errors.New("native MDS session absence receipt scope or bound identities differ")
	}
	var sessions []struct{ ID uint64 }
	if err := json.Unmarshal(result.Sessions, &sessions); err != nil || len(sessions) > 1024 {
		return result, errors.New("native MDS command session listing is invalid")
	}
	for _, s := range sessions {
		if s.ID == full || s.ID == readonly {
			return result, errors.New("MDS command client acquired a CephFS session")
		}
	}
	return result, nil
}

type mdsCommandDials struct {
	MDS, OSD atomic.Uint32
	mu       sync.Mutex
	relays   []*testcluster.StreamRelay
}

func (d *mdsCommandDials) latest(t *testing.T) *testcluster.StreamRelay {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.relays) == 0 {
		t.Fatal("MDS relay was not installed")
	}
	return d.relays[len(d.relays)-1]
}

// mdsCommandClient dials an MDS-only client whose MDS connections pass through
// counted, closable stream relays. configure may adjust options (for example
// EnableCephFS) before Dial; the dial wrapper and limits are fixed here.
func mdsCommandClient(t *testing.T, ctx context.Context, identity, keyPath, endpoint string, enabled bool, configure ...func(*cephmsgr.Options)) (*cephmsgr.Client, *mdsCommandDials) {
	t.Helper()
	options := integrationOptions(t)
	encoded, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal("read dedicated command credential", err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	want, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != want {
		t.Fatal("dedicated command credential does not match selected key profile")
	}
	options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	options.Identity, options.EnableMDS, options.MaxMDSConnections = identity, enabled, 1
	options.EnableOSD, options.EnableMDSMaps = false, false
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Hour, 2*time.Hour
	options.ConnectTimeout = 5 * time.Second
	options.MaxFrameSize = 1 << 20
	for _, f := range configure {
		f(&options)
	}
	dials := &mdsCommandDials{}
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, address string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(address)
		if strings.HasPrefix(role, "osd.") {
			dials.OSD.Add(1)
			return nil, errors.New("MDS-only acceptance attempted an OSD connection")
		}
		if address != endpoint {
			return dial(call, network, address)
		}
		dials.MDS.Add(1)
		remote, err := dial(call, network, address)
		if err != nil {
			return nil, err
		}
		relay := testcluster.NewStreamRelay(remote)
		dials.mu.Lock()
		dials.relays = append(dials.relays, relay)
		dials.mu.Unlock()
		return relay, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate MDS-only command client", err)
	}
	t.Cleanup(func() {
		c.Close()
		dials.mu.Lock()
		defer dials.mu.Unlock()
		for _, relay := range dials.relays {
			relay.Close()
			relay.Wait()
		}
	})
	return c, dials
}

type mdsCommandCase struct {
	Name                                string                    `json:"name"`
	Command                             cephmsgr.Command          `json:"command"`
	Result                              cephmsgr.MDSCommandResult `json:"result"`
	Error                               string                    `json:"error,omitempty"`
	Before, After                       *cephmsgr.TicketState     `json:",omitempty"`
	WriteChunksBefore, WriteChunksAfter uint64                    `json:",omitempty"`
}

func TestCephMDSCommandsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_COMMANDS") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_COMMANDS=1 for owned raw MDS command acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("MDS-only command acceptance requires its owned legacy/no-HASHPSPOOL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	var scope nativeMDSCommands
	var fresh nativeMDSFresh
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-commands-oracle.json"), &scope); err != nil {
		t.Fatal(err)
	}
	if err := readMDSFreshJSON(filepath.Join(dir, "mds-fresh-oracle.json"), &fresh); err != nil {
		t.Fatal(err)
	}
	if scope.Source != mdsFreshSource || scope.Token != fresh.Token || scope.FSID != os.Getenv("CEPH_MSGR_FSID") || scope.FSID != fresh.FSID || scope.FSCID != fresh.FSCID || scope.FSName != fresh.FSName || scope.MDSGID != fresh.MDS.GID || fresh.MDS.Rank != 0 || fresh.MDS.State != "up:active" || scope.Identity != "client.mds-commands-"+scope.Token || scope.Identity != os.Getenv("CEPH_MSGR_MDS_COMMANDS_IDENTITY") || scope.ReadonlyIdentity != fresh.ClientIdentity || scope.ReadonlyIdentity != os.Getenv("CEPH_MSGR_MDS_FRESH_IDENTITY") {
		t.Fatal("owned MDS command and active-daemon oracle identities differ")
	}
	input, err := hex.DecodeString(scope.InputHex)
	if err != nil || !bytes.Equal(input, []byte{0, 255, 'm', 'd', 's', 0}) || !json.Valid([]byte(scope.CommandJSON.Version)) || !json.Valid([]byte(scope.CommandJSON.Invalid)) {
		t.Fatal("fixed native command/input oracle differs")
	}
	target := cephmsgr.MDSTarget{FilesystemID: scope.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce)}
	full, fullDials := mdsCommandClient(t, ctx, scope.Identity, os.Getenv("CEPH_MSGR_MDS_COMMANDS_KEY_FILE"), fresh.MDS.Endpoint, true)
	readonly, readDials := mdsCommandClient(t, ctx, scope.ReadonlyIdentity, os.Getenv("CEPH_MSGR_MDS_FRESH_KEY_FILE"), fresh.MDS.Endpoint, true)
	fullID, readID := full.Snapshot().GlobalID, readonly.Snapshot().GlobalID
	if fullID == 0 || readID == 0 || fullID == readID {
		t.Fatal("independent command clients did not retain distinct authenticated identities")
	}
	connection, err := full.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	roConnection, err := readonly.OpenMDS(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	var cases []mdsCommandCase
	natives := map[string]nativeMDSCommandReceipt{}
	var before, after nativeMDSCommandSessions
	var baseline cephmsgr.MDSCommandResult
	closedHandles := uint32(0)
	t.Cleanup(func() {
		if connection != nil {
			connection.Close()
		}
		roConnection.Close()
		data, err := json.Marshal(struct {
			Scope                                                      nativeMDSCommands  `json:"scope"`
			Target                                                     cephmsgr.MDSTarget `json:"target"`
			FullGlobalID, ReadonlyGlobalID                             uint64
			Native                                                     map[string]nativeMDSCommandReceipt `json:"native"`
			Before, After                                              nativeMDSCommandSessions
			Cases                                                      []mdsCommandCase `json:"cases"`
			FullMDSDials, ReadonlyMDSDials, OSDDials, ExplicitlyClosed uint32
		}{scope, target, fullID, readID, natives, before, after, cases, fullDials.MDS.Load(), readDials.MDS.Load(), fullDials.OSD.Load() + readDials.OSD.Load(), closedHandles})
		if err == nil && len(data) > 4<<20 {
			err = errors.New("MDS command evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-commands-results.json"), data)
		}
		if err != nil {
			t.Error("preserve MDS command public evidence", err)
		}
	})
	version := cephmsgr.Command{JSON: []byte(scope.CommandJSON.Version), Input: input}
	invalid := cephmsgr.Command{JSON: []byte(scope.CommandJSON.Invalid), Input: input}
	for _, action := range []string{"version", "invalid", "readonly-version"} {
		native, e := mdsCommandNative(ctx, dir, scope, action)
		natives[action] = native
		if e != nil {
			t.Fatal(action, e)
		}
		if native.GlobalID == fullID || native.GlobalID == readID {
			t.Fatal("native command oracle reused a Go client identity")
		}
	}
	before, err = mdsCommandSessions(ctx, dir, scope, fullID, readID)
	if err != nil {
		t.Fatal("native session absence before commands", err)
	}
	check := func(t *testing.T, name string, result cephmsgr.MDSCommandResult, callErr error, command cephmsgr.Command, native nativeMDSCommandReceipt, gid uint64) {
		t.Helper()
		data, _ := hex.DecodeString(native.DataHex)
		status, _ := hex.DecodeString(native.StatusUTF8Hex)
		var server *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		var rejection *cephmsgr.AuthenticationError
		if result.Code != native.Code || !bytes.Equal(result.Data, data) || !bytes.Equal([]byte(result.Message), status) || result.Submission.Transaction == 0 || result.Submission.GlobalID != gid || result.Submission.ConnectionGeneration == 0 || result.Submission.Target != target || errors.As(callErr, &unknown) || errors.As(callErr, &rejection) || native.Code >= 0 && callErr != nil || native.Code < 0 && (!errors.As(callErr, &server) || server.Code != native.Code || server.Message != result.Message) {
			t.Fatalf("MDS raw result/identity differs from native: code=%d bytes=%d tid=%d error=%v", result.Code, len(result.Data), result.Submission.Transaction, callErr)
		}
		entry := mdsCommandCase{Name: name, Command: command, Result: result}
		if callErr != nil {
			entry.Error = callErr.Error()
		}
		cases = append(cases, entry)
	}
	if !t.Run("native_version_raw_bytes", func(t *testing.T) {
		if natives["version"].Code != 0 {
			t.Fatal("native version failed", natives["version"].Code)
		}
		var e error
		baseline, e = connection.Command(ctx, version)
		check(t, "version", baseline, e, version, natives["version"], fullID)
	}) {
		return
	}
	if !t.Run("signed_unknown_prefix", func(t *testing.T) {
		if natives["invalid"].Code >= 0 {
			t.Fatal("native unknown command unexpectedly succeeded")
		}
		r, e := connection.Command(ctx, invalid)
		check(t, "invalid", r, e, invalid, natives["invalid"], fullID)
	}) {
		return
	}
	if !t.Run("readonly_permission_denial", func(t *testing.T) {
		if natives["readonly-version"].Code != -13 {
			t.Fatal("native read-only command did not return EACCES")
		}
		r, e := roConnection.Command(ctx, version)
		check(t, "readonly", r, e, version, natives["readonly-version"], readID)
	}) {
		return
	}
	if !t.Run("concurrent_transactions_and_raw_outputs", func(t *testing.T) {
		type result struct {
			command cephmsgr.Command
			native  nativeMDSCommandReceipt
			reply   cephmsgr.MDSCommandResult
			err     error
		}
		done := make(chan result, 8)
		for i := 0; i < cap(done); i++ {
			go func(i int) {
				command, native := version, natives["version"]
				if i%2 != 0 {
					command, native = invalid, natives["invalid"]
				}
				r, e := connection.Command(ctx, command)
				done <- result{command, native, r, e}
			}(i)
		}
		seen := map[uint64]bool{}
		for range cap(done) {
			select {
			case got := <-done:
				check(t, "concurrent", got.reply, got.err, got.command, got.native, fullID)
				s := got.reply.Submission
				if seen[s.Transaction] || s.ConnectionGeneration != baseline.Submission.ConnectionGeneration {
					t.Fatal("concurrent reply reused identity")
				}
				seen[s.Transaction] = true
			case <-ctx.Done():
				t.Fatal("concurrent MDS commands did not finish", ctx.Err())
			}
		}
	}) {
		return
	}
	if !t.Run("target_cache_limits_and_disabled_admission", func(t *testing.T) {
		dials := fullDials.MDS.Load()
		same := target
		same.Address = strings.TrimPrefix(target.Address, "v2:")
		cached, e := full.OpenMDS(ctx, same)
		if e != nil || cached != connection {
			t.Fatal("equivalent exact target did not reuse handle", e)
		}
		wrong := target
		wrong.Address = fmt.Sprintf("v2:%s/%d", fresh.MDS.Endpoint, fresh.MDS.Nonce^1)
		if _, e := full.OpenMDS(ctx, wrong); !errors.Is(e, cephmsgr.ErrMDSTargetChanged) {
			t.Fatal("different target replaced cached handle", e)
		}
		limited := target
		limited.Rank++
		if _, e := full.OpenMDS(ctx, limited); !errors.Is(e, cephmsgr.ErrMDSConnectionLimit) {
			t.Fatal("MDS handle limit not enforced", e)
		}
		for _, bad := range []cephmsgr.MDSTarget{{FilesystemID: -1, Rank: 0, Address: target.Address}, {FilesystemID: scope.FSCID, Rank: -1, Address: target.Address}, {FilesystemID: scope.FSCID, Rank: 0, Address: fresh.MDS.Endpoint}} {
			if _, e := full.OpenMDS(ctx, bad); e == nil {
				t.Fatal("malformed target admitted")
			}
		}
		if fullDials.MDS.Load() != dials {
			t.Fatal("rejected target reached the network")
		}
		disabled, stats := mdsCommandClient(t, ctx, scope.Identity, os.Getenv("CEPH_MSGR_MDS_COMMANDS_KEY_FILE"), fresh.MDS.Endpoint, false)
		if _, e := disabled.OpenMDS(ctx, target); !errors.Is(e, cephmsgr.ErrMDSDisabled) || stats.MDS.Load() != 0 || stats.OSD.Load() != 0 {
			t.Fatal("disabled MDS admitted a network attempt", e)
		}
		disabled.Close()
	}) {
		return
	}
	if !t.Run("precanceled_command_has_no_submission", func(t *testing.T) {
		prior, e := connection.Command(ctx, version)
		check(t, "before-precancel", prior, e, version, natives["version"], fullID)
		relay := fullDials.latest(t)
		writes := relay.WriteChunks.Load()
		call, stop := context.WithCancel(ctx)
		stop()
		r, e := connection.Command(call, version)
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(e, context.Canceled) || errors.As(e, &unknown) || r.Submission != (cephmsgr.MDSCommandSubmission{}) || relay.WriteChunks.Load() != writes {
			t.Fatal("pre-cancel consumed identity or sent bytes", e)
		}
		next, e := connection.Command(ctx, version)
		check(t, "after-precancel", next, e, version, natives["version"], fullID)
		if next.Submission.Transaction != prior.Submission.Transaction+1 {
			t.Fatal("pre-cancel consumed transaction identity")
		}
	}) {
		return
	}
	if !t.Run("scheduled_mds_ticket_renewal_and_explicit_new_connection", func(t *testing.T) {
		previous := full.Snapshot()
		if previous.MDSTicket.Expires.IsZero() || previous.MDSTicket.RenewAfter.IsZero() {
			t.Fatal("MDS service ticket scheduling metadata missing")
		}
		deadline := time.Now().Add(25 * time.Second)
		if previous.MDSTicket.Expires.Before(deadline) {
			deadline = previous.MDSTicket.Expires
		}
		wait, stop := context.WithDeadline(ctx, deadline)
		defer stop()
		renewed := waitClientState(t, full, wait, func(s cephmsgr.State) bool {
			return s.GlobalID == fullID && s.MDSTicket.Expires.After(previous.MDSTicket.Expires)
		})
		if !time.Now().Before(previous.MDSTicket.Expires) || renewed.AuthRejection != nil {
			t.Fatal("MDS ticket did not renew before prior expiry")
		}
		old, e := connection.Command(ctx, version)
		check(t, "renewed-existing", old, e, version, natives["version"], fullID)
		cases[len(cases)-1].Before = &previous.MDSTicket
		cases[len(cases)-1].After = &renewed.MDSTicket
		connection.Close()
		closedHandles++
		connection, e = full.OpenMDS(ctx, target)
		if e != nil {
			t.Fatal("explicit new connection with renewed MDS ticket", e)
		}
		r, e := connection.Command(ctx, version)
		check(t, "renewed-fresh", r, e, version, natives["version"], fullID)
		if r.Submission.ConnectionGeneration <= old.Submission.ConnectionGeneration || r.Submission.Transaction <= old.Submission.Transaction {
			t.Fatal("renewal/new connection reused submission identity")
		}
		baseline = r
	}) {
		return
	}
	var uncertain cephmsgr.MDSCommandResult
	for _, closeHandle := range []bool{false, true} {
		name := "sent_cancel_preserves_unknown_submission"
		if closeHandle {
			name = "sent_close_preserves_unknown_submission"
		}
		if !t.Run(name, func(t *testing.T) {
			relay := fullDials.latest(t)
			writes := relay.WriteChunks.Load()
			relay.ArmReadPause()
			dials := fullDials.MDS.Load()
			call, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan struct {
				reply cephmsgr.MDSCommandResult
				err   error
			}, 1)
			go func() {
				r, e := connection.Command(call, version)
				done <- struct {
					reply cephmsgr.MDSCommandResult
					err   error
				}{r, e}
			}()
			select {
			case <-relay.ReadBlocked:
			case got := <-done:
				t.Fatal("command ended before receive withholding", got.err)
			case <-ctx.Done():
				t.Fatal("MDS receive withholding did not activate", ctx.Err())
			}
			// The receive gate may catch an earlier transport ACK or a peer
			// keepalive. Independently require outgoing command bytes before
			// canceling; the gate alone cannot establish a started submission.
			progress := time.NewTicker(time.Millisecond)
			defer progress.Stop()
			for relay.WriteChunks.Load() == writes {
				select {
				case <-progress.C:
				case <-ctx.Done():
					t.Fatal("MDS command did not reach the outgoing relay", ctx.Err())
				}
			}
			if closeHandle {
				connection.Close()
				closedHandles++
			} else {
				stop()
			}
			var got struct {
				reply cephmsgr.MDSCommandResult
				err   error
			}
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("uncertain call did not finish", ctx.Err())
			}
			var unknown *cephmsgr.OutcomeUnknownError
			want := error(context.Canceled)
			if closeHandle {
				want = cephmsgr.ErrClosed
			}
			s := got.reply.Submission
			if !errors.As(got.err, &unknown) || !errors.Is(got.err, want) || s.Transaction <= baseline.Submission.Transaction || s.GlobalID != fullID || s.ConnectionGeneration != baseline.Submission.ConnectionGeneration || s.Target != target || len(got.reply.Data) != 0 || fullDials.MDS.Load() != dials {
				t.Fatal("uncertain command lost original identity or redialed", got.err)
			}
			cases = append(cases, mdsCommandCase{Name: name, Command: version, Result: got.reply, Error: got.err.Error(), WriteChunksBefore: writes, WriteChunksAfter: relay.WriteChunks.Load()})
			uncertain = got.reply
			connection.Close()
			relay.Close()
			relay.Wait()
			select {
			case <-connection.Done():
			case <-ctx.Done():
				t.Fatal("closed MDS handle did not become terminal")
			}
			if fullDials.MDS.Load() != dials {
				t.Fatal("terminal MDS connection reconnected automatically")
			}
			if !closeHandle {
				closedHandles++
				var e error
				connection, e = full.OpenMDS(ctx, target)
				if e != nil {
					t.Fatal(e)
				}
				baseline, e = connection.Command(ctx, version)
				check(t, "explicit-after-cancel", baseline, e, version, natives["version"], fullID)
				if baseline.Submission.ConnectionGeneration <= s.ConnectionGeneration || baseline.Submission.Transaction <= s.Transaction {
					t.Fatal("caller-selected new connection reused unknown identity")
				}
			}
		}) {
			return
		}
	}
	if !t.Run("closed_local_refusal_and_explicit_new_connection", func(t *testing.T) {
		r, e := connection.Command(ctx, version)
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(e, cephmsgr.ErrClosed) || errors.As(e, &unknown) || r.Submission != (cephmsgr.MDSCommandSubmission{}) {
			t.Fatal("closed handle submitted another command", e)
		}
		connection, e = full.OpenMDS(ctx, target)
		if e != nil {
			t.Fatal(e)
		}
		r, e = connection.Command(ctx, version)
		check(t, "explicit-after-close", r, e, version, natives["version"], fullID)
		if r.Submission.Transaction <= uncertain.Submission.Transaction || r.Submission.ConnectionGeneration <= uncertain.Submission.ConnectionGeneration {
			t.Fatal("fresh caller-selected command reused unknown identity")
		}
	}) {
		return
	}
	t.Run("native_session_absence_and_no_osd_connections", func(t *testing.T) {
		after, err = mdsCommandSessions(ctx, dir, scope, fullID, readID)
		if err != nil {
			t.Fatal("native session absence after commands", err)
		}
		if after.RequestID <= before.RequestID || fullDials.OSD.Load() != 0 || readDials.OSD.Load() != 0 || fullDials.MDS.Load() != 4 || readDials.MDS.Load() != 1 || full.Snapshot().GlobalID != fullID || readonly.Snapshot().GlobalID != readID {
			t.Fatal("command lifecycle changed native session scope or routed to OSD")
		}
	})
}
