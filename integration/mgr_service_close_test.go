package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type mgrServiceCloseReceipt struct {
	Fault                  string  `json:"fault"`
	ClientGID              uint64  `json:"client_gid"`
	ReadWithheld           bool    `json:"read_withheld"`
	WritesDiscarded        int32   `json:"writes_discarded"`
	Unconfirmed            bool    `json:"unconfirmed"`
	Canceled               bool    `json:"canceled"`
	Terminal               bool    `json:"terminal"`
	RepeatedClosePreserved bool    `json:"repeated_close_preserved"`
	PresentAfterLocalClose bool    `json:"present_after_local_close"`
	AbsentBeforeCancel     bool    `json:"absent_before_cancel"`
	FinalAbsent            bool    `json:"final_absent"`
	AbsenceSeconds         float64 `json:"absence_seconds"`
	TotalWatchPolls        int     `json:"total_watch_polls"`
	ManagerDials           int32   `json:"manager_dials"`
}

func testMgrServiceUnconfirmedClose(t *testing.T, ctx context.Context) {
	t.Helper()
	var receipts []mgrServiceCloseReceipt
	defer func() {
		data, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "mgr-service-close.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve service close receipt", err)
		}
	}()
	for _, fault := range []string{"receive-withheld", "send-discarded"} {
		if !t.Run(fault, func(t *testing.T) {
			r := mgrServiceCloseReceipt{Fault: fault}
			defer func() { receipts = append(receipts, r) }()
			var readArmed, dropWrites atomic.Bool
			var dials, discarded atomic.Int32
			blocked, dropped := make(chan struct{}), make(chan struct{})
			var readOnce, writeOnce sync.Once
			options := integrationOptions(t)
			options.EnableMgrService = true
			dial := options.DialContext
			options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
				conn, err := dial(call, network, endpoint)
				if err != nil {
					return nil, err
				}
				_, role, _ := testcluster.SplitPeer(endpoint)
				if !strings.HasPrefix(role, "mgr.") {
					return conn, nil
				}
				dials.Add(1)
				return &testcluster.LostReplyConn{Conn: conn, Armed: &readArmed, DropWrites: &dropWrites, Closed: make(chan struct{}),
					SignalBlocked:      func() { readOnce.Do(func() { close(blocked) }) },
					SignalWriteDropped: func() { discarded.Add(1); writeOnce.Do(func() { close(dropped) }) }}, nil
			}
			c, err := cephmsgr.Dial(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			service, daemon := "ceph-msgr-close", fault
			svc, err := c.OpenMgrService(ctx, cephmsgr.MgrServiceOptions{Service: service, Daemon: daemon,
				Metadata: map[string]string{"fault": fault}, Status: map[string]string{"state": "open"}, DisableBeacon: true})
			if err != nil {
				t.Fatal("open fault service", err)
			}
			// Send one explicit beacon so native expiry has an observed baseline.
			if err := svc.Report(ctx, map[string]string{"state": "beacon"}, nil); err != nil {
				t.Fatal(err)
			}
			visible := serviceWatch(t, ctx, service, daemon, "present", map[string]string{"fault": fault}, map[string]string{"state": "beacon"}, nil)
			var entry struct {
				GID uint64 `json:"gid"`
			}
			if !visible.ServiceMatched || json.Unmarshal(visible.ServiceEntry, &entry) != nil || entry.GID != c.Snapshot().GlobalID || dials.Load() != 1 {
				t.Fatal("native identity or dedicated connection differs", entry)
			}
			r.ClientGID = entry.GID
			// Arm only the established dedicated MGR connection. No ordinary
			// MGR commands, automatic beacons or handshake bytes are faulted.
			readArmed.Store(fault == "receive-withheld")
			dropWrites.Store(fault == "send-discarded")
			call, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- svc.Close(call) }()
			signal := blocked
			if fault == "send-discarded" {
				signal = dropped
			}
			select {
			case <-signal:
			case err := <-finished:
				t.Fatal("Close returned before the fault boundary", err)
			case <-call.Done():
				t.Fatal("fault did not activate within the close deadline", call.Err())
			}
			if fault == "receive-withheld" {
				r.ReadWithheld = true
				// A native read establishes remote removal while Close still
				// cannot see its acknowledgement; cancellation happens later.
				r.AbsentBeforeCancel = serviceWatch(t, ctx, service, daemon, "absent", nil, nil, nil).ServiceMatched
				if !r.AbsentBeforeCancel {
					t.Fatal("native close removal was not observed")
				}
			}
			select {
			case err := <-finished:
				t.Fatal("Close invented acknowledgement", err)
			default:
			}
			cancel()
			var closeErr error
			select {
			case closeErr = <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled Close exceeded its local completion bound")
			case <-ctx.Done():
				t.Fatal("canceled Close did not finish", ctx.Err())
			}
			r.Unconfirmed, r.Canceled = errors.Is(closeErr, cephmsgr.ErrMgrServiceCloseUnconfirmed), errors.Is(closeErr, context.Canceled)
			select {
			case <-svc.Done():
				r.Terminal = true
			default:
			}
			r.RepeatedClosePreserved = svc.Close(ctx) == closeErr
			if !r.Unconfirmed || !r.Canceled || !r.Terminal || !r.RepeatedClosePreserved {
				t.Fatal("unconfirmed Close lost cancellation or terminal result", closeErr)
			}
			if fault == "send-discarded" {
				// The successful local write was deliberately never forwarded.
				// A native post-close observation must still see the service.
				r.PresentAfterLocalClose = serviceWatch(t, ctx, service, daemon, "present", map[string]string{"fault": fault}, nil, nil).ServiceMatched
				if !r.PresentAfterLocalClose {
					t.Fatal("local close was mistaken for unregistering")
				}
			}
			absence := serviceWatchInput(service, daemon, "absent", nil, nil, nil)
			absence.WaitSeconds, absence.StableSeconds = 90, 3
			gone := nativePool(t, ctx, "servicewatch", absence)
			r.FinalAbsent, r.AbsenceSeconds, r.TotalWatchPolls = gone.ServiceMatched, gone.ServiceStableSeconds, gone.ServiceObservations
			r.WritesDiscarded, r.ManagerDials = discarded.Load(), dials.Load()
			if !r.FinalAbsent || r.AbsenceSeconds < 3 || r.ManagerDials != 1 || fault == "send-discarded" && r.WritesDiscarded == 0 {
				t.Fatal("native expiry/removal or no-reconnect observation failed", r)
			}
			t.Logf("close fault %s: unconfirmed/canceled, discarded writes=%d, MGR dials=%d, final native absence %.1fs over %d total polls",
				fault, r.WritesDiscarded, r.ManagerDials, r.AbsenceSeconds, r.TotalWatchPolls)
		}) {
			return
		}
	}
}
