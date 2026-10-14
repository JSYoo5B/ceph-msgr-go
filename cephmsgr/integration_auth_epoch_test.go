package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephServiceKeyEpochIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_AUTH_EPOCH") != "1" {
		t.Skip("requires an isolated service-key disposal fixture")
	}
	admin, ctx := fixtureClient(t, time.Minute)
	options := integrationOptions(t)
	dial := options.DialContext
	var managerDials atomic.Int32
	heldDials, coldDials := &epochDialHistory{}, &epochDialHistory{}
	options.DialContext = epochObservedDial(dial, heldDials, &managerDials)
	coldOptions := integrationOptions(t)
	coldDial := coldOptions.DialContext
	coldOptions.DialContext = epochObservedDial(coldDial, coldDials, nil)
	held, err := Dial(ctx, options)
	if err != nil {
		t.Fatalf("authenticate held client: %q", fmt.Sprint(err))
	}
	defer held.Close()
	if err := held.WaitMgrReady(ctx); err != nil {
		t.Fatalf("establish MGR before service-key disposal: %q", fmt.Sprint(err))
	}
	cold, err := Dial(ctx, coldOptions)
	if err != nil {
		t.Fatalf("authenticate cold client: %q", fmt.Sprint(err))
	}
	defer cold.Close()
	// These are cached-state observations, not transport capture or proof of
	// the exact ticket selected inside the later MGR attempt. Secret IDs may be
	// reused after a wipe; equality alone never identifies the key generation.
	logState := func(cycle int, role, phase string, client *Client, history *epochDialHistory, before, renewed []byte) {
		t.Helper()
		observation := observeEpochAuth(client, before, renewed)
		encoded, err := json.Marshal(observation)
		if err != nil {
			t.Fatal("encode non-secret authentication observation")
		}
		t.Logf("service-key cached observation cycle=%d client=%s phase=%s state=%s MON-dials=%v", cycle, role, phase, encoded, history.snapshot())
	}
	heldInitial, coldInitial := held.Snapshot(), cold.Snapshot()
	if heldInitial.GlobalID == 0 || coldInitial.GlobalID == 0 || coldInitial.Manager.Ready || managerDials.Load() != 1 {
		t.Fatal("fixture did not establish one held and one cold MGR client")
	}

	type serverMap struct {
		Epoch     uint32  `json:"epoch"`
		AuthEpoch *uint32 `json:"auth_epoch"`
	}
	queryMap := func() serverMap {
		t.Helper()
		result, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mon dump","format":"json"}`)})
		var m serverMap
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &m) != nil || m.Epoch == 0 || m.AuthEpoch == nil {
			t.Fatalf("query current Tentacle authentication epoch: %q", fmt.Sprint(err))
		}
		return m
	}
	previous := queryMap()
	for cycle := 1; cycle <= 2; cycle++ {
		// Use a newly authenticated cold client in each cycle. Its first MGR
		// handshake must use the replacement ticket, not an existing session.
		if cycle > 1 {
			cold.Close()
			coldDials = &epochDialHistory{}
			coldOptions.DialContext = epochObservedDial(coldDial, coldDials, nil)
			cold, err = Dial(ctx, coldOptions)
			if err != nil {
				t.Fatalf("authenticate next cold client: %q", fmt.Sprint(err))
			}
			defer cold.Close()
		}
		oldHeld, oldCold := held.Snapshot(), cold.Snapshot()
		heldBeforeBlob := held.snapshotAuth().Tickets[cephx.ServiceMgr].Blob
		coldBeforeBlob := cold.snapshotAuth().Tickets[cephx.ServiceMgr].Blob
		logState(cycle, "held", "before-wipe", held, heldDials, heldBeforeBlob, nil)
		logState(cycle, "cold", "before-wipe", cold, coldDials, coldBeforeBlob, nil)
		for _, s := range []State{oldHeld, oldCold} {
			if time.Until(s.AuthTicket.RenewAfter) < time.Minute || time.Until(s.MgrTicket.RenewAfter) < time.Minute {
				t.Fatal("fixture tickets are too short to distinguish epoch notification from scheduled renewal")
			}
		}
		result, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"auth wipe-rotating-service-keys"}`)})
		if err != nil || result.Code != 0 {
			// The mutation is submitted once. Never hide an uncertain result
			// by issuing another wipe in a new session.
			t.Fatalf("dispose rotating service keys once: %q", fmt.Sprint(err))
		}
		current := queryMap()
		if current.Epoch <= previous.Epoch || *current.AuthEpoch <= *previous.AuthEpoch {
			t.Fatal("successful service-key disposal did not advance server MonMap/authentication epochs")
		}
		renew, cancel := context.WithTimeout(ctx, 15*time.Second)
		for i, client := range []*Client{held, cold} {
			before := []State{oldHeld, oldCold}[i]
			state := waitClientState(t, client, renew, func(s State) bool {
				return s.Monitor.Ready && s.Monitor.MapEpoch >= current.Epoch && s.AuthTicket.Expires.After(before.AuthTicket.Expires) && s.MgrTicket.Expires.After(before.MgrTicket.Expires)
			})
			if state.GlobalID != before.GlobalID || state.AuthRejection != nil || !time.Now().Before(before.AuthTicket.RenewAfter) || !time.Now().Before(before.MgrTicket.RenewAfter) {
				t.Fatal("epoch renewal lost identity, rejected the retained proof, or waited for scheduled renewal", state)
			}
		}
		cancel()
		heldRenewedBlob := held.snapshotAuth().Tickets[cephx.ServiceMgr].Blob
		coldRenewedBlob := cold.snapshotAuth().Tickets[cephx.ServiceMgr].Blob
		logState(cycle, "held", "after-renewal", held, heldDials, heldBeforeBlob, heldRenewedBlob)
		logState(cycle, "cold", "after-renewal", cold, coldDials, coldBeforeBlob, coldRenewedBlob)
		if err := held.WaitMgrReady(ctx); err != nil || managerDials.Load() != 1 {
			t.Fatalf("service-key disposal replaced an authenticated held MGR connection: dials=%d error=%q", managerDials.Load(), fmt.Sprint(err))
		}
		state := held.Snapshot()
		if state.Manager.GlobalID != heldInitial.Manager.GlobalID || state.Manager.Name != heldInitial.Manager.Name {
			t.Fatal("service-key disposal unexpectedly changed the active manager")
		}
		if err := fixtureRecoveryRead(ctx, held, true); err != nil {
			t.Fatalf("held MGR after service-key disposal: %q", fmt.Sprint(err))
		}
		// Daemon rotating secrets arrive in a separate CephX exchange. Tie
		// the independent CLI probes to the new local MON epochs and this
		// exact MGR identity before the one Go cold handshake. An
		// unpinned CLI read alone can use a different MON's service ticket.
		gate, stop := context.WithTimeout(ctx, 20*time.Second)
		identity := fmt.Sprintf("%s %d %d %d", state.Manager.Name, state.Manager.GlobalID, current.Epoch, *current.AuthEpoch)
		err = testcluster.ControlDaemon(gate, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "verify", "service-keys", identity)
		stop()
		if err != nil {
			t.Fatalf("independent MGR replacement-key readiness: %q", fmt.Sprint(err))
		}
		if err := held.WaitMgrReady(ctx); err != nil {
			t.Fatalf("held MGR after replacement-key readiness: %q", fmt.Sprint(err))
		}
		state = held.Snapshot()
		if state.Manager.GlobalID != heldInitial.Manager.GlobalID || state.Manager.Name != heldInitial.Manager.Name || managerDials.Load() != 1 {
			t.Fatalf("replacement-key readiness changed the held MGR: name=%s global-id=%d dials=%d", state.Manager.Name, state.Manager.GlobalID, managerDials.Load())
		}
		logState(cycle, "held", "before-cold-first-MGR", held, heldDials, heldBeforeBlob, heldRenewedBlob)
		logState(cycle, "cold", "before-first-MGR", cold, coldDials, coldBeforeBlob, coldRenewedBlob)
		if err := cold.WaitMgrReady(ctx); err != nil {
			logState(cycle, "cold", "first-MGR-rejected", cold, coldDials, coldBeforeBlob, coldRenewedBlob)
			t.Fatalf("cold MGR with replacement service ticket: %q", fmt.Sprint(err))
		}
		logState(cycle, "cold", "first-MGR-accepted", cold, coldDials, coldBeforeBlob, coldRenewedBlob)
		if err := fixtureRecoveryRead(ctx, cold, true); err != nil {
			t.Fatalf("cold MGR command after service-key disposal: %q", fmt.Sprint(err))
		}
		t.Logf("service-key disposal cycle=%d server-map-epoch=%d auth-epoch=%d retained-client-id=%d held-MGR-dials=%d; cold MGR authenticated before scheduled renewal", cycle, current.Epoch, *current.AuthEpoch, heldInitial.GlobalID, managerDials.Load())
		previous = current
	}
}

// epochAuthObservation contains only non-secret metadata. Every cached field
// except the displayed endpoint is copied in one client-lock critical section.
// Endpoint is read afterward from that same held-session object because custom
// RemoteAddr implementations must not run while holding the product lock.
// Neither observation phase nor SecretID proves issuance/install/key generation
// or the exact authorizer used by a subsequent call.
type epochAuthObservation struct {
	ObservedAt                 time.Time `json:"observed_at"`
	ClientID                   uint64    `json:"client_id"`
	MonEpoch                   uint32    `json:"mon_epoch"`
	AuthEpoch                  uint32    `json:"auth_epoch"`
	MonSessionPresent          bool      `json:"mon_session_present"`
	MonAdmitted                bool      `json:"mon_admitted"`
	MonEndpoint                string    `json:"mon_endpoint"`
	MgrEpoch                   uint32    `json:"mgr_epoch"`
	MgrName                    string    `json:"mgr_name"`
	MgrID                      uint64    `json:"mgr_id"`
	MgrAvailable               bool      `json:"mgr_available"`
	MgrSessionPresent          bool      `json:"mgr_session_present"`
	AuthRejected               bool      `json:"auth_rejected"`
	AuthSecretID               uint64    `json:"auth_secret_id"`
	AuthExpires                time.Time `json:"auth_expires"`
	AuthRenewAfter             time.Time `json:"auth_renew_after"`
	MgrSecretID                uint64    `json:"mgr_secret_id"`
	MgrCipher                  uint16    `json:"mgr_cipher"`
	MgrExpires                 time.Time `json:"mgr_expires"`
	MgrRenewAfter              time.Time `json:"mgr_renew_after"`
	MgrBlobSameBefore          bool      `json:"mgr_blob_same_before"`
	MgrRenewedReferencePresent bool      `json:"mgr_renewed_reference_present"`
	MgrBlobSameRenewed         bool      `json:"mgr_blob_same_renewed"`
}

func observeEpochAuth(c *Client, before, renewed []byte) epochAuthObservation {
	c.mu.Lock()
	auth, mgr := c.auth.Tickets[cephx.ServiceAuth], c.auth.Tickets[cephx.ServiceMgr]
	mon := c.mon
	result := epochAuthObservation{
		ObservedAt: time.Now().UTC(), ClientID: c.auth.GlobalID,
		MonEpoch: c.monMap.Epoch, AuthEpoch: c.monMap.AuthEpoch,
		MonSessionPresent: mon != nil, MonAdmitted: !c.closed && c.authErr == nil && c.monReady && mon != nil,
		MgrEpoch: c.mgrMap.Epoch, MgrName: c.mgrMap.Name, MgrID: c.mgrMap.GlobalID,
		MgrAvailable: c.mgrMap.Available, MgrSessionPresent: c.mgr != nil, AuthRejected: c.authErr != nil,
		AuthSecretID: auth.SecretID, AuthExpires: auth.Expires, AuthRenewAfter: auth.RenewAfter,
		MgrSecretID: mgr.SecretID, MgrCipher: mgr.Key.Type(), MgrExpires: mgr.Expires, MgrRenewAfter: mgr.RenewAfter,
		MgrBlobSameBefore: bytes.Equal(mgr.Blob, before), MgrRenewedReferencePresent: renewed != nil,
		MgrBlobSameRenewed: renewed != nil && bytes.Equal(mgr.Blob, renewed),
	}
	c.mu.Unlock()
	if mon != nil {
		if endpoint := mon.RemoteAddr(); endpoint != nil {
			result.MonEndpoint = endpoint.String()
		}
	}
	return result
}

type epochDialHistory struct {
	mu       sync.Mutex
	monitors []string
}

func (h *epochDialHistory) record(role string) {
	if role != "mon.a" && role != "mon.b" && role != "mon.c" {
		return
	}
	h.mu.Lock()
	h.monitors = append(h.monitors, role)
	h.mu.Unlock()
}
func (h *epochDialHistory) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.monitors...)
}
func epochObservedDial(dial func(context.Context, string, string) (net.Conn, error), h *epochDialHistory, managerDials *atomic.Int32) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err == nil {
			_, role, _ := testcluster.SplitPeer(endpoint)
			h.record(role)
			if managerDials != nil && (role == "mgr.a" || role == "mgr.b") {
				managerDials.Add(1)
			}
		}
		return conn, err
	}
}

func TestEpochAuthObservationDoesNotRetainSecrets(t *testing.T) {
	old := []byte("never-log-old-ticket")
	replacement := []byte("never-log-replacement-ticket")
	c := &Client{auth: &cephx.Client{GlobalID: 42, Tickets: map[uint32]cephx.Ticket{
		cephx.ServiceAuth: {SecretID: 5, Blob: []byte("never-log-auth-proof"), Expires: time.Unix(400, 0), RenewAfter: time.Unix(300, 0)},
		cephx.ServiceMgr:  {SecretID: 2, Blob: replacement, Expires: time.Unix(400, 0), RenewAfter: time.Unix(300, 0)},
	}}, monMap: maps.Mon{Epoch: 7, AuthEpoch: 11}, mgrMap: maps.Mgr{Epoch: 8, Name: "a", GlobalID: 99, Available: true}}
	got := observeEpochAuth(c, old, replacement)
	if got.MgrBlobSameBefore || !got.MgrBlobSameRenewed || !got.MgrRenewedReferencePresent || got.MgrSecretID != 2 || got.AuthSecretID != 5 || got.AuthEpoch != 11 || got.MonEpoch != 7 || got.ClientID != 42 || got.MgrID != 99 {
		t.Fatal("non-secret observation lost provenance fields")
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{string(old), string(replacement), "never-log-auth-proof"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("observation contains secret ticket/proof bytes")
		}
	}
	// The observation owns no credential slices: changing the original ticket
	// cannot alter a completed record or retain its mutable byte storage.
	replacement[0] = 'X'
	after, _ := json.Marshal(got)
	if !bytes.Equal(data, after) {
		t.Fatal("observation aliases private credential storage")
	}
	withoutRenewed := observeEpochAuth(c, c.auth.Tickets[cephx.ServiceMgr].Blob, nil)
	if !withoutRenewed.MgrBlobSameBefore || withoutRenewed.MgrBlobSameRenewed || withoutRenewed.MgrRenewedReferencePresent {
		t.Fatal("missing renewed reference was reported as a match")
	}
}

func TestEpochDialHistoriesAreIndependentAndOwned(t *testing.T) {
	held, cold := &epochDialHistory{}, &epochDialHistory{}
	held.record("mon.a")
	cold.record("mon.b")
	held.record("mgr.a")
	first := held.snapshot()
	first[0] = "caller-changed"
	if got := held.snapshot(); len(got) != 1 || got[0] != "mon.a" {
		t.Fatal("held history aliases caller or retained a non-MON role")
	}
	if got := cold.snapshot(); len(got) != 1 || got[0] != "mon.b" {
		t.Fatal("cold history mixed with held history")
	}
}

func TestEpochAuthObservationKeepsConcurrentFieldsConsistent(t *testing.T) {
	c := &Client{auth: &cephx.Client{GlobalID: 100, Tickets: map[uint32]cephx.Ticket{}}, mgrMap: maps.Mgr{GlobalID: 200}}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for epoch := uint32(1); epoch <= 256; epoch++ {
			c.mu.Lock()
			c.auth.GlobalID = uint64(100 + epoch)
			c.monMap.Epoch, c.monMap.AuthEpoch = epoch, epoch
			c.mgrMap.Epoch, c.mgrMap.GlobalID = epoch, uint64(200+epoch)
			c.auth.Tickets[cephx.ServiceMgr] = cephx.Ticket{SecretID: uint64(epoch), Blob: []byte("private-ticket-not-retained")}
			c.mu.Unlock()
		}
	}()
	for i := 0; i < 256; i++ {
		got := observeEpochAuth(c, nil, nil)
		if got.MonEpoch != got.AuthEpoch || got.MgrEpoch != got.AuthEpoch || got.ClientID != uint64(100+got.AuthEpoch) || got.MgrID != uint64(200+got.AuthEpoch) || got.MgrSecretID != uint64(got.AuthEpoch) {
			t.Fatal("concurrent observation combined different cached generations")
		}
	}
	<-finished
}
