package cephmsgr

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestCephServiceKeyEpochIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_AUTH_EPOCH") != "1" {
		t.Skip("requires an isolated service-key disposal fixture")
	}
	admin, ctx := fixtureClient(t, time.Minute)
	trace := newEpochTrace(t)
	heldContext := context.WithValue(ctx, authTraceContextKey{}, trace.hooks("held"))
	coldContext := context.WithValue(ctx, authTraceContextKey{}, trace.hooks("cold"))
	options := integrationOptions(t)
	dial := options.DialContext
	var managerDials atomic.Int32
	heldDials, coldDials := &epochDialHistory{}, &epochDialHistory{}
	options.DialContext = epochObservedDial(dial, heldDials, &managerDials)
	coldOptions := integrationOptions(t)
	coldDial := coldOptions.DialContext
	coldOptions.DialContext = epochObservedDial(coldDial, coldDials, nil)
	held, err := Dial(heldContext, options)
	if err != nil {
		t.Fatalf("authenticate held client: %q", fmt.Sprint(err))
	}
	defer held.Close()
	if err := held.WaitMgrReady(ctx); err != nil {
		t.Fatalf("establish MGR before service-key disposal: %q", fmt.Sprint(err))
	}
	cold, err := Dial(coldContext, coldOptions)
	if err != nil {
		t.Fatalf("authenticate cold client: %q", fmt.Sprint(err))
	}
	defer cold.Close()
	// Cached observations remain distinct from the linked selection/write trace
	// below. Secret IDs may be
	// reused after a wipe; equality alone never identifies the key generation.
	logState := func(cycle int, role, phase string, client *Client, history *epochDialHistory, before, renewed []byte) {
		t.Helper()
		observation := observeEpochAuth(client, before, renewed)
		installedTickets := trace.ticketReferences(client.snapshotAuth())
		installedJSON, err := json.Marshal(installedTickets)
		if err != nil {
			t.Fatal("encode secret-free ticket installation references")
		}
		encoded, err := json.Marshal(observation)
		if err != nil {
			t.Fatal("encode non-secret authentication observation")
		}
		t.Logf("service-key cached observation cycle=%d client=%s phase=%s state=%s installed-ticket-references=%s MON-dials=%v", cycle, role, phase, encoded, installedJSON, history.snapshot())
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
		trace.beginCycle(cycle)
		// Use a newly authenticated cold client in each cycle. Its first MGR
		// handshake must use the replacement ticket, not an existing session.
		if cycle > 1 {
			cold.Close()
			coldDials = &epochDialHistory{}
			coldOptions.DialContext = epochObservedDial(coldDial, coldDials, nil)
			cold, err = Dial(coldContext, coldOptions)
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
		expectedTicket := cold.snapshotAuth().Tickets[cephx.ServiceMgr]
		join, stopJoin := context.WithTimeout(ctx, 5*time.Second)
		expectedInstall, joinErr := trace.waitReference(join, expectedTicket)
		stopJoin()
		if joinErr != nil || expectedInstall.Cycle != cycle || expectedInstall.AuthEpoch < *current.AuthEpoch {
			t.Fatal("renewed cold ticket lacks this cycle's authenticated issuer-session provenance")
		}
		logAttempts := func() []epochMgrAttempt {
			attempts := trace.snapshot("cold", cycle)
			data, err := json.Marshal(attempts)
			if err != nil {
				t.Fatal("encode secret-free authorizer provenance")
			}
			t.Logf("service-key consumer auth provenance cycle=%d attempts=%s; cached epochs are observations; native verifier-cache equality unavailable", cycle, data)
			return attempts
		}
		if err := cold.WaitMgrReady(ctx); err != nil {
			logAttempts()
			logState(cycle, "cold", "first-MGR-rejected", cold, coldDials, coldBeforeBlob, coldRenewedBlob)
			t.Fatalf("cold MGR with replacement service ticket: %q", fmt.Sprint(err))
		}
		attempts := logAttempts()
		// This profile measures the first cold handshake. A later successful
		// attempt must not turn an earlier rejection or uncertain write into a pass.
		if len(attempts) != 1 || !epochAttemptMatches(attempts[0], expectedInstall, cycle, state.Manager.Name, state.Manager.GlobalID) ||
			!attempts[0].AuthDoneVerified || !attempts[0].HandshakeCompleted {
			t.Fatal("cold MGR lacks one first renewed-ticket selection/encoding/completed-write handshake")
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

// epochTrace keeps only metadata and keyed, run-local labels. Receipt identity
// means successful local parse, not issuance/key generation. The stock fixture
// does not expose the native MGR verifier cache; no equality with it is claimed.
type epochTrace struct {
	mu       sync.Mutex
	key      [32]byte
	cycle    atomic.Int64
	changed  chan struct{}
	installs map[any]epochTicketInstall
	attempts []*epochMgrAttempt
}

type epochTicketInstall struct {
	Client            string    `json:"client"`
	Cycle             int       `json:"cycle"`
	Sequence          uint64    `json:"client_install_sequence"`
	Service           uint32    `json:"service"`
	ClientID          uint64    `json:"client_id"`
	SecretID          uint64    `json:"secret_id"`
	Expires           time.Time `json:"expires"`
	RenewAfter        time.Time `json:"renew_after"`
	BlobLabel         string    `json:"blob_run_hmac"`
	IssuerSession     string    `json:"authenticated_issuer_session_run_hmac"`
	IssuerEndpoint    string    `json:"issuer_connection_endpoint"`
	IssuerFixtureRole string    `json:"issuer_fixture_route_role"`
	MonEpoch          uint32    `json:"issuer_cached_mon_epoch"`
	AuthEpoch         uint32    `json:"issuer_cached_auth_epoch"`
}

type epochAuthWrite struct {
	Tag      uint8 `json:"tag"`
	Bytes    int   `json:"local_bytes_written"`
	Complete bool  `json:"local_frame_write_completed"`
}

type epochMgrAttempt struct {
	// Opaque receipt has no credential/key/blob fields and is never serialized.
	receipt            any
	Client             string              `json:"client"`
	Cycle              int                 `json:"cycle"`
	Attempt            uint64              `json:"attempt"`
	ClientID           uint64              `json:"selected_client_id"`
	SecretID           uint64              `json:"selected_secret_id"`
	SelectedBlob       string              `json:"selected_blob_run_hmac"`
	EncodedBlob        string              `json:"encoded_blob_run_hmac"`
	EncodedSame        bool                `json:"encoded_blob_matches_selection"`
	EncodedRounds      int                 `json:"encoded_rounds"`
	MgrName            string              `json:"selected_mgr_name"`
	MgrID              uint64              `json:"selected_mgr_gid"`
	MgrEpoch           uint32              `json:"selected_cached_mgr_epoch"`
	Endpoint           string              `json:"selected_mgr_endpoint"`
	Writes             []epochAuthWrite    `json:"protocol_writes"`
	AuthDoneVerified   bool                `json:"authorizer_auth_done_verified"`
	HandshakeCompleted bool                `json:"handshake_completed"`
	Outcome            string              `json:"handshake_outcome"`
	RefusalMethod      uint32              `json:"auth_refusal_method,omitempty"`
	RefusalCode        int32               `json:"auth_refusal_code,omitempty"`
	Install            *epochTicketInstall `json:"selected_ticket_install,omitempty"`
}

func newEpochTrace(t *testing.T) *epochTrace {
	t.Helper()
	r := &epochTrace{installs: make(map[any]epochTicketInstall), changed: make(chan struct{})}
	if _, err := rand.Read(r.key[:]); err != nil {
		t.Fatal("create run-local provenance label key")
	}
	return r
}

func (r *epochTrace) label(domain string, data []byte) string {
	h := hmac.New(sha256.New, r.key[:])
	h.Write([]byte(domain))
	h.Write([]byte{0})
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
func (r *epochTrace) beginCycle(cycle int) {
	r.cycle.Store(int64(cycle))
}
func (r *epochTrace) hooks(client string) *authTraceHooks {
	return &authTraceHooks{
		cycle: &r.cycle,
		installed: func(issuer authIssuerObservation, auth *cephx.Client) {
			_, role, _ := testcluster.SplitPeer(issuer.endpoint)
			r.mu.Lock()
			defer r.mu.Unlock()
			sessionLabel := r.label("issuer-session", []byte(fmt.Sprintf("%s:%d:%d:%d:%p", client, issuer.cycle, issuer.sequence, auth.GlobalID, issuer.session)))
			for _, service := range []uint32{cephx.ServiceAuth, cephx.ServiceMgr, cephx.ServiceOSD} {
				ticket, ok := auth.Tickets[service]
				receipt := ticket.ReceiptIdentity()
				if !ok || receipt == nil {
					continue
				}
				if _, retained := r.installs[receipt]; retained {
					continue
				}
				r.installs[receipt] = epochTicketInstall{Client: client, Cycle: issuer.cycle, Sequence: issuer.sequence, Service: service,
					ClientID: auth.GlobalID, SecretID: ticket.SecretID, Expires: ticket.Expires, RenewAfter: ticket.RenewAfter,
					BlobLabel: r.label("ticket-blob", ticket.Blob), IssuerSession: sessionLabel, IssuerEndpoint: issuer.endpoint,
					IssuerFixtureRole: role, MonEpoch: issuer.monMap.Epoch, AuthEpoch: issuer.monMap.AuthEpoch}
			}
			close(r.changed)
			r.changed = make(chan struct{})
		},
		manager: func(cycle int, auth *cephx.Client, target maps.Mgr, address msgr.Address, underlying session.Authenticator) (session.Authenticator, func(error)) {
			ticket := auth.Tickets[cephx.ServiceMgr]
			r.mu.Lock()
			a := &epochMgrAttempt{receipt: ticket.ReceiptIdentity(), Client: client, Cycle: cycle, Attempt: uint64(len(r.attempts) + 1),
				ClientID: auth.GlobalID, SecretID: ticket.SecretID, SelectedBlob: r.label("ticket-blob", ticket.Blob),
				MgrName: target.Name, MgrID: target.GlobalID, MgrEpoch: target.Epoch, Endpoint: dialAddress(address), Outcome: "started"}
			r.attempts = append(r.attempts, a)
			r.mu.Unlock()
			wrapped := &epochTracedAuth{Authenticator: underlying, trace: r, attempt: a}
			return wrapped, func(err error) {
				r.mu.Lock()
				defer r.mu.Unlock()
				a.HandshakeCompleted = err == nil
				a.Outcome = "transport_or_protocol_failure_acceptance_unknown"
				var refusal *cephx.AuthenticationError
				switch {
				case err == nil:
					a.Outcome = "authenticated_handshake_complete"
				case errors.As(err, &refusal):
					a.Outcome = "explicit_authentication_refusal"
					a.RefusalMethod, a.RefusalCode = refusal.Method, refusal.Code
				case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
					a.Outcome = "local_context_ended_acceptance_unknown"
				}
			}
		},
	}
}

type epochTracedAuth struct {
	session.Authenticator
	trace   *epochTrace
	attempt *epochMgrAttempt
}

func (a *epochTracedAuth) observeEncoded(payload []byte) {
	// Parse the fresh authorizer's plaintext envelope only, never decrypt or
	// retain its proof. No authorizer bytes are emitted or captured from a socket.
	d := wire.NewDecoder(payload)
	version, clientID, service := d.U8(), d.U64(), d.U32()
	ticketVersion, secretID, blob := d.U8(), d.U64(), d.Bytes()
	d.Bytes() // encrypted nonce proof: discarded without recording.
	valid := d.Done() == nil && version == 1 && ticketVersion == 1 && service == cephx.ServiceMgr
	label := a.trace.label("ticket-blob", blob)
	a.trace.mu.Lock()
	defer a.trace.mu.Unlock()
	match := valid && clientID == a.attempt.ClientID && secretID == a.attempt.SecretID && label == a.attempt.SelectedBlob
	if a.attempt.EncodedRounds == 0 {
		a.attempt.EncodedSame = match
	} else {
		a.attempt.EncodedSame = a.attempt.EncodedSame && match
	}
	a.attempt.EncodedRounds++
	a.attempt.EncodedBlob = label
}
func (a *epochTracedAuth) Initial() ([]byte, error) {
	p, err := a.Authenticator.Initial()
	if err == nil {
		a.observeEncoded(p)
	}
	return p, err
}
func (a *epochTracedAuth) More(p []byte) ([]byte, error) {
	out, err := a.Authenticator.More(p)
	if err == nil {
		a.observeEncoded(out)
	}
	return out, err
}
func (a *epochTracedAuth) Done(id uint64, p []byte, secret bool) (cephx.Key, []byte, error) {
	key, out, err := a.Authenticator.Done(id, p, secret)
	a.trace.mu.Lock()
	a.attempt.AuthDoneVerified = err == nil
	a.trace.mu.Unlock()
	return key, out, err
}
func (a *epochTracedAuth) AuthWriteObserved(tag msgr.Tag, n int, completed bool) {
	a.trace.mu.Lock()
	defer a.trace.mu.Unlock()
	a.attempt.Writes = append(a.attempt.Writes, epochAuthWrite{Tag: uint8(tag), Bytes: n, Complete: completed})
}

func (r *epochTrace) reference(ticket cephx.Ticket) (epochTicketInstall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.installs[ticket.ReceiptIdentity()]
	return v, ok
}
func (r *epochTrace) snapshot(client string, cycle int) []epochMgrAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []epochMgrAttempt
	for _, attempt := range r.attempts {
		if attempt.Client != client || attempt.Cycle != cycle {
			continue
		}
		v := *attempt
		v.Writes = append([]epochAuthWrite(nil), attempt.Writes...)
		if install, ok := r.installs[v.receipt]; ok {
			v.Install = &install
		}
		out = append(out, v)
	}
	return out
}

func epochAttemptMatches(a epochMgrAttempt, expected epochTicketInstall, cycle int, mgrName string, mgrID uint64) bool {
	if a.Install == nil || a.Client != expected.Client || a.Cycle != cycle || expected.Cycle != cycle ||
		*a.Install != expected || expected.Service != cephx.ServiceMgr || a.MgrName != mgrName || a.MgrID != mgrID ||
		a.ClientID != expected.ClientID || a.SecretID != expected.SecretID || a.SelectedBlob != expected.BlobLabel ||
		!a.EncodedSame || a.EncodedRounds == 0 || len(a.Writes) != a.EncodedRounds {
		return false
	}
	for i, w := range a.Writes {
		want := msgr.AuthRequest
		if i > 0 {
			want = msgr.AuthRequestMore
		}
		if w.Tag != uint8(want) || w.Bytes <= 0 || !w.Complete {
			return false
		}
	}
	return true // A complete write chain still says nothing about server acceptance.
}

// These encrypted synthetic replies exercise local provenance, not native
// interoperability or rotating-key generation. Reused SecretID/blob is deliberate.
func epochParsedAuth(t *testing.T, previous *cephx.Client, manager bool) *cephx.Client {
	t.Helper()
	key, encoded := mockCredential()
	var c *cephx.Client
	if previous == nil {
		var err error
		c, err = cephx.NewClient("client.provenance", key.value, 0)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		c = copyAuth(previous)
	}
	authTicket, err := mockTicket(key.value, encoded, cephx.ServiceAuth, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var mgrTicket []byte
	if manager {
		mgrTicket, err = mockTicket(key.value, encoded, cephx.ServiceMgr, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
	}
	e := wire.Encoder{}
	e.U16(0x100)
	e.U32(0)
	e.Raw(authTicket)
	e.Bytes(nil)
	e.Bytes(mgrTicket)
	if _, _, err := c.Finish(42, e.Data, false); err != nil {
		t.Fatal(err)
	}
	return c
}

func epochTestIssuer(cycle int, sequence uint64, endpoint string) authIssuerObservation {
	return authIssuerObservation{cycle: cycle, sequence: sequence, session: &session.Session{}, endpoint: endpoint, monMap: maps.Mon{Epoch: 10, AuthEpoch: 4}}
}

func TestEpochTraceRetainsExactIssuerForRetainedTicket(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	first := epochParsedAuth(t, nil, true)
	hooks.installed(epochTestIssuer(1, 1, "first-monitor:3300"), first)
	original, ok := r.reference(first.Tickets[cephx.ServiceMgr])
	if !ok {
		t.Fatal("missing first receipt")
	}
	// Only AUTH is replaced by this response. MGR came from the first MON.
	second := epochParsedAuth(t, first, false)
	hooks.installed(epochTestIssuer(1, 2, "second-monitor:3300"), second)
	retained, ok := r.reference(second.Tickets[cephx.ServiceMgr])
	if !ok || retained != original {
		t.Fatal("retained ticket issuer/install sequence was overwritten")
	}
	for _, snapshot := range []*cephx.Client{copyAuth(second), invalidateAuthTickets(second)} {
		if snapshot.Tickets[cephx.ServiceMgr].ReceiptIdentity() != first.Tickets[cephx.ServiceMgr].ReceiptIdentity() {
			t.Fatal("copy or invalidation changed parse receipt")
		}
	}
	authInstall, ok := r.reference(second.Tickets[cephx.ServiceAuth])
	if !ok || authInstall.Sequence != 2 || authInstall.IssuerEndpoint != "second-monitor:3300" {
		t.Fatal("new AUTH parse did not retain its own issuer")
	}
}

func TestEpochTraceRejectsWrongCycleIssuerAndReusedSecretID(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	old := epochParsedAuth(t, nil, true)
	hooks.installed(epochTestIssuer(1, 1, "first-monitor:3300"), old)
	r.beginCycle(2)
	current := epochParsedAuth(t, old, true)
	hooks.installed(epochTestIssuer(2, 2, "second-monitor:3300"), current)
	oldInstall, _ := r.reference(old.Tickets[cephx.ServiceMgr])
	expected, _ := r.reference(current.Tickets[cephx.ServiceMgr])
	if oldInstall.SecretID != expected.SecretID || oldInstall.BlobLabel != expected.BlobLabel || old.Tickets[cephx.ServiceMgr].ReceiptIdentity() == current.Tickets[cephx.ServiceMgr].ReceiptIdentity() {
		t.Fatal("reused ID/blob did not retain distinct parse identity")
	}
	underlying, err := current.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	auth, finish := hooks.manager(2, current, maps.Mgr{Name: "a", GlobalID: 77, Epoch: 8}, msgr.Address{}, session.ServiceAuth{Authorizer: underlying})
	if _, err := auth.Initial(); err != nil {
		t.Fatal(err)
	}
	auth.(*epochTracedAuth).AuthWriteObserved(msgr.AuthRequest, 128, true)
	finish(nil)
	attempt := r.snapshot("cold", 2)[0]
	if !epochAttemptMatches(attempt, expected, 2, "a", 77) {
		t.Fatal("matching provenance rejected")
	}
	if epochAttemptMatches(attempt, oldInstall, 2, "a", 77) {
		t.Fatal("same ID/blob hid wrong cycle and issuer")
	}
	wrongIssuer := expected
	wrongIssuer.IssuerEndpoint = "other-monitor:3300"
	if epochAttemptMatches(attempt, wrongIssuer, 2, "a", 77) {
		t.Fatal("wrong issuer accepted")
	}
	wrongCycle := expected
	wrongCycle.Cycle = 1
	if epochAttemptMatches(attempt, wrongCycle, 2, "a", 77) {
		t.Fatal("wrong cycle accepted")
	}
	if epochAttemptMatches(attempt, expected, 2, "b", 77) || epochAttemptMatches(attempt, expected, 2, "a", 78) {
		t.Fatal("wrong actual manager identity accepted")
	}
}

func TestEpochTraceFreezesSelectionAcrossReplacementAndLateInstallDelivery(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	selected := epochParsedAuth(t, nil, true)
	underlying, err := selected.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	auth, finish := hooks.manager(1, selected, maps.Mgr{Name: "a", GlobalID: 77}, msgr.Address{}, session.ServiceAuth{Authorizer: underlying})
	// Installation callback delivery can race selection. Join by opaque receipt,
	// not callback delivery order or a later cached c.auth pointer.
	hooks.installed(epochTestIssuer(1, 1, "first-monitor:3300"), selected)
	replacement := epochParsedAuth(t, selected, true)
	hooks.installed(epochTestIssuer(1, 2, "second-monitor:3300"), replacement)
	if _, err := auth.Initial(); err != nil {
		t.Fatal(err)
	}
	auth.(*epochTracedAuth).AuthWriteObserved(msgr.AuthRequest, 128, true)
	finish(context.Canceled)
	expected, _ := r.reference(selected.Tickets[cephx.ServiceMgr])
	newer, _ := r.reference(replacement.Tickets[cephx.ServiceMgr])
	attempt := r.snapshot("cold", 1)[0]
	if !epochAttemptMatches(attempt, expected, 1, "a", 77) || epochAttemptMatches(attempt, newer, 1, "a", 77) {
		t.Fatal("selected authorizer was attributed to later cached replacement")
	}
	if attempt.HandshakeCompleted || attempt.AuthDoneVerified || attempt.Outcome != "local_context_ended_acceptance_unknown" {
		t.Fatal("canceled wait implied server acceptance")
	}
	// Selected ticket and actual encoded authorizer must agree independently.
	wrong, err := replacement.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	tampered := copyAuth(replacement)
	ticket := tampered.Tickets[cephx.ServiceMgr]
	ticket.Blob = []byte("different-private-ticket")
	tampered.Tickets[cephx.ServiceMgr] = ticket
	wrapped, _ := hooks.manager(1, tampered, maps.Mgr{Name: "a", GlobalID: 77}, msgr.Address{}, session.ServiceAuth{Authorizer: wrong})
	if _, err := wrapped.Initial(); err != nil {
		t.Fatal(err)
	}
	if r.snapshot("cold", 1)[1].EncodedSame {
		t.Fatal("wrong frozen authorizer Blob matched selected ticket")
	}
}

func TestEpochTraceRedactionAndPartialWriteFacts(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	client := epochParsedAuth(t, nil, true)
	hooks.installed(epochTestIssuer(1, 1, "monitor:3300"), client)
	expected, _ := r.reference(client.Tickets[cephx.ServiceMgr])
	underlying, err := client.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	auth, finish := hooks.manager(1, client, maps.Mgr{Name: "a", GlobalID: 77}, msgr.Address{}, session.ServiceAuth{Authorizer: underlying})
	payload, err := auth.Initial()
	if err != nil {
		t.Fatal(err)
	}
	auth.(*epochTracedAuth).AuthWriteObserved(msgr.AuthRequest, 7, false)
	finish(errors.New("never-log-private-transport-error"))
	attempt := r.snapshot("cold", 1)[0]
	if epochAttemptMatches(attempt, expected, 1, "a", 77) || attempt.HandshakeCompleted || attempt.AuthDoneVerified {
		t.Fatal("partial send was reported as accepted/completed")
	}
	data, err := json.Marshal(attempt)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"opaque", "never-log-private-transport-error", string(payload), string(client.Tickets[cephx.ServiceMgr].Blob)} {
		if strings.Contains(string(data), secret) {
			t.Fatal("provenance leaked private ticket/authorizer/error")
		}
	}
	secondRun := newEpochTrace(t)
	if r.label("ticket-blob", []byte("opaque")) == secondRun.label("ticket-blob", []byte("opaque")) {
		t.Fatal("labels correlate separate runs")
	}
	attempt.Writes[0].Complete = true
	if r.snapshot("cold", 1)[0].Writes[0].Complete {
		t.Fatal("snapshot aliases recorded write metadata")
	}
}

func TestEpochTraceCallbacksRunAfterPublicationOutsideClientLockAndTolerateClose(t *testing.T) {
	for _, closeDuringCallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%v", closeDuringCallback), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			trace := newEpochTrace(t)
			trace.beginCycle(1)
			hooks := trace.hooks("cold")
			install := hooks.installed
			var owner atomic.Pointer[Client]
			callback := make(chan State, 1)
			installed := make(chan struct{}, 1)
			closed := make(chan struct{})
			hooks.installed = func(info authIssuerObservation, auth *cephx.Client) {
				c := owner.Load()
				if c != nil {
					// Reentrant Snapshot would deadlock if Client.mu were held.
					callback <- c.Snapshot()
					if closeDuringCallback {
						// Close must be asynchronous because a production callback
						// can run on the supervisor worker Close joins.
						go func() { c.Close(); close(closed) }()
						<-c.ctx.Done()
					}
				}
				install(info, auth)
				if c != nil {
					installed <- struct{}{}
				}
			}
			c, err := Dial(context.WithValue(ctx, authTraceContextKey{}, hooks), mockOptions(t, 20, [16]byte{1}))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			owner.Store(c)
			// Only the supervisor reconnects after bootstrap. A direct
			// connectMonitor call would race its first loop iteration, which
			// can retire this candidate as its own predecessor (GOMAXPROCS=1).
			c.mu.Lock()
			first := c.mon
			c.mu.Unlock()
			first.Fail(errors.New("test: force supervised MON renewal"))
			select {
			case state := <-callback:
				if !state.Monitor.Ready || state.GlobalID == 0 {
					t.Fatal("callback preceded actual ticket publication")
				}
			case <-ctx.Done():
				t.Fatal("install callback blocked while reentering Snapshot")
			}
			select {
			case <-installed:
			case <-ctx.Done():
				t.Fatal("install callback/Close race did not complete")
			}
			if !closeDuringCallback {
				if s, err := c.monitor(ctx); err != nil || s == first {
					t.Fatal("supervised renewal did not publish a fresh MON session", err)
				}
			}
			if closeDuringCallback {
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("asynchronous Close did not complete")
				}
			}
			current := c.snapshotAuth().Tickets[cephx.ServiceMgr]
			if proof, ok := trace.reference(current); !ok || proof.Sequence != 2 {
				t.Fatal("published renewed ticket was not attributed to second install")
			}
		})
	}
}

func TestEpochTraceManagerCallbackCanReenterSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	trace := newEpochTrace(t)
	trace.beginCycle(1)
	hooks := trace.hooks("cold")
	manager := hooks.manager
	var owner atomic.Pointer[Client]
	var reentered atomic.Bool
	hooks.manager = func(cycle int, auth *cephx.Client, target maps.Mgr, address msgr.Address, underlying session.Authenticator) (session.Authenticator, func(error)) {
		if c := owner.Load(); c != nil {
			state := c.Snapshot()
			if state.Manager.GlobalID != target.GlobalID {
				t.Error("selection used a different manager snapshot")
			}
			reentered.Store(true)
		}
		return manager(cycle, auth, target, address, underlying)
	}
	c, err := Dial(context.WithValue(ctx, authTraceContextKey{}, hooks), mockOptions(t, 20, [16]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	owner.Store(c)
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	if !reentered.Load() {
		t.Fatal("manager selection hook did not run")
	}
	attempts := trace.snapshot("cold", 1)
	expected, ok := trace.reference(c.snapshotAuth().Tickets[cephx.ServiceMgr])
	state := c.Snapshot()
	if !ok || len(attempts) != 1 || !epochAttemptMatches(attempts[0], expected, 1, state.Manager.Name, state.Manager.GlobalID) || !attempts[0].AuthDoneVerified || !attempts[0].HandshakeCompleted {
		t.Fatal("synthetic full handshake lost selection/write/acceptance provenance")
	}
}

// Notification joins delayed observer delivery, without another authentication
// exchange, dial, service-key wipe, or sleep. ctx bounds local observation only.
func (r *epochTrace) waitReference(ctx context.Context, ticket cephx.Ticket) (epochTicketInstall, error) {
	receipt := ticket.ReceiptIdentity()
	if receipt == nil {
		return epochTicketInstall{}, errors.New("no parsed-ticket receipt")
	}
	for {
		if err := ctx.Err(); err != nil {
			return epochTicketInstall{}, err
		}
		r.mu.Lock()
		v, ok := r.installs[receipt]
		changed := r.changed
		r.mu.Unlock()
		if ok {
			return v, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return epochTicketInstall{}, ctx.Err()
		}
	}
}

func TestEpochTraceWaitsForDelayedInstallWithOriginalPublicationCycle(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	client := epochParsedAuth(t, nil, true)
	// Boundary metadata is captured before callback delivery gets delayed.
	issuer := epochTestIssuer(1, 1, "first-monitor:3300")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	joinedContext := &epochJoinWaitingContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan epochTicketInstall, 1)
	failed := make(chan error, 1)
	go func() {
		v, err := r.waitReference(joinedContext, client.Tickets[cephx.ServiceMgr])
		if err != nil {
			failed <- err
			return
		}
		result <- v
	}()
	select {
	case <-joinedContext.waiting:
	case <-ctx.Done():
		t.Fatal("join did not wait for unpublished observation")
	}
	select {
	case <-result:
		t.Fatal("missing receipt was fabricated before install callback")
	default:
	}
	r.beginCycle(2)
	hooks.installed(issuer, client)
	select {
	case v := <-result:
		if v.Cycle != 1 || v.Sequence != 1 || v.IssuerEndpoint != "first-monitor:3300" {
			t.Fatal("late delivery used callback's later cycle")
		}
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("delayed install join did not wake")
	}
	missing := epochParsedAuth(t, client, true)
	canceled, stop := context.WithCancel(context.Background())
	cancelJoin := &epochJoinWaitingContext{Context: canceled, waiting: make(chan struct{})}
	waiting := make(chan error, 1)
	go func() { _, err := r.waitReference(cancelJoin, missing.Tickets[cephx.ServiceMgr]); waiting <- err }()
	select {
	case <-cancelJoin.waiting:
	case <-ctx.Done():
		t.Fatal("cancel test did not enter observation wait")
	}
	stop()
	select {
	case err := <-waiting:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("join lost cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("observation join did not cancel")
	}
}

type epochJoinWaitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *epochJoinWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
func (r *epochTrace) ticketReferences(auth *cephx.Client) []epochTicketInstall {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []epochTicketInstall
	for _, service := range []uint32{cephx.ServiceAuth, cephx.ServiceMgr, cephx.ServiceOSD} {
		if install, ok := r.installs[auth.Tickets[service].ReceiptIdentity()]; ok {
			out = append(out, install)
		}
	}
	return out
}

func TestEpochTraceKeepsExplicitRefusalSeparateFromCompletedWrite(t *testing.T) {
	r := newEpochTrace(t)
	r.beginCycle(1)
	hooks := r.hooks("cold")
	client := epochParsedAuth(t, nil, true)
	hooks.installed(epochTestIssuer(1, 1, "monitor:3300"), client)
	underlying, err := client.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	auth, finish := hooks.manager(1, client, maps.Mgr{Name: "a", GlobalID: 77}, msgr.Address{}, session.ServiceAuth{Authorizer: underlying})
	if _, err := auth.Initial(); err != nil {
		t.Fatal(err)
	}
	auth.(*epochTracedAuth).AuthWriteObserved(msgr.AuthRequest, 128, true)
	finish(&cephx.AuthenticationError{Method: 2, Code: -13})
	attempt := r.snapshot("cold", 1)[0]
	if !attempt.Writes[0].Complete || attempt.AuthDoneVerified || attempt.HandshakeCompleted || attempt.Outcome != "explicit_authentication_refusal" || attempt.RefusalMethod != 2 || attempt.RefusalCode != -13 {
		t.Fatal("server refusal was hidden or confused with successful local write")
	}
}
