package cephmsgr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// epochAuthBundleRequest is the cold client's frozen record of its first
// rejected MGR authentication in the fixture's collect-auth-bundle schema
// (integration/fixture/auth_bundle.go). Ticket labels are the trace's opaque
// run-scoped HMACs; no key, ticket or proof is included.
type epochAuthBundleRequest struct {
	Label      string `json:"label"`
	Provenance struct {
		Producer string `json:"producer"`
		Subject  string `json:"subject"`
		Run      string `json:"run"`
	} `json:"provenance"`
	Expected struct {
		FSID     string   `json:"fsid"`
		Monitors []string `json:"monitors"`
		Manager  string   `json:"manager"`
		GID      uint64   `json:"gid"`
	} `json:"expected_target"`
	Rejection struct {
		Run         string    `json:"run"`
		Cycle       int       `json:"cycle"`
		Attempt     int       `json:"attempt"`
		At          time.Time `json:"at"`
		Target      string    `json:"target"`
		TargetGID   uint64    `json:"target_gid"`
		ClientID    uint64    `json:"client_id,omitempty"`
		Service     int       `json:"service,omitempty"`
		SecretID    *uint64   `json:"secret_id,omitempty"`
		TicketLabel string    `json:"ticket_label,omitempty"`
		Result      int       `json:"result"`
		Error       string    `json:"error"`
	} `json:"first_failure"`
	Held *struct {
		ClientID uint64 `json:"client_id"`
		Usable   bool   `json:"usable"`
	} `json:"held_session,omitempty"`
	Tickets []epochAuthBundleTicket `json:"consumer_tickets,omitempty"`
}

type epochAuthBundleTicket struct {
	Run             string `json:"run"`
	Cycle           int    `json:"cycle"`
	Service         int    `json:"service"`
	SecretID        uint64 `json:"secret_id"`
	Label           string `json:"label"`
	IssuerMonmap    uint64 `json:"issuer_monmap_epoch,omitempty"`
	IssuerAuthEpoch uint64 `json:"issuer_auth_epoch,omitempty"`
}

// freezeEpochAuthFailure records the first cold MGR rejection of cycle with
// the identities the clients saw, before any diagnostic runs.
func freezeEpochAuthFailure(t *testing.T, cycle int, at time.Time, cold, held State, attempts []epochMgrAttempt, installs []epochTicketInstall, err error) epochAuthBundleRequest {
	var request epochAuthBundleRequest
	request.Label = "auth-epoch"
	request.Provenance.Producer = "ceph-msgr-go integration/fixture"
	request.Provenance.Subject = "ceph-msgr-go cephmsgr cold client"
	request.Provenance.Run = t.Name()
	request.Expected.FSID = cold.FSID
	for _, member := range cold.Monitor.Members {
		request.Expected.Monitors = append(request.Expected.Monitors, member.Name)
	}
	slices.Sort(request.Expected.Monitors)
	request.Expected.Manager, request.Expected.GID = cold.Manager.Name, cold.Manager.GlobalID
	rejection := &request.Rejection
	rejection.Run, rejection.Cycle, rejection.At, rejection.ClientID, rejection.Error = t.Name(), cycle, at.UTC(), cold.GlobalID, fmt.Sprint(err)
	if len(attempts) > 0 {
		first := attempts[0]
		request.Expected.Manager, request.Expected.GID = first.MgrName, first.MgrID
		secretID := first.SecretID
		rejection.Attempt, rejection.ClientID, rejection.SecretID, rejection.TicketLabel, rejection.Result = int(first.Attempt), first.ClientID, &secretID, first.SelectedBlob, int(first.RefusalCode)
		if first.Install != nil {
			rejection.Service = int(first.Install.Service)
		}
	}
	if request.Expected.Manager != "" {
		rejection.Target = "mgr." + request.Expected.Manager
	}
	rejection.TargetGID = request.Expected.GID
	request.Held = &struct {
		ClientID uint64 `json:"client_id"`
		Usable   bool   `json:"usable"`
	}{held.GlobalID, held.Manager.Ready}
	for _, install := range installs {
		request.Tickets = append(request.Tickets, epochAuthBundleTicket{Run: t.Name(), Cycle: install.Cycle, Service: int(install.Service), SecretID: install.SecretID,
			Label: install.BlobLabel, IssuerMonmap: uint64(install.MonEpoch), IssuerAuthEpoch: uint64(install.AuthEpoch)})
	}
	return request
}

// collectEpochAuthBundle asks the fixture for the bounded first-failure
// bundle and logs it. It never changes the test's outcome: a bundle that
// cannot be collected is logged as unavailable before the caller reports the
// original rejection. The fixture never repeats the authentication or the
// wipe, raises no debug level, and reports generation equality as
// unavailable/unverified.
func collectEpochAuthBundle(t *testing.T, request epochAuthBundleRequest) {
	t.Helper()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	input, err := json.Marshal(request)
	if control == "" || err != nil {
		t.Logf("AUTH_FIRST_FAILURE_BUNDLE unavailable: control=%t encode=%v", control != "", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	if err := testcluster.RequestControl(ctx, control, "collect-auth-bundle", "auth-bundle-collect", base64.RawURLEncoding.EncodeToString(input)); err != nil {
		t.Logf("AUTH_FIRST_FAILURE_BUNDLE unavailable: %v", err)
		return
	}
	data, err := os.ReadFile(filepath.Join(control, "auth-first-failure-bundle.json"))
	var receipt struct {
		Label     string `json:"label"`
		Rejection struct {
			At time.Time `json:"at"`
		} `json:"first_failure"`
		GenerationEquality string          `json:"generation_equality"`
		Verdict            json.RawMessage `json:"verdict"`
	}
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &receipt) != nil || receipt.Label != request.Label || !receipt.Rejection.At.Equal(request.Rejection.At) {
		t.Logf("AUTH_FIRST_FAILURE_BUNDLE unavailable: receipt %v", err)
		return
	}
	t.Logf("AUTH_FIRST_FAILURE_BUNDLE generation_equality=%s verdict=%s", receipt.GenerationEquality, receipt.Verdict)
	t.Logf("AUTH_FIRST_FAILURE_BUNDLE %s", data)
}

func TestFreezeEpochAuthFailureCarriesNoSecrets(t *testing.T) {
	attempt := epochMgrAttempt{receipt: "never-log-receipt", Client: "cold", Cycle: 1, Attempt: 1, ClientID: 42, SecretID: 3, SelectedBlob: "hmac-label",
		MgrName: "a", MgrID: 4161, RefusalCode: -13, Install: &epochTicketInstall{Service: 16}}
	cold := State{FSID: "f5", GlobalID: 42, Monitor: MonitorState{Members: []MonitorMember{{Name: "b"}, {Name: "a"}, {Name: "c"}}}, Manager: ManagerState{Name: "b", GlobalID: 4170}}
	held := State{GlobalID: 41, Manager: ManagerState{Ready: true}}
	request := freezeEpochAuthFailure(t, 1, time.Unix(100, 0), cold, held, []epochMgrAttempt{attempt},
		[]epochTicketInstall{{Cycle: 1, Service: 16, SecretID: 3, BlobLabel: "hmac-label", MonEpoch: 5, AuthEpoch: 3}}, errors.New("authentication rejected"))
	data, err := json.Marshal(request)
	// The selected attempt, not the client's later MgrMap view, names the target.
	if err != nil || request.Rejection.Target != "mgr.a" || request.Expected.GID != 4161 || request.Rejection.Result != -13 || *request.Rejection.SecretID != 3 ||
		!slices.Equal(request.Expected.Monitors, []string{"a", "b", "c"}) || len(request.Tickets) != 1 || request.Held == nil || !request.Held.Usable {
		t.Fatal("frozen request", string(data), err)
	}
	if bytes.Contains(data, []byte("never-log-receipt")) {
		t.Fatal("request carries the private attempt receipt")
	}
}
