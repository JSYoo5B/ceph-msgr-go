package integration_test

import (
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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// authFirstFailureRequest is this test's frozen record of a first rejected
// MGR authentication, in the fixture's collect-auth-bundle schema
// (integration/fixture/auth_bundle.go). It holds no key, ticket or proof.
type authFirstFailureRequest struct {
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
		Run       string    `json:"run"`
		Cycle     int       `json:"cycle"`
		Attempt   int       `json:"attempt"`
		At        time.Time `json:"at"`
		Target    string    `json:"target"`
		TargetGID uint64    `json:"target_gid"`
		ClientID  uint64    `json:"client_id,omitempty"`
		Result    int       `json:"result"`
		Error     string    `json:"error"`
	} `json:"first_failure"`
}

// freezeAuthFirstFailure records the rejection and the identities the client
// last saw, before any diagnostic runs.
func freezeAuthFirstFailure(t *testing.T, label string, state cephmsgr.State, cycle int, at time.Time, err error) authFirstFailureRequest {
	var request authFirstFailureRequest
	request.Label = label
	request.Provenance.Producer = "ceph-msgr-go integration/fixture"
	request.Provenance.Subject = "ceph-msgr-go cephmsgr client"
	request.Provenance.Run = t.Name()
	request.Expected.FSID = state.FSID
	for _, member := range state.Monitor.Members {
		request.Expected.Monitors = append(request.Expected.Monitors, member.Name)
	}
	slices.Sort(request.Expected.Monitors)
	request.Expected.Manager, request.Expected.GID = state.Manager.Name, state.Manager.GlobalID
	rejection := &request.Rejection
	rejection.Run, rejection.Cycle, rejection.Attempt, rejection.At = t.Name(), cycle, 1, at.UTC()
	if state.Manager.Name != "" {
		rejection.Target = "mgr." + state.Manager.Name
	}
	rejection.TargetGID, rejection.ClientID, rejection.Error = state.Manager.GlobalID, state.GlobalID, fmt.Sprint(err)
	var auth *cephmsgr.AuthenticationError
	if errors.As(err, &auth) {
		rejection.Result = int(auth.Code)
	}
	return request
}

// collectAuthFirstFailureBundle asks the fixture for the bounded first-failure
// bundle and logs its receipt. It never changes the test's outcome: a bundle
// that cannot be collected is logged as unavailable, and the caller reports
// its original failure afterwards. The fixture never repeats the tested
// authentication, raises no debug level and reports generation equality as
// unavailable/unverified.
func collectAuthFirstFailureBundle(t *testing.T, control string, request authFirstFailureRequest) {
	t.Helper()
	input, err := json.Marshal(request)
	if err != nil {
		t.Logf("AUTH_FIRST_FAILURE_BUNDLE unavailable: encode request: %v", err)
		return
	}
	// A context of its own: the test's may have ended with the failure. The
	// fixture's budget is one minute plus a stop allowance.
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
