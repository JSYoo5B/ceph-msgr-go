package cephmsgr

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Select only an owned bootstrap endpoint. The authenticated map still governs
// later recovery; this is not a persistent pin or a product routing policy.
func currentMonTellBootstrap(options Options, target string) (Options, error) {
	if !monTellTargets[target] {
		return Options{}, errors.New("current MON Tell requires an owned MON name")
	}
	endpoint := testcluster.PeerEndpoint("mon." + target)
	if endpoint == "" {
		return Options{}, errors.New("current MON Tell requires the owned MON endpoint")
	}
	_, role, err := testcluster.SplitPeer(endpoint)
	if err != nil || role != "mon."+target {
		return Options{}, errors.New("current MON Tell bootstrap endpoint is not the owned target")
	}
	options.Monitors = []string{endpoint}
	return options, nil
}

func writeCurrentMonTellOutcomeIdentity(t *testing.T, control string, identity monTellOutcomeIdentity) {
	t.Helper()
	value := struct {
		Channel string `json:"channel"`
		monTellOutcomeIdentity
	}{Channel: "current-mon", monTellOutcomeIdentity: identity}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "mon-current-tell-outcome-identity.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentMonTellBootstrapUsesOnlyTheOwnedEndpoint(t *testing.T) {
	t.Setenv("CEPH_MSGR_TEST_PEERS", `{"mon.a":"192.0.2.1:3300","mon.b":"192.0.2.2:3300","mgr.a":"192.0.2.3"}`)
	original := Options{Monitors: []string{"192.0.2.1:3300", "192.0.2.2:3300"}, Identity: "client.test", MaxInFlight: 7}
	got, err := currentMonTellBootstrap(original, "b")
	if err != nil || len(got.Monitors) != 1 || got.Monitors[0] != "192.0.2.2:3300" || got.Identity != original.Identity || got.MaxInFlight != original.MaxInFlight {
		t.Fatal("owned bootstrap selection changed other caller options", got, err)
	}
	if len(original.Monitors) != 2 || original.Monitors[0] != "192.0.2.1:3300" {
		t.Fatal("caller bootstrap slice was mutated", original.Monitors)
	}
	for _, target := range []string{"", "mon.b", "*", "d", "c"} {
		if _, err := currentMonTellBootstrap(original, target); err == nil {
			t.Fatal("missing or non-owned MON selection was accepted", target)
		}
	}
	t.Setenv("CEPH_MSGR_TEST_PEERS", `{"mon.b":"not-an-endpoint"}`)
	if _, err := currentMonTellBootstrap(original, "b"); err == nil {
		t.Fatal("malformed owned endpoint accepted")
	}
}

func TestCurrentMonTellIdentityKeepsItsChannelAndPrimaryAttribution(t *testing.T) {
	control := t.TempDir()
	writeCurrentMonTellOutcomeIdentity(t, control, monTellOutcomeIdentity{PrimaryGlobalID: 77, Target: "mon.b", OutcomeUnknown: true})
	data, err := os.ReadFile(filepath.Join(control, "mon-current-tell-outcome-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["channel"]) != `"current-mon"` || string(fields["primary_client_global_id"]) != "77" || string(fields["outcome_unknown"]) != "true" || string(fields["tell_session_global_id_observed"]) != "false" {
		t.Fatal("current-channel evidence lost its attribution or uncertainty", string(data))
	}
	if _, err := os.Stat(filepath.Join(control, "mon-tell-outcome-identity.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("current-channel receipt overwrote the private-channel receipt", err)
	}
}
