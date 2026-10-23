package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSoakReceiptCollectionIsImmutable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "osd-soak-v1-steady-worker-0.json")
	for _, data := range []string{"first", "first"} {
		if err := saveSoakReceipt(strings.NewReader(data), path); err != nil {
			t.Fatal("identical collection should be idempotent", err)
		}
	}
	if err := saveSoakReceipt(strings.NewReader("changed"), path); err == nil {
		t.Fatal("a later observation replaced the first receipt")
	}
	if names, err := soakReceiptNames(dir); err != nil || len(names) != 1 {
		t.Fatal("closed receipt name missing", names, err)
	}
	if err := os.Symlink(path, filepath.Join(dir, "osd-soak-v1-steady-worker-1.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := soakReceiptNames(dir); err == nil {
		t.Fatal("symlink receipt was admitted")
	}
}

func TestSoakSourceSHAArchiveAssertion(t *testing.T) {
	root := t.TempDir()
	sha := strings.Repeat("a", 40)
	if got := (profile{osdSoak: "4m", sourceSHA: sha}).testEnvironment("/out", "")["CEPH_MSGR_TEST_SOURCE_SHA"]; got != sha {
		t.Fatal("soak test source assertion not passed to test runtime", got)
	}
	if _, exists := (profile{}).testEnvironment("/out", "")["CEPH_MSGR_TEST_SOURCE_SHA"]; exists {
		t.Fatal("non-soak environment gained a source assertion")
	}
	if got, err := soakSourceSHA(root, sha); err != nil || got != sha {
		t.Fatal("archive source assertion refused", got, err)
	}
	for _, value := range []string{"", "short", strings.Repeat("A", 40), sha + "x"} {
		if _, err := soakSourceSHA(root, value); err == nil {
			t.Fatal("invalid archive source accepted", value)
		}
	}
}

func TestSoakAdmissionAndFinalizationUseSelectedParents(t *testing.T) {
	for _, tc := range []struct {
		name, replicas, store, selector string
		want                            []string
	}{
		{"single-memstore-default", "1", "memstore", "", []string{"steady"}},
		{"single-bluestore-default", "1", "bluestore", "", []string{"steady"}},
		{"replicated-pair", "3", "bluestore", "", []string{"steady", "failover"}},
		{"replicated-steady", "3", "bluestore", "^TestCephOSDSoakIntegration$", []string{"steady"}},
		{"single-steady", "1", "memstore", "^TestCephOSDSoakIntegration$", []string{"steady"}},
		{"replicated-failover", "3", "bluestore", "^TestCephOSDSoakFailoverIntegration$", []string{"failover"}},
		{"single-failover-unavailable", "1", "memstore", "^TestCephOSDSoakFailoverIntegration$", nil},
		{"different-parent", "3", "bluestore", "^TestCephOSDMutationIntegration$", nil},
		{"invalid-pattern", "3", "bluestore", "[", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_SOAK": "4m", "CEPH_MSGR_TEST_OSD_REPLICAS": tc.replicas, "CEPH_MSGR_TEST_OSD_STORE": tc.store, "CEPH_MSGR_TEST_RUN": tc.selector}
			p, err := loadProfile(func(key string) string { return env[key] })
			if tc.want == nil {
				if err == nil {
					t.Fatal("empty admitted workload reached fixture startup")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := soakExpectedTests(p); err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatal("admission receipt set differs", got, err)
			}
			project, out := t.TempDir(), t.TempDir()
			if err := os.Mkdir(filepath.Join(project, "tools"), 0o700); err != nil {
				t.Fatal(err)
			}
			// Exercise the real finalizer's external-verifier argument boundary.
			probe := "import json,sys\nfrom pathlib import Path\na=sys.argv[1:]\nPath(__file__).with_suffix('.args').write_text(json.dumps([a[i+1] for i,v in enumerate(a) if v=='--test']))\n"
			if err := os.WriteFile(filepath.Join(project, "tools", "osd_soak_receipts.py"), []byte(probe), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, "fixture-manifest.json"), []byte(`{"image_id":"synthetic","architecture":"amd64"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := finishSoakReceipts(project, out, p, nil, nil, time.Second); err != nil {
				t.Fatal(err)
			}
			var actual []string
			data, err := os.ReadFile(filepath.Join(project, "tools", "osd_soak_receipts.args"))
			if err != nil || json.Unmarshal(data, &actual) != nil || !reflect.DeepEqual(actual, tc.want) {
				t.Fatal("finalizer verifier expectations differ", string(data), err)
			}
			var end struct {
				Tests []string `json:"expected_tests"`
			}
			data, err = os.ReadFile(filepath.Join(out, "osd-soak-v1-fixture-termination.json"))
			if err != nil || json.Unmarshal(data, &end) != nil || !reflect.DeepEqual(end.Tests, tc.want) {
				t.Fatal("termination selection differs", string(data), err)
			}
		})
	}
	if err := finishSoakReceipts("", "", profile{}, errors.New("unused"), nil, 0); err != nil {
		t.Fatal("non-soak finalizer was activated", err)
	}
	if err := finishSoakReceipts("", "", profile{osdSoak: "4m", testRun: "no-workload"}, nil, nil, 0); err == nil {
		t.Fatal("unsupported selection reached receipt collection")
	}
}
