package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephKeyringIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	data, err := os.ReadFile(filepath.Join(control, "clients.keyring"))
	if err != nil {
		t.Fatal("read native multi-identity keyring", err)
	}
	type credential struct {
		identity string
		key      cephmsgr.Key
	}
	var credentials []credential
	for _, test := range []struct{ identity, oracle string }{{"client.test", "key"}, {"client.readonly", "readonly.key"}} {
		key, err := cephmsgr.ParseKeyring(data, test.identity)
		if err != nil {
			t.Fatal("select native keyring identity", test.identity, err)
		}
		encoded, err := os.ReadFile(filepath.Join(control, test.oracle))
		if err != nil {
			t.Fatal("read independent ceph-authtool key oracle", err)
		}
		oracle, err := cephmsgr.ParseKey(strings.TrimSpace(string(encoded)))
		if err != nil || !reflect.DeepEqual(key, oracle) {
			t.Fatal("keyring selection differs from native --print-key", test.identity, err)
		}
		credentials = append(credentials, credential{identity: test.identity, key: key})
	}
	if _, err := cephmsgr.ParseKeyring(data, "client.absent"); err == nil {
		t.Fatal("absent identity selected another entity")
	}
	clear(data) // Returned keys must remain usable independently of the input.
	var previousID uint64
	for _, credential := range credentials {
		options := integrationOptions(t)
		options.Identity, options.Key = credential.identity, credential.key
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("authenticate from native text keyring", credential.identity, err)
		}
		func() {
			defer client.Close()
			state := client.Snapshot()
			if state.GlobalID == 0 || state.GlobalID == previousID || state.FSID != options.ExpectedFSID {
				t.Fatal("keyring identity did not establish a distinct admitted client", state)
			}
			previousID = state.GlobalID
			command, err := cephmsgr.NewCommand("status", map[string]any{"format": "json"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.MonCommand(ctx, command)
			var status struct {
				FSID string `json:"fsid"`
			}
			if err != nil || json.Unmarshal(result.Data, &status) != nil || status.FSID != state.FSID {
				t.Fatal("MON read using selected keyring credential", credential.identity, err)
			}
			command, err = cephmsgr.NewCommand("iostat", map[string]any{"width": 80, "print_header": false})
			if err != nil {
				t.Fatal(err)
			}
			if result, err := client.MgrCommand(ctx, command); err != nil || len(result.Data) == 0 {
				t.Fatal("MGR read using selected keyring credential", credential.identity, err)
			}
			t.Logf("%s: native text keyring matches --print-key and authenticates MON/MGR", credential.identity)
		}()
	}
}
