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
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephTellDescriptionsIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	c, ctx := fixtureClient(t, time.Minute)
	if err := testcluster.ControlDaemon(ctx, control, "verify", "tell-descriptions", "initial"); err != nil {
		t.Fatal("native Tell catalog oracle", err)
	}
	metadata, err := os.ReadFile(filepath.Join(control, "tell-command-oracle-summary.json"))
	var manager struct {
		Name string `json:"manager_name"`
		ID   uint64 `json:"manager_id"`
	}
	if err != nil || json.Unmarshal(metadata, &manager) != nil {
		t.Fatal("native Tell oracle manager", err)
	}
	state := c.Snapshot()
	monName := ""
	for i, endpoint := range []string{"33300", "33301", "33302"} {
		if strings.HasSuffix(state.Monitor.Endpoint, ":"+endpoint) {
			monName = []string{"a", "b", "c"}[i]
		}
	}
	if monName == "" || manager.Name != state.Manager.Name || manager.ID != state.Manager.GlobalID {
		t.Fatal("native oracle target differs from admitted fixture target", state)
	}
	for _, route := range []struct {
		name string
		call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
	}{
		{"mon-" + monName, c.MonTell},
		{"mon-b", func(ctx context.Context, command cephmsgr.Command) (cephmsgr.Result, error) {
			return c.MonTellTo(ctx, "b", command)
		}},
		{"mgr", c.MgrTell},
	} {
		catalog, err := fetchCatalog(ctx, route.call)
		if err != nil {
			t.Fatal(route.name, "Tell descriptions", err)
		}
		oracle, err := os.ReadFile(filepath.Join(control, "tell-command-oracle-"+route.name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		// Both remote Tell and native CLI use admin_socket's fixed ALL feature
		// formatter. Every attribute, including boolean positional/req, must
		// match exactly; management catalog normalization is inapplicable here.
		actual, expected := catalogObject(t, catalog.Result.Data), catalogObject(t, oracle)
		if !reflect.DeepEqual(actual, expected) || len(catalog.Commands) != len(expected) {
			t.Fatal(route.name, "Tell catalog differs from native CLI", len(catalog.Commands), len(expected))
		}
		for _, command := range catalog.Commands {
			if command.Module != "" || command.Permission != "" || command.Flags != 0 {
				t.Fatal(route.name, "admin catalog invented management metadata")
			}
		}
		version := catalogDescription(t, catalog, "version")
		request, _ := json.Marshal(map[string]string{"prefix": version.Prefix, "format": "json"})
		result, err := route.call(ctx, cephmsgr.Command{JSON: request})
		var output tellVersion
		if err != nil || json.Unmarshal(result.Data, &output) != nil || output.Release != "tentacle" || !strings.HasPrefix(output.Version, "20.2.") {
			t.Fatal(route.name, "discovered daemon version command", err)
		}
		t.Logf("%s: %d daemon-local descriptions match native CLI; discovered version=%s", route.name, len(catalog.Commands), output.Version)
	}
}
