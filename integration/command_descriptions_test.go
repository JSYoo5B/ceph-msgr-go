package integration_test

import (
	"bytes"
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

func catalogObject(t *testing.T, data []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || len(object) == 0 {
		t.Fatal("invalid command catalog", err)
	}
	return object
}

// The fixed formatter uses negotiated connection features, not a JSON option.
// Native librados advertises SERVER_QUINCY: req is bool and positional may be
// present. Our connection does not: req retains its source string and positional
// is omitted. Normalize comparison copies only, leaving both raw outputs intact.
// Even the Tentacle source typo req="fasle" must survive in the product result.
func normalizeCatalogComparison(object map[string]any, native bool) {
	for _, raw := range object {
		entry := raw.(map[string]any)
		for _, part := range entry["sig"].([]any) {
			argument, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if required, exists := argument["req"]; exists {
				argument["req"] = required == true || required == "true" || required == "True"
			}
			if native {
				delete(argument, "positional")
			}
		}
	}
}

func compareNativeCatalog(t *testing.T, role string, catalog cephmsgr.CommandDescriptions, oracle []byte) {
	t.Helper()
	actual, expected := catalogObject(t, catalog.Result.Data), catalogObject(t, oracle)
	if catalog.Result.Code != 0 || len(catalog.Commands) != len(expected) {
		t.Fatal(role, "catalog count or server code", len(catalog.Commands), len(expected), catalog.Result.Code)
	}
	normalizeCatalogComparison(actual, false)
	normalizeCatalogComparison(expected, true)
	for i, description := range catalog.Commands {
		if i > 0 && catalog.Commands[i-1].ID >= description.ID {
			t.Fatal(role, "catalog IDs are not sorted and unique")
		}
		if !reflect.DeepEqual(actual[description.ID], expected[description.ID]) {
			t.Fatalf("%s %s differs from independent native catalog", role, description.ID)
		}
		var raw map[string]any
		if err := json.Unmarshal(description.Raw, &raw); err != nil || description.Help != raw["help"] || description.Module != raw["module"] || description.Permission != raw["perm"] {
			t.Fatal(role, description.ID, "typed metadata differs from raw response", err)
		}
	}
	t.Logf("%s: all %d descriptors match native catalog; %d raw bytes preserved", role, len(catalog.Commands), len(catalog.Result.Data))
}

func catalogDescription(t *testing.T, catalog cephmsgr.CommandDescriptions, prefix string) cephmsgr.CommandDescription {
	t.Helper()
	for _, description := range catalog.Commands {
		if description.Prefix == prefix {
			return description
		}
	}
	t.Fatal("catalog has no", prefix)
	return cephmsgr.CommandDescription{}
}

func fetchNativeCatalogs(t *testing.T, ctx context.Context, c *cephmsgr.Client, control, label string) (cephmsgr.CommandDescriptions, cephmsgr.CommandDescriptions) {
	t.Helper()
	if err := testcluster.ControlDaemon(ctx, control, "verify", "command-descriptions", label); err != nil {
		t.Fatal("fresh native command catalog", err)
	}
	data, err := os.ReadFile(filepath.Join(control, "command-oracle-summary.json"))
	var summary struct {
		Label string `json:"label"`
	}
	if err != nil || json.Unmarshal(data, &summary) != nil || summary.Label != label {
		t.Fatal("native catalog oracle barrier", err)
	}
	mon, err := c.MonCommandDescriptions(ctx)
	if err != nil {
		t.Fatal("MON command descriptions", err, mon.Result.Code)
	}
	mgr, err := c.MgrCommandDescriptions(ctx)
	if err != nil {
		t.Fatal("MGR command descriptions", err, mgr.Result.Code)
	}
	for _, route := range []struct {
		name    string
		catalog cephmsgr.CommandDescriptions
	}{{"mon", mon}, {"mgr", mgr}} {
		oracle, err := os.ReadFile(filepath.Join(control, "command-oracle-"+route.name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		compareNativeCatalog(t, route.name, route.catalog, oracle)
	}
	return mon, mgr
}

func TestCephCommandDescriptionsIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	c, ctx := fixtureClient(t, time.Minute)
	mon, mgr := fetchNativeCatalogs(t, ctx, c, control, "initial")
	config := catalogDescription(t, mon, "config get")
	if config.Module != "config" || config.Permission != "r" || len(config.Signature) != 4 || config.Signature[2].Argument.Name != "who" || config.Signature[3].Argument.Name != "key" || string(config.Signature[3].Argument.Attributes["req"]) != `"false"` {
		t.Fatal("MON config get argument description", config)
	}
	iostat := catalogDescription(t, mgr, "iostat")
	if iostat.Permission != "r" || len(iostat.Signature) != 3 || iostat.Signature[1].Argument.Type != "CephInt" || iostat.Signature[2].Argument.Type != "CephBool" {
		t.Fatal("MGR iostat argument description", iostat)
	}
	forwarded := catalogDescription(t, mon, "iostat")
	if forwarded.Flags&cephmsgr.CommandFlagManager == 0 || forwarded.Flags&cephmsgr.CommandFlagPoll == 0 || iostat.Flags != 0 {
		t.Fatal("MON forwarding metadata differs from direct MGR metadata")
	}
	// Execute read-only commands using discovered prefixes and named arguments.
	request, _ := json.Marshal(map[string]any{"prefix": config.Prefix, "who": "client.test", "key": "debug_ms"})
	if result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: request}); err != nil || len(result.Data) == 0 {
		t.Fatal("discovered MON command", err)
	}
	request, _ = json.Marshal(map[string]any{"prefix": iostat.Prefix, "width": 80, "print_header": true})
	if result, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: request}); err != nil || !strings.Contains(string(result.Data), "Read IOPS") {
		t.Fatal("discovered MGR command", err)
	}
	manager := c.Snapshot().Manager
	request, _ = json.Marshal(map[string]string{"prefix": "mgr fail", "who": manager.Name})
	// Submit this fixture mutation once. A new catalog fetch is a separate read,
	// never automatic replay of the fail command or any uncertain command.
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: request}); err != nil {
		t.Fatal("MGR replacement", err)
	}
	replacement := waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Manager.Available && state.Manager.GlobalID != manager.GlobalID
	})
	_, fresh := fetchNativeCatalogs(t, ctx, c, control, "mgr-replacement")
	_ = catalogDescription(t, fresh, "balancer status")
	_ = catalogDescription(t, fresh, "pg stat")
	t.Logf("fresh catalog from replacement MGR %s (global ID %d)", replacement.Manager.Name, replacement.Manager.GlobalID)
}
