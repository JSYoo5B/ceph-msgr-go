package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// ciCatalogPath is the scenario catalog relative to the repository root. The
// rules mirror tools/scenario_catalog.py, the loader of every Python reader.
const ciCatalogPath = ".github/scenarios/catalog.d"

const ciCatalogIndex = "_catalog.json"

var ciCatalogFragment = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}\.json$`)

// ciLoadCatalog returns the scenario rows of a fragment directory, in file
// name order and file order within a fragment, or of a single catalog file.
// A duplicate identifier, a row in another group's file and a file that is
// neither the index nor a group fragment are refused.
func ciLoadCatalog(path string) ([]ciScenario, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		object, err := ciCatalogObject(path, "version", "scenarios", "fixed_jobs")
		if err != nil {
			return nil, err
		}
		if err := ciCatalogVersion(object, filepath.Base(path)); err != nil {
			return nil, err
		}
		var rows []ciScenario
		if err := json.Unmarshal(object["scenarios"], &rows); err != nil || rows == nil {
			return nil, fmt.Errorf("%s has no scenario list", filepath.Base(path))
		}
		return rows, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var fragments []string
	index := false
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case !entry.Type().IsRegular():
			return nil, fmt.Errorf("unknown catalog entry %s (only regular files)", name)
		case name == ciCatalogIndex:
			index = true
		case ciCatalogFragment.MatchString(name):
			fragments = append(fragments, name)
		default:
			return nil, fmt.Errorf("unknown catalog file %s", name)
		}
	}
	if !index {
		return nil, fmt.Errorf("catalog directory has no %s", ciCatalogIndex)
	}
	head, err := ciCatalogObject(filepath.Join(path, ciCatalogIndex), "version", "fixed_jobs")
	if err != nil {
		return nil, err
	}
	if err := ciCatalogVersion(head, ciCatalogIndex); err != nil {
		return nil, err
	}
	var scenarios []ciScenario
	owner := map[string]string{}
	for _, name := range fragments {
		group := strings.TrimSuffix(name, ".json")
		object, err := ciCatalogObject(filepath.Join(path, name), "scenarios")
		if err != nil {
			return nil, err
		}
		var raw []json.RawMessage
		if err := json.Unmarshal(object["scenarios"], &raw); err != nil || len(raw) == 0 {
			return nil, fmt.Errorf("%s has no scenarios", name)
		}
		for position, data := range raw {
			var row struct {
				ID    *string `json:"id"`
				Group *string `json:"group"`
			}
			var scenario ciScenario
			if json.Unmarshal(data, &row) != nil || json.Unmarshal(data, &scenario) != nil {
				return nil, fmt.Errorf("%s: row %d is not a catalog row", name, position)
			}
			label := fmt.Sprintf("row %d", position)
			if row.ID != nil {
				label = *row.ID
			}
			if row.Group == nil || *row.Group != group {
				return nil, fmt.Errorf("%s: %s is not in group %q", name, label, group)
			}
			if row.ID != nil {
				if previous, ok := owner[*row.ID]; ok {
					return nil, fmt.Errorf("duplicate identifier %s in %s and %s", *row.ID, previous, name)
				}
				owner[*row.ID] = name
			}
			scenarios = append(scenarios, scenario)
		}
	}
	return scenarios, nil
}

func ciCatalogObject(path string, allowed ...string) (map[string]json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, fmt.Errorf("%s is not a JSON object", filepath.Base(path))
	}
	for key := range object {
		if !slices.Contains(allowed, key) {
			return nil, fmt.Errorf("%s has unknown key %s", filepath.Base(path), key)
		}
	}
	return object, nil
}

func ciCatalogVersion(object map[string]json.RawMessage, name string) error {
	var version int
	if json.Unmarshal(object["version"], &version) != nil || version != 1 {
		return fmt.Errorf("%s: unsupported catalog version", name)
	}
	return nil
}

// ciRepositoryCatalog loads the repository's catalog for a fixture test.
func ciRepositoryCatalog(t *testing.T) []ciScenario {
	t.Helper()
	rows, err := ciLoadCatalog(filepath.Join("..", "..", filepath.FromSlash(ciCatalogPath)))
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func ciCatalogWrite(t *testing.T, dir, name, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCICatalogLoaderRefusesInconsistentFragments(t *testing.T) {
	row := func(id, group string) string {
		return fmt.Sprintf(`{"id":%q,"group":%q,"params":{"key":"aes"}}`, id, group)
	}
	setup := func(t *testing.T) string {
		dir := t.TempDir()
		ciCatalogWrite(t, dir, ciCatalogIndex, `{"version":1,"fixed_jobs":[]}`)
		ciCatalogWrite(t, dir, "osd.json", `{"scenarios":[`+row("osd.b", "osd")+`,`+row("osd.a", "osd")+`]}`)
		ciCatalogWrite(t, dir, "mon-mgr.json", `{"scenarios":[`+row("mon-mgr.a", "mon-mgr")+`]}`)
		return dir
	}
	rows, err := ciLoadCatalog(setup(t))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	if want := []string{"mon-mgr.a", "osd.b", "osd.a"}; !slices.Equal(ids, want) {
		t.Fatalf("composed order = %v, want %v", ids, want)
	}
	for _, tc := range []struct {
		name, want string
		change     func(t *testing.T, dir string)
	}{
		{"duplicate across fragments", "duplicate identifier osd.a in osd.json and rbd.json", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "rbd.json", `{"scenarios":[`+row("osd.a", "rbd")+`]}`)
		}},
		{"row in the wrong group's file", `rbd.json: rbd.a is not in group "rbd"`, func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "rbd.json", `{"scenarios":[`+row("rbd.a", "osd")+`]}`)
		}},
		{"row without a group", `rbd.json: rbd.a is not in group "rbd"`, func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "rbd.json", `{"scenarios":[{"id":"rbd.a","params":{}}]}`)
		}},
		{"unknown file", "unknown catalog file README.md", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "README.md", "{}")
		}},
		{"unknown json file", "unknown catalog file _other.json", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "_other.json", "{}")
		}},
		{"subdirectory", "unknown catalog entry nested", func(t *testing.T, dir string) {
			if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", "unknown catalog entry link.json", func(t *testing.T, dir string) {
			if err := os.Symlink("osd.json", filepath.Join(dir, "link.json")); err != nil {
				t.Skip("symlinks unavailable:", err)
			}
		}},
		{"empty fragment", "rbd.json has no scenarios", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "rbd.json", `{"scenarios":[]}`)
		}},
		{"unknown fragment key", "rbd.json has unknown key group", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, "rbd.json", `{"group":"rbd","scenarios":[`+row("rbd.a", "rbd")+`]}`)
		}},
		{"missing index", "catalog directory has no _catalog.json", func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, ciCatalogIndex)); err != nil {
				t.Fatal(err)
			}
		}},
		{"unsupported version", "_catalog.json: unsupported catalog version", func(t *testing.T, dir string) {
			ciCatalogWrite(t, dir, ciCatalogIndex, `{"version":2,"fixed_jobs":[]}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := setup(t)
			tc.change(t, dir)
			_, err := ciLoadCatalog(dir)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCICatalogLoaderReadsTheSingleFile(t *testing.T) {
	dir := t.TempDir()
	ciCatalogWrite(t, dir, "catalog.json", `{"version":1,"scenarios":[{"id":"a","group":"g","params":{"key":"aes"}}],"fixed_jobs":[]}`)
	rows, err := ciLoadCatalog(filepath.Join(dir, "catalog.json"))
	if err != nil || len(rows) != 1 || rows[0].ID != "a" || rows[0].Params["key"] != "aes" {
		t.Fatalf("rows = %#v, err = %v", rows, err)
	}
	ciCatalogWrite(t, dir, "catalog.json", `{"version":1}`)
	if _, err := ciLoadCatalog(filepath.Join(dir, "catalog.json")); err == nil {
		t.Fatal("a single file without scenarios was accepted")
	}
}
