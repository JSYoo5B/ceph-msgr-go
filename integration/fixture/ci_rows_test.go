package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// The scenario catalog is the set of rows declared beside the native tests
// ("//scenario:row" and "//scenario:suite" directives; .github/scenarios/README.md).
// The declarations have one reader, tools/scenario_declarations.py, used by
// the planner. This module never parses them: its catalog tests take the
// rows the planner read. tools/scenario_plan.py validate passes them in
// CEPH_MSGR_PLANNED_ROWS (a JSON list of {id, params}); any other run asks
// the planner for them (scenario_plan.py rows).
const ciPlannedRows = "CEPH_MSGR_PLANNED_ROWS"

var ciCatalogRows = sync.OnceValues(func() ([]ciScenario, error) {
	var data []byte
	var err error
	if path := os.Getenv(ciPlannedRows); path != "" {
		data, err = os.ReadFile(path)
	} else {
		command := exec.Command("python3", filepath.Join("tools", "scenario_plan.py"), "rows")
		command.Dir = filepath.Join("..", "..")
		command.Stderr = os.Stderr
		data, err = command.Output()
	}
	if err != nil {
		return nil, err
	}
	var rows []ciScenario
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
})

// ciRepositoryCatalog returns the rows the planner read from the declarations.
func ciRepositoryCatalog(t *testing.T) []ciScenario {
	t.Helper()
	rows, err := ciCatalogRows()
	if err != nil {
		t.Fatal("scenario rows from the planner:", err)
	}
	if len(rows) == 0 {
		t.Fatal("no scenario is declared")
	}
	return rows
}
