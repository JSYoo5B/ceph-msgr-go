package session

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestFeatureLedgerMasks binds each mask to its role and column, rather than
// accepting the same value elsewhere in the document.
func TestFeatureLedgerMasks(t *testing.T) {
	doc, err := os.ReadFile("FEATURE_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	mgrS, mgrR := handshakeFeatures(16, nil)
	monS, monR := handshakeFeatures(1, nil)
	osdS, osdR := handshakeFeatures(4, nil)
	want := map[string][2]uint64{
		"Base (MGR and any role without a row below)": {mgrS, mgrR},
		"MON (entity type 1)":                         {monS, monR},
		"OSD (entity type 4)":                         {osdS, osdR},
		"OSD replicated-PG v8 profile (opt-in)":       {replicatedPGV8Features, replicatedPGV8Features},
		"OSD reply-v8 profile (opt-in)":               {replicatedPGReplyV8Features, replicatedPGReplyV8Features},
		"OSD request-v9 profile (opt-in)":             {replicatedPGRequestV9Features, replicatedPGRequestV9Features},
		"Fresh MDS session":                           {Features, RequiredFeatures},
		"Fresh CephFS session":                        {CephFSFeatures, CephFSFeatures&^Features | RequiredFeatures},
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 5 {
			continue
		}
		role := strings.TrimSpace(cells[1])
		masks, ok := want[role]
		if !ok {
			continue
		}
		if seen[role] {
			t.Errorf("duplicate role %s", role)
		}
		seen[role] = true
		for column, mask := range masks {
			got := strings.TrimPrefix(strings.TrimSpace(cells[column+2]), "adds ")
			if expected := fmt.Sprintf("`0x%016x`", mask); got != expected {
				t.Errorf("%s column %d: got %s, want %s", role, column+1, got, expected)
			}
		}
	}
	for role := range want {
		if !seen[role] {
			t.Errorf("missing role %s", role)
		}
	}
}
