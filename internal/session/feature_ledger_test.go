package session

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestFeatureLedgerMasks keeps FEATURE_LEDGER.md identical to the masks the
// handshake advertises.
func TestFeatureLedgerMasks(t *testing.T) {
	doc, err := os.ReadFile("FEATURE_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	mgrS, mgrR := handshakeFeatures(16, nil)
	monS, monR := handshakeFeatures(1, nil)
	osdS, osdR := handshakeFeatures(4, nil)
	for name, got := range map[string]uint64{
		"base supported":      mgrS,
		"base required":       mgrR,
		"mon supported":       monS,
		"mon required":        monR,
		"osd supported":       osdS,
		"osd required":        osdR,
		"v8 profile":          replicatedPGV8Features,
		"reply v8 profile":    replicatedPGReplyV8Features,
		"request v9 profile":  replicatedPGRequestV9Features,
		"cephfs supported":    CephFSFeatures,
		"cephfs required":     CephFSFeatures&^Features | RequiredFeatures,
		"fresh mds supported": Features,
		"fresh mds required":  RequiredFeatures,
	} {
		if want := fmt.Sprintf("`0x%016x`", got); !strings.Contains(string(doc), want) {
			t.Errorf("%s mask %s is not in FEATURE_LEDGER.md", name, want)
		}
	}
}
