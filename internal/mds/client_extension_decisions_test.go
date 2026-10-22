package mds

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// tentacleCephFSFeatureNames is the pinned feature_names array
// (mds/cephfs_features.cc:11-38, Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964); its last index is
// CEPHFS_FEATURE_MAX (mds/cephfs_features.h:54). A newer pinned source with
// more features must extend this list and the decision table together.
var tentacleCephFSFeatureNames = []string{
	"reserved", "reserved", "reserved", "reserved", "reserved",
	"jewel", "kraken", "luminous", "mimic", "reply_encoding",
	"reclaim_client", "lazy_caps_wanted", "multi_reconnect", "deleg_ino",
	"metric_collect", "alternate_name", "notify_session_state", "op_getvxattr",
	"32bits_retry_fwd", "new_snaprealm_info", "has_owner_uidgid",
	"client_mds_auth_caps", "charmap", "blockdiff",
}

const decisionsDocument = "CLIENT_EXTENSION_DECISIONS.md"

var (
	decisionGaps = map[string]bool{"codec": true, "lifecycle": true, "policy": true, "optional": true, "evidence": true}
	decisions    = map[string]bool{"implement": true, "defer": true, "consumer-owned": true}
	markdownLink = regexp.MustCompile(`\]\(([^)#]+)(?:#[^)]*)?\)`)
	goTest       = regexp.MustCompile(`\[(Test\w+)\]\(([^)]+_test\.go)\)`)
)

type decisionRow struct {
	name, session, cephfs, gap, decision string
}

// decisionTable reads the "Feature table" section: one row per bit.
func decisionTable(t *testing.T, text string) map[int]decisionRow {
	t.Helper()
	start := strings.Index(text, "\n## Feature table\n")
	if start < 0 {
		t.Fatal("missing Feature table section")
	}
	section := text[start+1:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	rows := map[int]decisionRow{}
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "| Bit ") || strings.HasPrefix(line, "| --- ") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		if len(cells) != 6 {
			t.Fatalf("decision row needs six cells: %q", line)
		}
		bit, err := strconv.Atoi(cells[0])
		if err != nil {
			t.Fatalf("decision row bit: %q", line)
		}
		if _, ok := rows[bit]; ok {
			t.Fatalf("duplicate decision row for bit %d", bit)
		}
		rows[bit] = decisionRow{strings.Trim(cells[1], "`"), cells[2], cells[3], cells[4], cells[5]}
	}
	return rows
}

func profileState(raw []byte, bit uint) string {
	if FeatureBitsetHas(raw, bit) {
		return "declared"
	}
	return "withheld"
}

// TestClientExtensionDecisionsCoverEveryPinnedFeature binds the decision
// table to the declared bitsets: a newly declared bit, a bit beyond the pinned
// list, a renamed feature or a withheld bit without a decision fails.
func TestClientExtensionDecisionsCoverEveryPinnedFeature(t *testing.T) {
	raw, err := os.ReadFile(decisionsDocument)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	rows := decisionTable(t, text)
	if len(rows) != len(tentacleCephFSFeatureNames) {
		t.Fatalf("decision table has %d rows, pinned source has %d features", len(rows), len(tentacleCephFSFeatureNames))
	}
	counts := map[string]int{}
	for bit, name := range tentacleCephFSFeatureNames {
		row, ok := rows[bit]
		if !ok {
			t.Fatalf("bit %d (%s) has no decision row", bit, name)
		}
		if row.name != name {
			t.Errorf("bit %d: row name %q, pinned name %q", bit, row.name, name)
		}
		session, cephfs := profileState(ClientFeatures, uint(bit)), profileState(CephFSClientFeatures, uint(bit))
		if row.session != session || row.cephfs != cephfs {
			t.Errorf("bit %d (%s): row says session %s / CephFS %s, code declares %s / %s", bit, name, row.session, row.cephfs, session, cephfs)
		}
		if session == "declared" && cephfs != "declared" {
			t.Errorf("bit %d: the CephFS profile must include every session-profile bit", bit)
		}
		if cephfs == "declared" {
			if row.gap != "-" || row.decision != "baseline" {
				t.Errorf("bit %d (%s): a declared bit is baseline with no gap", bit, name)
			}
			continue
		}
		if !decisions[row.decision] {
			t.Errorf("bit %d (%s): withheld bit needs implement, defer or consumer-owned, got %q", bit, name, row.decision)
		}
		counts[row.decision]++
		gaps := strings.Split(row.gap, ", ")
		for _, gap := range gaps {
			if !decisionGaps[gap] {
				t.Errorf("bit %d (%s): unknown gap %q", bit, name, gap)
			}
		}
		if heading := "\n### Bit " + strconv.Itoa(bit) + ": " + name + "\n"; !strings.Contains(text, heading) {
			t.Errorf("bit %d (%s): missing detail section %q", bit, name, strings.TrimSpace(heading))
		}
	}
	summary := "implement " + strconv.Itoa(counts["implement"]) + ", defer " + strconv.Itoa(counts["defer"]) + ", consumer-owned " + strconv.Itoa(counts["consumer-owned"]) + "."
	if !strings.Contains(strings.Join(strings.Fields(text), " "), summary) {
		t.Errorf("decision counts sentence does not match the table: want %q", summary)
	}
	// No declared bit may lie outside the pinned list.
	for _, profile := range [][]byte{ClientFeatures, CephFSClientFeatures} {
		for bit := uint(len(tentacleCephFSFeatureNames)); bit < uint(len(profile))*8; bit++ {
			if FeatureBitsetHas(profile, bit) {
				t.Errorf("declared bit %d is beyond the pinned feature list", bit)
			}
		}
	}
	// Codec constants named after a feature must agree with the pinned bit.
	for name, bit := range map[string]int{"reply_encoding": FeatureReplyEncoding, "reclaim_client": FeatureReclaimClient, "32bits_retry_fwd": Feature32BitsRetryFwd,
		"new_snaprealm_info": FeatureNewSnaprealmInfo, "has_owner_uidgid": FeatureHasOwnerUIDGID} {
		if tentacleCephFSFeatureNames[bit] != name {
			t.Errorf("Go feature constant for %s is bit %d (%s)", name, bit, tentacleCephFSFeatureNames[bit])
		}
	}
}

// TestClientExtensionDecisionReferencesExist checks repository-local links
// and that each linked Go test selector is defined in the linked file. It does
// not run those tests or judge the prose.
func TestClientExtensionDecisionReferencesExist(t *testing.T) {
	raw, err := os.ReadFile(decisionsDocument)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, match := range markdownLink.FindAllStringSubmatch(string(raw), -1) {
		target, err := filepath.Abs(match[1])
		if err != nil {
			t.Fatal(err)
		}
		if rel, err := filepath.Rel(root, target); err != nil || strings.HasPrefix(rel, "..") {
			t.Errorf("link leaves the repository: %s", match[1])
			continue
		}
		if info, err := os.Stat(target); err != nil || info.IsDir() {
			t.Errorf("missing linked file: %s", match[1])
		}
	}
	for _, match := range goTest.FindAllStringSubmatch(string(raw), -1) {
		source, err := os.ReadFile(match[2])
		if err != nil {
			t.Errorf("missing test file %s", match[2])
			continue
		}
		if !strings.Contains(string(source), "\nfunc "+match[1]+"(t *testing.T)") {
			t.Errorf("%s does not define %s", match[2], match[1])
		}
	}
}
