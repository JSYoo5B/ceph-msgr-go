package cephmsgr

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// Receipt shape published by tools/mgr_command_outcome_oracle.py for an absent
// explicit entry, effective mode "upmap" and one bracketed active MGR.
const nativeMgrReceiptSample = `{"baseline":{"present":false},"phase":"restored","request_id":3,"token":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","views":{"balancer_status":{"code":0,"data":"ewogICAgImFjdGl2ZSI6IHRydWUsCiAgICAibW9kZSI6ICJ1cG1hcCIsCiAgICAicGxhbnMiOiBbXQp9","status":""},"config_entries":{"code":0,"data_bytes":137,"data_sha256":"a68563265988fc9328450b71ee18d8ab31594c655a5ddb15b79af9c27ab9d080","entries":[],"status":""},"config_get":{"code":0,"data":"dXBtYXAK","status":""},"manager_after":{"active_gid":4100,"active_name":"a","available":true,"code":0,"data_bytes":130,"data_sha256":"a780","epoch":9,"status":""},"manager_before":{"active_gid":4100,"active_name":"a","available":true,"code":0,"data_bytes":130,"data_sha256":"a780","epoch":9,"status":""}}}`

func TestNativeMgrReceiptSeparatesEffectiveModeFromExplicitEntry(t *testing.T) {
	var receipt nativeMgrOutcomeReceipt
	if err := json.Unmarshal([]byte(nativeMgrReceiptSample), &receipt); err != nil {
		t.Fatal(err)
	}
	views := receipt.Views
	if name, gid, stable := nativeMgrStable(views); !stable || name != "a" || gid != 4100 || nativeMgrMode(views) != "upmap" {
		t.Fatal("receipt lost its bracketed MGR identity or raw balancer mode", name, gid, stable)
	}
	if !nativeMgrMatches(views, nativeMgrState{mode: "upmap"}) {
		t.Fatal("absent explicit entry with effective upmap was not recognized")
	}
	// The same effective value does not establish an explicit override.
	if nativeMgrMatches(views, nativeMgrState{"upmap", explicitMgrMode("upmap")}) {
		t.Fatal("effective value alone was accepted as an explicit entry")
	}
	views.ConfigEntries.Entries = explicitMgrMode("upmap")
	if !nativeMgrMatches(views, nativeMgrState{"upmap", explicitMgrMode("upmap")}) || nativeMgrMatches(views, nativeMgrState{mode: "upmap"}) {
		t.Fatal("explicit entry presence was not compared exactly")
	}
	failed := receipt.Views
	failed.ConfigEntries.Code, failed.ConfigEntries.Entries = -13, nil
	if nativeMgrMatches(failed, nativeMgrState{mode: "upmap"}) {
		t.Fatal("a refused config dump established entry absence")
	}
	failed = receipt.Views
	failed.ConfigGet.Data = []byte("upmap")
	if nativeMgrMatches(failed, nativeMgrState{mode: "upmap"}) {
		t.Fatal("MON effective bytes were not compared exactly")
	}
	changed := receipt.Views
	gid := uint64(4101)
	changed.ManagerAfter.ActiveGID = &gid
	if _, _, stable := nativeMgrStable(changed); stable {
		t.Fatal("an active MGR change inside the bracket was accepted")
	}
}

func TestMgrOutcomeProbeRecordsManagerCommandType(t *testing.T) {
	message := msgr.CommandMessage(true, [16]byte{2}, []string{`{"prefix":"balancer mode","mode":"none"}`}, nil)
	message.Transaction, message.Sequence = 4, 9
	var encoded bytes.Buffer
	if err := msgr.NewWriter(&encoded, 128<<10).Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
	probe := &monOutcomeWrites{}
	probe.enabled.Store(true)
	conn := &monOutcomeConn{Conn: monOutcomeSink{}, record: probe, connection: 2, peer: "mgr.b"}
	_, _ = conn.Write(encoded.Bytes())
	writes, err := probe.matching(message.Front, nil)
	if err != nil || len(writes) != 1 || writes[0].Type != msgr.MgrCommandMessage || writes[0].Transaction != 4 || writes[0].Sequence != 9 || writes[0].Peer != "mgr.b" || !writes[0].Complete {
		t.Fatal("CRC MGR command identity", writes, err)
	}
}

func TestMgrOutcomeIdentityReceiptReadableAcrossFixtureUIDs(t *testing.T) {
	control := t.TempDir()
	writeMgrOutcomeIdentity(t, control, mgrOutcomeIdentity{OriginalManager: "a", OriginalGID: 4100, PaddedWrites: 1, OutcomeUnknown: true})
	path := filepath.Join(control, "mgr-command-outcome-identity.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0044 != 0044 {
		t.Fatal("non-secret identity metadata is unreadable across host/container UIDs", info, err)
	}
	data, err := os.ReadFile(path)
	var observed mgrOutcomeIdentity
	if err != nil || json.Unmarshal(data, &observed) != nil || observed.OriginalGID != 4100 || observed.PaddedWrites != 1 || !observed.OutcomeUnknown {
		t.Fatal("identity receipt lost the original MGR, local write count or unknown outcome", err)
	}
}
