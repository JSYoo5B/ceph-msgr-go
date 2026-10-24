package cephmsgr

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObserveNativeServiceHolderRejectsOpenRequestsBeforePublishing(t *testing.T) {
	control := t.TempDir()
	token := strings.Repeat("a", 32)
	for _, request := range [][2]string{
		{"short", "start"}, {strings.ToUpper(token), "start"}, {token + " x", "start"}, {"../" + token[3:], "start"},
		{token, "restore"}, {token, ""}, {token, "START"}, {token, "start extra"}, {token, "kill"},
	} {
		if _, err := observeNativeServiceHolder(context.Background(), control, request[0], request[1]); err == nil {
			t.Fatal("open request was delegated", request)
		}
	}
	if entries, err := os.ReadDir(control); err != nil || len(entries) != 0 {
		t.Fatal("rejected requests published control files", entries, err)
	}
}

// serviceHolderController answers one request the way cluster.sh does: it reads
// the published request, writes the named receipt and then the acknowledgement.
func serviceHolderController(t *testing.T, control string, receipt func(id, token, phase string) string) {
	t.Helper()
	go func() {
		request := filepath.Join(control, "observe-service-holder")
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			data, err := os.ReadFile(request)
			if err != nil {
				continue
			}
			fields := strings.Fields(string(data))
			if len(fields) != 3 {
				return
			}
			_ = os.Remove(request)
			_ = os.WriteFile(filepath.Join(control, "native-service-holder-"+fields[1]+"."+fields[2]+".json"), []byte(receipt(fields[0], fields[1], fields[2])), 0o644)
			_ = os.WriteFile(filepath.Join(control, "service-holder-observed."+fields[0]), nil, 0o644)
			return
		}
	}()
}

func serviceHolderReceiptJSON(id, token, phase string, edit func(map[string]any)) string {
	value := map[string]any{"request_id": json.Number(id), "token": token, "phase": phase,
		"daemon": "holder-" + token[:16], "service": "ceph-msgr"}
	if edit != nil {
		edit(value)
	}
	data, _ := json.Marshal(value)
	return string(data)
}

func TestObserveNativeServiceHolderRejectsMismatchedReceipts(t *testing.T) {
	token := strings.Repeat("b", 32)
	for name, edit := range map[string]func(map[string]any){
		"token":   func(v map[string]any) { v["token"] = strings.Repeat("c", 32) },
		"phase":   func(v map[string]any) { v["phase"] = "observe" },
		"daemon":  func(v map[string]any) { v["daemon"] = "holder-0000000000000000" },
		"service": func(v map[string]any) { v["service"] = "rgw" },
		"request": func(v map[string]any) { v["request_id"] = 0 },
	} {
		control := t.TempDir()
		serviceHolderController(t, control, func(id, token, phase string) string {
			return serviceHolderReceiptJSON(id, token, phase, edit)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := observeNativeServiceHolder(ctx, control, token, "start"); err == nil || !strings.Contains(err.Error(), "mismatched identity") {
			t.Fatal(name, "mismatched receipt was accepted", err)
		}
		cancel()
	}
	control := t.TempDir()
	serviceHolderController(t, control, func(id, token, phase string) string { return serviceHolderReceiptJSON(id, token, phase, nil) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if receipt, err := observeNativeServiceHolder(ctx, control, token, "start"); err != nil || receipt.Daemon != "holder-"+token[:16] {
		t.Fatal("matching receipt was rejected", err)
	}
}

func TestObserveNativeServiceHolderBoundsReceipt(t *testing.T) {
	control := t.TempDir()
	token := strings.Repeat("d", 32)
	serviceHolderController(t, control, func(id, token, phase string) string { return strings.Repeat(" ", 256<<10+1) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := observeNativeServiceHolder(ctx, control, token, "observe"); err == nil || !strings.Contains(err.Error(), "exceeds its bound") {
		t.Fatal("oversized receipt was accepted", err)
	}
}

const nativeServiceHolderStartSample = `{"baseline":{"code":0,"epoch":7,"owned_present":false,"service_daemons":0},"daemon":"holder-bbbbbbbbbbbbbbbb","phase":"start","ready":{"pid":901,"gid":4242,"lifetime_seconds":120},"request_id":3,"service":"ceph-msgr","state":"ready","token":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`

const nativeServiceHolderObserveSample = `{"baseline":{"code":0,"epoch":7,"owned_present":false,"service_daemons":0},"daemon":"holder-bbbbbbbbbbbbbbbb","gid_matches_holder":true,"holder":{"alive":true,"exited":null,"failed":false,"ready":true,"started":true},"phase":"observe","request_id":4,"service":"ceph-msgr","token":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","view":{"code":0,"entry":{"daemon":"holder-bbbbbbbbbbbbbbbb","gid":4242,"role":"holder","start_epoch":5},"epoch":9,"present":true,"service_daemons":1}}`

func decodeServiceHolder(t *testing.T, text string, edit func(map[string]any)) nativeServiceHolderReceipt {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(value)
	}
	data, _ := json.Marshal(value)
	var receipt nativeServiceHolderReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestServiceHolderReadyAcceptsOnlyAnAbsentBaselineReadyHolder(t *testing.T) {
	ready, err := serviceHolderReady(decodeServiceHolder(t, nativeServiceHolderStartSample, nil))
	if err != nil || ready.PID != 901 || ready.GID != 4242 || ready.LifetimeSeconds != 120 {
		t.Fatal("ready holder was not decoded", ready, err)
	}
	for name, edit := range map[string]func(map[string]any){
		"phase":         func(v map[string]any) { v["phase"] = "observe" },
		"state":         func(v map[string]any) { v["state"] = "holder-spawn-outcome-unknown" },
		"no baseline":   func(v map[string]any) { v["baseline"] = nil },
		"present":       func(v map[string]any) { v["baseline"].(map[string]any)["owned_present"] = true },
		"unreadable":    func(v map[string]any) { v["baseline"].(map[string]any)["code"] = -13 },
		"missing code":  func(v map[string]any) { delete(v["baseline"].(map[string]any), "code") },
		"no readiness":  func(v map[string]any) { delete(v, "ready") },
		"zero gid":      func(v map[string]any) { v["ready"].(map[string]any)["gid"] = 0 },
		"zero pid":      func(v map[string]any) { v["ready"].(map[string]any)["pid"] = 0 },
		"no lifetime":   func(v map[string]any) { v["ready"].(map[string]any)["lifetime_seconds"] = 0 },
		"string gid":    func(v map[string]any) { v["ready"].(map[string]any)["gid"] = "4242" },
		"negative gid":  func(v map[string]any) { v["ready"].(map[string]any)["gid"] = -1 },
		"readiness nil": func(v map[string]any) { v["ready"] = nil },
	} {
		if _, err := serviceHolderReady(decodeServiceHolder(t, nativeServiceHolderStartSample, edit)); err == nil {
			// A receipt that cannot decode never reaches here; a decoded one must be refused.
			t.Fatal(name, "start receipt was accepted")
		}
	}
}

func TestServiceHolderViewsRequireTheHoldersOwnIdentity(t *testing.T) {
	ready := nativeServiceHolderReady{PID: 901, GID: 4242, LifetimeSeconds: 120}
	if !serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, nil), ready) {
		t.Fatal("matching holder view was not recognized")
	}
	for name, edit := range map[string]func(map[string]any){
		"phase":     func(v map[string]any) { v["phase"] = "cleanup" },
		"absent":    func(v map[string]any) { v["view"].(map[string]any)["present"] = false },
		"no entry":  func(v map[string]any) { v["view"].(map[string]any)["entry"] = nil },
		"other gid": func(v map[string]any) { v["view"].(map[string]any)["entry"].(map[string]any)["gid"] = 1 },
		"gid flag":  func(v map[string]any) { v["gid_matches_holder"] = false },
		"role":      func(v map[string]any) { v["view"].(map[string]any)["entry"].(map[string]any)["role"] = "other" },
		"daemon": func(v map[string]any) {
			v["view"].(map[string]any)["entry"].(map[string]any)["daemon"] = "holder-0000000000000000"
		},
		"refused":     func(v map[string]any) { v["view"].(map[string]any)["code"] = -13 },
		"no view":     func(v map[string]any) { v["view"] = nil },
		"no epoch":    func(v map[string]any) { v["view"].(map[string]any)["epoch"] = nil },
		"missing gid": func(v map[string]any) { delete(v["view"].(map[string]any)["entry"].(map[string]any), "gid") },
	} {
		if serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, edit), ready) {
			t.Fatal(name, "foreign or unreadable view was accepted as presence")
		}
	}
	if serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, nil), nativeServiceHolderReady{GID: 7}) {
		t.Fatal("a different holder GID matched")
	}
}

func TestServiceHolderAbsenceNeedsAReadableDump(t *testing.T) {
	absent := func(v map[string]any) {
		v["view"] = map[string]any{"code": 0, "epoch": 9, "present": false, "entry": nil, "service_daemons": 0}
	}
	if !serviceHolderAbsent(decodeServiceHolder(t, nativeServiceHolderObserveSample, absent)) {
		t.Fatal("readable dump without the daemon was not absence")
	}
	for name, edit := range map[string]func(map[string]any){
		"present":    nil,
		"refused":    func(v map[string]any) { absent(v); v["view"].(map[string]any)["code"] = -110 },
		"undecoded":  func(v map[string]any) { absent(v); v["view"].(map[string]any)["code"] = nil },
		"no epoch":   func(v map[string]any) { absent(v); v["view"].(map[string]any)["epoch"] = nil },
		"no view":    func(v map[string]any) { v["view"] = nil },
		"wrong step": func(v map[string]any) { absent(v); v["phase"] = "start" },
	} {
		if serviceHolderAbsent(decodeServiceHolder(t, nativeServiceHolderObserveSample, edit)) {
			t.Fatal(name, "was accepted as native absence")
		}
	}
}
