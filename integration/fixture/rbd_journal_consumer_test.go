package main

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

const rbdJournalConsumerSelector = "^TestCephRBDJournalConsumerIntegration$"

// The consumer sequence is admitted only under its exact selector on the
// single-OSD fixture, and only it gets the consumer plan.
func TestRBDJournalConsumerSelectorIsExclusive(t *testing.T) {
	three := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}
	one := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1"}
	p, err := rbdJournalProfile(t, one, rbdJournalConsumerSelector)
	if err != nil || p.testRun != rbdJournalConsumerSelector || p.rbdReplicas() != "1" || !slices.Equal(rbdJournalActionsFor(p), rbdJournalConsumerActions) {
		t.Fatal("consumer selector admission differs", p.testRun, err)
	}
	plan := rbdJournalPlanOf(strings.Repeat("a1", 16), p)
	if !plan.consumer() || plan.JournalOrder != rbdJournalTrimOrder || plan.objects() != rbdJournalConsumerObjectCount || !plan.owned(strings.Repeat("a1", 16)) {
		t.Fatal("consumer plan differs", plan)
	}
	if _, err := rbdJournalProfile(t, three, rbdJournalConsumerSelector); err == nil {
		t.Fatal("consumer selector admitted on three OSDs")
	}
	for _, widened := range []string{strings.TrimSuffix(rbdJournalConsumerSelector, "$"), strings.TrimPrefix(rbdJournalConsumerSelector, "^")} {
		if _, err := rbdJournalProfile(t, one, widened); err == nil {
			t.Fatal("widened consumer selector admitted", widened)
		}
	}
	for _, selector := range []string{rbdJournalTrimSelector, rbdJournalLostAppendSelector, rbdJournalReplicaHoldSelector} {
		if rbdJournalPlanOf(strings.Repeat("a1", 16), rbdSelectionProfile(t, selector)).consumer() {
			t.Fatal("another selection gets the consumer plan", selector)
		}
	}
	token := strings.Repeat("a1", 16)
	for _, actions := range [][]string{rbdJournalActions, rbdJournalLostAppendActions, rbdJournalTrimActions, rbdJournalReplicaHoldActions} {
		for next := 0; next <= len(actions); next++ {
			if _, _, err := parseRBDJournalRequest(actions, []string{"7", token, "produce"}, token, 6, next); err == nil {
				t.Fatal("produce admitted in another sequence", next)
			}
		}
	}
	for _, action := range rbdJournalConsumerActions {
		if !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
			t.Fatal("native journal driver does not admit", action)
		}
	}
	if rbdJournalReadOnly("produce") || rbdJournalInspected("produce") ||
		rbdJournalTarget(rbdJournalPlan{Image: "i", GoClient: "g"}, "produce") != "i:exclusive-lock,native-write,journal-trim,journal-client:g:disconnected" {
		t.Fatal("consumer action classification differs")
	}
	// Plans other than the consumer plan carry none of its fields.
	trim := rbdJournalTrimPlanFor(token)
	trim.LaggyEvents = plan.LaggyEvents
	if trim.owned(token) {
		t.Fatal("a trim plan with producer fields is owned")
	}
	broken := plan
	broken.MaxConcurrentObjectSets = 0
	if broken.owned(token) {
		t.Fatal("a consumer plan without its lag bound is owned")
	}
}

// The producer's output is accepted for the consumer plan only, and every
// part of its phases is checked.
func TestRBDJournalProduceOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdJournalConsumerPlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, action := range rbdJournalConsumerActions {
		if _, err := decodeRBDJournalNative(action, token, 9, plan, encode(rbdJournalOutput(token, plan, action))); err != nil {
			t.Fatal("well-formed consumer journal output refused", action, err)
		}
	}
	phase := func(v map[string]any, i int) map[string]any { return v["phases_produce"].([]map[string]any)[i] }
	for name, change := range map[string]func(map[string]any){
		"without phases":              func(v map[string]any) { delete(v, "phases_produce") },
		"without the laggy log":       func(v map[string]any) { delete(v, "laggy_log") },
		"without the trim log":        func(v map[string]any) { delete(v, "trim_log") },
		"without its final wait":      func(v map[string]any) { delete(v, "final_wait_ns") },
		"a phase missing":             func(v map[string]any) { v["phases_produce"] = v["phases_produce"].([]map[string]any)[:2] },
		"phases out of order":         func(v map[string]any) { phase(v, 0)["phase"], phase(v, 1)["phase"] = "trimmed", "produced" },
		"a phase without the lock":    func(v map[string]any) { phase(v, 1)["is_owner"] = false },
		"a phase without its wait":    func(v map[string]any) { delete(phase(v, 2), "wait") },
		"a phase wait without a poll": func(v map[string]any) { phase(v, 0)["wait"] = map[string]any{"polls": 0, "waited_ns": 1, "met": true} },
		"a phase with another blob": func(v map[string]any) {
			phase(v, 1)["blob"].(map[string]any)["name"] = rbdJournalProduceBlobName("produced")
		},
		"a phase blob of another length":  func(v map[string]any) { phase(v, 0)["blob"].(map[string]any)["length"] = 1 },
		"the first batch missing":         func(v map[string]any) { delete(phase(v, 0), "events") },
		"writes in the trimmed phase":     func(v map[string]any) { phase(v, 1)["events"] = phase(v, 0)["events"] },
		"both batches in the first phase": func(v map[string]any) { phase(v, 0)["events"] = v["events"] },
		"a phase with eight objects": func(v map[string]any) {
			phase(v, 2)["journal_objects"] = phase(v, 2)["journal_objects"].([]map[string]any)[:8]
		},
		"a phase exec on another object":  func(v map[string]any) { phase(v, 2)["class_calls"] = rbdJournalOutputCalls("journal.other") },
		"a phase without its proceed":     func(v map[string]any) { delete(phase(v, 2), "proceed_ns") },
		"the second batch missing":        func(v map[string]any) { v["events"] = v["events"].([]map[string]any)[:8] },
		"a laggy log over its bound":      func(v map[string]any) { v["laggy_log"].(map[string]any)["matched"] = 0 },
		"an observation of eight objects": func(v map[string]any) { v["journal_objects"] = v["journal_objects"].([]map[string]any)[:8] },
	} {
		value := rbdJournalOutput(token, plan, "produce")
		change(value)
		if _, err := decodeRBDJournalNative("produce", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed producer output accepted:", name)
		}
	}
	// The producer belongs to the consumer plan only.
	trim := rbdJournalTrimPlanFor(token)
	if _, err := decodeRBDJournalNative("produce", token, 9, trim, encode(rbdJournalOutput(token, plan, "produce"))); err == nil {
		t.Fatal("producer output accepted for the trim plan")
	}
	other := rbdJournalOutput(token, plan, "observe-registered")
	other["phases_produce"] = rbdJournalOutput(token, plan, "produce")["phases_produce"]
	if _, err := decodeRBDJournalNative("observe-registered", token, 9, plan, encode(other)); err == nil {
		t.Fatal("an observation with producer phases accepted")
	}
}

// rbdJournalOutputCalls is the synthetic exec list of rbdJournalOutput on
// header.
func rbdJournalOutputCalls(header string) []map[string]any {
	calls := []map[string]any{}
	for _, method := range rbdJournalCalls {
		calls = append(calls, map[string]any{"object": header, "class": "journal", "method": method, "input_hex": "", "code": 0, "output_hex": "00"})
	}
	return calls
}
