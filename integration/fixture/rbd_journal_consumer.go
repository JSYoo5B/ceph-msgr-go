package main

import (
	"fmt"
	"slices"
)

// The journal consumer sequence turns the roles of the trimming sequence
// around: native librbd owns the image and journals, and the Go test is a
// registered journal client that reads the journal and commits its own
// position, as a mirroring consumer would. The image is created with the
// trim plan's 4 KiB journal objects (rbdJournalTrimOrder). After the setup
// the Go test registers its client; `observe-registered` is the read-only
// observation of that. `disconnect-oracle` flags the native oracle client
// disconnected, so that only the image client and the Go client can keep an
// object set. `produce` is one native read-write instance that owns the
// lock for the rest of the sequence (proceed signals rbd-journal-proceed-<n>,
// published state rbd-journal-state.json):
//
//   - `produced`: its first batch of writes is journaled and applied, and its
//     image client's commit position has left object set 0, while the Go
//     client's position still names set 0. Its JournalTrimmer keeps the set
//     (journal/JournalTrimmer.cc handle_metadata_updated at rbdBaseSource).
//   - `trimmed`: after the Go test committed a position past set 0 and
//     notified the journal header, the trimmer removed the set and raised
//     the journal's minimum set.
//   - `disconnected`: its second batch takes the active set more than
//     rbd_journal_max_concurrent_object_sets (the plan's, one) past the Go
//     client's position. When it next commits its own position its journal
//     metadata flags the Go client disconnected
//     (journal/JournalMetadata.cc schedule_laggy_clients_disconnect), and
//     the trimmer, which skips disconnected clients, trims past the Go
//     client's position.
//
// After the last proceed signal the instance closes and the driver observes
// the journal as every action does. The producer's debug log keeps the
// trimmer's lines (trim_log) and the laggy-client lines (laggy_log).

// capRBDJournalConsumer admits the journal consumer sequence: its test runs
// alone on a fresh RBD fixture whose journaling image is created with the
// consumer plan, and the journal control replaces the ordinary sequence with
// rbdJournalConsumerActions.
var capRBDJournalConsumer = defineExclusiveCapability(fixtureRBD, "rbd-journal-consumer",
	"run one test alone on a fresh RBD fixture with the journal consumer sequence (small journal objects, native oracle client disconnect, native producer with laggy-client disconnect and trim)",
	"RBD=1", nil)

var rbdJournalConsumerActions = []string{"setup", "observe-registered", "disconnect-oracle", "produce"}

// rbdJournalConsumerPhases is the producer's phase sequence.
var rbdJournalConsumerPhases = []string{"produced", "trimmed", "disconnected"}

// rbdJournalConsumerObjectCount is the number of journal data objects the
// consumer plan's observations read: four object sets of the default splay
// width.
const rbdJournalConsumerObjectCount = 16

// rbdJournalConsumerMaxSets is the plan's rbd_journal_max_concurrent_object_sets.
const rbdJournalConsumerMaxSets = 1

// rbdJournalConsumerPlanFor is the consumer sequence's plan: the trim plan's
// image and setup, then the producer's two batches of eight 3 KiB writes
// each, at offsets no setup event and no other write touches. With 4 KiB
// journal objects one such entry fills an object, so each batch of eight
// spans two object sets.
func rbdJournalConsumerPlanFor(token string) rbdJournalPlan {
	const k = 1 << 10
	p := rbdJournalTrimPlanFor(token)
	p.MaxConcurrentObjectSets = rbdJournalConsumerMaxSets
	for _, at := range []uint64{8, 16, 24, 32, 40, 48, 56, 80} {
		p.ProduceEvents = append(p.ProduceEvents, rbdJournalEvent{"write", at * k, 3 * k})
	}
	for _, at := range []uint64{88, 96, 104, 112, 120, 144, 152, 160} {
		p.LaggyEvents = append(p.LaggyEvents, rbdJournalEvent{"write", at * k, 3 * k})
	}
	return p
}

// rbdJournalPlanOf is the journaling image's plan for the selected profile.
func rbdJournalPlanOf(token string, p profile) rbdJournalPlan {
	if p.admits(capRBDJournalConsumer) {
		return rbdJournalConsumerPlanFor(token)
	}
	plan := rbdJournalPlanFor(token, p.admits(capRBDJournalTrim))
	if p.admits(capRBDJournalErasureHold) {
		plan.JournalPool = rbdGroupSecondPoolName(token)
	}
	return plan
}

// consumer says whether the plan is the consumer plan.
func (p rbdJournalPlan) consumer() bool { return len(p.ProduceEvents) != 0 }

// objects is the number of journal data objects every observation of the
// plan reads.
func (p rbdJournalPlan) objects() int {
	if p.consumer() {
		return rbdJournalConsumerObjectCount
	}
	return rbdJournalObjectCount
}

// consumerOwned checks the consumer plan's own fields: none outside it, and
// in it the trim plan's journal order, the one-set lag bound and two batches
// of writes only.
func (p rbdJournalPlan) consumerOwned() bool {
	writes := func(events []rbdJournalEvent) bool {
		return len(events) > 0 && !slices.ContainsFunc(events, func(e rbdJournalEvent) bool { return e.Kind != "write" || e.Offset+e.Length > p.Size })
	}
	if !p.consumer() {
		return len(p.LaggyEvents) == 0 && p.MaxConcurrentObjectSets == 0
	}
	return p.JournalOrder == rbdJournalTrimOrder && p.MaxConcurrentObjectSets == rbdJournalConsumerMaxSets && writes(p.ProduceEvents) && writes(p.LaggyEvents)
}

// rbdJournalProducePhase is one published phase of the producer: whether it
// owns the lock, the journal header's exec outputs (rbdJournalCalls), the
// journal objects with the binary view of their bytes, the writes of that
// phase and the bounded wait that preceded it.
type rbdJournalProducePhase struct {
	Phase          string              `json:"phase"`
	TimeNS         int64               `json:"time_ns"`
	IsOwner        bool                `json:"is_owner"`
	ClassCalls     []rbdWriteClassCall `json:"class_calls"`
	JournalObjects []rbdJournalObject  `json:"journal_objects"`
	Blob           rbdBaseBlob         `json:"blob"`
	Events         []rbdJournalApplied `json:"events,omitempty"`
	Wait           *rbdJournalWait     `json:"wait"`
	ProceedNS      int64               `json:"proceed_ns,omitempty"`
}

// rbdJournalWait is one of the producer's bounded waits: how often it looked,
// how long it waited and whether the state appeared.
type rbdJournalWait struct {
	Polls    int   `json:"polls"`
	WaitedNS int64 `json:"waited_ns"`
	Met      bool  `json:"met"`
}

// rbdJournalProduceBlobName is the binary view of the journal objects of one
// producer phase.
func rbdJournalProduceBlobName(phase string) string {
	return "rbd-journal-produce-" + phase + "-journal-objects.bin"
}

// rbdJournalProducePhasesValid checks the producer's phase list, whose exec
// outputs are of the journal header header: every phase
// once and in order, each with its observation and wait, the lock owned
// throughout, and the writes of the plan's two batches in the first and the
// last phase.
func rbdJournalProducePhasesValid(phases []rbdJournalProducePhase, plan rbdJournalPlan, header string) bool {
	if len(phases) != len(rbdJournalConsumerPhases) {
		return false
	}
	for i, p := range phases {
		ok := p.Phase == rbdJournalConsumerPhases[i] && p.TimeNS > 0 && (i == 0 || p.TimeNS >= phases[i-1].TimeNS) && p.IsOwner && len(p.ClassCalls) == len(rbdJournalCalls) &&
			len(p.JournalObjects) == plan.objects() && p.Blob.Name == rbdJournalProduceBlobName(p.Phase) && len(p.Blob.SHA256) == 64 && p.Wait != nil && p.Wait.Polls > 0 && p.Wait.WaitedNS >= 0
		for j, method := range rbdJournalCalls {
			if j < len(p.ClassCalls) {
				ok = ok && rbdLockCallValid(&p.ClassCalls[j], header, "journal", method)
			}
		}
		offset := uint64(0)
		for j, o := range p.JournalObjects {
			ok = ok && o.Number == uint64(j) && o.Offset == offset && o.Size <= rbdJournalObjectLimit && (o.Exists && len(o.SHA256) == 64 || !o.Exists && o.Size == 0 && o.SHA256 == "")
			offset += o.Size
		}
		ok = ok && p.Blob.Length == offset
		switch p.Phase {
		case "produced":
			ok = ok && rbdJournalEventsValid(p.Events, "produce", plan.ProduceEvents) && p.ProceedNS == 0
		case "trimmed":
			ok = ok && p.Events == nil && p.ProceedNS > 0
		case "disconnected":
			ok = ok && rbdJournalEventsValid(p.Events, "laggy", plan.LaggyEvents) && p.ProceedNS > 0
		}
		if !ok {
			return false
		}
	}
	return true
}

// checkRBDBlob verifies that one published binary view is named name and
// matches its receipt.
func checkRBDBlob(out, name string, blob rbdBaseBlob) error {
	if blob.Name != name {
		return fmt.Errorf("native RBD journal binary view %s is misnamed", name)
	}
	return checkRBDGroupBlob(out, blob)
}
