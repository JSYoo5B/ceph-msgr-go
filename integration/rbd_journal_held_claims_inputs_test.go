package integration_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

// rbdJournalHeldBreaker removes or truncates one input of a held-request
// record; it reports false when the record has no such input to break.
type rbdJournalHeldBreaker struct {
	name  string
	apply func(r *rbdJournalLostAppendRecord) bool
}

// rbdJournalHeldBreakers are every observation, read and record part the
// held-request and pending-append claims judge, each one missing, and every
// list those claims index, each one cut short. view names the expected
// native observations of the sample.
func rbdJournalHeldBreakers(views []string) []rbdJournalHeldBreaker {
	var out []rbdJournalHeldBreaker
	for _, v := range views {
		out = append(out, rbdJournalHeldBreaker{"view " + v + " missing", func(r *rbdJournalLostAppendRecord) bool { delete(r.Views, v); return true }},
			rbdJournalHeldBreaker{"view " + v + " of another action", func(r *rbdJournalLostAppendRecord) bool { r.Views[v].Action = "observe-other"; return true }})
	}
	reads := map[string]func(h *rbdJournalHeld) **nativeRBDJournalHeldRead{
		"placed": func(h *rbdJournalHeld) **nativeRBDJournalHeldRead { return &h.Placed },
		"held":   func(h *rbdJournalHeld) **nativeRBDJournalHeldRead { return &h.Held },
		"settled": func(h *rbdJournalHeld) **nativeRBDJournalHeldRead {
			return &h.Settled
		},
		"lost": func(h *rbdJournalHeld) **nativeRBDJournalHeldRead { return &h.Lost },
		"back": func(h *rbdJournalHeld) **nativeRBDJournalHeldRead {
			if h.Pending == nil {
				var none *nativeRBDJournalHeldRead
				return &none
			}
			return &h.Pending.Back
		},
	}
	for _, name := range slices.Sorted(maps.Keys(reads)) {
		at := reads[name]
		out = append(out,
			rbdJournalHeldBreaker{name + " read missing", func(r *rbdJournalLostAppendRecord) bool {
				p := at(r.Held)
				if *p == nil {
					return false
				}
				*p = nil
				return true
			}},
			rbdJournalHeldBreaker{name + " read with one placement", func(r *rbdJournalLostAppendRecord) bool {
				p := at(r.Held)
				if *p == nil {
					return false
				}
				(*p).Objects = (*p).Objects[:1]
				return true
			}},
			rbdJournalHeldBreaker{name + " read without placements", func(r *rbdJournalLostAppendRecord) bool {
				p := at(r.Held)
				if *p == nil {
					return false
				}
				(*p).Objects = nil
				return true
			}},
			rbdJournalHeldBreaker{name + " read without its PG query", func(r *rbdJournalLostAppendRecord) bool {
				p := at(r.Held)
				if *p == nil || (*p).Query == nil {
					return false
				}
				(*p).Query = nil
				return true
			}},
			rbdJournalHeldBreaker{name + " read whose PG has no acting set", func(r *rbdJournalLostAppendRecord) bool {
				p := at(r.Held)
				if *p == nil || len((*p).Objects) == 0 {
					return false
				}
				for i := range (*p).Objects {
					(*p).Objects[i].Acting = nil
				}
				return true
			}})
	}
	part := func(name string, f func(r *rbdJournalLostAppendRecord) bool) {
		out = append(out, rbdJournalHeldBreaker{name, f})
	}
	part("gate state missing", func(r *rbdJournalLostAppendRecord) bool { r.Gate = nil; return true })
	part("outcome missing", func(r *rbdJournalLostAppendRecord) bool { r.Outcome = nil; return true })
	part("dial count missing", func(r *rbdJournalLostAppendRecord) bool { r.DialsTotal = nil; return true })
	part("held ops missing", func(r *rbdJournalLostAppendRecord) bool { r.Held.Held.Ops = nil; return true })
	part("held op without events", func(r *rbdJournalLostAppendRecord) bool {
		for i := range r.Held.Held.Ops {
			r.Held.Held.Ops[i].Events = nil
		}
		return true
	})
	part("ping missing", func(r *rbdJournalLostAppendRecord) bool { r.Held.PingBefore = nil; return true })
	part("subject acting set missing", func(r *rbdJournalLostAppendRecord) bool { r.Held.GoActing = nil; return true })
	part("lease missing", func(r *rbdJournalLostAppendRecord) bool { ok := r.Held.Lease != nil; r.Held.Lease = nil; return ok })
	part("kill missing", func(r *rbdJournalLostAppendRecord) bool { ok := r.Held.Kill != nil; r.Held.Kill = nil; return ok })
	part("reconnect missing", func(r *rbdJournalLostAppendRecord) bool {
		ok := r.Held.Reconnect != nil
		r.Held.Reconnect = nil
		return ok
	})
	part("dials before close missing", func(r *rbdJournalLostAppendRecord) bool {
		ok := r.DialsBeforeClose != nil
		r.DialsBeforeClose = nil
		return ok
	})
	part("primary change missing", func(r *rbdJournalLostAppendRecord) bool {
		ok := r.Held.PrimaryChange != nil
		r.Held.PrimaryChange = nil
		return ok
	})
	part("primary change without placements", func(r *rbdJournalLostAppendRecord) bool {
		if r.Held.PrimaryChange == nil {
			return false
		}
		r.Held.PrimaryChange.Objects = nil
		return true
	})
	part("primary change to no acting set", func(r *rbdJournalLostAppendRecord) bool {
		if r.Held.PrimaryChange == nil {
			return false
		}
		for i := range r.Held.PrimaryChange.Objects {
			r.Held.PrimaryChange.Objects[i].After.Acting, r.Held.PrimaryChange.Objects[i].After.Up = nil, nil
		}
		return true
	})
	part("advance missing", func(r *rbdJournalLostAppendRecord) bool { ok := r.Held.Advance != nil; r.Held.Advance = nil; return ok })
	part("pending appends missing", func(r *rbdJournalLostAppendRecord) bool {
		ok := r.Held.Pending != nil
		r.Held.Pending = nil
		return ok
	})
	part("one pending append fewer", func(r *rbdJournalLostAppendRecord) bool {
		if r.Held.Pending == nil {
			return false
		}
		r.Held.Pending.Calls = r.Held.Pending.Calls[:len(r.Held.Pending.Calls)-1]
		return true
	})
	part("a pending append's outcome missing", func(r *rbdJournalLostAppendRecord) bool {
		if r.Held.Pending == nil {
			return false
		}
		r.Held.Pending.Calls[len(r.Held.Pending.Calls)-1].Outcome = nil
		return true
	})
	part("setup inspection missing", func(r *rbdJournalLostAppendRecord) bool {
		v := r.Views["setup"]
		v.InspectTotal, v.InspectErrors = nil, nil
		return true
	})
	part("replay inspection missing", func(r *rbdJournalLostAppendRecord) bool {
		v := r.Views["replay"]
		v.InspectTotal, v.InspectErrors = nil, nil
		return true
	})
	return out
}

// rbdJournalHeldJudgeSafely judges r at stage and reports a panic as a
// failure of its own.
func rbdJournalHeldJudgeSafely(r *rbdJournalLostAppendRecord, stage string) (problems []string, panicked any) {
	defer func() { panicked = recover() }()
	_, order := rbdJournalHeldPlan(r.Held.Mode)
	r.Order = slices.Clone(order[:slices.Index(order, stage)+1])
	return rbdJournalLostAppendClaims(r, stage), nil
}

// TestRBDJournalHeldClaimsRefuseMissingInputs breaks every input of every
// held-request and pending-append sample, one at a time: an expected native
// observation or fixture read missing, of another action, with its lists cut
// short or its PG query gone, and every other record part the claims judge.
// At each stage from the first one that uses the input on, the claims must
// return a finding and must not panic; at the final stage every break must
// be refused.
func TestRBDJournalHeldClaimsRefuseMissingInputs(t *testing.T) {
	samples := map[string]func() *rbdJournalLostAppendRecord{}
	for _, mode := range []string{rbdJournalVariantReplicaHold, rbdJournalVariantHeldPrimaryChange, rbdJournalVariantDeadPrimary} {
		samples[mode] = func() *rbdJournalLostAppendRecord { return rbdJournalHeldSample(mode) }
	}
	for _, mode := range []string{rbdJournalVariantPendingHold, rbdJournalVariantPrimaryRestart} {
		samples[mode] = func() *rbdJournalLostAppendRecord { return rbdJournalHeldPendingSample(mode) }
	}
	for _, mode := range slices.Sorted(maps.Keys(samples)) {
		sample := samples[mode]
		stages, _ := rbdJournalHeldPlan(mode)
		final := stages[len(stages)-1]
		breakers := rbdJournalHeldBreakers(slices.Sorted(maps.Keys(sample().Views)))
		applied := 0
		for _, b := range breakers {
			for _, stage := range stages {
				r := sample()
				if !b.apply(r) {
					continue
				}
				problems, panicked := rbdJournalHeldJudgeSafely(r, stage)
				if panicked != nil {
					t.Errorf("%s, %s: the claims panicked at %s: %v", mode, b.name, stage, panicked)
				} else if stage == final && len(problems) == 0 {
					t.Errorf("%s, %s: admitted at %s", mode, b.name, stage)
				}
				if stage == final {
					applied++
				}
			}
		}
		if applied < len(sample().Views)*2+10 {
			t.Fatal(fmt.Sprintf("%s: only %d breaks applied", mode, applied))
		}
	}
}
