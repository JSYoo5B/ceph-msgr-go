package main

import "testing"

func TestOSDAppliedEpochRequiresSelectedDaemon(t *testing.T) {
	// Reproduce the CI evidence: osd.0 is fresh and trimmed, but the request
	// targets osd.1. Another daemon's readiness cannot authorize this target.
	receipt := []byte(`{"whoami":0,"cluster_fsid":"fixture","newest_map":"33","cluster_osdmap_trim_lower_bound":5}`)
	if ready, err := osdAppliedEpoch(receipt, 1, "fixture", 33); err == nil || ready {
		t.Fatal("accepted a different daemon's applied-map receipt", ready, err)
	}
	if ready, err := osdAppliedEpoch(receipt, 0, "fixture", 33); err != nil || !ready {
		t.Fatal("rejected the actual selected daemon", ready, err)
	}
}

func TestOSDAppliedEpochRejectsInvalidIdentityAndBounds(t *testing.T) {
	for _, receipt := range []string{
		`{"cluster_fsid":"fixture","newest_map":"33","cluster_osdmap_trim_lower_bound":5}`,
		`{"whoami":1,"cluster_fsid":"other","newest_map":"33","cluster_osdmap_trim_lower_bound":5}`,
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"broken","cluster_osdmap_trim_lower_bound":5}`,
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"4294967296","cluster_osdmap_trim_lower_bound":5}`,
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"0","cluster_osdmap_trim_lower_bound":0}`,
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"33","cluster_osdmap_trim_lower_bound":34}`,
	} {
		if ready, err := osdAppliedEpoch([]byte(receipt), 1, "fixture", 33); err == nil || ready {
			t.Fatal("accepted malformed native evidence", receipt, ready, err)
		}
	}
	for _, receipt := range []string{
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"32","cluster_osdmap_trim_lower_bound":5}`,
		`{"whoami":1,"cluster_fsid":"fixture","newest_map":"33","cluster_osdmap_trim_lower_bound":1}`,
	} {
		if ready, err := osdAppliedEpoch([]byte(receipt), 1, "fixture", 33); err != nil || ready {
			t.Fatal("published readiness before the target applied and trimmed", ready, err)
		}
	}
}
