package main

import (
	"strings"
	"testing"
)

func TestMDSFreshPoolPolicyRejectsHashPSPoolOnEveryPool(t *testing.T) {
	const name = "ceph-msgr-fresh-0123"
	pools := func(entries ...poolPolicyEntry) poolPolicy {
		return poolPolicy{Epoch: 30, FSID: "f5", Pools: append([]poolPolicyEntry{
			{ID: 1, Name: ".mgr"}, {ID: 2, Name: "ceph-msgr-osd-fixture"},
		}, entries...)}
	}
	owned := []poolPolicyEntry{{ID: 3, Name: name + "-meta"}, {ID: 4, Name: name + "-data"}}
	if err := validateMDSFreshPoolPolicy(pools(owned...), name, true); err != nil {
		t.Fatal(err)
	}
	for test, policy := range map[string]poolPolicy{
		"metadata pool numeric bit": pools(poolPolicyEntry{ID: 3, Name: name + "-meta", Flags: 1}, owned[1]),
		"data pool flag name":       pools(owned[0], poolPolicyEntry{ID: 4, Name: name + "-data", FlagNames: "hashpspool"}),
		"automatic .mgr pool":       {Epoch: 30, FSID: "f5", Pools: append([]poolPolicyEntry{{ID: 1, Name: ".mgr", Flags: 1}, {ID: 2, Name: "ceph-msgr-osd-fixture"}}, owned...)},
		"missing owned pool":        pools(owned[0]),
		"missing map identity":      {Pools: pools(owned...).Pools},
	} {
		if err := validateMDSFreshPoolPolicy(policy, name, true); err == nil || !strings.Contains(err.Error(), "MDS fresh") {
			t.Errorf("%s accepted: %v", test, err)
		}
	}
}
