package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func bootstrapSections(t *testing.T, config string) map[string]map[string]string {
	t.Helper()
	sections := map[string]map[string]string{}
	var section string
	for _, line := range strings.Split(config, "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.Trim(line, "[]")
			if sections[section] != nil {
				t.Fatal("duplicate bootstrap section", section)
			}
			sections[section] = map[string]string{}
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if !ok || section == "" || sections[section][key] != "" {
			t.Fatal("malformed/duplicate bootstrap key", line)
		}
		sections[section][key] = value
	}
	return sections
}

func TestFixtureBootstrapHashPSPoolOnlyOSDGlobalBeforeMon(t *testing.T) {
	for _, store := range []string{"memstore", "bluestore"} {
		for _, replicas := range []string{"1", "3"} {
			p := profile{osd: "1", osdStore: store, osdReplicas: replicas}
			config := fixtureBootstrapConfig(p)
			sections := bootstrapSections(t, config)
			if sections["global"]["osd_pool_default_flag_hashpspool"] != "false" ||
				sections["mon"]["osd_pool_default_flag_hashpspool"] != "" ||
				strings.Index(config, "osd_pool_default_flag_hashpspool = false") > strings.Index(config, "[mon]") {
				t.Fatal("pool default must reach every daemon's global bootstrap before startup", config)
			}
			if sections["global"]["osd_crush_update_on_start"] != "false" || sections["global"]["osd_beacon_report_interval"] != "1" ||
				sections["mon"]["paxos_service_trim_min"] != "1" || sections["mon"]["mon_min_osdmap_epochs"] != "5" || sections["mon"]["mon_osd_force_trim_to"] != "5" {
				t.Fatal("OSD map lifecycle bootstrap settings changed", config)
			}
		}
	}
	for _, p := range []profile{{}, {idleSessions: "1"}, {shortTickets: "1"}, {authEpoch: "1"}} {
		if strings.Contains(fixtureBootstrapConfig(p), "hashpspool") {
			t.Fatal("non-OSD admission profile acquired OSD pool overrides", p)
		}
	}
}

func TestFixtureBootstrapPreservesTimingProfiles(t *testing.T) {
	for _, test := range []struct {
		p    profile
		tick string
		sub  string
	}{
		{profile{}, "5", "86400"}, {profile{authEpoch: "1"}, "5", "86400"},
		{profile{shortTickets: "1"}, "1", "86400"},
		{profile{idleSessions: "1"}, "1", "2"},
		{profile{idleSessions: "1", osd: "1"}, "1", "2"},
	} {
		global := bootstrapSections(t, fixtureBootstrapConfig(test.p))["global"]
		if global["mon_tick_interval"] != test.tick || global["mon_subscribe_interval"] != test.sub {
			t.Fatal("MON timing changed", test.p, global)
		}
		if test.p.idleSessions == "1" && (global["mon_session_timeout"] != "3" || global["mon_client_ping_interval"] != "1" || global["mon_client_hunt_interval"] != "1") {
			t.Fatal("idle-session recovery timing changed", global)
		}
	}
}

func legacyPolicy(t *testing.T, input string) poolPolicy {
	t.Helper()
	result, err := decodeLegacyPoolPolicy([]byte(input), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return result
}

const readyLegacyPools = `{"epoch":13,"fsid":"fixture","pools":[{"pool":1,"pool_name":".mgr","flags":0,"flags_names":""},{"pool":2,"pool_name":"ceph-msgr-osd-fixture","flags":32,"flags_names":"nodelete"}]}`
const delayedManagerPool = `{"epoch":12,"fsid":"fixture","pools":[{"pool":2,"pool_name":"ceph-msgr-osd-fixture","flags":0,"flags_names":""}]}`

func TestLegacyPoolPolicyWaitsOnlyForReadOnlyMgrObservation(t *testing.T) {
	fetches, waits := 0, 0
	policy, err := waitLegacyPoolPolicy(context.Background(), true, func(ctx context.Context) (poolPolicy, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("pool observation has no bounded context")
		}
		fetches++
		if fetches == 1 {
			return legacyPolicy(t, delayedManagerPool), nil
		}
		return legacyPolicy(t, readyLegacyPools), nil
	}, func(ctx context.Context, delay time.Duration) error {
		waits++
		if delay != 250*time.Millisecond || ctx.Err() != nil {
			t.Fatal("invalid native observation backoff", delay, ctx.Err())
		}
		return nil
	})
	if err != nil || fetches != 2 || waits != 1 || policy.Epoch != 13 || len(policy.Pools) != 2 {
		t.Fatal("delayed background .mgr creation lost final actual policy", err, fetches, waits, policy)
	}
}

func TestLegacyPoolPolicyRejectsHashInEveryPoolBeforeWaiting(t *testing.T) {
	for _, input := range []string{
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":".mgr","flags":1}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":2,"pool_name":"ceph-msgr-osd-fixture","flags":0,"flags_names":"hashpspool"}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":3,"pool_name":"other","flags":0,"flags_names":"nodelete,hashpspool,noscrub"}]}`,
	} {
		fetches := 0
		_, err := waitLegacyPoolPolicy(context.Background(), true, func(context.Context) (poolPolicy, error) {
			fetches++
			return legacyPolicy(t, input), nil
		}, func(context.Context, time.Duration) error {
			t.Fatal("unsupported pool policy reached polling")
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "HASHPSPOOL") || fetches != 1 {
			t.Fatal("HASHPSPOOL admitted or retried", input, err, fetches)
		}
	}
}

func TestLegacyPoolPolicyRequiredNamesAndNoManagerProfile(t *testing.T) {
	if err := validateLegacyPoolPolicy(legacyPolicy(t, delayedManagerPool), false); err != nil {
		t.Fatal("zero-manager profile requires automatic .mgr", err)
	}
	for _, input := range []string{`{"epoch":12,"fsid":"fixture","pools":[]}`, `{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":".mgr","flags":0}]}`} {
		if err := validateLegacyPoolPolicy(legacyPolicy(t, input), true); !errors.Is(err, errLegacyPoolsNotReady) {
			t.Fatal("missing actual required pool admitted", err)
		}
	}
}

func TestLegacyPoolPolicyStrictMapIdentityAndActualFlags(t *testing.T) {
	for _, raw := range []string{
		`{}`, `null`, `{"epoch":0,"fsid":"fixture","pools":[]}`,
		`{"epoch":12,"fsid":"other","pools":[]}`,
		`{"epoch":12,"fsid":"fixture"}`,
		`{"epoch":12,"fsid":"fixture","pools":null}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":".mgr"}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":".mgr","flags":null}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool_name":".mgr","flags":0}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":-1,"pool_name":".mgr","flags":0}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":"","flags":0}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":"a","flags":0},{"pool":1,"pool_name":"b","flags":0}]}`,
		`{"epoch":12,"fsid":"fixture","pools":[{"pool":1,"pool_name":"a","flags":0},{"pool":2,"pool_name":"a","flags":0}]}`,
		`{"epoch":4294967296,"fsid":"fixture","pools":[]}`,
	} {
		if _, err := decodeLegacyPoolPolicy([]byte(raw), "fixture"); err == nil || errors.Is(err, errLegacyPoolsNotReady) {
			t.Fatal("malformed/wrong-identity actual policy became readiness", raw, err)
		}
	}
	if _, err := decodeLegacyPoolPolicy([]byte(readyLegacyPools), ""); err == nil {
		t.Fatal("empty unpublished expected fixture FSID admitted")
	}
	if _, err := decodeLegacyPoolPolicy(make([]byte, (1<<20)+1), "fixture"); err == nil {
		t.Fatal("oversized native JSON admitted")
	}
	// Native flags_names is display-only; numeric zero is still required and
	// a valid zero-flags map need not contain that optional display label.
	policy := legacyPolicy(t, `{"epoch":1,"fsid":"fixture","pools":[{"pool":0,"pool_name":"ceph-msgr-osd-fixture","flags":0}]}`)
	if policy.Pools[0].ID != 0 || policy.Pools[0].Flags != 0 || validateLegacyPoolPolicy(policy, false) != nil {
		t.Fatal("valid actual numeric identity/zero flags rejected", policy)
	}
	reads := 0
	_, err := waitLegacyPoolPolicy(context.Background(), true, func(context.Context) (poolPolicy, error) {
		reads++
		return poolPolicy{}, nil
	}, func(context.Context, time.Duration) error {
		t.Fatal("zero-status/missing-identity observation reached readiness polling")
		return nil
	})
	if err == nil || errors.Is(err, errLegacyPoolsNotReady) || reads != 1 {
		t.Fatal("invalid successful status observation treated as pending", err, reads)
	}
}

func TestLegacyPoolPolicyCancellationAndObservationErrorDoNotReadAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := waitLegacyPoolPolicy(ctx, true, func(context.Context) (poolPolicy, error) {
		t.Fatal("canceled parent dispatched observation")
		return poolPolicy{}, nil
	}, waitLegacyPoolObservation)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("lost parent cancellation", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	fetches := 0
	_, err = waitLegacyPoolPolicy(ctx, true, func(context.Context) (poolPolicy, error) {
		fetches++
		return legacyPolicy(t, delayedManagerPool), nil
	}, func(context.Context, time.Duration) error {
		cancel()
		return nil // the loop must recheck ctx even when a waiter does not
	})
	if fetches != 1 || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled polling dispatched another native read", fetches, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	_, err = waitLegacyPoolPolicy(ctx, true, func(context.Context) (poolPolicy, error) {
		cancel()
		return legacyPolicy(t, readyLegacyPools), nil
	}, func(context.Context, time.Duration) error {
		t.Fatal("canceled successful observation reached wait")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("successful observation erased parent cancellation", err)
	}
	first := errors.New("native policy read incomplete")
	_, err = waitLegacyPoolPolicy(context.Background(), true, func(context.Context) (poolPolicy, error) {
		return poolPolicy{}, first
	}, func(context.Context, time.Duration) error {
		t.Fatal("failed/incomplete observation was retried")
		return nil
	})
	if !errors.Is(err, first) {
		t.Fatal("lost native observation error", err)
	}
}

func TestLegacyPoolPolicyCanceledTimerReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitLegacyPoolObservation(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal("native readiness timer ignored cancellation", err)
	}
}

func TestPGV8BootstrapKeepsLegacyCRUSHAndEnablesHashPSPool(t *testing.T) {
	p := profile{osd: "1", osdProfile: "replicated-pg-v8"}
	sections := bootstrapSections(t, fixtureBootstrapConfig(p))
	if sections["osd"]["osd_op_history_size"] != "1024" {
		t.Fatal("native ACK history observation is not configured", sections)
	}
	if sections["global"]["osd_pool_default_flag_hashpspool"] != "true" || sections["global"]["osd_crush_update_on_start"] != "false" {
		t.Fatal("v8 changed narrow bootstrap profile", sections)
	}
	policy := poolPolicy{Epoch: 12, FSID: "fixture", Pools: []poolPolicyEntry{{ID: 1, Name: ".mgr", Flags: 1, FlagNames: "hashpspool"}, {ID: 2, Name: "ceph-msgr-osd-fixture", Flags: 1, FlagNames: "hashpspool"}}}
	if err := validateReplicatedPGPoolPolicy(policy, true); err != nil {
		t.Fatal(err)
	}
	if err := validateLegacyPoolPolicy(policy, true); err == nil {
		t.Fatal("v6 admitted hashpspool")
	}
	for _, field := range []string{"numeric", "named"} {
		bad := policy
		bad.Pools = append([]poolPolicyEntry(nil), policy.Pools...)
		if field == "numeric" {
			bad.Pools[0].Flags = 0
		} else {
			bad.Pools[0].FlagNames = ""
		}
		if err := validateReplicatedPGPoolPolicy(bad, true); err == nil {
			t.Fatal("v8 admitted inconsistent hashpspool", field)
		}
	}
}
