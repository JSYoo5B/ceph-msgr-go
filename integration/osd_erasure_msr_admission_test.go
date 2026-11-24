package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// msrRuleReceipt is the owned-pool oracle's mode msrrule
// (tools/osd_pool_mode_msrrule_oracle.py).
type msrRuleReceipt struct {
	MSR struct {
		Action         string          `json:"action"`
		Prior          string          `json:"prior"`
		Baseline       string          `json:"baseline"`
		Epoch          uint32          `json:"epoch"`
		RuleCreate     *msrRuleRefusal `json:"rule_create"`
		BaselineChange *msrRuleRefusal `json:"baseline_change"`
		RulePresent    bool            `json:"rule_present"`
		ProfilePresent bool            `json:"profile_present"`
		Owned          bool            `json:"owned"`
		Restored       bool            `json:"restored"`
		Rule           json.RawMessage `json:"rule"`
		// Outcome is "unknown" for an apply whose reply and read were
		// dropped on purpose (lose=readback).
		Outcome string `json:"outcome"`
	} `json:"msr_rule"`
}

type msrRuleRefusal struct {
	Code   int32  `json:"code"`
	Status string `json:"status"`
}

// crushMSRFeature is include/ceph_features.h CRUSH_MSR, bit 38. Its second
// incarnation bit (57) is one this client implements and sends, so a peer
// that requires CRUSH_MSR reports exactly this bit as missing.
const crushMSRFeature = uint64(1) << 38

var missingFeatures = regexp.MustCompile(`missing 0x([0-9a-f]+)`)

// judgeMissingCRUSHMSR accepts only the explicit feature refusal of a peer
// that requires exactly CRUSH_MSR beyond what this client sends.
func judgeMissingCRUSHMSR(err error) error {
	if err == nil {
		return errors.New("the session was admitted")
	}
	want := fmt.Sprintf("peer requires 0x%016x", crushMSRFeature)
	if !errors.Is(err, cephmsgr.ErrUnsupportedFeatures) || !strings.Contains(err.Error(), want) {
		return fmt.Errorf("not the feature refusal %q: %w", want, err)
	}
	return nil
}

// refusedForCRUSHMSR repeats attempt until it is refused for CRUSH_MSR and
// then requires three more refusals in a row. Before the first refusal an
// admitted attempt is tolerated, because a daemon applies the new map's
// requirement only when it has the map; any other error, an admission after
// a refusal, or no refusal within the bound fails.
func refusedForCRUSHMSR(attempt func() error, bound time.Duration, pause func()) (int, error) {
	deadline := time.Now().Add(bound)
	for admitted := 0; ; admitted++ {
		err := attempt()
		if err == nil {
			if time.Now().After(deadline) {
				return admitted, fmt.Errorf("still admitted after %s and %d attempts", bound, admitted+1)
			}
			pause()
			continue
		}
		if judged := judgeMissingCRUSHMSR(err); judged != nil {
			return admitted, judged
		}
		for i := 0; i < 3; i++ {
			if judged := judgeMissingCRUSHMSR(attempt()); judged != nil {
				return admitted, fmt.Errorf("after a refusal: %w", judged)
			}
		}
		return admitted, nil
	}
}

// TestCephOSDErasureMSRRuleAdmissionIntegration shows what this client does
// on a cluster whose map requires a feature it does not implement, CRUSH_MSR,
// on the four-OSD BlueStore topology. A CRUSH rule of an MSR type (an
// erasure-code profile with crush-osds-per-failure-domain above one, pinned
// ErasureCode::create_rule, v20.2.4) makes OSDMap::get_features require
// CRUSH_MSR from every client, and MONs and OSDs then refuse a session
// without it at the identification step. The rule has no pool.
//
//   - Before the rule: a new Go session and an OSD connection are admitted,
//     the control for the refusals below.
//   - The MON's own guards, observed natively once each: with
//     require_min_compat_client below squid the rule creation is refused
//     (-EINVAL, "new crush map requires client version squid",
//     OSDMonitor::validate_crush_against_features), and raising that
//     baseline without yes_i_really_mean_it is refused (-EPERM) because a
//     connected client lacks CRUSH_MSR; the Go session is such a client.
//   - A mutation with an uncertain outcome: the oracle raises the baseline,
//     sends the rule creation and stops before any read, holding no more
//     than a lost reply and read would leave. The Go session sees the rule in the map; the restore
//     must remove it and the profile and set the baseline back, from the
//     intent it recorded before the commands, and the MON must then refuse
//     the rule again as in the first probe.
//   - With the baseline raised and the rule present: a new Go session to a
//     MON, and a new OSD connection of the session that was already
//     admitted, must fail with ErrUnsupportedFeatures naming exactly
//     CRUSH_MSR ("peer requires 0x0000004000000000"). A daemon that has not
//     applied the map yet may still admit an attempt; after the first
//     refusal three more attempts must be refused. No feature bit is added
//     to pass: the refusal is the result. The already admitted MON session
//     still answers a command, since the requirement is applied when a
//     session is identified. The native client, which has the feature, seeds
//     and reads an object of an owned replicated pool at the same time.
//   - After the rule and the profile are removed and the baseline is set
//     back, each confirmed by a native read, a new Go session and an OSD
//     request are admitted again.
//
// Not verified: an erasure-coded pool that uses an MSR rule (placement,
// I/O), requests on an OSD connection opened before the rule, nondefault
// MSR tunables, and SERVER_TENTACLE, which no map of the pinned release
// requires from a client (OSDMap::get_features).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasuremsrruleadmission key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to MON/MGR
//scenario:covers CLIENT to OSD
//fixture:isolated raises require_min_compat_client and adds a CRUSH MSR rule every client session must satisfy; both are restored
func TestCephOSDErasureMSRRuleAdmissionIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "4" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || !isReplicatedPGFixtureProfile() {
		t.Skip("the MSR rule admission control runs on the four-OSD BlueStore topology with a replicated-PG profile")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	msr := func(ctx context.Context, action string) msrRuleReceipt {
		t.Helper()
		var r msrRuleReceipt
		nativePoolResult(t, ctx, "msrrule", map[string]any{"action": action}, &r)
		if r.MSR.Action != action {
			t.Fatal("msrrule receipt is for another action", r.MSR.Action)
		}
		return r
	}
	dial := func() (*cephmsgr.Client, error) {
		options := integrationOptions(t)
		options.EnableOSD, options.OSDProfile = true, replicatedPGFixtureOSDProfile(t)
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		return cephmsgr.Dial(attempt, options)
	}
	// stat sends one STAT of the fixture's object through a new OSD
	// connection of the client and closes it.
	stat := func(client *cephmsgr.Client) error {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		o, err := client.OpenOSD(attempt, cephmsgr.OSDTarget{ID: base.ID, Address: base.Address})
		if err != nil {
			return err
		}
		defer o.Close()
		_, err = o.Request(attempt, replicatedPGRequest(base, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
		return err
	}
	pause := func() { time.Sleep(500 * time.Millisecond) }
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := msr(cleanup, "restore"); !r.MSR.Restored {
			t.Error("the MSR rule, its profile or require_min_compat_client was not restored", r.MSR.RulePresent, r.MSR.ProfilePresent, r.MSR.Baseline)
		}
	})

	fresh, err := dial()
	if err != nil {
		t.Fatal("a new session before the rule", err)
	}
	if err := stat(fresh); err != nil {
		t.Fatal("an OSD request before the rule", err)
	}
	fresh.Close()
	pools := createOwnedPools(t, ctx, base, 1)

	probe := msr(ctx, "probe")
	if probe.MSR.RuleCreate == nil || probe.MSR.RuleCreate.Code != -22 || !strings.Contains(probe.MSR.RuleCreate.Status, "requires client version squid") {
		t.Fatalf("the MON did not refuse the MSR rule below the squid client baseline: %+v", probe.MSR.RuleCreate)
	}
	if probe.MSR.RulePresent || probe.MSR.Baseline != probe.MSR.Prior {
		t.Fatal("a refused step changed the map", probe.MSR.RulePresent, probe.MSR.Baseline, probe.MSR.Prior)
	}
	if err := judgeBaselineGuard(probe.MSR.BaselineChange); err != nil {
		t.Fatal(err)
	}
	t.Logf("native guards at require_min_compat_client %s: rule creation %d %q; baseline change %d %q",
		probe.MSR.Prior, probe.MSR.RuleCreate.Code, probe.MSR.RuleCreate.Status, probe.MSR.BaselineChange.Code, probe.MSR.BaselineChange.Status)

	// A mutation whose reply and read are lost: the oracle raises the
	// baseline, sends the rule creation and stops before any read. The rule
	// is then in the map with nothing but the recorded intent naming it,
	// and the restore must still return the map to its start.
	var lost msrRuleReceipt
	nativePoolResult(t, ctx, "msrrule", map[string]any{"action": "apply", "lose": "readback"}, &lost)
	if lost.MSR.Outcome != "unknown" || len(lost.MSR.Rule) != 0 || lost.MSR.Baseline != "" {
		t.Fatalf("the apply with a lost readback reported more than an unknown outcome: %+v", lost.MSR)
	}
	if rules := monRuleNames(t, ctx, c); !slices.Contains(rules, msrOwnedRule) {
		t.Fatal("the rule creation with the lost readback did not reach the map", rules)
	}
	undone := msr(ctx, "restore")
	if !undone.MSR.Restored || !undone.MSR.Owned || undone.MSR.RulePresent || undone.MSR.ProfilePresent || undone.MSR.Baseline != probe.MSR.Prior {
		t.Fatalf("restore after a lost readback left the rule, the profile or the raised baseline: %+v", undone.MSR)
	}
	if rules := monRuleNames(t, ctx, c); slices.Contains(rules, msrOwnedRule) {
		t.Fatal("the Go session still lists the rule after the restore", rules)
	}
	t.Logf("restore after a lost readback: rule and profile absent, require_min_compat_client %s", undone.MSR.Baseline)
	if again := msr(ctx, "probe"); again.MSR.Prior != probe.MSR.Prior || again.MSR.RuleCreate == nil || again.MSR.RuleCreate.Code != -22 || again.MSR.RulePresent {
		t.Fatalf("the restored map does not refuse the rule as the first probe saw: %+v", again.MSR)
	}

	applied := msr(ctx, "apply")
	if applied.MSR.Baseline != "squid" || len(applied.MSR.Rule) == 0 {
		t.Fatal("the MSR rule was not applied", applied.MSR.Baseline)
	}
	admitted, err := refusedForCRUSHMSR(func() error {
		client, err := dial()
		if err == nil {
			client.Close()
		}
		return err
	}, 30*time.Second, pause)
	if err != nil {
		t.Fatal("a new MON session with the MSR rule present:", err)
	}
	t.Logf("new MON sessions refused for CRUSH_MSR at epoch %d (%d earlier attempts admitted)", applied.MSR.Epoch, admitted)
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd crush rule ls","format":"json"}`)}); err != nil {
		t.Fatal("the MON session admitted before the rule no longer answers", err)
	}
	admitted, err = refusedForCRUSHMSR(func() error { return stat(c) }, 30*time.Second, pause)
	if err != nil {
		t.Fatal("a new OSD connection with the MSR rule present:", err)
	}
	t.Logf("new OSD connections refused for CRUSH_MSR (%d earlier attempts admitted)", admitted)
	native := poolSeed(t, ctx, 1, "msr-native", "", []byte("native client with CRUSH_MSR"), nil)
	if native.Version == 0 || pools.ids[1] == 0 {
		t.Fatal("the native client did no I/O with the MSR rule present", native.Version)
	}

	back := msr(ctx, "restore")
	restored = back.MSR.Restored
	if !back.MSR.Restored || !back.MSR.Owned || back.MSR.RulePresent || back.MSR.ProfilePresent || back.MSR.Baseline != probe.MSR.Prior {
		t.Fatalf("restore: %+v", back.MSR)
	}
	deadline := time.Now().Add(30 * time.Second)
	for attempts := 1; ; attempts++ {
		client, err := dial()
		if err == nil {
			err = stat(client)
			client.Close()
		}
		if err == nil {
			t.Logf("a new session and an OSD request are admitted again after the restore (attempt %d)", attempts)
			break
		}
		// Only the refusal the removed rule caused may still be seen.
		if judged := judgeMissingCRUSHMSR(err); judged != nil {
			t.Fatal("after the restore:", judged)
		}
		if time.Now().After(deadline) {
			t.Fatal("new sessions are still refused for CRUSH_MSR 30 s after the restore")
		}
		pause()
	}
}

// msrOwnedRule is the rule of tools/osd_pool_mode_msrrule_oracle.py.
const msrOwnedRule = "ceph-msgr-owned-msr-rule"

// monRuleNames lists the CRUSH rules through the Go client's MON session.
func monRuleNames(t *testing.T, ctx context.Context, c *cephmsgr.Client) []string {
	t.Helper()
	got, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd crush rule ls","format":"json"}`)})
	var names []string
	if err != nil || json.Unmarshal(got.Data, &names) != nil {
		t.Fatal("authenticated crush rule list", err)
	}
	return names
}

// judgeBaselineGuard checks the MON's refusal to raise
// require_min_compat_client without the confirmation flag: -EPERM, and at
// least one connected client reported as missing CRUSH_MSR.
func judgeBaselineGuard(r *msrRuleRefusal) error {
	if r == nil || r.Code != -1 || !strings.Contains(r.Status, "look like") {
		return fmt.Errorf("the MON did not refuse the baseline change for a connected client: %+v", r)
	}
	for _, match := range missingFeatures.FindAllStringSubmatch(r.Status, -1) {
		if mask, err := strconv.ParseUint(match[1], 16, 64); err == nil && mask&crushMSRFeature != 0 {
			return nil
		}
	}
	return fmt.Errorf("the MON's refusal names no client missing CRUSH_MSR: %q", r.Status)
}

// The refusal judgement accepts only the explicit CRUSH_MSR feature refusal,
// the attempt loop refuses an admission after a refusal and an unbounded
// admission, and the guard judgement needs a client missing CRUSH_MSR.
func TestErasureMSRAdmissionJudgement(t *testing.T) {
	refusal := fmt.Errorf("dial mon: %w: peer requires 0x%016x", cephmsgr.ErrUnsupportedFeatures, crushMSRFeature)
	if err := judgeMissingCRUSHMSR(refusal); err != nil {
		t.Fatal("the CRUSH_MSR refusal was not accepted", err)
	}
	for name, err := range map[string]error{
		"admitted":      nil,
		"another bit":   fmt.Errorf("%w: peer requires 0x%016x", cephmsgr.ErrUnsupportedFeatures, uint64(1)<<50),
		"two bits":      fmt.Errorf("%w: peer requires 0x%016x", cephmsgr.ErrUnsupportedFeatures, crushMSRFeature|1<<50),
		"other error":   errors.New("peer requires 0x0000004000000000"),
		"timeout":       context.DeadlineExceeded,
		"no mask named": cephmsgr.ErrUnsupportedFeatures,
	} {
		if judgeMissingCRUSHMSR(err) == nil {
			t.Fatal("accepted as the CRUSH_MSR refusal:", name)
		}
	}
	script := func(results ...error) func() error {
		i := 0
		return func() error {
			if i >= len(results) {
				return results[len(results)-1]
			}
			i++
			return results[i-1]
		}
	}
	if n, err := refusedForCRUSHMSR(script(nil, nil, refusal), time.Minute, func() {}); err != nil || n != 2 {
		t.Fatal("two admissions before a lasting refusal", n, err)
	}
	if _, err := refusedForCRUSHMSR(script(refusal, refusal, nil), time.Minute, func() {}); err == nil {
		t.Fatal("an admission after a refusal passed")
	}
	if _, err := refusedForCRUSHMSR(script(nil, context.DeadlineExceeded), time.Minute, func() {}); err == nil {
		t.Fatal("another error passed as the refusal")
	}
	if _, err := refusedForCRUSHMSR(script(nil), -time.Second, func() {}); err == nil {
		t.Fatal("an admission that never ends passed")
	}
	guard := &msrRuleRefusal{Code: -1, Status: "cannot set require_min_compat_client to squid: 1 connected client(s) look like luminous (missing 0x4000000000); add --yes-i-really-mean-it to do it anyway"}
	if err := judgeBaselineGuard(guard); err != nil {
		t.Fatal("the pinned guard text was refused", err)
	}
	for name, r := range map[string]*msrRuleRefusal{
		"absent":         nil,
		"accepted":       {Code: 0, Status: "set require_min_compat_client to squid"},
		"another reason": {Code: -1, Status: "osdmap current utilizes features that require squid"},
		"another bit":    {Code: -1, Status: "1 connected client(s) look like reef (missing 0x4000000000000)"},
	} {
		if judgeBaselineGuard(r) == nil {
			t.Fatal("accepted as the baseline guard:", name)
		}
	}
}
