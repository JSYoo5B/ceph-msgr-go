package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func budgetEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// Without a job deadline every suite keeps the profile's static timeout,
// verbatim, as before the deadline existed.
func TestSuiteTimeoutWithoutJobDeadlineIsTheStaticTimeout(t *testing.T) {
	b, err := loadJobBudget(budgetEnv(nil))
	if err != nil || !b.deadline.IsZero() || b.explicit {
		t.Fatal("an empty environment gave", b, err)
	}
	for _, static := range []string{"10m", "30m", " invalid-timeout "} {
		for _, perTest := range []bool{false, true} {
			if got, err := b.suiteTimeout(profile{testTimeout: static}, time.Now(), perTest); err != nil || got != static {
				t.Errorf("static %q perTest=%v: %q, %v", static, perTest, got, err)
			}
		}
	}
}

// With a job deadline the default selection may use all the time before the
// teardown reserve, also past the static default; an explicit timeout, an
// explicit selection and a sequence's single test keep the shorter static
// bound.
func TestSuiteTimeoutIsBoundedByTheJobDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	osd := profile{osd: "1", testTimeout: "30m"}
	reserve := teardownReserve(osd)
	if reserve != diagnosticsReserve+osd.cleanupBudget() || reserve != 3*time.Minute {
		t.Fatal("OSD teardown reserve", reserve)
	}
	deadline := func(left time.Duration) string { return strconv.FormatInt(now.Add(left).Unix(), 10) }
	for _, test := range []struct {
		name     string
		left     time.Duration
		explicit bool
		selected bool
		perTest  bool
		want     string
	}{
		{"default selection gets the job's remainder", 40 * time.Minute, false, false, false, (37 * time.Minute).String()},
		{"remainder below the static default", 20 * time.Minute, false, false, false, (17 * time.Minute).String()},
		{"explicit timeout is shorter", 40 * time.Minute, true, false, false, (30 * time.Minute).String()},
		{"explicit timeout is longer than the remainder", 25 * time.Minute, true, false, false, (22 * time.Minute).String()},
		{"explicit selection keeps the static bound", 40 * time.Minute, false, true, false, (30 * time.Minute).String()},
		{"explicit selection near the deadline", 12 * time.Minute, false, true, false, (9 * time.Minute).String()},
		{"sequence test keeps its static bound", 100 * time.Minute, false, false, true, (30 * time.Minute).String()},
		{"sequence test near the deadline", 10 * time.Minute, false, false, true, (7 * time.Minute).String()},
	} {
		env := map[string]string{jobDeadlineEnv: deadline(test.left)}
		if test.explicit {
			env[timeoutEnv] = "30m"
		}
		if test.selected {
			env[runEnv] = "^TestCephOSDWriteSameFailoverIntegration$"
		}
		b, err := loadJobBudget(budgetEnv(env))
		if err != nil {
			t.Fatal(test.name, err)
		}
		got, err := b.suiteTimeout(osd, now, test.perTest)
		if err != nil || got != test.want {
			t.Errorf("%s: %q, %v; want %q", test.name, got, err, test.want)
		}
		if _, err := time.ParseDuration(got); err != nil {
			t.Errorf("%s: %q is not a -test.timeout value", test.name, got)
		}
	}
}

// A suite is not started when the job leaves less than a minute before the
// teardown reserve, or after the deadline; a malformed deadline is refused.
func TestSuiteTimeoutRefusesAnExhaustedOrMalformedDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	p := profile{testTimeout: "10m"}
	for _, left := range []time.Duration{teardownReserve(p) + minimumSuiteBudget - time.Second, 0, -time.Hour} {
		b, err := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: strconv.FormatInt(now.Add(left).Unix(), 10)}))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := b.suiteTimeout(p, now, false); err == nil || !strings.Contains(err.Error(), "teardown reserve") {
			t.Errorf("%s left: %q, %v", left, got, err)
		}
	}
	for _, value := range []string{"soon", "-5", "0", "1.5", "17e8"} {
		if _, err := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: value})); err == nil {
			t.Errorf("deadline %q was admitted", value)
		}
	}
	b, _ := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: strconv.FormatInt(now.Add(time.Hour).Unix(), 10), timeoutEnv: "soon"}))
	if _, err := b.suiteTimeout(profile{testTimeout: "soon"}, now, false); err == nil {
		t.Error("an unparsable explicit timeout was used")
	}
}
