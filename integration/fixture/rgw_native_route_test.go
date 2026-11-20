package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func rgwRouteEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func TestRGWNativeRouteAdmitsOnlyOneCheckpointName(t *testing.T) {
	for _, name := range ciDeclaredRoutedTests(t) {
		p, err := loadProfile(rgwRouteEnv(map[string]string{rgwNativeEnv: "1", "CEPH_MSGR_TEST_RUN": "^" + name + "$"}))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if p.rgwNative != "1" || p.testRun != "^"+name+"$" || !p.runsDefault() {
			t.Fatalf("%s: profile %+v", name, p)
		}
	}
	p, err := loadProfile(rgwRouteEnv(map[string]string{}))
	if err != nil || p.rgwNative != "0" {
		t.Fatalf("default profile routes: %+v %v", p, err)
	}
	refused := map[string]map[string]string{
		"no selector":       {},
		"broad selector":    {"CEPH_MSGR_TEST_RUN": "^TestRGWNative"},
		"pattern selector":  {"CEPH_MSGR_TEST_RUN": "^TestRGWNative.*Integration$"},
		"class selector":    {"CEPH_MSGR_TEST_RUN": "^TestRGWNative[A-Z]Integration$"},
		"no end anchor":     {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration"},
		"no start anchor":   {"CEPH_MSGR_TEST_RUN": "TestRGWNativeCheckpointIntegration$"},
		"subtest":           {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$/x"},
		"unit test":         {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeHistoryRequiresAttributedDecodedObjectReception$"},
		"product test":      {"CEPH_MSGR_TEST_RUN": "^TestCephMonPingIntegration$"},
		"both checkpoints":  {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$|^TestRGWNativeMultiStripeIntegration$"},
		"unanchored":        {"CEPH_MSGR_TEST_RUN": "TestRGWNativeCheckpointIntegration"},
		"fault and another": {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeBackendFaultIntegration$|^TestRGWNativeCheckpointIntegration$"},
		"fault unit test":   {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeFaultMapParsers$"},
		"fault prefix":      {"CEPH_MSGR_TEST_RUN": "^TestRGWNativeBackendFault"},
		"OSD fixture":       {"CEPH_MSGR_TEST_OSD": "1"},
		"legacy key":        {"CEPH_MSGR_TEST_KEY_TYPE": "aes", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes"},
		"CRC mode":          {"CEPH_MSGR_TEST_CONNECTION_MODE": "crc"},
		"IPv6":              {"CEPH_MSGR_TEST_IP_FAMILY": "6"},
		"host runtime":      {"CEPH_MSGR_TEST_RUNTIME": "host"},
		"other image":       {"CEPH_MSGR_TEST_IMAGE": "quay.io/ceph/ceph:v20.2.4"},
		"MON ping":          {"CEPH_MSGR_TEST_MON_PING": "1"},
		"stress":            {"CEPH_MSGR_STRESS_DURATION": "1m"},
		"capability":        {"CEPH_MSGR_TEST_CAPABILITIES": "osd-erasure-width"},
		"MDS":               {"CEPH_MSGR_TEST_MDS_FRESH": "1"},
		"unknown value":     {rgwNativeEnv: "2"},
		"sequence of both":  {"CEPH_MSGR_TEST_SEQUENCE": "^TestRGWNativeCheckpointIntegration$ ^TestRGWNativeMultiStripeIntegration$", "CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$|^TestRGWNativeMultiStripeIntegration$"},
		"managers disabled": {"CEPH_MSGR_TEST_MGR_COUNT": "0"},
	}
	for name, changes := range refused {
		values := map[string]string{rgwNativeEnv: "1", "CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$"}
		for key, value := range changes {
			values[key] = value
		}
		if name == "no selector" {
			delete(values, "CEPH_MSGR_TEST_RUN")
		}
		if p, err := loadProfile(rgwRouteEnv(values)); err == nil {
			t.Errorf("%s: admitted %+v", name, p)
		}
	}
	// Without the routing flag the checkpoint is no selection of the
	// controller's fixture either: no product suite declares it.
	if _, err := loadProfile(rgwRouteEnv(map[string]string{"CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$"})); err != nil {
		t.Fatalf("an explicit selector stays the product fixture's decision: %v", err)
	}
}

func TestRGWNativeRouteRunsOneBoundedCheckpointCommand(t *testing.T) {
	for _, name := range ciDeclaredRoutedTests(t) {
		args := rgwNativeArguments(name)
		want := []string{"test", "-mod=readonly", "-json", "-count=1", "-timeout=20m", "-run", "^" + name + "$", "."}
		if !slices.Equal(args, want) {
			t.Fatalf("%s: %v", name, args)
		}
	}
}

func TestRGWNativeRouteRefusesAMissingDiagnosticsDirectoryAndAnOldStream(t *testing.T) {
	p, err := loadProfile(rgwRouteEnv(map[string]string{rgwNativeEnv: "1", "CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$"}))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runRGWNative(context.Background(), t.TempDir(), p, &out); err == nil || !strings.Contains(err.Error(), "CEPH_MSGR_TEST_DIAGNOSTICS") {
		t.Fatalf("ran without a diagnostics directory: %v", err)
	}
	p.diagnostics = t.TempDir()
	if err := os.WriteFile(filepath.Join(p.diagnostics, rgwNativeStream), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runRGWNative(context.Background(), t.TempDir(), p, &out); err == nil || !strings.Contains(err.Error(), "stream") {
		t.Fatalf("reused an earlier stream: %v", err)
	}
	ordinary, err := loadProfile(rgwRouteEnv(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	ordinary.diagnostics = t.TempDir()
	if err := runRGWNative(context.Background(), t.TempDir(), ordinary, &out); err == nil {
		t.Fatal("routed an unrouted profile")
	}
	if out.Len() != 0 {
		t.Fatalf("printed a suite before refusing: %q", out.String())
	}
}

func TestRGWNativeRouteKeepsTheStreamAndPrintsItsOutput(t *testing.T) {
	input := strings.Join([]string{
		`{"Action":"start","Package":"p"}`,
		`{"Action":"run","Package":"p","Test":"TestRGWNativeCheckpointIntegration"}`,
		`{"Action":"output","Package":"p","Test":"TestRGWNativeCheckpointIntegration","Output":"=== RUN   TestRGWNativeCheckpointIntegration\n"}`,
		`{"Action":"output","Package":"p","Test":"TestRGWNativeCheckpointIntegration","Output":"--- PASS: TestRGWNativeCheckpointIntegration (1.00s)\n"}`,
		`{"Action":"pass","Package":"p","Test":"TestRGWNativeCheckpointIntegration"}`,
		`not an event`,
		`{"Action":"output","Package":"p","Output":"PASS\n"}`,
	}, "\n") + "\n"
	var stream, text bytes.Buffer
	if err := copyTestStream(strings.NewReader(input), &stream, &text); err != nil {
		t.Fatal(err)
	}
	if stream.String() != input {
		t.Fatalf("stream %q", stream.String())
	}
	if want := "=== RUN   TestRGWNativeCheckpointIntegration\n--- PASS: TestRGWNativeCheckpointIntegration (1.00s)\nnot an event\nPASS\n"; text.String() != want {
		t.Fatalf("text %q", text.String())
	}
}

// TestRGWNativeRouteSelectsOnlyItsOwnSuite: a routed row selects the RGW
// checkpoints only, and an ordinary row never selects them, even when its
// selector would match their names.
func TestRGWNativeRouteSelectsOnlyItsOwnSuite(t *testing.T) {
	sources := map[string]string{
		"integration/TestCephAIntegration":                "integration/a_test.go",
		"fixture/TestRGWNativeCheckpointIntegration":      "integration/fixture/c_test.go",
		"fixture/TestRGWNativeMultiStripeIntegration":     "integration/fixture/m_test.go",
		"cephmsgr/TestRGWNativeCheckpointIntegration":     "cephmsgr/x_test.go",
		"integration/TestRGWNativeMultiStripeIntegration": "integration/y_test.go",
	}
	rows := []ciScenario{
		{ID: "product", Params: map[string]string{"test_run": "Integration$"}},
		{ID: "checkpoint", Params: map[string]string{"rgw_native": "1", "test_run": "^TestRGWNativeCheckpointIntegration$"}},
		{ID: "multistripe", Params: map[string]string{"rgw_native": "1", "test_run": "^TestRGWNativeMultiStripeIntegration$"}},
	}
	selection, problems := ciSelection(sources, rows)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	candidates := map[string][]string{}
	for _, test := range selection {
		candidates[test.Test] = test.Candidates
	}
	want := map[string][]string{
		"integration/TestCephAIntegration":                {"product"},
		"fixture/TestRGWNativeCheckpointIntegration":      {"checkpoint"},
		"fixture/TestRGWNativeMultiStripeIntegration":     {"multistripe"},
		"cephmsgr/TestRGWNativeCheckpointIntegration":     {"product"},
		"integration/TestRGWNativeMultiStripeIntegration": {"product"},
	}
	for test, rows := range want {
		if !slices.Equal(candidates[test], rows) {
			t.Errorf("%s: candidates %v, want %v", test, candidates[test], rows)
		}
	}
	if _, problems := ciSelection(sources, rows[:1]); !slices.Contains(problems, "no catalog selector includes fixture/TestRGWNativeCheckpointIntegration") {
		t.Fatalf("an unrouted checkpoint was not reported: %v", problems)
	}
}

// TestRGWNativeRouteRefusesANameTheModuleDoesNotList: a selector of the
// checkpoint name form that names no test of this module, or a test the name
// rule admits but Go does not list, is refused before go test runs it.
func TestRGWNativeRouteRefusesANameTheModuleDoesNotList(t *testing.T) {
	ctx := context.Background()
	if err := rgwNativeListed(ctx, ".", "TestRGWNativeCheckpointIntegration"); err != nil {
		t.Fatal(err)
	}
	err := rgwNativeListed(ctx, ".", "TestRGWNativeOtherIntegration")
	if err == nil || !strings.Contains(err.Error(), "lists []") {
		t.Fatalf("listed an unknown checkpoint: %v", err)
	}
	// A routed profile of an unknown name loads, and its run stops at the
	// listing: no suite line is printed and no checkpoint runs.
	p, err := loadProfile(rgwRouteEnv(map[string]string{rgwNativeEnv: "1", "CEPH_MSGR_TEST_RUN": "^TestRGWNativeOtherIntegration$"}))
	if err != nil {
		t.Fatal(err)
	}
	p.diagnostics = t.TempDir()
	project, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runRGWNative(ctx, project, p, &out); err == nil || !strings.Contains(err.Error(), "TestRGWNativeOtherIntegration") {
		t.Fatalf("ran an unknown checkpoint: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("printed a suite before refusing: %q", out.String())
	}
}
