package cephmsgr

import (
	"strings"
	"testing"
)

// The Python holder and this package compute the large registration's
// padding independently. Both test suites pin the same summary of the same
// token (tools/test_service_map_holder_oracle.py PINNED_PAD_SHA256).
func TestServiceHolderPaddingMatchesTheHoldersPinnedSummary(t *testing.T) {
	token := strings.Repeat("ab", 16)
	padding := serviceHolderPadding(token, "start-large")
	got := summarizeServiceHolderPadding(padding)
	want := serviceHolderPadSummary{Keys: 4096, Bytes: 4096 * (18 + 360), SHA256: "bcb31ca7d81a4ea3bce4a8b313d143a6892ac110fff87cb14117db468ac6c2ec"}
	if got != want {
		t.Fatal("padding differs from the holder's", got)
	}
	wide := serviceHolderPadSummary{Keys: 65537, Bytes: 65537 * (19 + 8), SHA256: "9d72c5a97078ad6ca3ed4c3f19abf97a2ffe13ac2009d99094f983b7437c5ff2"}
	if got := summarizeServiceHolderPadding(serviceHolderPadding(token, "start-wide")); got != wide {
		t.Fatal("wide padding differs from the holder's", got)
	}
	if len(serviceHolderPadding(token, "start")) != 0 || len(serviceHolderPadding(token, "observe")) != 0 {
		t.Fatal("an unpadded step has padding")
	}
	// Unrelated metadata, such as the native host inventory, is not padding.
	padding["hostname"], padding["ceph_msgr_role"] = "h", "holder"
	if summarizeServiceHolderPadding(padding) != want {
		t.Fatal("unrelated metadata changed the padding summary")
	}
	padding["ceph_msgr_pad_0000"] += "x"
	if changed := summarizeServiceHolderPadding(padding); changed.SHA256 == want.SHA256 || changed.Bytes != want.Bytes+1 {
		t.Fatal("a changed padding value kept its summary", changed)
	}
	if summarizeServiceHolderPadding(serviceHolderPadding(strings.Repeat("cd", 16), "start-large")).SHA256 == want.SHA256 {
		t.Fatal("another token has the same padding")
	}
}

// A start receipt must report the registration size of its own step, and a
// view is the holder's only with exactly the padding the holder registered.
func TestServiceHolderReceiptsBindTheRegistrationSize(t *testing.T) {
	token := strings.Repeat("b", 32)
	large := summarizeServiceHolderPadding(serviceHolderPadding(token, "start-large"))
	wide := summarizeServiceHolderPadding(serviceHolderPadding(token, "start-wide"))
	pad := func(target map[string]any, summary serviceHolderPadSummary) {
		target["pad_keys"], target["pad_bytes"], target["pad_sha256"] = summary.Keys, summary.Bytes, summary.SHA256
	}
	asLarge := func(v map[string]any) {
		v["phase"] = "start-large"
		pad(v["ready"].(map[string]any), large)
	}
	ready, err := serviceHolderReady(decodeServiceHolder(t, nativeServiceHolderStartSample, asLarge))
	if err != nil || ready.serviceHolderPadSummary != large || ready.GID != 4242 {
		t.Fatal("large start receipt was not decoded", ready, err)
	}
	for name, edit := range map[string]func(map[string]any){
		"large step, small registration": func(v map[string]any) { v["phase"] = "start-large" },
		"small step, large registration": func(v map[string]any) { pad(v["ready"].(map[string]any), large) },
		"another token's padding": func(v map[string]any) {
			asLarge(v)
			pad(v["ready"].(map[string]any), summarizeServiceHolderPadding(serviceHolderPadding(strings.Repeat("c", 32), "start-large")))
		},
		"wide step, large registration": func(v map[string]any) { asLarge(v); v["phase"] = "start-wide" },
		"large step, wide registration": func(v map[string]any) { asLarge(v); pad(v["ready"].(map[string]any), wide) },
		"one key short":                 func(v map[string]any) { asLarge(v); v["ready"].(map[string]any)["pad_keys"] = large.Keys - 1 },
	} {
		if _, err := serviceHolderReady(decodeServiceHolder(t, nativeServiceHolderStartSample, edit)); err == nil || !strings.Contains(err.Error(), "registration size") {
			t.Fatal(name, "start receipt was accepted", err)
		}
	}
	asWide := func(v map[string]any) {
		v["phase"] = "start-wide"
		pad(v["ready"].(map[string]any), wide)
	}
	if got, err := serviceHolderReady(decodeServiceHolder(t, nativeServiceHolderStartSample, asWide)); err != nil || got.serviceHolderPadSummary != wide {
		t.Fatal("wide start receipt was not decoded", got, err)
	}
	padded := func(v map[string]any) { pad(v["view"].(map[string]any)["entry"].(map[string]any), large) }
	if !serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, padded), ready) {
		t.Fatal("native view with the registered padding was not recognized")
	}
	if serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, nil), ready) {
		t.Fatal("native view without the registered padding was accepted")
	}
	small := nativeServiceHolderReady{PID: 901, GID: 4242, LifetimeSeconds: 120, serviceHolderPadSummary: summarizeServiceHolderPadding(nil)}
	if serviceHolderPresent(decodeServiceHolder(t, nativeServiceHolderObserveSample, padded), small) {
		t.Fatal("native view with unregistered padding was accepted")
	}
}
