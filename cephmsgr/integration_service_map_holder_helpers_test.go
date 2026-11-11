package cephmsgr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only requests and receipts of tools/service_map_holder_oracle.py. The
// holder is a development fixture: it registers one synthetic service daemon
// as a native librados client and keeps it alive under independent control. It
// is not a product service-registration API.
var (
	serviceHolderTokenPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
	serviceHolderPhases       = map[string]bool{"start": true, "start-large": true, "start-wide": true, "observe": true, "release": true, "cleanup": true}
)

type nativeServiceHolderBaseline struct {
	Code          *int32  `json:"code"`
	Epoch         *uint32 `json:"epoch"`
	OwnedPresent  bool    `json:"owned_present"`
	ServiceDaemon *int    `json:"service_daemons"`
}

type nativeServiceHolderEntry struct {
	GID        *uint64 `json:"gid"`
	StartEpoch *uint32 `json:"start_epoch"`
	Role       *string `json:"role"`
	Daemon     *string `json:"daemon"`
	// The padding of a large registration as the native dump shows it: key
	// count, key and value bytes, and the SHA-256 of its sorted pairs.
	serviceHolderPadSummary
}

// serviceHolderPadSummary summarizes the "ceph_msgr_pad_" metadata keys of one
// daemon. The receipts carry only this summary, never the values.
type serviceHolderPadSummary struct {
	Keys   int    `json:"pad_keys"`
	Bytes  int    `json:"pad_bytes"`
	SHA256 string `json:"pad_sha256"`
}

const serviceHolderPadPrefix = "ceph_msgr_pad_"

// serviceHolderPadShapes is the padding of each start step: key count, value
// bytes and key index width. start-large is about 1.55 MiB of metadata;
// start-wide is one entry more than the decoder admits in one collection.
var serviceHolderPadShapes = map[string][3]int{"start": {0, 0, 4}, "start-large": {4096, 360, 4}, "start-wide": {65537, 8, 5}}

// serviceHolderPadding recomputes, independently of the Python holder, the
// fixed metadata that a start step of this token registers.
func serviceHolderPadding(token, phase string) map[string]string {
	shape := serviceHolderPadShapes[phase]
	padding := make(map[string]string, shape[0])
	for i := range shape[0] {
		digest := sha256.Sum256(fmt.Appendf(nil, "%s:%d", token, i))
		text := hex.EncodeToString(digest[:])
		padding[fmt.Sprintf("%s%0*d", serviceHolderPadPrefix, shape[2], i)] = strings.Repeat(text, shape[1]/len(text)+1)[:shape[1]]
	}
	return padding
}

// summarizeServiceHolderPadding applies the holder's summary to any metadata
// map: a Go-decoded daemon's, or the recomputed expectation.
func summarizeServiceHolderPadding(metadata map[string]string) serviceHolderPadSummary {
	var keys []string
	for key := range metadata {
		if strings.HasPrefix(key, serviceHolderPadPrefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	digest := sha256.New()
	summary := serviceHolderPadSummary{Keys: len(keys)}
	for _, key := range keys {
		summary.Bytes += len(key) + len(metadata[key])
		digest.Write([]byte(key + "\x00" + metadata[key] + "\x00"))
	}
	summary.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return summary
}

type nativeServiceHolderView struct {
	Code          *int32                    `json:"code"`
	Epoch         *uint32                   `json:"epoch"`
	Present       bool                      `json:"present"`
	Entry         *nativeServiceHolderEntry `json:"entry"`
	ServiceDaemon *int                      `json:"service_daemons"`
}

type nativeServiceHolderProcess struct {
	Started bool                     `json:"started"`
	Ready   bool                     `json:"ready"`
	Failed  bool                     `json:"failed"`
	Alive   bool                     `json:"alive"`
	Exited  *nativeServiceHolderExit `json:"exited"`
}

type nativeServiceHolderExit struct {
	Reason           string `json:"reason"`
	ShutdownReturned bool   `json:"shutdown_returned"`
}

type nativeServiceHolderReceipt struct {
	RequestID int64                        `json:"request_id"`
	Token     string                       `json:"token"`
	Phase     string                       `json:"phase"`
	Daemon    string                       `json:"daemon"`
	Service   string                       `json:"service"`
	State     string                       `json:"state"`
	Baseline  *nativeServiceHolderBaseline `json:"baseline"`
	// Ready is the readiness report of start: pid, GID and lifetime cap.
	Ready json.RawMessage `json:"ready"`
	// Holder is the process state seen by observe and cleanup.
	Holder   *nativeServiceHolderProcess `json:"holder"`
	Exited   *nativeServiceHolderExit    `json:"exited"`
	View     *nativeServiceHolderView    `json:"view"`
	Killed   bool                        `json:"killed"`
	Restored *bool                       `json:"restored"`
	// GIDMatchesHolder is true only when the dump's GID equals the instance ID
	// the holder reported after rados_service_register.
	GIDMatchesHolder bool `json:"gid_matches_holder"`
}

type nativeServiceHolderReady struct {
	PID             int64  `json:"pid"`
	GID             uint64 `json:"gid"`
	LifetimeSeconds int    `json:"lifetime_seconds"`
	// The padding the holder registered: empty for start, the token's fixed
	// padding for start-large and start-wide.
	serviceHolderPadSummary
}

// observeNativeServiceHolder requests one closed phase. The token and phase are
// validated before a request file is published, so an open request is never
// delegated to the fixture.
func observeNativeServiceHolder(ctx context.Context, control, token, phase string) (nativeServiceHolderReceipt, error) {
	var receipt nativeServiceHolderReceipt
	if !serviceHolderTokenPattern.MatchString(token) || !serviceHolderPhases[phase] {
		return receipt, errors.New("invalid native ServiceMap holder request")
	}
	if err := testcluster.RequestControl(ctx, control, "observe-service-holder", "service-holder-observed", token, phase); err != nil {
		return receipt, err
	}
	file, err := os.Open(filepath.Join(control, "native-service-holder-"+token+"."+phase+".json"))
	if err != nil {
		return receipt, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return receipt, errors.New("native ServiceMap holder receipt exceeds its bound")
	}
	if err = json.Unmarshal(data, &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase ||
		receipt.Service != "ceph-msgr" || receipt.Daemon != "holder-"+token[:16] {
		return receipt, errors.New("native ServiceMap holder receipt has a mismatched identity")
	}
	return receipt, nil
}

// serviceHolderReady decodes the readiness report of a start receipt.
func serviceHolderReady(receipt nativeServiceHolderReceipt) (nativeServiceHolderReady, error) {
	var ready nativeServiceHolderReady
	if _, start := serviceHolderPadShapes[receipt.Phase]; !start || receipt.State != "ready" || receipt.Baseline == nil ||
		receipt.Baseline.OwnedPresent || receipt.Baseline.Code == nil || *receipt.Baseline.Code != 0 || receipt.Baseline.Epoch == nil {
		return ready, errors.New("start receipt is not a ready holder over an absent baseline")
	}
	if err := json.Unmarshal(receipt.Ready, &ready); err != nil || ready.PID <= 0 || ready.GID == 0 || ready.LifetimeSeconds <= 0 {
		return ready, errors.New("start receipt has no usable holder readiness")
	}
	if ready.serviceHolderPadSummary != summarizeServiceHolderPadding(serviceHolderPadding(receipt.Token, receipt.Phase)) {
		return ready, errors.New("start receipt reports a different registration size")
	}
	return ready, nil
}

// serviceHolderPresent reports whether a fresh native view shows the owned
// daemon with the holder's own GID, synthetic identity metadata and exactly
// the padding that the holder registered.
func serviceHolderPresent(receipt nativeServiceHolderReceipt, ready nativeServiceHolderReady) bool {
	view := receipt.View
	if receipt.Phase != "observe" || view == nil || view.Code == nil || *view.Code != 0 || view.Epoch == nil || !view.Present || view.Entry == nil {
		return false
	}
	entry := view.Entry
	return entry.GID != nil && *entry.GID == ready.GID && receipt.GIDMatchesHolder &&
		entry.Role != nil && *entry.Role == "holder" && entry.Daemon != nil && *entry.Daemon == receipt.Daemon &&
		entry.serviceHolderPadSummary == ready.serviceHolderPadSummary
}

// serviceHolderAbsent reports a readable fresh native view without the owned
// daemon. An unreadable dump is never an absence.
func serviceHolderAbsent(receipt nativeServiceHolderReceipt) bool {
	view := receipt.View
	return receipt.Phase == "observe" && serviceHolderAbsentView(view)
}

// serviceHolderAbsentView is a readable, well-formed dump (success code, epoch
// and a counted owned service) without the owned daemon or entry.
func serviceHolderAbsentView(view *nativeServiceHolderView) bool {
	return view != nil && view.Code != nil && *view.Code == 0 && view.Epoch != nil && view.ServiceDaemon != nil &&
		!view.Present && view.Entry == nil
}

// serviceHolderRestored accepts a cleanup receipt only when the worker reports
// restoration and its fresh view is itself a readable absence.
func serviceHolderRestored(receipt nativeServiceHolderReceipt) bool {
	return receipt.Phase == "cleanup" && receipt.Restored != nil && *receipt.Restored && !receipt.Killed &&
		serviceHolderAbsentView(receipt.View)
}

// waitServiceHolder repeats only the read-only observe phase until accept holds.
func waitServiceHolder(t *testing.T, ctx context.Context, control, token, label string, accept func(nativeServiceHolderReceipt) bool) nativeServiceHolderReceipt {
	t.Helper()
	for {
		receipt, err := observeNativeServiceHolder(ctx, control, token, "observe")
		if err != nil {
			t.Fatal("native ServiceMap holder observation", label, err)
		}
		if accept(receipt) {
			return receipt
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native ServiceMap holder did not reach %s: %v; view=%+v holder=%+v", label, ctx.Err(), receipt.View, receipt.Holder)
		case <-time.After(time.Second):
		}
	}
}
