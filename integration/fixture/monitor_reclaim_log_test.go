package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func TestMonitorReclaimFilterDoesNotChargeUnrelatedHistoryToEvidenceBound(t *testing.T) {
	const debug = "2026-10-14T23:52:20.500+0000 7ff7ec39b640 20 mon.a@0(peon).auth unrelated native debug history\n"
	// Reproduce the CI failure's >32 MiB of MON stdout without changing the
	// existing refusal-evidence limit or dropping the later relevant line.
	history := strings.Repeat(debug, monitorReclaimLogLimit/len(debug)+1)
	line := reclaimProcessStart + " 7ff7ec39b640  0 cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket\n"
	input := io.MultiReader(strings.NewReader(history), strings.NewReader(line), strings.NewReader(history))
	got, err := scanMonitorReclaimRefusals(newMonitorReclaimLogReader(context.Background(), input, "b"), "b", reclaimSince(t), len(line))
	if err != nil || !reflect.DeepEqual(got, []uint64{15059}) {
		t.Fatal("unrelated MON history consumed refusal budget", got, err)
	}
}

func TestMonitorReclaimFilterPreservesMalformedCandidatesAndResourceFailures(t *testing.T) {
	malformed := reclaimProcessStart + " 7ff7ec39b640  0 audit cmd=[cephx server mgr.b:  attempt to reclaim global_id 15059 using bad ticket]\n"
	if _, err := scanMonitorReclaimRefusals(newMonitorReclaimLogReader(context.Background(), strings.NewReader(malformed), "b"), "b", reclaimSince(t), len(malformed)); err == nil {
		t.Fatal("malformed attribution hidden by candidate filter")
	}
	inputErr := errors.New("native logs unavailable")
	if _, err := io.ReadAll(newMonitorReclaimLogReader(context.Background(), iotest.ErrReader(inputErr), "b")); !errors.Is(err, inputErr) {
		t.Fatal("stream error hidden", err)
	}
	if _, err := io.ReadAll(newMonitorReclaimLogReader(context.Background(), strings.NewReader(strings.Repeat("x", monitorReclaimLineLimit+1)), "b")); err == nil {
		t.Fatal("unbounded unrelated log line accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := io.ReadAll(newMonitorReclaimLogReader(ctx, strings.NewReader("unrelated\n"), "b")); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled log scan continued", err)
	}
}
