package main

import (
	"errors"
	"strings"
	"testing"
)

func TestScanIgnoredReportsCountsOnlyTheRequestedGID(t *testing.T) {
	log := strings.Join([]string{
		"debug 2026-11-21T09:00:00.000+0000 7f mon.b@1(peon).paxosservice(mgrstat 1..9) ignoring report from non-active mgr 4127",
		"debug 2026-11-21T09:00:01.000+0000 7f mon.b@1(peon).paxosservice(mgrstat 1..9) ignoring report from non-active mgr 41270",
		"debug 2026-11-21T09:00:02.000+0000 7f mon.b@1(peon).paxosservice(mgrstat 1..9) ignoring report from non-active mgr 4127 ",
		"debug 2026-11-21T09:00:03.000+0000 7f mon.b@1(peon).paxosservice(mgrstat 1..9) prepare_report digest, 1 health checks",
		"ignoring report from non-active mgr 4127 trailing",
	}, "\n")
	n, err := scanIgnoredReports(strings.NewReader(log), 4127, 1<<20)
	if err != nil || n != 2 {
		t.Fatal("ignored reports of GID 4127", n, err)
	}
	if _, err := scanIgnoredReports(strings.NewReader(log), 4127, 64); !errors.Is(err, errContainerLogExceedsBound) {
		t.Fatal("an oversized log was not refused", err)
	}
}
