package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// mgrReportMONLogLimit bounds the complete MON container log one
// mgr-report-mon-log request scans. The MGR report test raises the MON's
// debug_mon to 10 only around its stale-report window.
const mgrReportMONLogLimit = 256 << 20

// Pinned ceph/ceph/blob/v20.2.4/src/mon/MgrStatMonitor.cc preprocess_report
// logs, at debug_mon 10, every MMonMgrReport it drops because its nonzero
// GID is not the MgrMap's active GID: "ignoring report from non-active mgr
// <gid>". The receiving MON logs it before any forwarding to the leader.
var monIgnoredReportLine = regexp.MustCompile(`ignoring report from non-active mgr (\d+)\s*$`)

// mgrReportMONLogReceipt is the public receipt of one request. Admitted
// reports whether this run declared capMgrDaemonControl. Ignored counts the
// lines of the requested GID in the MON's complete log since its process
// started; it holds a count, no log text.
type mgrReportMONLogReceipt struct {
	Label    string `json:"label"`
	Admitted bool   `json:"admitted"`
	Monitor  string `json:"monitor"`
	GID      uint64 `json:"gid"`
	Ignored  int    `json:"ignored"`
}

// scanIgnoredReports counts the ignored-report lines of gid in one complete
// log within limit bytes.
func scanIgnoredReports(r io.Reader, gid uint64, limit int) (int, error) {
	counted := &countedReader{r: io.LimitReader(r, int64(limit)+1)}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	want := strconv.FormatUint(gid, 10)
	n := 0
	for scanner.Scan() {
		if m := monIgnoredReportLine.FindStringSubmatch(scanner.Text()); m != nil && m[1] == want {
			n++
		}
	}
	if counted.n > int64(limit) {
		return 0, errContainerLogExceedsBound
	}
	return n, scanner.Err()
}

// mgrReportMONLog answers "<id> <label> <mon> <gid>": it scans that MON's
// complete container log and replaces mgr-report-mon-log.json atomically.
func (c *fixtureCluster) mgrReportMONLog(ctx context.Context, out string, fields []string) error {
	if len(fields) != 4 {
		return errors.New("invalid MGR report MON log request")
	}
	gid, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil || gid == 0 {
		return fmt.Errorf("invalid MGR report GID %q", fields[3])
	}
	receipt := mgrReportMONLogReceipt{Label: fields[1], Admitted: c.profile.admits(capMgrDaemonControl), Monitor: fields[2], GID: gid}
	if receipt.Admitted {
		daemon, err := c.daemon(ctx, "mon", fields[2])
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, time.Minute)
		err = withContainerLog(call, daemon.Logs, func(r io.Reader) error {
			var scanErr error
			receipt.Ignored, scanErr = scanIgnoredReports(r, gid, mgrReportMONLogLimit)
			return scanErr
		})
		cancel()
		if err != nil {
			return fmt.Errorf("mon.%s log for %s: %w", fields[2], fields[1], err)
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary := filepath.Join(out, "mgr-report-mon-log.tmp")
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, "mgr-report-mon-log.json"))
}

var _ = defineRequestControl("mgr-report-mon-log", controlAfterRanked, (*fixtureCluster).mgrReportMONLog)

var _ = collectDiagnosticFiles("mgr-report-mon-log.json")
