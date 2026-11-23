package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (c *fixtureCluster) runXattrOracle(ctx context.Context, out string) error {
	return c.runXattrCommand(ctx, out, false)
}

func (c *fixtureCluster) runXattrCommand(ctx context.Context, out string, compare bool) error {
	args := []string{"python3", "/out/osd_xattr_native.py"}
	receipt := "osd-xattr-native-command.json"
	if compare {
		args = append(args, "--compare")
		receipt = "osd-xattr-command.json"
	}
	args = append(args, "/tmp/ceph-msgr-test", "/out")
	return c.runOracleCommand(ctx, out, args, receipt, "native xattr oracle")
}

func (c *fixtureCluster) runMutationCommand(ctx context.Context, out string) error {
	return c.runOracleCommand(ctx, out,
		[]string{"python3", "/out/osd_mutation_oracle.py", "/tmp/ceph-msgr-test", "/out"},
		"osd-mutation-command.json", "native mutation oracle")
}

func (c *fixtureCluster) controlOSDCompoundRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-compound")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "snapshot" || c.profile.osd != "1" {
			return errors.New("invalid compound oracle request")
		}
		if err = c.exec(ctx, "python3", "/out/osd_compound_oracle.py", "/tmp/ceph-msgr-test", "/out"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-compound-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-compound", frozenControlRank(22), (*fixtureCluster).controlOSDCompoundRequest)

func (c *fixtureCluster) controlOSDMutationRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-mutation")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "compare" || c.profile.osd != "1" {
			return errors.New("invalid mutation oracle request")
		}
		if err = c.runMutationCommand(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mutation-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-mutation", frozenControlRank(27), (*fixtureCluster).controlOSDMutationRequest)

func (c *fixtureCluster) controlOSDXattrLimitRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "limit-osd-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid scoped xattr limit request")
		}
		var scope *xattrLimitsScope
		switch fields[1] {
		case "apply":
			if c.xattrLimits != nil {
				return errors.New("xattr limit scope already owned")
			}
			c.xattrLimits, err = beginXattrLimits(ctx, c.cluster)
			scope = c.xattrLimits
		case "restore":
			if c.xattrLimits == nil {
				return errors.New("xattr limit scope is not owned")
			}
			scope = c.xattrLimits
			err = scope.Restore(ctx)
			if err == nil {
				c.xattrLimits = nil
			}
		default:
			return errors.New("invalid xattr limit action")
		}
		if scope != nil {
			data, encodeErr := json.Marshal(scope.Receipt())
			if encodeErr == nil {
				encodeErr = os.WriteFile(filepath.Join(out, "osd-xattr-limits.json"), data, 0600)
			}
			err = errors.Join(err, encodeErr)
		}
		if err != nil {
			return fmt.Errorf("scoped native xattr limits: %w", err)
		}
		if err = publishAck(out, "osd-xattr-limit."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-xattr-limit", frozenControlRank(28), (*fixtureCluster).controlOSDXattrLimitRequest)

func (c *fixtureCluster) controlOSDOMAPRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-omap")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid native OMAP oracle request")
		}
		switch fields[1] {
		case "prepare", "observe", "compare", "cleanup":
		default:
			return errors.New("invalid native OMAP oracle action")
		}
		args := []string{"python3", "/out/osd_omap_native.py", "--" + fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-omap-command.json", "native OMAP oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-omap-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-omap", frozenControlRank(29), (*fixtureCluster).controlOSDOMAPRequest)

func (c *fixtureCluster) controlOSDOMAPWriteRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-omap-write")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid native OMAP write oracle request")
		}
		switch fields[1] {
		case "prepare", "observe", "copy", "assert", "exec", "compare", "cleanup",
			"snapcreate", "snapwrite", "snapread", "snaprollback", "listsnaps":
		default:
			return errors.New("invalid native OMAP write oracle action")
		}
		args := []string{"python3", "/out/osd_omap_write_native.py", "--" + fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-omap-write-command.json", "native OMAP write oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-omap-write-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-omap-write", frozenControlRank(30), (*fixtureCluster).controlOSDOMAPWriteRequest)

func (c *fixtureCluster) controlOSDWatchNotifyRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-watch-notify")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || (fields[1] != "start" && fields[1] != "observe" && fields[1] != "finish") {
			return errors.New("invalid native watch notify oracle request")
		}
		args := []string{"python3", "/out/osd_watch_notify_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-watch-notify-command.json", "native watch notify oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-watch-notify-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-watch-notify", frozenControlRank(36), (*fixtureCluster).controlOSDWatchNotifyRequest)

func (c *fixtureCluster) controlOSDSparseReadRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-sparse-read")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native sparse-read oracle request")
		}
		args := []string{"python3", "/out/osd_sparse_read_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-sparse-read-command.json", "native sparse-read oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-sparse-read-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-sparse-read", frozenControlRank(37), (*fixtureCluster).controlOSDSparseReadRequest)

func (c *fixtureCluster) controlOSDPGListRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-pg-list")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native pg-list oracle request")
		}
		args := []string{"python3", "/out/osd_pg_list_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-pg-list-command.json", "native pg-list oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-pg-list-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-pg-list", frozenControlRank(38), (*fixtureCluster).controlOSDPGListRequest)

func (c *fixtureCluster) controlOSDU64XattrRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-u64-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || c.profile.u64XattrOracle == "" || fields[1] != "compare" {
			return errors.New("invalid native U64 xattr oracle request")
		}
		args := []string{"python3", "/out/osd_u64_xattr_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-u64-xattr-command.json", "native U64 xattr oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-u64-xattr-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-u64-xattr", frozenControlRank(39), (*fixtureCluster).controlOSDU64XattrRequest)

func (c *fixtureCluster) controlOSDWriteSameRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-writesame")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || c.profile.writeSameOracle == "" || fields[1] != "compare" {
			return errors.New("invalid native WRITESAME oracle request")
		}
		args := []string{"python3", "/out/osd_writesame_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-writesame-command.json", "native WRITESAME oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-writesame-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-writesame", frozenControlRank(40), (*fixtureCluster).controlOSDWriteSameRequest)

func (c *fixtureCluster) controlOSDChecksumCompoundRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-checksum-compound")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "compare" {
			return errors.New("invalid native CHECKSUM compound comparison request")
		}
		args := []string{"python3", "/out/osd_checksum_compound_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-checksum-compound-command.json", "native CHECKSUM compound comparison"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-checksum-compound-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-checksum-compound", frozenControlRank(41), (*fixtureCluster).controlOSDChecksumCompoundRequest)

func (c *fixtureCluster) controlOSDEnforceSnapContextRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-enforce-snap-context")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || (fields[1] != "observe" && fields[1] != "ordinary-write") {
			return errors.New("invalid native ENFORCE_SNAPC observation request")
		}
		args := []string{"python3", "/out/osd_enforce_snap_context_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-enforce-snap-context-command.json", "native pool-snapshot context observation"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-enforce-snap-context-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-enforce-snap-context", frozenControlRank(42), (*fixtureCluster).controlOSDEnforceSnapContextRequest)

func (c *fixtureCluster) controlOSDSetChunkRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-set-chunk")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != setChunkNativeAction {
			return errors.New("invalid native SET_CHUNK checkpoint request")
		}
		if err = c.setChunkNative(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "osd-set-chunk-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-set-chunk", frozenControlRank(43), (*fixtureCluster).controlOSDSetChunkRequest)

func (c *fixtureCluster) controlOSDChecksumRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-checksum")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "compare" {
			return errors.New("invalid native checksum oracle request")
		}
		args := []string{"python3", "/out/osd_checksum_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-checksum-command.json", "native checksum oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-checksum-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-checksum", frozenControlRank(48), (*fixtureCluster).controlOSDChecksumRequest)

func (c *fixtureCluster) controlOSDMapExtRangeRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-mapext-range")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native exact-range extent map oracle request")
		}
		args := []string{"python3", "/out/osd_mapext_range_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-mapext-range-command.json", "native exact-range extent map oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mapext-range-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-mapext-range", frozenControlRank(49), (*fixtureCluster).controlOSDMapExtRangeRequest)

func (c *fixtureCluster) controlOSDMapExtRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-mapext")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native extent map oracle request")
		}
		args := []string{"python3", "/out/osd_mapext_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-mapext-command.json", "native extent map oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mapext-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-mapext", frozenControlRank(50), (*fixtureCluster).controlOSDMapExtRequest)

func (c *fixtureCluster) controlOSDWatchersRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-watchers")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || (fields[1] != "start" && fields[1] != "observe" && fields[1] != "stop") {
			return errors.New("invalid native watcher oracle request")
		}
		args := []string{"python3", "/out/osd_watchers_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-watchers-command.json", "native watcher oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-watchers-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-watchers", frozenControlRank(51), (*fixtureCluster).controlOSDWatchersRequest)

func (c *fixtureCluster) controlOSDCommandRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-command")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "run" {
			return errors.New("invalid native OSD command oracle request")
		}
		args := []string{"python3", "/out/osd_command_native.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-command-command.json", "native OSD command oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-command-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-command", frozenControlRank(52), (*fixtureCluster).controlOSDCommandRequest)

func (c *fixtureCluster) controlOSDXattrNativeRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-xattr-native")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "matrix" || c.profile.osd != "1" {
			return errors.New("invalid native xattr oracle request")
		}
		if err = c.runXattrOracle(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "osd-xattr-native-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-xattr-native", frozenControlRank(53), (*fixtureCluster).controlOSDXattrNativeRequest)

func (c *fixtureCluster) controlOSDXattrRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "compare" || c.profile.osd != "1" {
			return errors.New("invalid xattr comparison oracle request")
		}
		if err = c.runXattrCommand(ctx, out, true); err != nil {
			return err
		}
		if err = publishAck(out, "osd-xattr-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-xattr", frozenControlRank(54), (*fixtureCluster).controlOSDXattrRequest)

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"osd-checksum-command.json",
	"osd-checksum-compound-command.json",
	"osd-checksum-compound-input.json",
	"osd-checksum-compound-result.json",
	"osd-checksum-compound-results.json",
	"osd-checksum-input.json",
	"osd-checksum-result.json",
	"osd-checksum-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-command-command.json",
	"osd-command-result.json",
	"osd-command-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-compound-result.json",
)

var _ = collectDiagnosticFiles(
	"osd-enforce-snap-context-command.json",
	"osd-enforce-snap-context-input.json",
	"osd-enforce-snap-context-native-write.json",
	"osd-enforce-snap-context-result.json",
	"osd-enforce-snap-context-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-mapext-command.json",
	"osd-mapext-input.json",
	"osd-mapext-range-command.json",
	"osd-mapext-range-input.json",
	"osd-mapext-range-result.json",
	"osd-mapext-result.json",
	"osd-mapext-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-mutation-command.json",
	"osd-mutation-result.json",
)

var _ = collectDiagnosticFiles(
	"osd-omap-command.json",
	"osd-omap-compare-results.json",
	"osd-omap-owned.json",
	"osd-omap-range-results.json",
	"osd-omap-read-results.json",
	"osd-omap-result.json",
	"osd-omap-write-command.json",
	"osd-omap-write-owned.json",
	"osd-omap-write-result.json",
	"osd-omap-write-results.json",
	"osd-omap-write-snaps.json",
)

var _ = collectDiagnosticFiles(
	"osd-pg-list-command.json",
	"osd-pg-list-input.json",
	"osd-pg-list-result.json",
	"osd-pg-list-results.json",
	"osd-pg-list-v8-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-sparse-read-command.json",
	"osd-sparse-read-input.json",
	"osd-sparse-read-result.json",
	"osd-sparse-read-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-u64-xattr-command.json",
	"osd-u64-xattr-input.json",
	"osd-u64-xattr-result.json",
	"osd-u64-xattr-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-watch-notify-command.json",
	"osd-watch-notify-go.json",
	"osd-watch-notify-input.json",
	"osd-watch-notify-result.json",
	"osd-watch-notify-results.json",
	"osd-watch-notify-status.json",
	"osd-watch-notify-worker.log",
	"osd-watch-reconnect-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-watchers-command.json",
	"osd-watchers-result.json",
	"osd-watchers-results.json",
	"osd-watchers-status.json",
	"osd-watchers-worker.log",
)

var _ = collectDiagnosticFiles(
	"osd-writesame-command.json",
	"osd-writesame-input.json",
	"osd-writesame-result.json",
	"osd-writesame-results.json",
)

var _ = collectDiagnosticFiles(
	"osd-xattr-command.json",
	"osd-xattr-limits.json",
	"osd-xattr-native-command.json",
	"osd-xattr-native.json",
	"osd-xattr-permission-go-remove.json",
	"osd-xattr-permission-go-set.json",
	"osd-xattr-permission-list.json",
	"osd-xattr-permission-remove.json",
	"osd-xattr-permission-set.json",
	"osd-xattr-result.json",
)
