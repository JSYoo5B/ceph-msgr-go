package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// These diagnostics are observations, never evidence that a failed mutation
// completed. They do not change its journal outcome or permit a replay.
type rbdClonePhase struct {
	Status    string `json:"status"`
	Token     string `json:"token,omitempty"`
	Action    string `json:"action,omitempty"`
	RequestID int64  `json:"request_id,omitempty"`
	Phase     string `json:"phase,omitempty"`
}

func rbdCloneDiagnosticScope(token, action string) bool {
	_, err := hex.DecodeString(token)
	return len(token) == 32 && token == strings.ToLower(token) && err == nil && slices.Contains(rbdCloneActions, action)
}

func readRBDClonePhase(out, token, action string, id int64) rbdClonePhase {
	invalid := rbdClonePhase{Status: "invalid"}
	if !rbdCloneDiagnosticScope(token, action) || id <= 0 {
		return invalid
	}
	path := filepath.Join(out, "rbd-clone-"+action+"-phase.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return rbdClonePhase{Status: "missing"}
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		return invalid
	}
	f, err := os.Open(path)
	if err != nil {
		return invalid
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return invalid
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	var phase rbdClonePhase
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &phase) != nil || phase.Token != token || phase.Action != action || phase.RequestID != id ||
		!slices.Contains([]string{"client-create", "client-connect", "traced-client-create", "traced-client-connect", "traced-ioctx-open", "traced-ioctx-close", "traced-client-shutdown", "trace-extract", "flatten-image-open", "flatten-call", "flatten-returned", "flatten-image-closed", "observe", "ioctx-close", "client-shutdown", "complete"}, phase.Phase) {
		return invalid
	}
	phase.Status = "captured"
	return phase
}

type rbdClonePartialTrace struct {
	Status      string               `json:"status"`
	Truncated   bool                 `json:"truncated"`
	Trace       []rbdCloneTrace      `json:"trace"`
	CopyupTrace []rbdCloneCopyupLine `json:"copyup_trace"`
}

// The independent process imports no Ceph bindings and reads only this scope's
// fixed trace file. It exports the same data-object projection as the driver,
// with bounded input, line width and record count. A partial final line is not
// an observation. Raw trace contents and filesystem errors stay private.
const rbdClonePartialTracePython = `
import json, os, stat, sys
INPUT_LIMIT, TRACE_LIMIT, TRACE_WIDTH, COPYUP_LIMIT = 4 << 20, 96, 600, 16

def collect(root, token, action):
    out = dict(status='invalid', truncated=False, trace=[], copyup_trace=[])
    if len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or action not in ('write', 'flatten'):
        return out
    path = root + '/ceph-msgr-rbd-clone-%s-%s.log' % (token, action)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        out['status'] = 'missing'
        return out
    except OSError:
        return out
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            return out
        with os.fdopen(fd, 'rb', closefd=False) as f:
            if info.st_size > INPUT_LIMIT:
                f.seek(info.st_size - INPUT_LIMIT)
                out['truncated'] = True
            data = f.read(INPUT_LIMIT)
        if out['truncated']:
            data = data.partition(b'\n')[2]
        if data and not data.endswith(b'\n'):
            data = data.rpartition(b'\n')[0]
            out['truncated'] = True
        for raw in data.splitlines():
            line = raw.decode('utf-8', errors='replace')
            marker = line.find('librbd::io::CopyupRequest: ')
            start = line.find('convert_copyup_extent_map: rbd_data.', marker) if marker >= 0 else -1
            if start >= 0:
                message = line[start:]
                if len(message.encode('utf-8')) > TRACE_WIDTH or len(out['copyup_trace']) >= COPYUP_LIMIT:
                    out['truncated'] = True
                else:
                    out['copyup_trace'].append(dict(message=message))
                continue
            start = line.find('osd_op')
            if start < 0 or 'rbd_data.' not in line[start:]:
                continue
            direction = 'send' if ' --> ' in line[:start] else 'receive' if '<== ' in line[:start] else None
            if direction is None:
                continue
            message = line[start:]
            for cut in (' -- 0x', ' ==== '):
                message = message.split(cut, 1)[0]
            if len(message.encode('utf-8')) > TRACE_WIDTH or len(out['trace']) >= TRACE_LIMIT:
                out['truncated'] = True
            else:
                out['trace'].append(dict(direction=direction, message=message))
        out['status'] = 'captured'
    except OSError:
        out = dict(status='read-error', truncated=False, trace=[], copyup_trace=[])
    finally:
        os.close(fd)
    return out
`

func (c *fixtureCluster) captureRBDCloneTrace(ctx context.Context, token, action string) rbdClonePartialTrace {
	failed := rbdClonePartialTrace{Status: "capture-error"}
	if !rbdCloneDiagnosticScope(token, action) || !rbdCloneTraced(action) {
		return rbdClonePartialTrace{Status: "invalid"}
	}
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
		Argv:    []string{"python3", "-c", rbdClonePartialTracePython + "\nprint(json.dumps(collect('/tmp', sys.argv[1], sys.argv[2])))\n", token, action},
		Timeout: 8 * time.Second, MaxStdout: 96 << 10, MaxStderr: 4096, Label: "owned RBD clone partial trace",
	})
	var result rbdClonePartialTrace
	if err != nil || !process.Complete() || process.ExitCode != 0 || json.Unmarshal(process.Stdout, &result) != nil ||
		!slices.Contains([]string{"captured", "missing", "invalid", "read-error"}, result.Status) ||
		len(result.Trace) != 0 && !rbdCloneTraceValid(result.Trace) || !rbdCloneCopyupTraceValid(result.CopyupTrace) {
		return failed
	}
	return result
}
