package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRBDClonePhaseReceiptScope(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, tc := range []struct {
		name, body, status string
		symlink            bool
	}{
		{"missing", "", "missing", false},
		{"valid", `{"token":"` + token + `","action":"flatten","request_id":6,"phase":"flatten-returned"}`, "captured", false},
		{"foreign token", `{"token":"other","action":"flatten","request_id":6,"phase":"flatten-call"}`, "invalid", false},
		{"foreign action", `{"token":"` + token + `","action":"write","request_id":6,"phase":"flatten-call"}`, "invalid", false},
		{"foreign request", `{"token":"` + token + `","action":"flatten","request_id":5,"phase":"flatten-call"}`, "invalid", false},
		{"unknown phase", `{"token":"` + token + `","action":"flatten","request_id":6,"phase":"invented"}`, "invalid", false},
		{"invalid json", "{", "invalid", false},
		{"oversized", strings.Repeat("x", 4097), "invalid", false},
		{"symlink", `{}`, "invalid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "rbd-clone-flatten-phase.json")
			if tc.body != "" {
				if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.symlink {
				if err := os.Rename(path, path+".target"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".target", path); err != nil {
					if runtime.GOOS == "windows" {
						t.Skip("symlink creation is unavailable without Windows developer privileges")
					}
					t.Fatal(err)
				}
			}
			got := readRBDClonePhase(root, token, "flatten", 6)
			if got.Status != tc.status {
				t.Fatalf("got %+v, want %s", got, tc.status)
			}
		})
	}
}

func TestRBDClonePhaseWriteFailuresDoNotSuppressCleanup(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	// Execute the actual phase function without importing native bindings.
	// An output failure must preserve both cleanup and the first error.
	const harness = `
import ast, json, os, sys, tempfile
from unittest import mock
tree = ast.parse(sys.stdin.read())
phase = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'phase')
exec(compile(ast.Module(body=[phase], type_ignores=[]), '<phase>', 'exec'))
OUT, action, token, request_id = tempfile.mkdtemp(), 'flatten', 'a1' * 16, '6'
try:
    for target in ('builtins.open', 'os.replace'):
        cleaned = False
        try:
            with mock.patch(target, side_effect=OSError('injected')):
                try:
                    raise RuntimeError('original')
                finally:
                    phase('traced-client-shutdown')
                    cleaned = True
        except RuntimeError as error:
            assert str(error) == 'original' and cleaned
        else:
            raise AssertionError('original error lost')
finally:
    import shutil
    shutil.rmtree(OUT)
`
	cmd := exec.Command(python, "-c", harness)
	cmd.Stdin = strings.NewReader(rbdCloneNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
}

func TestRBDClonePartialTraceProjection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the trace reader runs in POSIX fixture containers")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	token := strings.Repeat("a1", 16)
	const send = "private-prefix --> peer osd_op(client.1.0:7 rbd_data.ab.0001 [read 0~4]) -- 0x123 private-suffix\n"
	const copyup = "private-prefix librbd::io::CopyupRequest: convert_copyup_extent_map: rbd_data.ab.0001 [0~4]\n"
	for _, tc := range []struct {
		name, data, status string
		traces, copyups    int
		truncated, symlink bool
	}{
		{"missing", "", "missing", 0, 0, false, false},
		{"projection", "auth key secret\nconfiguration private\n" + send + copyup + "osd_op rbd_data.no-direction\n", "captured", 1, 1, false, false},
		{"partial last line", send + " --> osd_op(rbd_data.incomplete", "captured", 1, 0, true, false},
		{"line width", " --> osd_op(rbd_data." + strings.Repeat("x", 601) + ")\n", "captured", 0, 0, true, false},
		{"record count", strings.Repeat(send, rbdCloneTraceLimit+1) + strings.Repeat(copyup, rbdCloneCopyupLimit+1), "captured", 96, 16, true, false},
		{"bounded tail", strings.Repeat("x", 4<<20) + "\n" + send, "captured", 1, 0, true, false},
		{"symlink", send, "invalid", 0, 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "ceph-msgr-rbd-clone-"+token+"-flatten.log")
			if tc.data != "" {
				if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.symlink {
				if err := os.Rename(path, path+".target"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".target", path); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(python, "-c", rbdClonePartialTracePython+"\nprint(json.dumps(collect(sys.argv[1], sys.argv[2], sys.argv[3])))", root, token, "flatten")
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			var got rbdClonePartialTrace
			if err := json.Unmarshal(output, &got); err != nil {
				t.Fatal(err, string(output))
			}
			if got.Status != tc.status || got.Truncated != tc.truncated || len(got.Trace) != tc.traces || len(got.CopyupTrace) != tc.copyups {
				t.Fatalf("unexpected projection %+v", got)
			}
			for _, secret := range []string{"private-prefix", "private-suffix", "auth key", "secret", "configuration"} {
				if strings.Contains(string(output), secret) {
					t.Fatal("raw trace escaped the projection:", secret)
				}
			}
		})
	}
}

func TestRBDCloneFatalSignalLeavesDiagnostics(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fatal-signal regression runs on the Linux validation runner")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	root, token := t.TempDir(), strings.Repeat("a1", 16)
	const crash = `
import ast, faulthandler, json, os, resource, signal, sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
faulthandler.enable(file=sys.stderr, all_threads=True)
tree = ast.parse(sys.stdin.read())
phase = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'phase')
exec(compile(ast.Module(body=[phase], type_ignores=[]), '<phase>', 'exec'))
OUT, token, action, request_id = sys.argv[1], sys.argv[2], 'flatten', '6'
with open(OUT + '/ceph-msgr-rbd-clone-' + token + '-flatten.log', 'w') as f:
    f.write('prefix --> peer osd_op(client.1.0:7 rbd_data.ab.0001 [read 0~4])\n')
phase('flatten-call')
os.kill(os.getpid(), signal.SIGSEGV)
`
	cmd := exec.Command(python, "-c", crash, root, token)
	cmd.Stdin = strings.NewReader(rbdCloneNativePython)
	output, crashErr := cmd.CombinedOutput()
	if crashErr == nil || !strings.Contains(string(output), "Fatal Python error: Segmentation fault") {
		t.Fatalf("fatal-signal injection did not produce a fault receipt: %v %s", crashErr, output)
	}
	phase := readRBDClonePhase(root, token, "flatten", 6)
	if phase.Status != "captured" || phase.Phase != "flatten-call" {
		t.Fatal("fatal signal lost phase receipt", phase)
	}
	// A fresh standard-library-only process can recover the safe projection
	// after the crashing process has exited, without replaying its operation.
	cmd = exec.Command(python, "-c", rbdClonePartialTracePython+"\nprint(json.dumps(collect(sys.argv[1], sys.argv[2], 'flatten')))", root, token)
	output, err = cmd.CombinedOutput()
	var trace rbdClonePartialTrace
	if err != nil || json.Unmarshal(output, &trace) != nil || trace.Status != "captured" || len(trace.Trace) != 1 {
		t.Fatalf("post-crash capture failed: %v %s", err, output)
	}
}
