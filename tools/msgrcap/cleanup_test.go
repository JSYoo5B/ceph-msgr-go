package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestLivePoCCleanupFailsTheJob replays cleanup.sh with an injected docker
// stub: a failed removal or an unverifiable absence must fail the process, and
// an earlier nonzero status is preserved.
func TestLivePoCCleanupFailsTheJob(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash unavailable")
	}
	script, err := filepath.Abs("cleanup.sh")
	if err != nil {
		t.Fatal(err)
	}
	stub := `#!/bin/sh
case "$1 $2" in
"rm -f") case "$FAULT" in *rm*) exit 1 ;; esac ;;
"network rm") case "$FAULT" in *rm*) exit 1 ;; esac ;;
"ps -a") case "$FAULT" in *ps*) exit 1 ;; *present*) echo m ;; esac ;;
"network ls") case "$FAULT" in *ps*) exit 1 ;; esac ;;
esac
exit 0
`
	for _, c := range []struct {
		name, fault string
		prior, want int
		result      string
	}{
		{"clean", "", 0, 0, "ok"},
		{"removal failure", "rm", 0, 1, "container removal failed; network removal failed"},
		{"unverifiable absence", "ps", 0, 1, "container absence unverifiable; network absence unverifiable"},
		{"container still present", "present", 0, 1, "container still present"},
		{"earlier failure preserved", "rm", 7, 7, "container removal failed; network removal failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bin, out, work := filepath.Join(dir, "bin"), filepath.Join(dir, "out"), filepath.Join(dir, "work")
			for _, d := range []string{bin, out, work} {
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(stub), 0o700); err != nil {
				t.Fatal(err)
			}
			driver := `set -euo pipefail; mon=m; net=n; work="$1"; out="$2"; tcpdump_pid=""; . "$3"; trap cleanup EXIT; exit "$4"`
			cmd := exec.Command(bash, "-c", driver, "driver", work, out, script, string(rune('0'+c.prior)))
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAULT="+c.fault)
			err := cmd.Run()
			code := 0
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if code != c.want {
				t.Fatalf("exit %d, want %d", code, c.want)
			}
			raw, err := os.ReadFile(filepath.Join(out, "cleanup.json"))
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Exit    int    `json:"exit_code"`
				Cleanup string `json:"cleanup"`
				Final   int    `json:"final_exit_code"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.Exit != c.prior || got.Final != c.want || got.Cleanup != c.result {
				t.Fatalf("cleanup.json %s", strings.TrimSpace(string(raw)))
			}
			if _, err := os.Stat(work); !os.IsNotExist(err) {
				t.Fatal("key work directory remains")
			}
		})
	}
}
