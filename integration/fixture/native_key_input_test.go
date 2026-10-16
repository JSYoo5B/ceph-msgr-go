package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const testNativeKey = "AQBexampleexampleexampleexampleexampleexA=="

func TestNativeKeyRecordFramesOneBase64Key(t *testing.T) {
	record, err := nativeKeyRecord([]byte("[client.x]\n\tkey = " + testNativeKey + "\n\tcaps mon = \"allow r\"\n"))
	if err != nil || string(record) != testNativeKey+"\n" {
		t.Fatalf("record %q, %v", record, err)
	}
	for name, keyring := range map[string]string{
		"missing":  "[client.x]\n",
		"multiple": "[client.x]\n\tkey = " + testNativeKey + "\n[client.y]\n\tkey = " + testNativeKey + "\n",
		"alphabet": "[client.x]\n\tkey = AQB$(echo)==\n",
		"too long": "[client.x]\n\tkey = " + strings.Repeat("A", nativeKeyInputLimit) + "\n",
	} {
		if _, err := nativeKeyRecord([]byte(keyring)); err == nil {
			t.Fatal(name, "keyring accepted")
		} else if strings.Contains(err.Error(), testNativeKey) || strings.Contains(err.Error(), "AQB$") {
			t.Fatal(name, "error reveals the key", err)
		}
	}
}

func TestKeyedNativeCommandUsesBoundedInputAndPrelude(t *testing.T) {
	input := ceph.NewNativeInput()
	original := ceph.NativeCommand{Argv: []string{"python3", "-c", "print(1)", "client.x"}, MaxStdout: 100, Label: "w"}
	command, err := keyedNativeCommand(original, input)
	if err != nil {
		t.Fatal(err)
	}
	if command.Input != input || command.MaxInput != nativeKeyInputLimit || command.MaxInput > ceph.MaxNativeCommandInput || command.MaxStdout != 100+nativeKeyReceiptLimit+1 {
		t.Fatal("input bounds", command.MaxInput, command.MaxStdout)
	}
	if command.Argv[2] != nativeKeyPrelude+"print(1)" || original.Argv[2] != "print(1)" || command.Argv[3] != "client.x" {
		t.Fatal("prelude or argv", command.Argv)
	}
	for _, argv := range [][]string{{"rbd", "-c", "x"}, {"python3", "x.py", "y"}, {"python3", "-c"}} {
		if _, err := keyedNativeCommand(ceph.NativeCommand{Argv: argv}, input); err == nil {
			t.Fatal("non-worker accepted", argv)
		}
	}
	if _, err := keyedNativeCommand(ceph.NativeCommand{Argv: original.Argv, Input: input, MaxInput: 1}, input); err == nil {
		t.Fatal("command with its own input accepted")
	}
}

func TestNativeKeyDeliveryChecksCountersSeparatelyFromComplete(t *testing.T) {
	n := len(testNativeKey) + 1
	if err := checkNativeKeyDelivery(ceph.NativeCommandResult{InputWritten: int64(n), InputDelivered: int64(n), InputEOF: true}, n); err != nil {
		t.Fatal(err)
	}
	for name, result := range map[string]ceph.NativeCommandResult{
		"partial stream":   {InputWritten: int64(n - 1), InputDelivered: int64(n - 1), InputEOF: true},
		"undelivered":      {InputWritten: int64(n), InputDelivered: int64(n - 1), InputEOF: true},
		"no eof":           {InputWritten: int64(n), InputDelivered: int64(n)},
		"nothing; timeout": {TimedOut: true},
	} {
		if err := checkNativeKeyDelivery(result, n); err == nil {
			t.Fatal(name, "delivery accepted")
		}
	}
}

func TestSplitNativeKeyReceipt(t *testing.T) {
	rest, err := splitNativeKeyReceipt([]byte("{\"native_key_input\":{\"bytes\":45,\"eof\":true}}\n{\"action\":\"stat\"}\n"), 45)
	if err != nil || string(rest) != "{\"action\":\"stat\"}\n" {
		t.Fatalf("rest %q, %v", rest, err)
	}
	for name, stdout := range map[string]string{
		"missing":   "{\"action\":\"stat\"}",
		"count":     "{\"native_key_input\":{\"bytes\":44,\"eof\":true}}\n{}",
		"no eof":    "{\"native_key_input\":{\"bytes\":45,\"eof\":false}}\n{}",
		"unknown":   "{\"native_key_input\":{\"bytes\":45,\"eof\":true,\"key\":\"x\"}}\n{}",
		"partial":   "{\"native_key_input\":{\"bytes\":45}}\n{}",
		"trailing":  "{\"native_key_input\":{\"bytes\":45,\"eof\":true}} {}\n{}",
		"oversized": "{\"native_key_input\":{\"bytes\":45,\"eof\":true}}" + strings.Repeat(" ", nativeKeyReceiptLimit) + "\n{}",
	} {
		if _, err := splitNativeKeyReceipt([]byte(stdout), 45); err == nil {
			t.Fatal(name, "receipt accepted")
		}
	}
}

// The prelude runs under the local python3, outside Ceph: it must read the
// one record to EOF, print only a byte-count receipt, write a 0600 keyring
// inside a private directory that is gone after exit, and fail without
// echoing malformed input.
func TestNativeKeyPreludeLocally(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	tmp := t.TempDir()
	worker := nativeKeyPrelude + `
import json, os, stat, sys
path = native_keyring_file('client.x')
assert native_keyring_file('client.x') == path
mode = stat.S_IMODE(os.stat(path).st_mode)
dmode = stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode)
with open(path) as f:
    body = f.read()
print(json.dumps({'len': len(NATIVE_KEY), 'mode': mode, 'dmode': dmode, 'body_ok': body == '[client.x]\n\tkey = %s\n' % NATIVE_KEY, 'dir': os.path.dirname(path)}))
`
	run := func(stdin []byte) ([]byte, []byte, error) {
		cmd := exec.Command(python, "-I", "-c", worker)
		cmd.Env = append(os.Environ(), "TMPDIR="+tmp)
		cmd.Stdin = bytes.NewReader(stdin)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		return stdout.Bytes(), stderr.Bytes(), err
	}
	record := []byte(testNativeKey + "\n")
	stdout, stderr, err := run(record)
	if err != nil {
		t.Fatal(err, string(stderr))
	}
	rest, err := splitNativeKeyReceipt(stdout, len(record))
	if err != nil {
		t.Fatal(err, string(stdout))
	}
	if bytes.Contains(stdout, []byte(testNativeKey)) || bytes.Contains(stderr, []byte(testNativeKey)) {
		t.Fatal("worker output echoes the key")
	}
	var got struct {
		Len, Mode, DMode int
		BodyOK           bool `json:"body_ok"`
		Dir              string
	}
	if err := json.Unmarshal(rest, &got); err != nil {
		t.Fatal(err)
	}
	if got.Len != len(testNativeKey) || got.Mode != 0o600 || got.DMode != 0o700 || !got.BodyOK || !strings.HasPrefix(got.Dir, tmp) {
		t.Fatal("prelude keyring", got)
	}
	if _, err := os.Stat(got.Dir); !os.IsNotExist(err) {
		t.Fatal("private keyring directory survived exit", err)
	}
	for name, input := range map[string][]byte{
		"empty":      nil,
		"no newline": []byte(testNativeKey),
		"two lines":  []byte(testNativeKey + "\n" + testNativeKey + "\n"),
		"alphabet":   []byte("AQB" + testNativeKey + "'\n"),
		"oversized":  bytes.Repeat([]byte("A"), nativeKeyInputLimit+1),
	} {
		stdout, stderr, err := run(input)
		if err == nil {
			t.Fatal(name, "malformed input accepted")
		}
		if len(input) > 0 && (bytes.Contains(stdout, input[:8]) || bytes.Contains(stderr, input[:8])) {
			t.Fatal(name, "error echoes input", string(stderr))
		}
		if entries, _ := filepath.Glob(filepath.Join(tmp, "ceph-msgr-key-*")); len(entries) != 0 {
			t.Fatal(name, "keyring directory left behind", entries)
		}
	}
}

// Every keyed worker must compile with the prelude and must not refer to a
// host keyring path.
func TestKeyedNativeWorkersCompileWithoutHostKeyrings(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	for name, script := range map[string]string{
		"cephfs": mdsCephFSNativePython, "completion": mdsCephFSCompletionNativePython, "data": mdsCephFSDataNativePython,
		"push": mdsCephFSPushNativePython, "ranks": mdsCephFSRanksNativePython, "rbd base": rbdBaseNativePython,
		"rbd clone": rbdCloneNativePython, "rbd journal": rbdJournalNativePython, "rbd lock": rbdLockNativePython, "rbd write": rbdWriteNativePython,
	} {
		if strings.Contains(script, ".keyring") || strings.Contains(script, "'keyring'") || strings.Contains(script, "keyring,") || !strings.Contains(script, "NATIVE_KEY") {
			t.Fatal(name, "worker still names a keyring path")
		}
		cmd := exec.Command(python, "-I", "-c", "import sys; compile(sys.stdin.read(), 'worker', 'exec')")
		cmd.Stdin = strings.NewReader(nativeKeyPrelude + script)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatal(name, err, string(output))
		}
	}
}
