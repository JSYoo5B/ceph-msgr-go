package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Native-only CephX keys reach their one-shot workers through the supervised
// standard input of ceph-testcontainers-go v0.3.0 (NativeCommand.Input), not
// through the host /out bind mount or argv. The worker reads exactly one
// bounded record to the end of its input before it touches Ceph and answers
// with a receipt line that names only the byte count; it never echoes the key.
// The Go side checks the transport counters (InputWritten, InputDelivered,
// InputEOF) and that receipt separately from Complete(), which covers only
// time and output limits.
const (
	// nativeKeyInputLimit bounds the whole input of one keyed worker. One
	// base64 CephX key (aes or aes256k) and its newline need well under 1 KiB.
	nativeKeyInputLimit = 4 << 10
	// nativeKeyReceiptLimit bounds the receipt line the prelude prints first.
	nativeKeyReceiptLimit = 128
)

// nativeKeyPrelude is prepended to a keyed worker's script. It defines
// NATIVE_KEY (the secret for conf={'key': NATIVE_KEY}) and
// native_keyring_file(identity), which writes a 0600 keyring inside a private
// 0700 directory of the container for a CLI subprocess that needs --keyring;
// the directory is removed at interpreter exit. A killed worker can leave that
// directory in the disposable client container, never on the host. Errors name
// only the failed property, never input bytes.
const nativeKeyPrelude = `
import sys as _nk_sys
def _nk_read():
    raw = _nk_sys.stdin.buffer.read(4097)
    if len(raw) > 4096:
        raise ValueError('native key input exceeds its bound')
    if raw.count(b'\n') != 1 or not raw.endswith(b'\n') or len(raw) < 2:
        raise ValueError('native key input is not one record')
    alphabet = b'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/='
    if any(b not in alphabet for b in raw[:-1]):
        raise ValueError('native key input is not base64')
    return raw[:-1].decode('ascii'), len(raw)
NATIVE_KEY, _nk_bytes = _nk_read()
_nk_sys.stdout.write('{"native_key_input":{"bytes":%d,"eof":true}}\n' % _nk_bytes)
_nk_sys.stdout.flush()
_nk_files = {}
def native_keyring_file(identity):
    if identity not in _nk_files:
        import atexit, os, shutil, tempfile
        directory = tempfile.mkdtemp(prefix='ceph-msgr-key-')
        atexit.register(shutil.rmtree, directory, True)
        path = os.path.join(directory, 'keyring')
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as f:
            f.write('[%s]\n\tkey = %s\n' % (identity, NATIVE_KEY))
        _nk_files[identity] = path
    return _nk_files[identity]
`

// nativeKeyRecord extracts the single key of a dedicated identity's keyring
// and frames it as the worker's one input record.
func nativeKeyRecord(keyring []byte) ([]byte, error) {
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return nil, err
	}
	if key == "" || len(key)+1 > nativeKeyInputLimit {
		return nil, errors.New("native key does not fit its input bound")
	}
	for _, b := range []byte(key) {
		if !(b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '+' || b == '/' || b == '=') {
			return nil, errors.New("native key is not base64")
		}
	}
	return []byte(key + "\n"), nil
}

type nativeKeyReceipt struct {
	NativeKeyInput *struct {
		Bytes *int64 `json:"bytes"`
		EOF   *bool  `json:"eof"`
	} `json:"native_key_input"`
}

// checkNativeKeyDelivery checks the transport counters for one key record:
// every byte the stream took reached the worker's standard input, and the end
// of input followed. Equal counters show delivery into a pipe only; the
// worker receipt is checked separately.
func checkNativeKeyDelivery(result ceph.NativeCommandResult, recordLen int) error {
	if result.InputWritten != int64(recordLen) || result.InputDelivered != result.InputWritten || !result.InputEOF {
		return fmt.Errorf("native key input delivery incomplete: written %d, delivered %d of %d bytes, eof %v", result.InputWritten, result.InputDelivered, recordLen, result.InputEOF)
	}
	return nil
}

// splitNativeKeyReceipt removes and checks the prelude's receipt line, and
// returns the worker's own output that follows it.
func splitNativeKeyReceipt(stdout []byte, recordLen int) ([]byte, error) {
	line, rest, ok := bytes.Cut(stdout, []byte("\n"))
	if !ok || len(line) > nativeKeyReceiptLimit {
		return nil, errors.New("native key receipt missing")
	}
	var receipt nativeKeyReceipt
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil || decoder.More() {
		return nil, errors.New("native key receipt malformed")
	}
	if receipt.NativeKeyInput == nil || receipt.NativeKeyInput.Bytes == nil || receipt.NativeKeyInput.EOF == nil ||
		*receipt.NativeKeyInput.Bytes != int64(recordLen) || !*receipt.NativeKeyInput.EOF {
		return nil, errors.New("native key receipt does not match the delivered record")
	}
	return rest, nil
}

// keyedNativeCommand prepares a python3 -c worker for one key record.
func keyedNativeCommand(command ceph.NativeCommand, input *ceph.NativeInput) (ceph.NativeCommand, error) {
	if len(command.Argv) < 3 || command.Argv[0] != "python3" || command.Argv[1] != "-c" || command.Input != nil || command.MaxInput != 0 {
		return ceph.NativeCommand{}, errors.New("keyed native command must be a python3 -c worker without its own input")
	}
	argv := append([]string(nil), command.Argv...)
	argv[2] = nativeKeyPrelude + argv[2]
	command.Argv = argv
	command.Input, command.MaxInput = input, nativeKeyInputLimit
	command.MaxStdout += nativeKeyReceiptLimit + 1
	return command, nil
}

// runKeyedNative runs a worker with its key on standard input. The returned
// result's Stdout is the worker's own output after the checked receipt line;
// a failed or incomplete process keeps its raw output and is left to the
// caller's exit/Complete checks.
func (c *fixtureCluster) runKeyedNative(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand, identity *ceph.ClientConfig) (ceph.NativeCommandResult, error) {
	if identity == nil {
		return ceph.NativeCommandResult{}, errors.New("keyed native command lacks its identity")
	}
	_, keyring, err := identity.ConnectionConfig()
	if err != nil {
		return ceph.NativeCommandResult{}, err
	}
	record, err := nativeKeyRecord(keyring)
	if err != nil {
		return ceph.NativeCommandResult{}, err
	}
	input := ceph.NewNativeInput()
	command, err = keyedNativeCommand(command, input)
	if err != nil {
		return ceph.NativeCommandResult{}, err
	}
	type outcome struct {
		result ceph.NativeCommandResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := c.cluster.RunNativeCommand(ctx, target, command)
		done <- outcome{result, err}
	}()
	// Write waits for the command to start; the supervisor buffers the
	// record, so the input can end at once. A context error here can follow
	// a transfer: the counters below settle what the worker could read, and
	// the record is never written twice.
	writeErr := input.Write(ctx, record)
	_ = input.Close()
	o := <-done
	if writeErr != nil {
		writeErr = fmt.Errorf("native key input: %w", writeErr)
	}
	if o.err != nil || writeErr != nil {
		return o.result, errors.Join(o.err, writeErr)
	}
	if err := checkNativeKeyDelivery(o.result, len(record)); err != nil {
		return o.result, err
	}
	if !o.result.Complete() || o.result.ExitCode != 0 {
		return o.result, nil
	}
	rest, err := splitNativeKeyReceipt(o.result.Stdout, len(record))
	if err != nil {
		return o.result, err
	}
	o.result.Stdout = rest
	return o.result, nil
}
