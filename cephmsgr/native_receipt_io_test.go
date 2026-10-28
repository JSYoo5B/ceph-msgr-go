package cephmsgr

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type receiptIOValue struct {
	Code   int32  `json:"code"`
	Data   []byte `json:"data"`
	Status string `json:"status"`
}

func TestNativeReceiptFilePreservesOpenFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	boundErr := errors.New("receipt bound")
	value := receiptIOValue{Code: 7}
	err := readNativeReceiptFile(path, 128<<10, boundErr, &value)
	var fileErr *os.PathError
	if !errors.Is(err, os.ErrNotExist) || !errors.As(err, &fileErr) || fileErr.Op != "open" || fileErr.Path != path || err == boundErr || value.Code != 7 {
		t.Fatal("open failure or untouched destination was not preserved", err, value)
	}
}

func TestNativeReceiptFileAcceptsExactByteLimit(t *testing.T) {
	// The three channel observers use these two byte budgets. Padding is part
	// of the input bound; raw signed status and binary bytes remain unchanged.
	for _, limit := range []int{128 << 10, 256 << 10} {
		path := filepath.Join(t.TempDir(), "receipt.json")
		payload := `{"code":-13,"data":"AP8=","status":"denied","future":true}`
		payload += strings.Repeat(" ", limit-len(payload))
		if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
		var value receiptIOValue
		if err := readNativeReceiptFile(path, int64(limit), errors.New("receipt bound"), &value); err != nil || value.Code != -13 || value.Status != "denied" || !bytes.Equal(value.Data, []byte{0, 0xff}) {
			t.Fatal("exact-limit receipt lost its raw result", limit, err, value)
		}
		// On Windows this also catches a file handle left open by the helper.
		if err := os.Remove(path); err != nil {
			t.Fatal("receipt file remained open after decoding", err)
		}
	}
}

func TestNativeReceiptDecodeRejectsOverflowBeforeUnmarshal(t *testing.T) {
	for _, limit := range []int{128 << 10, 256 << 10} {
		payload := `{"code":-13}` + strings.Repeat(" ", limit)
		reader := &receiptReadMeter{Reader: strings.NewReader(payload)}
		boundErr := errors.New("channel-specific receipt bound")
		value := receiptIOValue{Code: 7}
		if err := decodeNativeReceipt(reader, int64(limit), boundErr, &value); err != boundErr || value.Code != 7 || reader.bytes != limit+1 {
			t.Fatal("overflow was decoded, relabeled or read beyond one lookahead byte", limit, err, value, reader.bytes)
		}
	}
}

func TestNativeReceiptDecodePreservesReadFailureClassification(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "receipt-")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	boundErr := errors.New("channel-specific receipt bound")
	value := receiptIOValue{Code: 7}
	// A real closed file deterministically fails Read on each supported OS.
	// Existing observers deliberately classify read errors with their bound
	// error, separately from file-open errors and malformed JSON.
	if err := decodeNativeReceipt(file, 128<<10, boundErr, &value); err != boundErr || value.Code != 7 {
		t.Fatal("read failure classification changed", err, value)
	}
}

func TestNativeReceiptDecodeRequiresWholeJSONValue(t *testing.T) {
	for _, input := range []string{"", `{"code":`, `{"code":0} {"code":-13}`, `{"code":0}garbage`} {
		boundErr := errors.New("receipt bound")
		value := receiptIOValue{Code: 7}
		err := decodeNativeReceipt(strings.NewReader(input), 128<<10, boundErr, &value)
		var syntaxErr *json.SyntaxError
		if !errors.As(err, &syntaxErr) || err == boundErr || value.Code != 7 {
			t.Fatal("partial, concatenated or malformed JSON was accepted or relabeled", input, err, value)
		}
	}
	var value receiptIOValue
	err := decodeNativeReceipt(strings.NewReader(`{"code":"denied"}`), 128<<10, errors.New("receipt bound"), &value)
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) {
		t.Fatal("JSON type failure was not preserved", err)
	}
}

type receiptReadMeter struct {
	io.Reader
	bytes int
}

func (r *receiptReadMeter) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}
