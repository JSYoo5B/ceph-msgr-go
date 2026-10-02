package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephRejectedKeyIntegration(t *testing.T) {
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	keyBytes[len(keyBytes)-1] ^= 1
	options.Key, err = cephmsgr.ParseKey(base64.StdEncoding.EncodeToString(keyBytes))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, options)
	if c != nil {
		c.Close()
		t.Fatal("incorrect credential authenticated")
	}
	var rejected *cephmsgr.AuthenticationError
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &rejected) || rejected.Method != 2 || rejected.Code >= 0 || errors.As(err, &unknown) {
		t.Fatal("authentication rejection lost server code", err)
	}
}

func TestCephCommandErrorsAndBinaryOutputIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MonCommand, c.MgrCommand} {
		result, err := call(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_command__"}`)})
		var serverError *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &serverError) || result.Code != -22 || serverError.Code != result.Code || errors.As(err, &unknown) || result.Message == "" {
			t.Fatal("unknown command reply lost server code/status", result, err)
		}
	}
	result, err := c.MgrCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"pg getmap"}`)})
	if err != nil || len(result.Data) < 16 || json.Valid(result.Data) || !bytes.ContainsRune(result.Data, 0) {
		t.Fatal("MGR binary pg map", err, len(result.Data))
	}
}

func TestCephReadOnlyPermissionsIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("mutation rejection checks require the disposable fixture")
	}
	options := integrationOptions(t)
	encoded, err := os.ReadFile(filepath.Join(control, "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	options.Identity = "client.readonly"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, tc := range []struct {
		call  func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		read  string
		write string
	}{
		{c.MonCommand, `{"prefix":"status","format":"json"}`, `{"prefix":"config set","who":"global","name":"debug_ms","value":"0"}`},
		{c.MgrCommand, `{"prefix":"pg stat","format":"json"}`, `{"prefix":"pg scrub","pgid":"1.0"}`},
	} {
		if _, err := tc.call(ctx, cephmsgr.Command{JSON: []byte(tc.read)}); err != nil {
			t.Fatal("read permission failed", err)
		}
		result, err := tc.call(ctx, cephmsgr.Command{JSON: []byte(tc.write)})
		var denied *cephmsgr.CommandError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &denied) || denied.Code != -13 || result.Code != -13 || errors.As(err, &unknown) {
			t.Fatal("write permission rejection lost", result, err)
		}
	}
}

func TestCephBinaryBulkInputIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("config-key writes require the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	input := bytes.Repeat([]byte{0, 255, 13, 10, 1}, 8192)
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"config-key set","key":"ceph-msgr-go-integration"}`), Input: input}); err != nil {
		t.Fatal("binary bulk input", err)
	}
	result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"config-key get","key":"ceph-msgr-go-integration"}`)})
	if err != nil || !bytes.Equal(result.Data, input) {
		t.Fatal("binary config-key value changed", err, len(result.Data))
	}
}
