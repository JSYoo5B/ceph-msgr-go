package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCephReplacedCredentialIntegration(t *testing.T) {
	admin, ctx := fixtureClient(t, time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	readKey := func(name string) []byte {
		t.Helper()
		encoded, err := os.ReadFile(filepath.Join(control, name))
		if err != nil {
			t.Fatal(err)
		}
		return bytes.TrimSpace(encoded)
	}
	original, replacement := readKey("revocable.key"), readKey("readonly.key")
	if bytes.Equal(original, replacement) {
		t.Fatal("fixture did not generate independent credentials")
	}
	keyring := func(encoded []byte) []byte {
		result := append([]byte("[client.revocable]\n\tkey = "), encoded...)
		return append(result, []byte("\n\tcaps mon = \"allow *\"\n\tcaps mgr = \"allow *\"\n")...)
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if _, err := admin.MonCommand(restore, Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring(original)}); err != nil {
			t.Error("restore fixture credential after key replacement", err)
		}
	}()
	options := integrationOptions(t)
	options.Identity = "client.revocable"
	var err error
	options.Key, err = ParseKey(string(original))
	if err != nil {
		t.Fatal(err)
	}
	old, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	initialID := old.snapshotAuth().GlobalID
	if err := fixtureRecoveryRead(ctx, old, true); err != nil {
		t.Fatal("original credential MGR command", err)
	}
	// Reuse another independently generated fixture key as replacement
	// material. Only client.revocable's key and explicit caps change; the
	// identity owning readonly.key retains its original read-only caps.
	if _, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring(replacement)}); err != nil {
		t.Fatal("import replacement fixture credential", err)
	}
	for {
		_, err := old.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
		if err != nil {
			var rejection *AuthenticationError
			var unknown *OutcomeUnknownError
			if !errors.As(err, &rejection) || rejection.Method != 2 || rejection.Code != -13 {
				t.Fatal("replaced key lost its explicit renewal rejection", err)
			}
			if !errors.As(err, &unknown) {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("replaced key did not reject the old client's renewal", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	_, err = old.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var rejection *AuthenticationError
	var unknown *OutcomeUnknownError
	if !errors.As(err, &rejection) || rejection.Code != -13 || errors.As(err, &unknown) {
		t.Fatal("old key admitted a new MGR command", err)
	}
	if old.snapshotAuth().GlobalID != initialID {
		t.Fatal("rejected client silently replaced its identity")
	}
	options.Key, err = ParseKey(string(replacement))
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("explicit new-key client authentication", err)
	}
	defer fresh.Close()
	if err := fixtureRecoveryRead(ctx, fresh, false); err != nil {
		t.Fatal("new credential MON command", err)
	}
	if err := fixtureRecoveryRead(ctx, fresh, true); err != nil {
		t.Fatal("new credential MGR command", err)
	}
	t.Log("key replacement: old client rejected renewal; explicit new-key client served MON/MGR")
}
