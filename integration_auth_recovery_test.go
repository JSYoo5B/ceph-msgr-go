package cephmsgr

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCephRevokedCredentialRecoveryIntegration(t *testing.T) {
	admin, ctx := fixtureClient(t, time.Minute)
	encoded, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "revocable.key"))
	if err != nil {
		t.Fatal(err)
	}
	options := integrationOptions(t)
	options.Identity = "client.revocable"
	options.Key, err = ParseKey(string(bytes.TrimSpace(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatal("MGR before key revocation", err)
	}
	if _, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"auth rm","entity":"client.revocable"}`)}); err != nil {
		t.Fatal("remove fixture credential", err)
	}
	// Independently observe the current daemon's explicit rejection. It is
	// distinct from ticket expiration or a locally canceled discovery wait.
	var server *AuthenticationError
	for {
		fresh, rejected := Dial(ctx, options)
		if fresh != nil {
			fresh.Close()
		} else {
			if !errors.As(rejected, &server) || server.Method != 2 || server.Code >= 0 {
				t.Fatal("fresh connection did not report explicit server rejection", rejected)
			}
			break
		}
		// The committed removal can reach followers after the command
		// response. A still-authenticated follower is not a rejection.
		select {
		case <-ctx.Done():
			t.Fatal("key removal did not reach authentication servers", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("revoked credential: method=%d code=%d", server.Method, server.Code)
	// The short TTL triggers real background reauthentication. No fixture
	// code changes the client's published tickets or forces the renewal.
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
		var actual *AuthenticationError
		if errors.As(err, &actual) {
			if actual.Method != server.Method || actual.Code != server.Code {
				t.Fatal("renewal rejection changed the server result", err)
			}
			var unknown *OutcomeUnknownError
			if errors.As(err, &unknown) {
				t.Fatal("unsubmitted command was marked uncertain", err)
			}
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("background authentication rejection never reached new MON calls", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	_, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var actual *AuthenticationError
	if !errors.As(err, &actual) || actual.Code != server.Code {
		t.Fatal("new MGR call lost the MON authentication rejection", err)
	}
	// Restore the same fixture key through an independently authenticated
	// administrator. The rejected client must recover without being rebuilt.
	keyring := append([]byte("[client.revocable]\n\tkey = "), bytes.TrimSpace(encoded)...)
	keyring = append(keyring, []byte("\n\tcaps mon = \"allow *\"\n\tcaps mgr = \"allow *\"\n")...)
	if _, err := admin.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"auth import"}`), Input: keyring}); err != nil {
		t.Fatal("restore fixture credential", err)
	}
	for {
		_, err = c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
		if err == nil {
			break
		}
		if !errors.As(err, &actual) {
			t.Fatal("unexpected recovery error after restoring credential", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("restored credential did not recover", ctx.Err())
		case <-ticker.C:
		}
	}
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); err != nil {
		t.Fatal("MGR after credential restoration", err)
	}
}
