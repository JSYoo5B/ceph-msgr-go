package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// handshakeEvent is one messenger event a stock daemon logged on a
// connection to a requested endpoint, read by the fixture (control
// mgr-handshake-log, integration/fixture/mgr_handshake_log.go, which lists
// the kinds) from the daemon's own container log, never through the Go
// library. Cookies are "0" or labels equal exactly when the raw values are.
type handshakeEvent struct {
	Time         string `json:"time"`
	Process      int    `json:"process"`
	Endpoint     int    `json:"endpoint"`
	Conn         string `json:"conn"`
	Kind         string `json:"kind"`
	State        string `json:"state"`
	Lossy        string `json:"lossy"`
	Peer         string `json:"peer"`
	ClientCookie string `json:"client_cookie"`
	ServerCookie string `json:"server_cookie"`
	Addrs        string `json:"addrs"`
	Target       string `json:"target"`
	GID          string `json:"gid"`
	GlobalSeq    string `json:"global_seq"`
	ConnectSeq   string `json:"connect_seq"`
	Supported    string `json:"features_supported"`
	Required     string `json:"features_required"`
	Flags        string `json:"flags"`
	Cookie       string `json:"cookie"`
	PeerCookie   string `json:"peer_cookie"`
	Full         string `json:"full"`
	Entity       string `json:"entity"`
	Message      string `json:"message"`
	Seq          string `json:"seq"`
}

func (e handshakeEvent) String() string {
	s := fmt.Sprintf("%s p%d %s %s s=%s l=%s", e.Time, e.Process, e.Conn, e.Kind, e.State, e.Lossy)
	switch {
	case strings.Contains(e.Kind, "ident"):
		s += fmt.Sprintf(" addrs=%s target=%s gid=%s gs=%s flags=%s cookie=%s", e.Addrs, e.Target, e.GID, e.GlobalSeq, e.Flags, e.Cookie)
	case strings.HasPrefix(e.Kind, "reconnect"):
		s += fmt.Sprintf(" cookies=%s/%s gs=%s cs=%s", e.Cookie, e.PeerCookie, e.GlobalSeq, e.ConnectSeq)
	case strings.HasPrefix(e.Kind, "reset"):
		s += " full=" + e.Full
	case e.Kind == "ready":
		s += fmt.Sprintf(" entity=%s cookies=%s/%s", e.Entity, e.Cookie, e.PeerCookie)
	case e.Kind == "message_sent":
		s += fmt.Sprintf(" %s seq=%s", e.Message, e.Seq)
	}
	return s
}

// at parses the log timestamp.
func (e handshakeEvent) at(t *testing.T) time.Time {
	t.Helper()
	at, err := time.Parse("2006-01-02T15:04:05.999999-0700", e.Time)
	if err != nil {
		t.Fatal("handshake event time", e.Time, err)
	}
	return at
}

// handshakeProcess is one daemon start banner of the container log.
type handshakeProcess struct {
	Process int    `json:"process"`
	Time    string `json:"time"`
	Name    string `json:"name"`
	PID     string `json:"pid"`
}

type handshakeLog struct {
	Daemon    string             `json:"daemon"`
	Endpoints []string           `json:"endpoints"`
	Processes []handshakeProcess `json:"processes"`
	Events    []handshakeEvent   `json:"events"`
}

// kinds returns the events of the given kinds.
func (l handshakeLog) kinds(kinds ...string) []handshakeEvent {
	var out []handshakeEvent
	for _, e := range l.Events {
		for _, k := range kinds {
			if e.Kind == k {
				out = append(out, e)
			}
		}
	}
	return out
}

// mgrHandshakeLog requests the handshake events daemon ("osd" or "mgr" and
// its name) logged for connections to endpoints ("<ip>:<port>/<nonce>").
func mgrHandshakeLog(t *testing.T, ctx context.Context, control, label, kind, name string, endpoints ...string) handshakeLog {
	t.Helper()
	args := append([]string{label, kind, name}, endpoints...)
	if err := testcluster.RequestControl(ctx, control, "mgr-handshake-log", "mgr-handshake-log-completed", args...); err != nil {
		t.Fatal("handshake log", label, err)
	}
	data, err := os.ReadFile(filepath.Join(control, "mgr-handshake-log.json"))
	var receipt struct {
		Label    string `json:"label"`
		Admitted bool   `json:"admitted"`
		handshakeLog
	}
	if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Label != label || !receipt.Admitted || len(receipt.Endpoints) != len(endpoints) {
		t.Fatal("handshake log receipt", label, err)
	}
	return receipt.handshakeLog
}
