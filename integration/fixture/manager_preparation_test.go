package main

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestManagerAuthRejectionRequiresLatestNativeProcess(t *testing.T) {
	const start = "0 ceph version 20.2.4 tentacle, process ceph-mgr\n"
	const rejected = "-1 monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied\n"
	for _, tc := range []struct {
		name, log string
		want      bool
	}{
		{"latest refuses", start + rejected, true},
		{"old refusal then healthy respawn", start + rejected + start, false},
		{"unattributed truncated log", rejected, false},
		{"other daemon error", start + "cephx: verify_authorizer could not get service secret for service mgr secret_id=14\n", false},
		{"different auth code", start + "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (5) Input/output error\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := managerAuthRejection(tc.log) != ""; got != tc.want {
				t.Fatalf("rejection=%t want=%t", got, tc.want)
			}
		})
	}
}

func TestManagerFailoverReadyRequiresExactOwnedActiveAndStandby(t *testing.T) {
	names := map[string]bool{"a": true, "b": true}
	status := ceph.ManagerStatus{Available: true, ActiveName: "a", ActiveGID: 100}
	if managerFailoverReady(status, names) {
		t.Fatal("available active alone cannot support failover")
	}
	status.Standbys = append(status.Standbys, struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	}{Name: "b", GID: 200})
	if !managerFailoverReady(status, names) {
		t.Fatal("healthy owned active and standby rejected")
	}
	for _, invalid := range []string{"a", "unknown", ""} {
		status.Standbys[0].Name = invalid
		if managerFailoverReady(status, names) {
			t.Fatal("invalid standby accepted", invalid)
		}
	}
	status.Standbys[0].Name, status.Standbys[0].GID = "b", 0
	if managerFailoverReady(status, names) {
		t.Fatal("unregistered standby accepted")
	}
}
