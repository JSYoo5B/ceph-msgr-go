package testcluster

import "testing"

func TestNativePeerIdentityDoesNotDependOnPorts(t *testing.T) {
	t.Setenv("CEPH_MSGR_TEST_PEERS", `{"mon.a":"172.22.0.2:3300","mon.b":"[::ffff:172.22.0.3]:3300","mgr.a":"172.22.0.4:6800","mgr.b":"172.22.0.5"}`)
	for _, tc := range []struct{ endpoint, want string }{
		{"172.22.0.2:3300", "mon.a"}, {"172.22.0.3:3300", "mon.b"}, {"[::ffff:172.22.0.3]:3300", "mon.b"},
		{"172.22.0.4:6801", "mgr.a"}, {"172.22.0.5:6800", "mgr.b"}, {"172.22.0.9:3300", ""}, {"172.22.0.2:6789", ""},
	} {
		_, got, err := SplitPeer(tc.endpoint)
		if err != nil || got != tc.want {
			t.Fatalf("%s role=%q want=%q error=%v", tc.endpoint, got, tc.want, err)
		}
	}
	if _, _, err := SplitPeer("not-an-endpoint"); err == nil {
		t.Fatal("invalid endpoint accepted")
	}
}
