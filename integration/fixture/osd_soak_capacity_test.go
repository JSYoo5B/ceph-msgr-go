package main

import "testing"

func TestOSDSoakCapacityOverrideIsClosedAndFinite(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want int64
	}{
		{"soak", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_SOAK": "4m"}, 4 << 30},
		{"ordinary replicated", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3"}, 0},
		{"single BlueStore", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_SOAK": "4m"}, 0},
		{"MemStore", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_SOAK": "4m"}, 0},
		{"other selector", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_SOAK": "4m", "CEPH_MSGR_TEST_RUN": "^TestCephOSDStatIntegration$"}, 0},
		{"no OSD", map[string]string{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := loadProfile(func(key string) string { return tc.env[key] })
			if err != nil {
				t.Fatal(err)
			}
			if got := fixtureOSDBlockSize(p); got != tc.want {
				t.Fatalf("block-size override = %d, want %d", got, tc.want)
			}
		})
	}
}
