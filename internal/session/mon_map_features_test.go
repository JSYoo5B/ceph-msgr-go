package session

import "testing"

// Bit 9 selects the implemented uint64 pool/PG wire representation. It does
// not start a subscription or advertise map application/CRUSH placement.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_features.h#L95
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSDMap.cc#L3334
func TestMONMapFeaturesKeepImplementedCapabilitiesScoped(t *testing.T) {
	const pgid64 = uint64(1 << 9)
	const objectLocator = uint64(1 << 8)
	const admission = uint64(1<<18 | 1<<25 | 1<<41 | 1<<48 | 1<<58)
	// INCSUBOSDMAP, PGPOOL3, OSDREPLYMUX, OSDENC, RADOS_BACKOFF/
	// OSDMAP_PG_UPMAP/RESEND_ON_SPLIT/CRUSH_CHOOSE_ARGS, CRUSH_MSR,
	// OSDMAP_ENC, OSD_POOLRESEND, NEW_OSDOP_ENCODING and encoder sentinels.
	const unimplemented = uint64(1<<10 | 1<<11 | 1<<12 | 1<<13 | 1<<21 | 1<<38 | 1<<39 | 1<<43 | 1<<56 | 1<<62 | 1<<63)
	for _, test := range []struct {
		name string
		role uint8
		want uint64
	}{
		{"MON", 1, Features | admission | pgid64},
		{"MGR", 16, Features},
		{"OSD", 4, Features | objectLocator | pgid64},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := featuresForRole(test.role)
			if got != test.want || got&unimplemented != 0 {
				t.Fatalf("%s advertises unsupported wire capabilities: got %#x want %#x", test.name, got, test.want)
			}
			if test.role != 1 && got&admission != 0 {
				t.Fatal("MON CRUSH admission exception leaked into another peer role")
			}
		})
	}
	if Features&pgid64 != 0 || Features&admission != 0 {
		t.Fatal("MON-specific additions changed the common feature mask")
	}
}
