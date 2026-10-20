package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// PGNLS_FILTER with the OSD's built-in "plain" filter lists only objects whose
// attribute equals the value; rejected objects still advance the cursor. The
// expected set comes from native xattr observation of every seeded object.
func TestCephOSDPGListFilterIntegration(t *testing.T) {
	fixture := osdFixture
	modern := isReplicatedPGFixtureProfile()
	if modern {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	createOwnedPools(t, ctx, base, 1)
	tags := map[string]string{}
	var names []string
	for i := 0; i < 9; i++ {
		name := fmt.Sprintf("filter-object-%d", i)
		var attrs []nativeXattrEntry
		switch i % 3 {
		case 0:
			attrs = []nativeXattrEntry{{Name: []byte("tag"), Value: []byte("keep")}}
		case 1:
			attrs = []nativeXattrEntry{{Name: []byte("tag"), Value: []byte("keeper")}}
		}
		poolSeed(t, ctx, 1, name, "", []byte(name), attrs)
		names = append(names, name)
	}
	// Expected from native observation, not from the seeds.
	want := map[string]bool{}
	byPG := map[string]osdFixtureOracle{}
	for _, name := range names {
		state := poolObserve(t, ctx, 1, name, "", "")
		tags[name] = xattrInventory(state)["tag"]
		if tags[name] == "keep" {
			want[name] = true
		}
		route := poolPlace(t, ctx, 1, name, "")
		if _, ok := byPG[route.PGID]; !ok {
			byPG[route.PGID] = route
		}
	}
	if len(want) != 3 {
		t.Fatal("native seeds differ", tags)
	}
	list := func(route osdFixtureOracle, filter *cephmsgr.OSDPGListFilter) ([]string, error) {
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		target := cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.Hash, Primary: route.ID}
		if modern {
			target.Seed = route.PG
		}
		var got []string
		var cursor []byte
		for page := 0; page < 50; page++ {
			reply, err := o.ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: route.Epoch, PG: target, Cursor: cursor, Count: 2, MaxOutputBytes: 4096, Filter: filter})
			if err != nil {
				return got, err
			}
			for _, e := range reply.Entries {
				got = append(got, e.Object)
			}
			if reply.End {
				return got, nil
			}
			cursor = reply.Cursor.Raw
		}
		t.Fatal("listing did not end")
		return nil, nil
	}
	plain := cephmsgr.OSDPlainPGListFilter("_tag", []byte("keep"))
	// cls_hello registers a class filter "hello" with the plain filter's
	// input and semantics; it exercises the object-class filter path.
	hello := cephmsgr.OSDPGListFilter{Name: "hello.hello", Input: plain.Input}
	var filtered, viaClass, all []string
	for _, route := range byPG {
		got, err := list(route, &plain)
		if err != nil {
			t.Fatal("filtered listing", err)
		}
		filtered = append(filtered, got...)
		got, err = list(route, &hello)
		if err != nil {
			t.Fatal("class-filtered listing", err)
		}
		viaClass = append(viaClass, got...)
		got, err = list(route, nil)
		if err != nil {
			t.Fatal("unfiltered listing", err)
		}
		all = append(all, got...)
	}
	sort.Strings(filtered)
	sort.Strings(viaClass)
	var expected []string
	for name := range want {
		expected = append(expected, name)
	}
	sort.Strings(expected)
	if fmt.Sprint(filtered) != fmt.Sprint(expected) || fmt.Sprint(viaClass) != fmt.Sprint(expected) || len(all) != len(names) {
		t.Fatalf("filtered %v, class filter %v, want %v; unfiltered %d of %d", filtered, viaClass, expected, len(all), len(names))
	}
	var route osdFixtureOracle
	for _, r := range byPG {
		route = r
		break
	}
	// A class outside osd_class_load_list is refused at load (-EPERM is
	// propagated); a missing filter in a loadable class and undecodable plain
	// input are -EINVAL.
	for name, tc := range map[string]struct {
		filter cephmsgr.OSDPGListFilter
		code   int32
	}{
		"class outside the load list":   {cephmsgr.OSDPGListFilter{Name: "nosuchclass.filter"}, -1},
		"missing filter in hello class": {cephmsgr.OSDPGListFilter{Name: "hello.nosuchfilter"}, -22},
		// The OSD reads the filter input and then the cursor from one stream,
		// so only input running past the end is unambiguously malformed.
		"plain input past the end": {cephmsgr.OSDPGListFilter{Name: "plain", Input: []byte{0xf0, 0xff, 0xff, 0xff}}, -22},
	} {
		_, err := list(route, &tc.filter)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != tc.code {
			t.Fatal(name, "expected a known refusal", tc.code, err)
		}
	}
	t.Logf("plain filter kept %v of %d objects across %d PGs", filtered, len(names), len(byPG))
}
