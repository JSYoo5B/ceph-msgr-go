package cephmsgr

import (
	"net/netip"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// OSDListWatchers observes the committed head object's watcher registrations.
// It must be the only operation, without flags, ReadSnapshot or SnapContext.
// This observation neither registers a watch nor establishes its liveness.
const OSDListWatchers OSDOpcode = OSDOpcode(osd.ListWatchers)

// OSDListWatchersInput bounds accepted output (at least 10 bytes for an empty
// listing). This is a local budget; the server receives no size restriction.
type OSDListWatchersInput struct{ MaxOutputBytes uint32 }

// OSDWatcher preserves one watch_item_t in server order. NameType/NameNumber
// are the wire entity_name_t, not the identity of this observing client.
// TimeoutSeconds is the registered timeout, not remaining watch lifetime.
type OSDWatcher struct {
	NameType       uint8
	NameNumber     int64
	Cookie         uint64
	TimeoutSeconds uint32
	Address        OSDWatcherAddress
}

// OSDWatcherAddress preserves entity_addr_t's type, nonce and wire IP family,
// including IPv6 flow/scope metadata and an unspecified address.
type OSDWatcherAddress struct {
	Type, Nonce       uint32
	Endpoint          netip.AddrPort
	FlowInfo, ScopeID uint32
}

func publicWatchers(data []byte) []OSDWatcher {
	entries, err := osd.DecodeWatchers(data)
	if err != nil {
		return nil
	}
	result := make([]OSDWatcher, len(entries))
	for i, w := range entries {
		a := w.Address
		result[i] = OSDWatcher{NameType: w.NameType, NameNumber: w.NameNumber, Cookie: w.Cookie, TimeoutSeconds: w.TimeoutSeconds, Address: OSDWatcherAddress{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID}}
	}
	return result
}
