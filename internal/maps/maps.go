// Package maps reads the subset of MON/MGR maps needed by management clients.
package maps

import (
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

var ErrRelease = errors.New("ceph: monitor minimum release is earlier than Tentacle")

type Mon struct {
	FSID           [16]byte
	Epoch          uint32
	AuthEpoch      uint32
	MinimumRelease uint8
	Members        []MonMember
	Addresses      []msgr.Address
}

// MonMember retains the name, current rank and complete advertised address
// vector. Members appear in rank order; Addresses on Mon contains only usable
// msgr2 endpoints for bootstrap.
type MonMember struct {
	Name      string
	Rank      uint32
	Addresses []msgr.Address
}

// DecodeMon returns no partial map on failure.
func DecodeMon(front []byte) (Mon, error) {
	var m Mon
	outer := wire.NewDecoder(front)
	blob := outer.Bytes()
	if err := outer.Done(); err != nil {
		return Mon{}, err
	}
	d := wire.NewDecoder(blob)
	v, p := d.Struct(10)
	if err := d.Done(); err != nil {
		return Mon{}, err
	}
	if v < 7 {
		return Mon{}, wire.ErrVersion
	}
	copy(m.FSID[:], p.Raw(16))
	m.Epoch = p.U32()
	p.Raw(16) // last_changed and created
	for i := 0; i < 2; i++ {
		_, features := p.Struct(1)
		features.U64()
		p.Fail(features.Err())
	}
	n := p.Count(10, 1024)
	byName := make(map[string][]msgr.Address, n)
	for i := 0; i < n && p.Err() == nil; i++ {
		name := p.String()
		_, info := p.Struct(6)
		infoName := info.String()
		addrs := msgr.DecodeAddresses(info)
		p.Fail(info.Err())
		if err := p.Err(); err != nil {
			return Mon{}, err
		}
		if name != infoName {
			return Mon{}, errors.New("ceph: inconsistent monitor name")
		}
		if _, exists := byName[name]; exists {
			return Mon{}, errors.New("ceph: duplicate monitor name")
		}
		byName[name] = addrs
	}
	ranks := p.Count(4, 1024)
	if err := p.Err(); err != nil {
		return Mon{}, err
	}
	// Tentacle MonMap add/remove/rename keep ranks.size() == mon_info.size().
	// Together with the checks below, ranks must cover every member once.
	if ranks != len(byName) {
		return Mon{}, errors.New("ceph: incomplete monitor ranks")
	}
	m.Members = make([]MonMember, 0, ranks)
	seen := make(map[string]bool)
	for i := 0; i < ranks; i++ {
		name := p.String()
		if err := p.Err(); err != nil {
			return Mon{}, err
		}
		addrs, ok := byName[name]
		if !ok || seen[name] {
			return Mon{}, errors.New("ceph: invalid monitor ranks")
		}
		seen[name] = true
		m.Members = append(m.Members, MonMember{Name: name, Rank: uint32(i), Addresses: addrs})
		for _, a := range addrs {
			if a.Type == 2 && a.Endpoint.IsValid() {
				m.Addresses = append(m.Addresses, a)
			}
		}
	}
	m.MinimumRelease = p.U8()
	if v >= 8 {
		n := p.Count(4, 1024) // removed ranks
		for i := 0; i < n; i++ {
			p.U32()
		}
		p.U8() // election strategy
		n = p.Count(4, 1024)
		for i := 0; i < n; i++ {
			_ = p.String() // disallowed leaders
		}
	}
	if v >= 9 {
		p.Bool() // stretch mode
		_ = p.String()
		n := p.Count(4, 1024)
		for i := 0; i < n; i++ {
			_ = p.String() // stretch marked-down monitors
		}
	}
	if v >= 10 {
		m.AuthEpoch = p.U32()
	}
	if err := p.Err(); err != nil {
		return Mon{}, err
	}
	if m.MinimumRelease < 20 {
		return Mon{}, fmt.Errorf("%w: %d", ErrRelease, m.MinimumRelease)
	}
	if m.FSID == [16]byte{} || len(m.Addresses) == 0 {
		return Mon{}, errors.New("ceph: empty monitor map")
	}
	// The envelope permits unrelated appended cipher policy and future fields.
	return m, nil
}

// Standby identifies a daemon advertised in the authenticated MgrMap. It does
// not include an address or promise that the daemon is ready for connections.
type Standby struct {
	Name     string
	GlobalID uint64
}

// ModuleInfo preserves the active daemon's advertised module capability.
// CanRun is not a running state; runtime failures are reported through health.
type ModuleInfo struct {
	Name        string
	CanRun      bool
	ErrorString string
}

type Mgr struct {
	Epoch            uint32
	GlobalID         uint64
	Available        bool
	Name             string
	Addresses        []msgr.Address
	Standbys         []Standby
	EnabledModules   []string // Explicit enabled set, separate from always-on modules.
	AvailableModules []ModuleInfo
}

func DecodeMgr(front []byte) (Mgr, error) {
	var m Mgr
	d := wire.NewDecoder(front)
	v, p := d.Struct(14)
	if err := d.Done(); err != nil {
		return m, err
	}
	if v < 6 {
		return m, wire.ErrVersion
	}
	m.Epoch = p.U32()
	all := msgr.DecodeAddresses(p)
	m.GlobalID = p.U64()
	m.Available = p.Bool()
	m.Name = p.String()
	n := p.Count(26, 65536) // uint64 key, envelope header, gid and name length
	if n > 0 {
		m.Standbys = make([]Standby, 0, n)
	}
	for i := 0; i < n && p.Err() == nil; i++ {
		p.U64() // StandbyInfo map key; the CLI exposes the value's gid.
		_, info := p.Struct(4)
		standby := Standby{GlobalID: info.U64(), Name: info.String()}
		p.Fail(info.Err())
		if p.Err() == nil {
			m.Standbys = append(m.Standbys, standby)
		}
		// Struct consumes the complete envelope. Module/feature metadata in
		// this child is outside management discovery and can be skipped.
	}
	n = p.Count(4, 65536)
	if n > 0 {
		m.EnabledModules = make([]string, 0, n)
	}
	for i := 0; i < n && p.Err() == nil; i++ {
		name := p.String()
		if p.Err() == nil {
			m.EnabledModules = append(m.EnabledModules, name)
		}
	}
	n = p.Count(8, 65536) // services: module name and URI; not exposed here
	for i := 0; i < n && p.Err() == nil; i++ {
		_ = p.String()
		_ = p.String()
	}
	n = p.Count(15, 65536) // envelope, name/error lengths and can_run
	if n > 0 {
		m.AvailableModules = make([]ModuleInfo, 0, n)
	}
	for i := 0; i < n && p.Err() == nil; i++ {
		_, info := p.Struct(2)
		module := ModuleInfo{Name: info.String(), CanRun: info.Bool(), ErrorString: info.String()}
		p.Fail(info.Err())
		if p.Err() == nil {
			m.AvailableModules = append(m.AvailableModules, module)
		}
		// Module options remain in the consumed child envelope.
	}
	for _, a := range all {
		if a.Type == 2 && a.Endpoint.IsValid() {
			m.Addresses = append(m.Addresses, a)
		}
	}
	if err := p.Err(); err != nil {
		return m, err
	}
	if m.Available && (m.GlobalID == 0 || len(m.Addresses) == 0) {
		return m, errors.New("ceph: invalid active manager")
	}
	return m, nil // The remaining timestamps, policies and client metadata are not needed.
}
