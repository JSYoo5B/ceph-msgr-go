package maps

import (
	"maps"
	"slices"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MgrCommand is one MonCommand descriptor a MGR daemon advertises in its
// beacon (ceph/ceph/blob/v20.2.4/src/mon/MonCommand.h). The strings are raw
// descriptors; this package neither parses nor executes them.
type MgrCommand struct {
	Command, Help, Module, Permissions string
	Flags                              uint64
}

// MgrClient is one RADOS client a MGR daemon reports for blocklisting: a
// name and its address vector, which may be empty.
type MgrClient struct {
	Name      string
	Addresses []msgr.Address
}

// MgrBeacon is the caller-supplied content of one MMgrBeacon. Every field is
// the consumer's: availability, advertised (not bound) addresses, modules,
// command descriptors, metadata, services, features and clients.
type MgrBeacon struct {
	GlobalID        uint64
	ServerAddresses []msgr.Address
	Available       bool
	Name            string // The bare MGR ID, without "mgr.".
	FSID            [16]byte
	Commands        []MgrCommand
	Metadata        map[string]string
	Services        map[string]string
	Modules         []ModuleInfo
	Features        uint64
	Clients         []MgrClient
}

// mgrBeaconPriority is CEPH_MSG_PRIO_HIGH, the MMgrBeacon constructor's.
const mgrBeaconPriority uint16 = 196

// EncodeMgrBeacon encodes MMgrBeacon header 11/8 in the pinned field order
// (ceph/ceph/blob/v20.2.4/src/messages/MMgrBeacon.h encode_payload): Paxos
// prefix, server_addrs, gid, available, name, fsid, the legacy module-name
// set, command_descs, metadata, services, modules, mgr_features,
// clients_addrs and clients_names. Ceph sets and maps encode sorted; the
// legacy name set, every map and each ModuleOption set (enum_allowed, tags,
// see_also) are therefore sorted and deduplicated here. Modules, command
// descriptors and addresses keep the caller's order. Clients are a native
// multimap: they are ordered by name, keeping the caller's order among equal
// names, with their names and address vectors in two parallel lists.
func EncodeMgrBeacon(b MgrBeacon) msgr.MessageData {
	e := wire.Encoder{}
	msgr.Paxos(&e)
	msgr.EncodeAddresses(&e, b.ServerAddresses)
	e.U64(b.GlobalID)
	if b.Available {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.String(b.Name)
	e.Raw(b.FSID[:])
	names := make([]string, 0, len(b.Modules))
	for _, module := range b.Modules {
		names = append(names, module.Name)
	}
	encodeStringSet(&e, names)
	e.U32(uint32(len(b.Commands)))
	for _, command := range b.Commands {
		body := wire.Encoder{}
		body.String(command.Command)
		body.String(command.Help)
		body.String(command.Module)
		body.String(command.Permissions)
		body.String("cli,rest") // MonCommand::encode_bare's removed availability field
		body.U64(command.Flags)
		e.Struct(1, 1, body.Data)
	}
	encodeStringMap(&e, b.Metadata)
	encodeStringMap(&e, b.Services)
	e.U32(uint32(len(b.Modules)))
	for _, module := range b.Modules {
		encodeModuleInfo(&e, module)
	}
	e.U64(b.Features)
	clients := slices.Clone(b.Clients)
	slices.SortStableFunc(clients, func(a, b MgrClient) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	})
	e.U32(uint32(len(clients)))
	for _, client := range clients {
		msgr.EncodeAddresses(&e, client.Addresses)
	}
	e.U32(uint32(len(clients)))
	for _, client := range clients {
		e.String(client.Name)
	}
	return msgr.MessageData{Type: msgr.MgrBeaconMessage, Version: 11, CompatVersion: 8, Priority: mgrBeaconPriority, Front: e.Data}
}

func encodeStringSet(e *wire.Encoder, values []string) {
	sorted := slices.Compact(slices.Sorted(slices.Values(values)))
	e.U32(uint32(len(sorted)))
	for _, value := range sorted {
		e.String(value)
	}
}

func encodeStringMap(e *wire.Encoder, values map[string]string) {
	e.U32(uint32(len(values)))
	for _, key := range slices.Sorted(maps.Keys(values)) {
		e.String(key)
		e.String(values[key])
	}
}

// encodeModuleInfo is MgrMap::ModuleInfo::encode, envelope 2/1, with its
// options map sorted by key.
func encodeModuleInfo(e *wire.Encoder, module ModuleInfo) {
	body := wire.Encoder{}
	body.String(module.Name)
	if module.CanRun {
		body.U8(1)
	} else {
		body.U8(0)
	}
	body.String(module.ErrorString)
	body.U32(uint32(len(module.Options)))
	for _, key := range slices.Sorted(maps.Keys(module.Options)) {
		body.String(key)
		encodeModuleOption(&body, module.Options[key])
	}
	e.Struct(2, 1, body.Data)
}

// encodeModuleOption is MgrMap::ModuleOption::encode, envelope 1/1.
func encodeModuleOption(e *wire.Encoder, option ModuleOption) {
	body := wire.Encoder{}
	body.String(option.Name)
	body.U8(option.Type)
	body.U8(option.Level)
	body.U32(option.Flags)
	body.String(option.DefaultValue)
	body.String(option.Min)
	body.String(option.Max)
	encodeStringSet(&body, option.EnumAllowed)
	body.String(option.Description)
	body.String(option.LongDescription)
	encodeStringSet(&body, option.Tags)
	encodeStringSet(&body, option.SeeAlso)
	e.Struct(1, 1, body.Data)
}
