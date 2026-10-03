package cephmsgr

import (
	"reflect"
	"sort"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func moduleOptionStateMap(active maps.Mgr, epoch uint32, options map[string]ManagerModuleOption) msgr.MessageData {
	base := mockMgrMapWithStandbys(active, epoch, nil)
	e := wire.Encoder{}
	e.Raw(base.Front[6 : len(base.Front)-12-mockMgrMapTailBytes])
	e.U32(1)
	e.String("custom")
	e.U32(0) // services
	e.U32(1) // available modules
	info := wire.Encoder{}
	info.String("custom")
	info.U8(1)
	info.String("raw load diagnostic")
	info.U32(uint32(len(options)))
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	strings := func(out *wire.Encoder, values []string) {
		out.U32(uint32(len(values)))
		for _, value := range values {
			out.String(value)
		}
	}
	for _, key := range keys {
		option := options[key]
		body := wire.Encoder{}
		body.String(option.Name)
		body.U8(option.Type)
		body.U8(option.Level)
		body.U32(option.Flags)
		body.String(option.DefaultValue)
		body.String(option.Min)
		body.String(option.Max)
		strings(&body, option.EnumAllowed)
		body.String(option.Description)
		body.String(option.LongDescription)
		strings(&body, option.Tags)
		strings(&body, option.SeeAlso)
		info.String(key)
		info.Struct(1, 1, body.Data)
	}
	e.Struct(2, 1, info.Data)
	mockMgrMapTail(&e)
	out := wire.Encoder{}
	out.Struct(14, 6, e.Data)
	return msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: out.Data}
}

func TestSnapshotManagerModuleOptionsOwnershipReplacementAndClose(t *testing.T) {
	active, err := maps.DecodeMgr(mockMgrMap(1, 99, 6800))
	if err != nil {
		t.Fatal(err)
	}
	f := namedTestFixtureFor(t, namedTestAddressB(), nil, nil)
	f.primary.next(t, f.ctx, msgr.SubscribeMessage)
	options := map[string]ManagerModuleOption{
		"empty": {},
		"map-key": {
			Name: "reported-option", Type: 255, Level: 254, Flags: 0xfedcba98,
			Min: "  lower bound\n", Max: "\x00raw", Description: "诊断\nraw description",
			LongDescription: "  long description  ", EnumAllowed: []string{"", "choice"},
			Tags: []string{"tag-a", "tag-b"}, SeeAlso: []string{"option-a", "option-b"},
		},
	}
	publish := func(epoch uint32, want map[string]ManagerModuleOption) State {
		t.Helper()
		f.primary.send(t, f.ctx, moduleOptionStateMap(active, epoch, want))
		waitClientState(t, f.c, f.ctx, func(s State) bool { return s.Manager.MapEpoch == epoch })
		got := f.c.Snapshot()
		if len(got.Manager.AvailableModules) != 1 || !reflect.DeepEqual(got.Manager.AvailableModules[0].Options, want) || got.Manager.AvailableModules[0].Name != "custom" || !got.Manager.AvailableModules[0].CanRun || got.Manager.AvailableModules[0].ErrorString != "raw load diagnostic" || !reflect.DeepEqual(got.Manager.EnabledModules, []string{"custom"}) || got.Manager.Ready || !got.Monitor.Ready {
			t.Fatal("option schema lost raw metadata or changed module state", got)
		}
		return got
	}
	before := publish(2, options)
	mutate := func(s State) {
		opts := s.Manager.AvailableModules[0].Options
		option := opts["map-key"]
		option.EnumAllowed[0], option.Tags[0], option.SeeAlso[0] = "caller enum", "caller tag", "caller reference"
		option.Description, option.Flags = "caller description", 0
		opts["map-key"] = option
		delete(opts, "empty")
		opts["caller-added"] = ManagerModuleOption{Name: "caller added"}
	}
	mutate(f.c.Snapshot())
	if got := f.c.Snapshot(); !reflect.DeepEqual(got.Manager.AvailableModules[0].Options, options) || !reflect.DeepEqual(before.Manager.AvailableModules[0].Options, options) {
		t.Fatal("Snapshot shared its option map or nested string slices", got, before)
	}
	// The next active daemon replaces the full schema; map keys are not merged.
	active.Name, active.GlobalID = "b", 100
	replacement := map[string]ManagerModuleOption{"replacement": {Name: "replacement", DefaultValue: "9283"}}
	after := publish(3, replacement)
	if after.Manager.Name != "b" || after.Manager.GlobalID != 100 || !reflect.DeepEqual(before.Manager.AvailableModules[0].Options, options) {
		t.Fatal("active daemon replacement changed an older schema snapshot", after, before)
	}
	empty := publish(4, nil)
	if empty.Manager.AvailableModules[0].Options != nil {
		t.Fatal("empty schema replacement retained old options", empty)
	}
	publish(5, options)
	if f.monDials.Load() != 1 || f.namedDials.Load() != 0 || f.mgrDials.Load() != 0 || len(f.primary.requests) != 0 || len(f.c.calls) != 0 {
		t.Fatal("option discovery dialed or sent a command")
	}
	if err := f.c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := f.c.Snapshot()
	if !closed.Closed || closed.Manager.Ready || closed.Manager.MapEpoch != 5 || !reflect.DeepEqual(closed.Manager.AvailableModules[0].Options, options) {
		t.Fatal("Close discarded the last authenticated option schema", closed)
	}
	mutate(closed)
	if got := f.c.Snapshot(); !reflect.DeepEqual(got.Manager.AvailableModules[0].Options, options) {
		t.Fatal("closed snapshots share option schema storage", got)
	}
}
