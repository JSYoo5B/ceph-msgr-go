package osd

import (
	"bytes"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func inactiveCopyFromOperations() map[string]Operation {
	return map[string]Operation{
		"watch register":  {Code: Watch, Watch: WatchInput{Cookie: 1, Action: WatchRegister}},
		"watch reconnect": {Code: Watch, Watch: WatchInput{Cookie: 1, Action: WatchReconnect}},
		"watch ping":      {Code: Watch, Watch: WatchInput{Cookie: 1, Action: WatchPing}},
		"unwatch":         {Code: Watch, Watch: WatchInput{Cookie: 1, Action: WatchUnwatch}},
		"notify":          {Code: Notify, Watch: WatchInput{Cookie: 1, ProtocolVersion: 1}},
		"notify ack":      {Code: NotifyAck, Watch: WatchInput{Cookie: 1, NotifyID: 1}},
		"sparse read":     {Code: SparseRead, Length: 1, SparseRead: SparseReadInput{MaxOutputBytes: 32}},
		"mapext":          {Code: MapExt, Length: 1, MapExt: MapExtInput{MaxOutputBytes: 32}},
		"checksum":        {Code: Checksum, Length: 1, Checksum: ChecksumInput{Type: ChecksumCRC32C}},
		"list watchers":   {Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 32}},
		"rollback":        {Code: Rollback, Snap: 1},
		"list snapshots":  {Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 32}},
		"unset manifest":  {Code: UnsetManifest},
		"stat":            {Code: Stat},
	}
}

func inactiveCopyFromFields() map[string]CopyFromInput {
	return map[string]CopyFromInput{
		"object":            {Object: "unexpected"},
		"pool":              {Locator: Locator{Pool: 1}},
		"key":               {Locator: Locator{Key: "key"}},
		"namespace":         {Locator: Locator{Namespace: "namespace"}},
		"hash":              {Locator: Locator{Hash: -1}},
		"snapshot":          {Snap: 1},
		"version":           {Version: 1},
		"flags":             {Flags: CopyFromTruncateSequence},
		"truncate sequence": {TruncateSequence: 2},
		"truncate size":     {TruncateSize: 1},
		"source fadvise":    {SourceFadvise: OpFadviseRandom},
	}
}

func inactiveCopyFromEncoders() map[string]func(Request) (msgr.MessageData, error) {
	return map[string]func(Request) (msgr.MessageData, error){
		"v6": func(r Request) (msgr.MessageData, error) {
			return EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
		},
		"v8": func(r Request) (msgr.MessageData, error) {
			return EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 1}, Shard: -1}, v8OracleFeatures, wire.DefaultLimit)
		},
		"v9": func(r Request) (msgr.MessageData, error) {
			return EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 1}, Shard: -1}, v9UnitFeatures, wire.DefaultLimit)
		},
	}
}

func TestInactiveCopyFromRejectedBeforeStandaloneBranches(t *testing.T) {
	for name, op := range inactiveCopyFromOperations() {
		t.Run(name, func(t *testing.T) {
			r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: []Operation{op}}
			if _, _, err := ValidateOperations(r.Operations, wire.DefaultLimit); err != nil {
				t.Fatal("baseline operation must be valid", err)
			}
			for version, encode := range inactiveCopyFromEncoders() {
				t.Run(version, func(t *testing.T) {
					if _, err := encode(r); err != nil {
						t.Fatal("baseline request must encode", err)
					}
					for field, input := range inactiveCopyFromFields() {
						t.Run(field, func(t *testing.T) {
							bad := r
							bad.Operations = []Operation{op}
							bad.Operations[0].CopyFrom = input
							if _, _, err := ValidateOperations(bad.Operations, wire.DefaultLimit); !errors.Is(err, ErrRequest) {
								t.Fatal("inactive source input admitted", err)
							}
							m, err := encode(bad)
							if !errors.Is(err, ErrRequest) || m.Type != 0 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
								t.Fatal("invalid input produced a sendable message", err)
							}
						})
					}
				})
			}
		})
	}
}

func TestActiveCopyFromAndRedirectStillEncode(t *testing.T) {
	copyOp := copyFromOperation(CopyFrom2)
	copyOp.CopyFrom.Flags = CopyFromTruncateSequence
	copyOp.CopyFrom.TruncateSequence, copyOp.CopyFrom.TruncateSize = 2, 3
	copyOp.CopyFrom.SourceFadvise = OpFadviseSequential
	redirect := copyFromOperation(SetRedirect)
	for name, ops := range map[string][]Operation{
		"copy from":       {copyFromOperation(CopyFrom)},
		"copy from2":      {copyOp},
		"copy then xattr": {copyOp, {Code: SetXattr, Xattr: XattrInput{Name: "key"}, Data: []byte("value")}},
		"redirect":        {redirect},
	} {
		t.Run(name, func(t *testing.T) {
			r := Request{MapEpoch: 11, Object: "destination", Locator: Locator{Pool: 7, Hash: -1}, Operations: ops}
			for version, encode := range inactiveCopyFromEncoders() {
				t.Run(version, func(t *testing.T) {
					m, err := encode(r)
					want := expectedCopyFromData(ops[0].CopyFrom)
					if len(ops) == 2 {
						want = append(want, []byte("keyvalue")...)
					}
					if err != nil || m.Type != RequestMessage || !bytes.Equal(m.Data, want) {
						t.Fatalf("active source union changed: data=%x want=%x error=%v", m.Data, want, err)
					}
				})
			}
		})
	}
}
