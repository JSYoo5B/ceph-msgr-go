package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDXattrOutcomeIntegration(t *testing.T) {
	observer, _, oracle, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	name := "go-xattr-unknown-\xff"
	value := []byte{0xff, 0, 'm', 'e', 't', 'a', 10, 0}
	observation := xattrNativeInput{Exists: true, Kind: "read", Names: []string{base64.StdEncoding.EncodeToString([]byte(name))}}
	initial := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if initial.Code != 0 || !bytes.Equal(initial.Data, original) || initial.Version == 0 {
		t.Fatal("independent native baseline differs from routed fixture object")
	}
	if _, exists := xattrInventory(initial)[name]; exists {
		t.Fatal("isolated outcome-test attribute already exists")
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		cleanup, err := observer.OpenOSD(restore, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
		if err != nil {
			t.Error("open explicitly targeted metadata outcome cleanup", err)
			return
		}
		defer cleanup.Close()
		request := osdObjectRequest(oracle,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(original)), Data: original})
		request.ModificationTime = cephmsgr.OSDTime{Seconds: oracle.Seconds, Nanoseconds: oracle.Nanoseconds}
		if _, err := cleanup.Request(restore, request); err != nil {
			t.Error("restore original fixture bytes/mtime and remove owned outcome attribute", err)
			return
		}
		after := nativeXattrCompare(t, restore, oracle, observation).Actual
		if !xattrSnapshotEqual(initial, after, false) {
			t.Error("metadata outcome cleanup changed preexisting native bytes/attributes/mtime")
		}
	}()
	options := integrationOptions(t)
	options.EnableOSD = true
	var armed atomic.Bool
	blocked := make(chan struct{}, 1)
	faultDaemon := "osd." + strconv.Itoa(int(oracle.ID))
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, daemon, _ := testcluster.SplitPeer(endpoint)
		if daemon != faultDaemon {
			return conn, nil
		}
		return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
			select {
			case blocked <- struct{}{}:
			default:
			}
		}}, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	armed.Store(true)
	tail := []byte{0, 0xff, 0x80, 10, 'x', 'a', 't', 't', 'r', '-', 'o', 'n', 'c', 'e', 0}
	request := osdObjectRequest(oracle,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value})
	request.ClientIncarnation = 47
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000600, Nanoseconds: 987654321}
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	finished := make(chan completion, 1)
	// One actual mixed vector is submitted. Receive withholding retains no
	// response bytes or keys; independent native reads establish execution.
	go func() { reply, err := o.Request(ctx, request); finished <- completion{reply, err} }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("metadata receive fault was not activated", ctx.Err())
	}
	want := append(bytes.Clone(original), tail...)
	wantAttrs := xattrInventory(initial)
	wantAttrs[name] = string(value)
	var executed nativeXattrSnapshot
	for {
		executed = nativeXattrCompare(t, ctx, oracle, observation).Actual
		if executed.Code == 0 && bytes.Equal(executed.Data, want) && xattrInventory(executed)[name] == string(value) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("mixed append/metadata execution was not independently observed", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if executed.Version <= initial.Version || executed.Seconds != int64(request.ModificationTime.Seconds) || executed.Nanos != int64(request.ModificationTime.Nanoseconds) || len(executed.Attributes) != len(wantAttrs) {
		t.Fatal("mixed native execution lacks version, explicit mtime or isolated inventory")
	}
	for key, expected := range wantAttrs {
		if actual, exists := xattrInventory(executed)[key]; !exists || actual != expected {
			t.Fatal("mixed vector altered unrelated native attributes", []byte(key))
		}
	}
	select {
	case result := <-finished:
		t.Fatalf("mixed mutation completed despite blocked receive: %v", result.err)
	default:
	}
	// Keep the caller context live while the explicit handle is closed.
	// Native execution knowledge cannot turn its absent reply into a receipt.
	if err := o.Close(); err != nil {
		t.Fatal("close handle with sent mixed metadata mutation", err)
	}
	var result completion
	select {
	case result = <-finished:
	case <-ctx.Done():
		t.Fatal("Close did not release mixed metadata mutation", ctx.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	submission := result.reply.Submission
	if ctx.Err() != nil || !errors.As(result.err, &unknown) || !errors.Is(result.err, cephmsgr.ErrClosed) || submission.Transaction == 0 || submission.GlobalID != c.Snapshot().GlobalID || submission.ClientIncarnation != 47 || submission.MapEpoch != oracle.Epoch || submission.Attempt != 0 || result.reply.Transaction != 0 || result.reply.GlobalID != 0 || result.reply.Code != 0 || result.reply.Flags != 0 || len(result.reply.Operations) != 0 || len(result.reply.RawFront) != 0 || len(result.reply.RawData) != 0 {
		t.Fatalf("absent mixed reply lost unknown outcome/local submission: %+v err=%v caller=%v", result.reply, result.err, ctx.Err())
	}
	if reply, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 1})); !errors.Is(err, cephmsgr.ErrClosed) || reply.Submission != (cephmsgr.OSDSubmission{}) {
		t.Fatal("closed metadata handle admitted another request", reply, err)
	}
	armed.Store(false)
	fresh := mutationConnection(t, c, ctx, oracle)
	defer fresh.Close()
	read, err := fresh.Request(ctx, osdObjectRequest(oracle,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name, MaxOutputBytes: uint32(len(value))}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
	if err != nil {
		t.Fatal("fresh explicitly targeted GET/read after metadata Close", err)
	}
	checkOSDMetadata(t, read, oracle)
	if read.Transaction <= submission.Transaction || read.UserVersion != executed.Version || len(read.Operations) != 3 || !bytes.Equal(read.Operations[0].Data, value) || read.Operations[0].Xattr.NameLength != uint32(len(name)) || read.Operations[0].Xattr.ValueLength != uint32(len(value)) || !bytes.Equal(read.Operations[1].Data, want) {
		t.Fatal("fresh metadata session changed local TID or observed bytes/attribute/version")
	}
	for _, op := range read.Operations {
		if op.Result != 0 {
			t.Fatal("fresh metadata observation has a failed operation", op)
		}
	}
	stat := read.Operations[2].Data
	if len(stat) != 16 || binary.LittleEndian.Uint64(stat) != uint64(len(want)) || binary.LittleEndian.Uint32(stat[8:]) != uint32(executed.Seconds) || binary.LittleEndian.Uint32(stat[12:]) != uint32(executed.Nanos) {
		t.Fatal("fresh mixed metadata stat differs from independent native size/mtime")
	}
	final := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if !xattrSnapshotEqual(executed, final, true) {
		t.Fatal("Close/reopen replayed the append or changed native metadata/version/mtime")
	}
	t.Log("one mixed APPEND+SETXATTR independently executed before handle Close; live caller retained OutcomeUnknown+ErrClosed/local identity; fresh explicit GET/read and native state proved no replay; no received ONDISK or native C raw per-op receipt is claimed")
}
