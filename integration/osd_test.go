package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

type osdFixtureOracle struct {
	Pool        int64  `json:"pool_id"`
	Name        string `json:"object"`
	ID          int32  `json:"osd_id"`
	Epoch       uint32 `json:"epoch"`
	Hash        uint32 `json:"hash"`
	PG          uint32 `json:"pg_seed"`
	Address     string `json:"address"`
	Size        uint64 `json:"size"`
	Seconds     uint32 `json:"mtime_seconds"`
	Nanoseconds uint32 `json:"mtime_nanoseconds"`
}

func osdFixture(t *testing.T) (*cephmsgr.Client, context.Context, osdFixtureOracle, []byte) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires the opt-in disposable OSD fixture")
	}
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	metadata, err := os.ReadFile(filepath.Join(directory, "osd-oracle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var oracle osdFixtureOracle
	if err := json.Unmarshal(metadata, &oracle); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "osd-native-read.bin"))
	if err != nil || uint64(len(data)) != oracle.Size {
		t.Fatal("native read oracle", err)
	}
	options := integrationOptions(t)
	options.EnableOSD = true
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if c.Snapshot().OSDTicket.Expires.IsZero() {
		t.Fatal("OSD service ticket missing")
	}
	return c, ctx, oracle, data
}

func osdObjectRequest(oracle osdFixtureOracle, operations ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
	return cephmsgr.OSDRequest{MapEpoch: oracle.Epoch, Object: cephmsgr.OSDObject{Pool: oracle.Pool, Name: oracle.Name, Hash: oracle.Hash}, Operations: operations}
}

func checkOSDMetadata(t *testing.T, reply cephmsgr.OSDReply, oracle osdFixtureOracle) {
	t.Helper()
	// Tentacle's v6 reply retains raw pg_t: its seed is the object's original
	// placement hash. The actual mapped PG in the native map oracle is separate.
	if reply.Code != 0 || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Object != oracle.Name || reply.PG.Pool != uint64(oracle.Pool) || reply.PG.Seed != oracle.Hash || reply.RetryAttempt != 0 || reply.MapEpoch < oracle.Epoch {
		t.Fatalf("OSD metadata: code=%d tid=%d gid=%d pool=%d seed=%x epoch=%d attempt=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG.Pool, reply.PG.Seed, reply.MapEpoch, reply.RetryAttempt)
	}
}

func TestCephOSDIOIntegration(t *testing.T) {
	c, ctx, oracle, data := osdFixture(t)
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
	if err != nil {
		t.Fatal("OSD service connection", err)
	}
	request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: oracle.Size})
	reply, err := o.Request(ctx, request)
	if err != nil {
		t.Fatal("native stat/read", err)
	}
	checkOSDMetadata(t, reply, oracle)
	if len(reply.Operations) != 2 || len(reply.Operations[0].Data) != 16 || reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 || !bytes.Equal(reply.Operations[1].Data, data) {
		t.Fatal("per-operation stat/read differs from native client", reply.Operations)
	}
	stat := reply.Operations[0].Data
	if binary.LittleEndian.Uint64(stat[:8]) != oracle.Size || binary.LittleEndian.Uint32(stat[8:12]) != oracle.Seconds || binary.LittleEndian.Uint32(stat[12:]) != oracle.Nanoseconds {
		t.Fatal("size or nanosecond mtime differs from native stat2", stat)
	}
	partial := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: 4093, Length: 731})
	reply, err = o.Request(ctx, partial)
	if err != nil || !bytes.Equal(reply.Operations[0].Data, data[4093:4824]) {
		t.Fatal("offset read differs from native bytes", err)
	}
	reply, err = o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: oracle.Size + 7, Length: 32}))
	if err != nil || len(reply.Operations[0].Data) != 0 {
		t.Fatal("EOF read", err)
	}
	missing := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	missing.Object.Name += "-absent"
	reply, err = o.Request(ctx, missing)
	var serverError *cephmsgr.OSDError
	// This native missing-object refusal occurs before evaluating the vector:
	// the aggregate is ENOENT while its unexecuted stat retains rval zero.
	if !errors.As(err, &serverError) || serverError.Code != -2 || reply.Code != -2 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || len(reply.Operations[0].Data) != 0 {
		t.Fatalf("missing-object error: aggregate=%d operations=%v error=%v", reply.Code, reply.Operations, err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	ids := make(chan uint64, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply, err := o.Request(ctx, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: uint64(i), Length: 97}))
			if err == nil && !bytes.Equal(reply.Operations[0].Data, data[i:i+97]) {
				err = errors.New("concurrent raw read changed bytes")
			}
			ids <- reply.Transaction
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(ids)
	for err := range results {
		if err != nil {
			t.Fatal("concurrent OSD request", err)
		}
	}
	seen := make(map[uint64]bool)
	for id := range ids {
		if id == 0 || seen[id] {
			t.Fatal("duplicate OSD request identity", id)
		}
		seen[id] = true
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := o.Request(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-transmission cancel", err)
	}
	t.Logf("native stat2 and %d binary bytes matched; partial/EOF/ENOENT and eight concurrent requests passed", len(data))
}

func TestCephOSDRenewalIntegration(t *testing.T) {
	c, ctx, oracle, data := osdFixture(t)
	target := cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address}
	o, err := c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 127})
	first, err := o.Request(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	before := c.Snapshot().OSDTicket
	waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.OSDTicket.Expires.After(before.Expires) && s.Monitor.Ready })
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = c.OpenOSD(ctx, target)
	if err != nil {
		t.Fatal("renewed OSD authorizer", err)
	}
	second, err := o.Request(ctx, request)
	if err != nil || second.Transaction <= first.Transaction || !bytes.Equal(second.Operations[0].Data, data[:127]) {
		t.Fatal("read using renewed ticket", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Request(ctx, request); !errors.Is(err, cephmsgr.ErrClosed) {
		t.Fatal("OSD operation after parent shutdown", err)
	}
	t.Log("OSD service ticket renewed; new session kept request identity and native read bytes; parent shutdown rejected later I/O")
}

func TestCephOSDUnsupportedFeaturesIntegration(t *testing.T) {
	c, ctx, oracle, _ := osdFixture(t)
	// Disposable configuration changes belong to the test consumer. The product
	// neither chooses a CRUSH profile nor advertises unimplemented algorithms.
	command := cephmsgr.Command{JSON: []byte(`{"prefix":"osd crush tunables","profile":"optimal"}`)}
	if _, err := c.MonCommand(ctx, command); err != nil {
		t.Fatal("prepare unsupported CRUSH gate", err)
	}
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.MonCommand(restore, cephmsgr.Command{JSON: []byte(`{"prefix":"osd crush tunables","profile":"legacy"}`)}); err != nil {
			t.Error("restore isolated legacy fixture", err)
		}
	}()
	target := cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address}
	for {
		o, err := c.OpenOSD(ctx, target)
		if errors.Is(err, cephmsgr.ErrUnsupportedFeatures) {
			t.Log("native OSD rejected unsupported CRUSH requirements before object request")
			return
		}
		if err != nil {
			t.Fatal("unexpected OSD gate error", err)
		}
		o.Close() // The daemon can receive the changed map after this setup.
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("OSD did not enforce required CRUSH feature", ctx.Err())
		}
	}
}
