package cephmsgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeMonOutcomeResult struct {
	Code   int32  `json:"code"`
	Data   []byte `json:"data"`
	Status string `json:"status"`
}

type nativeMonOutcomeReceipt struct {
	RequestID int64                   `json:"request_id"`
	Token     string                  `json:"token"`
	Phase     string                  `json:"phase"`
	Result    nativeMonOutcomeResult  `json:"result"`
	Removal   *nativeMonOutcomeResult `json:"removal,omitempty"`
}

func observeNativeMonOutcome(ctx context.Context, control, token, phase string) (nativeMonOutcomeReceipt, error) {
	var receipt nativeMonOutcomeReceipt
	if err := testcluster.RequestControl(ctx, control, "observe-mon-command", "mon-command-observed", token, phase); err != nil {
		return receipt, err
	}
	if err := readNativeReceiptFile(filepath.Join(control, "native-mon-no-replay-"+token+"."+phase+".json"), 128<<10, errors.New("native MON observation exceeds its receipt bound"), &receipt); err != nil {
		return receipt, err
	}
	if receipt.RequestID <= 0 || receipt.Token != token || receipt.Phase != phase || len(receipt.Result.Data) > 64<<10 || len(receipt.Result.Status) > 8192 {
		return receipt, errors.New("native MON observation has a mismatched identity or result bound")
	}
	return receipt, nil
}

func requireNativeMonValue(t *testing.T, ctx context.Context, control, token, phase string, expected []byte, finished <-chan error) nativeMonOutcomeReceipt {
	t.Helper()
	for {
		receipt, err := observeNativeMonOutcome(ctx, control, token, phase)
		if err != nil {
			t.Fatal("native MON observation", phase, err)
		}
		if receipt.Result.Code == 0 && bytes.Equal(receipt.Result.Data, expected) {
			return receipt
		}
		if receipt.Result.Code != -2 {
			t.Fatalf("native MON %s: unexpected signed code=%d, data bytes=%d, status=%q", phase, receipt.Result.Code, len(receipt.Result.Data), receipt.Result.Status)
		}
		select {
		case err := <-finished:
			t.Fatal("original caller ended before native execution was observed", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// This probe decodes complete uncompressed CRC frames supplied to net.Conn.Write,
// not TCP packet boundaries. It records local MON, MGR and Tell (type 97) command writes, never
// server executions. Secure frames cannot yield this receipt and retain the
// separate size probe.
type monOutcomeWrite struct {
	Type        uint16
	Connection  uint64
	Peer        string
	Transaction uint64
	Sequence    uint64
	FrontSHA256 [32]byte
	DataSHA256  [32]byte
	DataBytes   int
	Written     int
	Complete    bool
}

type monOutcomeWrites struct {
	enabled atomic.Bool
	mu      sync.Mutex
	writes  []monOutcomeWrite
	err     error
	// published is closed and replaced whenever a record or an error is
	// published, so that a waiter observes it without polling.
	published chan struct{}
}

// publish wakes every waiter. The caller holds mu.
func (p *monOutcomeWrites) publish() {
	if p.published != nil {
		close(p.published)
		p.published = nil
	}
}

// await returns once a write with this content has returned locally and its
// record is published. A record is published only after the underlying Write
// returned, on the writer's goroutine; a reply to that write can be read
// before then, so a reply barrier alone does not order the record.
func (p *monOutcomeWrites) await(ctx context.Context, front, data []byte) error {
	return p.awaitCount(ctx, front, data, 1)
}

// awaitCount is await for the count-th write with this content. A later write
// of content that was already written once (the same read on another
// connection) is not distinguished by content, so its waiter names how many
// records it expects; it never returns on the earlier ones alone.
func (p *monOutcomeWrites) awaitCount(ctx context.Context, front, data []byte, count int) error {
	want := [2][32]byte{sha256.Sum256(front), sha256.Sum256(data)}
	for {
		p.mu.Lock()
		err := p.err
		found := 0
		for _, observed := range p.writes {
			if observed.FrontSHA256 == want[0] && observed.DataSHA256 == want[1] {
				found++
			}
		}
		if err == nil && found >= count {
			p.mu.Unlock()
			return nil
		}
		if p.published == nil {
			p.published = make(chan struct{})
		}
		published := p.published
		p.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-published:
		case <-ctx.Done():
			return fmt.Errorf("no completed local write of the command was published: %w", ctx.Err())
		}
	}
}

type monOutcomeIdentity struct {
	OriginalGlobalID uint64           `json:"original_global_id"`
	Submitted        monOutcomeWrite  `json:"submitted"`
	OutcomeUnknown   bool             `json:"outcome_unknown"`
	RecoveryGlobalID uint64           `json:"recovery_global_id"`
	Recovery         *monOutcomeWrite `json:"recovery,omitempty"`
}

func writeMonOutcomeIdentity(t *testing.T, control string, value monOutcomeIdentity) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret metadata must also be readable by the non-root controller
	// when the Go suite runs as root inside the private fixture directory.
	if err := os.WriteFile(filepath.Join(control, "mon-command-outcome-identity.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

// monOutcomeGate withholds replies on exactly one connection. It arms only for
// an outgoing frame written to the named fixture role: a complete CRC frame of
// the named message type, or, when frames cannot be decoded (secure modes), a
// write of at least fallbackBytes. It never inspects traffic to any other role.
type monOutcomeGate struct {
	Peer          string
	Type          uint16
	FallbackBytes int
	Arm           func()
}

type monOutcomeConn struct {
	net.Conn
	record     *monOutcomeWrites
	connection uint64
	peer       string
	gate       *monOutcomeGate
}

func (c *monOutcomeConn) Write(p []byte) (int, error) {
	framed := len(p) >= 32 && p[0] == byte(msgr.Message) && msgr.CRC(0, p[:28]) == binary.LittleEndian.Uint32(p[28:32])
	var message msgr.MessageData
	var parseErr error
	var remaining int
	if framed {
		reader := bytes.NewReader(p)
		var frame msgr.Frame
		if frame, parseErr = msgr.NewReader(reader, 128<<10).Read(); parseErr == nil {
			message, parseErr = msgr.DecodeMessage(frame)
		}
		remaining = reader.Len()
	}
	if g := c.gate; g != nil && c.peer == g.Peer && g.Arm != nil {
		// Arm before the write so a reply cannot be delivered between the
		// local write and the gate.
		if (framed && parseErr == nil && remaining == 0 && message.Type == g.Type) || (!framed && g.FallbackBytes > 0 && len(p) >= g.FallbackBytes) {
			g.Arm()
		}
	}
	n, err := c.Conn.Write(p)
	if !c.record.enabled.Load() || !framed {
		return n, err
	}
	c.record.mu.Lock()
	defer c.record.mu.Unlock()
	if parseErr != nil || remaining != 0 || len(c.record.writes) >= 64 {
		c.record.err = errors.New("CRC command write did not decode as one bounded frame")
		c.record.publish()
		return n, err
	}
	if message.Type == msgr.MonCommandMessage || message.Type == msgr.MgrCommandMessage || message.Type == msgr.TellCommandMessage {
		c.record.writes = append(c.record.writes, monOutcomeWrite{
			Type: message.Type, Connection: c.connection, Peer: c.peer, Transaction: message.Transaction,
			Sequence: message.Sequence, FrontSHA256: sha256.Sum256(message.Front),
			DataSHA256: sha256.Sum256(message.Data), DataBytes: len(message.Data),
			Written: n, Complete: n == len(p) && err == nil,
		})
		c.record.publish()
	}
	return n, err
}

func (p *monOutcomeWrites) matching(front, data []byte) ([]monOutcomeWrite, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var matches []monOutcomeWrite
	for _, observed := range p.writes {
		if observed.FrontSHA256 == sha256.Sum256(front) && observed.DataSHA256 == sha256.Sum256(data) {
			matches = append(matches, observed)
		}
	}
	return matches, p.err
}

func requireMonOutcomeWrite(t *testing.T, probe *monOutcomeWrites, front, data []byte) monOutcomeWrite {
	t.Helper()
	writes, err := probe.matching(front, data)
	if err != nil || len(writes) != 1 || !writes[0].Complete || writes[0].Transaction == 0 || writes[0].Sequence == 0 {
		t.Fatal("expected exactly one complete identified CRC command write", writes, err)
	}
	return writes[0]
}

// awaitMonOutcomeWrite is requireMonOutcomeWrite for a write whose completion
// is not ordered before the caller: it first waits, within ctx, for the
// writer's goroutine to publish the record.
func awaitMonOutcomeWrite(t *testing.T, ctx context.Context, probe *monOutcomeWrites, front, data []byte) monOutcomeWrite {
	t.Helper()
	if err := probe.await(ctx, front, data); err != nil {
		t.Fatal("expected one complete identified CRC command write:", err)
	}
	return requireMonOutcomeWrite(t, probe, front, data)
}

// awaitMonOutcomeWrites waits, within ctx, for the count-th write with this
// content to be published and returns every record with it. The caller keeps
// its own exact-count and per-connection requirements.
func awaitMonOutcomeWrites(t *testing.T, ctx context.Context, probe *monOutcomeWrites, front, data []byte, count int) []monOutcomeWrite {
	t.Helper()
	if err := probe.awaitCount(ctx, front, data, count); err != nil {
		t.Fatal("expected", count, "published CRC command writes:", err)
	}
	writes, err := probe.matching(front, data)
	if err != nil {
		t.Fatal(err)
	}
	return writes
}

// awaitPaddedWrites waits, within ctx, until the padded-write counter shows at
// least want writes. commandWriteProbe counts a write after it returned on the
// writer's goroutine, which no reply or native observation orders before the
// caller.
func awaitPaddedWrites(t *testing.T, ctx context.Context, writes *atomic.Int32, want int32) int32 {
	t.Helper()
	for writes.Load() < want {
		select {
		case <-ctx.Done():
			t.Fatal("the padded local write was never counted", writes.Load(), want, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	return writes.Load()
}

func monOutcomeResultSummary(receipt nativeMonOutcomeReceipt) string {
	return fmt.Sprintf("phase=%s request=%d code=%d data_bytes=%d data_sha256=%x status=%q", receipt.Phase,
		receipt.RequestID, receipt.Result.Code, len(receipt.Result.Data), sha256.Sum256(receipt.Result.Data), receipt.Result.Status)
}
