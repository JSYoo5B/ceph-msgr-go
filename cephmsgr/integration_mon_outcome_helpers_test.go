package cephmsgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	file, err := os.Open(filepath.Join(control, "native-mon-no-replay-"+token+"."+phase+".json"))
	if err != nil {
		return receipt, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (128<<10)+1))
	if err != nil || len(data) > 128<<10 {
		return receipt, errors.New("native MON observation exceeds its receipt bound")
	}
	if err = json.Unmarshal(data, &receipt); err != nil {
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
// not TCP packet boundaries. It records local MON and MGR command writes, never
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

type monOutcomeConn struct {
	net.Conn
	record     *monOutcomeWrites
	connection uint64
	peer       string
}

func (c *monOutcomeConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if !c.record.enabled.Load() || len(p) < 32 || p[0] != byte(msgr.Message) || msgr.CRC(0, p[:28]) != binary.LittleEndian.Uint32(p[28:32]) {
		return n, err
	}
	reader := bytes.NewReader(p)
	frame, parseErr := msgr.NewReader(reader, 128<<10).Read()
	var message msgr.MessageData
	if parseErr == nil {
		message, parseErr = msgr.DecodeMessage(frame)
	}
	c.record.mu.Lock()
	defer c.record.mu.Unlock()
	if parseErr != nil || reader.Len() != 0 || len(c.record.writes) >= 64 {
		c.record.err = errors.New("CRC command write did not decode as one bounded frame")
		return n, err
	}
	if message.Type == msgr.MonCommandMessage || message.Type == msgr.MgrCommandMessage {
		c.record.writes = append(c.record.writes, monOutcomeWrite{
			Type: message.Type, Connection: c.connection, Peer: c.peer, Transaction: message.Transaction,
			Sequence: message.Sequence, FrontSHA256: sha256.Sum256(message.Front),
			DataSHA256: sha256.Sum256(message.Data), DataBytes: len(message.Data),
			Written: n, Complete: n == len(p) && err == nil,
		})
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

func monOutcomeResultSummary(receipt nativeMonOutcomeReceipt) string {
	return fmt.Sprintf("phase=%s request=%d code=%d data_bytes=%d data_sha256=%x status=%q", receipt.Phase,
		receipt.RequestID, receipt.Result.Code, len(receipt.Result.Data), sha256.Sum256(receipt.Result.Data), receipt.Result.Status)
}
