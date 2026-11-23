package integration_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// verdictRecorder is a testing.TB whose Fatal ends only the goroutine that
// called it, so a verdict's refusal can be asserted.
type verdictRecorder struct {
	testing.TB
	message string
}

func (r *verdictRecorder) Helper() {}
func (r *verdictRecorder) Fatal(args ...any) {
	r.message = fmt.Sprint(args...)
	runtime.Goexit()
}
func (r *verdictRecorder) Fatalf(format string, args ...any) {
	r.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// verdictFails runs check and returns the message it failed with, or "".
func verdictFails(check func(testing.TB)) string {
	r := &verdictRecorder{}
	done := make(chan struct{})
	finished := false
	go func() {
		defer close(done)
		check(r)
		finished = true
	}()
	<-done
	if finished {
		return ""
	}
	return r.message
}

// rcVerdictCase is one compound, its Go reply and its native run as the
// verdict accepts them.
type rcVerdictCase struct {
	ops     []readCompoundOp
	code    int32
	failing int
	model   readCompoundModel
	reply   cephmsgr.OSDReply
	err     error
	native  nativeReadCompound
}

func (c rcVerdictCase) clone() rcVerdictCase {
	c.reply.Operations = append([]cephmsgr.OSDOperationResult(nil), c.reply.Operations...)
	c.reply.RawData = bytes.Clone(c.reply.RawData)
	ops := append([]nativeReadCompoundOutput(nil), c.native.Native.Operations...)
	c.native.Native.Operations = ops
	return c
}

func (c rcVerdictCase) check(t testing.TB) {
	checkReadCompound(t, "verdict", c.ops, c.code, c.failing, c.model, c.reply, c.err, c.native)
}

func rcPrval(v int32) *int32 { return &v }

func rcVerdictModel() readCompoundModel {
	return readCompoundModel{object: "o", data: []byte("0123456789abcdef"), attrs: map[string][]byte{"a": []byte("alpha"), "b": {0, 1}},
		omap: map[string][]byte{"k1": []byte("v1"), "k2": {0}, "k3": []byte("v3")}, version: 9, seconds: 1700000700, nanos: 5}
}

// rcVerdictGuarded is READ, STAT, GETXATTRS, a false CMPXATTR and a READ
// the OSD never ran, as the OSD and the native client answer it.
func rcVerdictGuarded() rcVerdictCase {
	m := rcVerdictModel()
	stat := binary.LittleEndian.AppendUint64(nil, 16)
	stat = binary.LittleEndian.AppendUint32(stat, 1700000700)
	stat = binary.LittleEndian.AppendUint32(stat, 5)
	xattrs := []byte{2, 0, 0, 0, 1, 0, 0, 0, 'a', 5, 0, 0, 0, 'a', 'l', 'p', 'h', 'a', 1, 0, 0, 0, 'b', 2, 0, 0, 0, 0, 1}
	c := rcVerdictCase{ops: []readCompoundOp{rcRead(0, 4), rcStat(), rcXattrs(), rcCmpXattr("a", cephmsgr.OSDXattrEqual, []byte("x")), rcRead(4, 4)},
		code: -125, failing: 3, model: m, err: &cephmsgr.OSDError{Code: -125}}
	c.reply = cephmsgr.OSDReply{Submission: cephmsgr.OSDSubmission{Transaction: 5}, Transaction: 5, Object: "o", Code: -125, Operations: []cephmsgr.OSDOperationResult{
		{Code: cephmsgr.OSDRead, Length: 4, PayloadLength: 4, Data: []byte("0123")},
		{Code: cephmsgr.OSDStat, PayloadLength: 16, Data: stat},
		{Code: cephmsgr.OSDGetXattrs, PayloadLength: uint32(len(xattrs)), Data: xattrs},
		{Code: cephmsgr.OSDCompareXattr, Result: -125},
		{Code: cephmsgr.OSDRead, Offset: 4, Length: 4},
	}}
	c.reply.RawData = append(append([]byte("0123"), stat...), xattrs...)
	c.native.Native.Aggregate = -125
	c.native.Native.Operations = []nativeReadCompoundOutput{
		{Op: "read", PrvalAvailable: true, Prval: rcPrval(0), Data: []byte("0123")},
		{Op: "stat", PrvalAvailable: true, Prval: rcPrval(0), Size: 16, Seconds: 1700000700, Nanos: 5},
		{Op: "getxattrs", PrvalAvailable: true, Prval: rcPrval(0), Attributes: []nativeXattrEntry{{Name: []byte("a"), Value: []byte("alpha")}, {Name: []byte("b"), Value: []byte{0, 1}}}},
		{Op: "cmpxattr"},
		{Op: "read", PrvalAvailable: true, Prval: rcPrval(-5), CountUnchanged: true},
	}
	return c
}

// rcVerdictOmapCall is an OMAP page, a READ and a read-mode CALL that
// succeed, then an OMAP_CMP that fails.
func rcVerdictOmapCall() rcVerdictCase {
	m := rcVerdictModel()
	page := []byte{2, 0, 0, 0, 2, 0, 0, 0, 'k', '2', 1, 0, 0, 0, 0, 2, 0, 0, 0, 'k', '3', 2, 0, 0, 0, 'v', '3', 0}
	hello := rcHello("x")
	c := rcVerdictCase{ops: []readCompoundOp{rcVals("k1", "k", 8), rcRead(8, 8), rcCall("hello", "say_hello", []byte("x"), hello), rcOmapCmp("k1", []byte("no"), cephmsgr.OSDOmapEqual)},
		code: -125, failing: 3, model: m, err: &cephmsgr.OSDError{Code: -125}}
	c.reply = cephmsgr.OSDReply{Submission: cephmsgr.OSDSubmission{Transaction: 6}, Transaction: 6, Object: "o", Code: -125, Operations: []cephmsgr.OSDOperationResult{
		{Code: cephmsgr.OSDGetOmapValues, PayloadLength: uint32(len(page)), Data: page},
		{Code: cephmsgr.OSDRead, Offset: 8, Length: 8, PayloadLength: 8, Data: []byte("89abcdef")},
		{Code: cephmsgr.OSDCall, PayloadLength: uint32(len(hello)), Data: hello},
		{Code: cephmsgr.OSDCompareOmap, Result: -125},
	}}
	c.reply.RawData = append(append(append([]byte(nil), page...), "89abcdef"...), hello...)
	more := false
	c.native.Native.Aggregate = -125
	c.native.Native.Operations = []nativeReadCompoundOutput{
		{Op: "vals", PrvalAvailable: true, Prval: rcPrval(0), Entries: []omapNativeEntry{{Key: []byte("k2"), Value: []byte{0}}, {Key: []byte("k3"), Value: []byte("v3")}}, More: &more},
		{Op: "read", PrvalAvailable: true, Prval: rcPrval(0), Data: []byte("89abcdef")},
		{Op: "exec", PrvalAvailable: true, Prval: rcPrval(0), Output: hello},
		{Op: "omapcmp", PrvalAvailable: true, Prval: rcPrval(-125)},
	}
	return c
}

// The verdict accepts the two answers and refuses each wrong aggregate,
// operation identity, result and output, in Go or natively, with a message
// that names what differs.
func TestReadCompoundVerdictRefusesEachDifference(t *testing.T) {
	for name, base := range map[string]rcVerdictCase{"guarded": rcVerdictGuarded(), "omap-call": rcVerdictOmapCall()} {
		if message := verdictFails(base.clone().check); message != "" {
			t.Fatal("the verdict refuses a correct answer", name, message)
		}
	}
	mutations := map[string]struct {
		base   func() rcVerdictCase
		edit   func(*rcVerdictCase)
		reason string
	}{
		"go-aggregate":         {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Code = -34 }, "aggregate"},
		"native-aggregate":     {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Aggregate = 0 }, "aggregate"},
		"unknown-outcome":      {rcVerdictGuarded, func(c *rcVerdictCase) { c.err = &cephmsgr.OutcomeUnknownError{Cause: errors.New("lost")} }, "known server code"},
		"no-error":             {rcVerdictGuarded, func(c *rcVerdictCase) { c.err = nil }, "known server code"},
		"other-object":         {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Object = "p" }, "identity"},
		"resent":               {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.RetryAttempt = 1 }, "attempt"},
		"other-transaction":    {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Transaction = 6 }, "identity"},
		"go-op-order":          {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[1].Code = cephmsgr.OSDRead }, "op 1 identity"},
		"native-op-order":      {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[2].Op = "stat" }, "op 2 identity"},
		"earlier-result":       {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[1].Result = -2 }, "op 1 result"},
		"earlier-native-prval": {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[0].Prval = rcPrval(-5) }, "op 0 result"},
		"failing-result":       {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[3].Result = -34 }, "op 3 result"},
		"failing-native-prval": {rcVerdictOmapCall, func(c *rcVerdictCase) { c.native.Native.Operations[3].Prval = rcPrval(0) }, "op 3 result"},
		"failing-output":       {rcVerdictOmapCall, func(c *rcVerdictCase) { c.reply.Operations[3].PayloadLength, c.reply.Operations[3].Data = 1, []byte{1} }, "failing op 3"},
		"later-result":         {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[4].Result = 4 }, "op 4 after the failure"},
		"later-output": {rcVerdictGuarded, func(c *rcVerdictCase) {
			c.reply.Operations[4].PayloadLength, c.reply.Operations[4].Data = 4, []byte("4567")
		}, "op 4 after the failure"},
		"later-native-output": {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[4].Data = []byte("4567") }, "native op 4 after the failure"},
		"read-bytes":          {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[0].Data = []byte("0124") }, "READ 0"},
		"native-read-bytes":   {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[0].Data = []byte("0124") }, "READ 0"},
		"read-length":         {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.Operations[0].Length = 3 }, "READ 0"},
		"stat-mtime":          {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[1].Nanos = 6 }, "STAT 1"},
		"stat-doubled": {rcVerdictGuarded, func(c *rcVerdictCase) {
			op := &c.reply.Operations[1]
			op.Data = append(bytes.Clone(op.Data), op.Data...)
		}, "STAT 1"},
		"xattr-value": {rcVerdictGuarded, func(c *rcVerdictCase) { c.native.Native.Operations[2].Attributes[0].Value = []byte("alphb") }, "GETXATTRS 2"},
		"omap-entry": {rcVerdictOmapCall, func(c *rcVerdictCase) {
			c.native.Native.Operations[0].Entries = c.native.Native.Operations[0].Entries[:1]
		}, "OMAP page 0"},
		"omap-more":          {rcVerdictOmapCall, func(c *rcVerdictCase) { more := true; c.native.Native.Operations[0].More = &more }, "OMAP page 0"},
		"call-output":        {rcVerdictOmapCall, func(c *rcVerdictCase) { c.reply.Operations[2].Data = append(rcHello("x"), rcHello("x")...) }, "CALL 2"},
		"native-call-output": {rcVerdictOmapCall, func(c *rcVerdictCase) { c.native.Native.Operations[2].Output = rcHello("y") }, "CALL 2"},
		"raw-partition":      {rcVerdictGuarded, func(c *rcVerdictCase) { c.reply.RawData = append(c.reply.RawData, 0) }, "partition"},
		"native-unfilled-stat": {rcVerdictGuarded, func(c *rcVerdictCase) {
			c.native.Native.Operations[1] = nativeReadCompoundOutput{Op: "stat", PrvalAvailable: true, Prval: rcPrval(0), Unchanged: true}
		}, "STAT 1"},
	}
	for name, m := range mutations {
		c := m.base().clone()
		m.edit(&c)
		message := verdictFails(c.check)
		if message == "" {
			t.Errorf("%s: the verdict accepted a wrong answer", name)
		} else if !bytes.Contains([]byte(message), []byte(m.reason)) {
			t.Errorf("%s: the verdict failed for another reason: %s", name, message)
		}
	}
}
