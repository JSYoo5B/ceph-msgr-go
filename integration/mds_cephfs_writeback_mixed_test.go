package integration_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

// Test-only consumer bookkeeping of the mixed-extent writeback recipe (issue
// #10 checkpoint 2): a confirmed prefix A, an extent B with an unknown
// outcome and a later confirmed extent C beyond B. Which size a metadata flush
// may report is the consumer's decision; the product only carries the object
// requests and cap messages.

// cephfsMixedWriteback is the recipe's file plan: three file ranges
// (offset, length), each one object extent. A and C share an object at
// disjoint offsets; B is the only extent of another object, so writing C
// can neither overwrite nor re-execute B.
type cephfsMixedWriteback struct {
	A, B, C [2]uint64
}

// end is the end of C, the largest completed end offset of the recipe.
func (m cephfsMixedWriteback) end() uint64 { return m.C[0] + m.C[1] }

// cephfsMixedWritebackPlan lays A, B and C out as consecutive stripe units
// and checks the object placement the recipe relies on.
func cephfsMixedWritebackPlan(l cephfsStriping) (cephfsMixedWriteback, error) {
	su := l.StripeUnit
	m := cephfsMixedWriteback{A: [2]uint64{0, su}, B: [2]uint64{su, su}, C: [2]uint64{2 * su, su}}
	var ext [3]cephfsObjectExtent
	for i, r := range [][2]uint64{m.A, m.B, m.C} {
		e := l.fileToExtents(r[0], r[1])
		if len(e) != 1 {
			return m, fmt.Errorf("file range %v is %d object extents, not one", r, len(e))
		}
		ext[i] = e[0]
	}
	a, b, c := ext[0], ext[1], ext[2]
	switch {
	case a.ObjectNo != c.ObjectNo || a.Offset+a.Length > c.Offset:
		return m, errors.New("A and C must be disjoint ranges of one object")
	case b.ObjectNo == a.ObjectNo:
		return m, errors.New("B must be the only extent of its own object")
	}
	return m, nil
}

// completedEnd is the largest end offset any completed write reaches, which
// a metadata flush must not report when a gap before it is not completed.
func (l *cephfsWritebackLedger) completedEnd() uint64 {
	var end uint64
	for _, w := range l.Writes {
		if w.Outcome != cephfsWriteCompleted {
			continue
		}
		for _, r := range w.Spans {
			end = max(end, r[0]+r[1])
		}
	}
	return end
}

// cephfsFileWrite is one completed write of a file range.
type cephfsFileWrite struct {
	Offset uint64
	Data   []byte
}

// cephfsObjectImages is the head content every written object holds after
// the given writes, in order, from absent objects: an object holds the bytes
// up to its highest written offset, with zeros in any gap.
func cephfsObjectImages(l cephfsStriping, writes []cephfsFileWrite) map[uint64][]byte {
	images := map[uint64][]byte{}
	for _, w := range writes {
		for _, e := range l.fileToExtents(w.Offset, uint64(len(w.Data))) {
			img := images[e.ObjectNo]
			if end := e.Offset + e.Length; uint64(len(img)) < end {
				img = append(img, make([]byte, end-uint64(len(img)))...)
			}
			pos := e.Offset
			for _, b := range e.Buffers {
				copy(img[pos:pos+b.Length], w.Data[b.Offset:b.Offset+b.Length])
				pos += b.Length
			}
			images[e.ObjectNo] = img
		}
	}
	return images
}

// cephfsCheckObjectContent compares a native object-bytes view with the
// expected images: an object with an image must exist with exactly that
// content, every other planned object must be absent.
func cephfsCheckObjectContent(native []nativeMDSCephFSDataObject, ino uint64, count int, images map[uint64][]byte) error {
	if len(native) != count {
		return fmt.Errorf("native view has %d objects, want %d", len(native), count)
	}
	for objno := range uint64(count) {
		n := native[objno]
		if n.ObjNo != int(objno) || n.Name != cephfsObjectName(ino, objno) {
			return fmt.Errorf("native object %d is %s", objno, n.Name)
		}
		image, want := images[objno]
		if !want {
			if n.Exists || n.Size != 0 || n.Digest != nil {
				return fmt.Errorf("object %s exists natively (size %d) but no completed write reached it", n.Name, n.Size)
			}
			continue
		}
		d := cephfsDataDigest(image)
		if !n.Exists || n.Size != uint64(len(image)) || n.Digest == nil || n.Digest.Length != d.Length || n.Digest.SHA256 != d.SHA256 || !slices.Equal(n.Digest.Chunks, d.Chunks) {
			return fmt.Errorf("object %s natively differs from its completed writes (exists %v, size %d, want %d)", n.Name, n.Exists, n.Size, len(image))
		}
	}
	return nil
}

// cephfsCapUpdatesBeyond returns the cap messages a writer sent that report a
// file size above bound.
func cephfsCapUpdatesBeyond(events []cephfsDirtyEvent, bound uint64) []cephfsDirtyEvent {
	var beyond []cephfsDirtyEvent
	for _, ev := range events {
		if ev.Kind == "cap-update" && ev.Size > bound {
			beyond = append(beyond, ev)
		}
	}
	return beyond
}

func TestCephFSMixedWritebackPlanBoundsAndContent(t *testing.T) {
	layout := cephfsStriping{65536, 2, 131072}
	m, err := cephfsMixedWritebackPlan(layout)
	if err != nil || m.A != [2]uint64{0, 65536} || m.B != [2]uint64{65536, 65536} || m.C != [2]uint64{131072, 65536} || m.end() != 196608 {
		t.Fatal("mixed-extent plan differs", m, err)
	}
	// With one stripe per object, C would follow A in object 0 too, but B
	// would share it: refused.
	if _, err := cephfsMixedWritebackPlan(cephfsStriping{65536, 1, 4 << 20}); err == nil {
		t.Fatal("a layout that puts B beside A and C was accepted")
	}
	// Three objects per period put C into a third object: refused.
	if _, err := cephfsMixedWritebackPlan(cephfsStriping{65536, 3, 131072}); err == nil {
		t.Fatal("a layout that separates A and C was accepted")
	}

	// A completed, B unknown, C completed beyond B: the committed end stays at
	// A while a completed write reaches C's end.
	l := &cephfsWritebackLedger{Writes: []cephfsWritebackWrite{
		{Phase: "a", Spans: [][2]uint64{m.A}, Outcome: cephfsWriteCompleted},
		{Phase: "b", Spans: [][2]uint64{m.B}, Outcome: cephfsWriteUncertain},
		{Phase: "c", Spans: [][2]uint64{m.C}, Outcome: cephfsWriteCompleted},
	}}
	if l.committedEnd(0) != m.A[1] || l.completedEnd() != m.end() {
		t.Fatal("mixed ledger bounds differ", l.committedEnd(0), l.completedEnd())
	}
	for i := range l.Writes {
		if l.mayResend(i) == nil {
			t.Fatal("recovery may resend write", i)
		}
	}
	// Were B known not applied, the committed end would still stop at A.
	l.Writes[1].Outcome = cephfsWriteFenced
	if l.committedEnd(0) != m.A[1] || l.mayResend(1) != nil {
		t.Fatal("a known-unapplied B changed the bound or the resend rule")
	}
	if (&cephfsWritebackLedger{}).completedEnd() != 0 {
		t.Fatal("an empty ledger has a completed end")
	}

	a, b, c := cephfsDataStream("a", m.A[1]), cephfsDataStream("b", m.B[1]), cephfsDataStream("c", m.C[1])
	images := cephfsObjectImages(layout, []cephfsFileWrite{{m.A[0], a}, {m.C[0], c}})
	if len(images) != 1 || !slices.Equal(images[0], append(slices.Clone(a), c...)) {
		t.Fatal("expected object images differ", len(images))
	}
	// A gap inside an object is zeros.
	if gap := cephfsObjectImages(layout, []cephfsFileWrite{{m.C[0], c}}); len(gap[0]) != 131072 || !slices.Equal(gap[0][:65536], make([]byte, 65536)) {
		t.Fatal("an unwritten object range is not zeros")
	}
	const ino = 0x10000000001
	view := func(obj0 []byte, obj1 []byte) []nativeMDSCephFSDataObject {
		out := []nativeMDSCephFSDataObject{{ObjNo: 0, Name: cephfsObjectName(ino, 0)}, {ObjNo: 1, Name: cephfsObjectName(ino, 1)}}
		for i, data := range [][]byte{obj0, obj1} {
			if data != nil {
				d := cephfsDataDigest(data)
				out[i].Exists, out[i].Size, out[i].Digest = true, uint64(len(data)), &d
			}
		}
		return out
	}
	if err := cephfsCheckObjectContent(view(images[0], nil), ino, 2, images); err != nil {
		t.Fatal("the expected A-then-C content was refused", err)
	}
	for name, v := range map[string][]nativeMDSCephFSDataObject{
		"B landed":            view(images[0], b),
		"C missing":           view(a, nil),
		"C written over A":    view(append(slices.Clone(c), c...), nil),
		"object 0 absent":     view(nil, nil),
		"one object reported": view(images[0], nil)[:1],
	} {
		if cephfsCheckObjectContent(v, ino, 2, images) == nil {
			t.Fatal("native content accepted:", name)
		}
	}

	events := []cephfsDirtyEvent{{Kind: "write", Size: m.end()}, {Kind: "cap-update", Size: m.A[1]}, {Kind: "cap-update", Size: 0}}
	if len(cephfsCapUpdatesBeyond(events, m.A[1])) != 0 {
		t.Fatal("cap updates within the committed end were reported")
	}
	events = append(events, cephfsDirtyEvent{Kind: "cap-update", Size: m.end()})
	if got := cephfsCapUpdatesBeyond(events, m.A[1]); len(got) != 1 || got[0].Size != m.end() {
		t.Fatal("a cap update claiming C's end was not reported", got)
	}
}
