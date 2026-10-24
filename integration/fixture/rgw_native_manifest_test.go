package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Fixture-only parsers for the multi-stripe RGW checkpoint. They interpret the
// pinned Tentacle `radosgw-admin object manifest` and `gc list` JSON only to
// join native observations; they are not an RGW schema for product consumers.

const (
	rgwNativeManifestLimit = 1 << 20
	rgwNativeStripeLimit   = 64
	rgwNativeGCListLimit   = 8 << 20
	rgwNativeGCEntryLimit  = 4096
)

// rgwNativePoolName accepts either a plain pool name or the pinned
// `{name, ns}` rgw_pool object, so the parser does not depend on one dump form.
type rgwNativePoolName struct {
	Name      string
	Namespace string
}

func (p *rgwNativePoolName) UnmarshalJSON(raw []byte) error {
	var name string
	if json.Unmarshal(raw, &name) == nil {
		p.Name, p.Namespace = name, ""
		return nil
	}
	var object struct {
		Name      string `json:"name"`
		Namespace string `json:"ns"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&object); err != nil {
		return errors.New("unsupported RGW pool identity encoding")
	}
	p.Name, p.Namespace = object.Name, object.Namespace
	return nil
}

type rgwNativeRawObject struct {
	Pool    rgwNativePoolName `json:"pool"`
	OID     string            `json:"oid"`
	Locator string            `json:"loc"`
}

type rgwNativeStripe struct {
	Index    int64              `json:"index"`
	PartID   uint64             `json:"part_id"`
	StripeID uint64             `json:"stripe_id"`
	Offset   uint64             `json:"offset"`
	Size     uint64             `json:"size"`
	Raw      rgwNativeRawObject `json:"raw_obj"`
}

// rgwNativeManifest keeps the byte-bearing stripes in logical order and, when
// the pinned command emitted one, the metadata-only head sentinel (index -1,
// size 0). The sentinel is retained for cleanup identity, never for bytes.
type rgwNativeManifest struct {
	Size     uint64
	Sentinel *rgwNativeRawObject
	Stripes  []rgwNativeStripe
}

// rgwNativeParseManifest validates a bounded, complete, nonoverlapping layout.
// Every raw identity must live in the data pool without namespace or locator,
// be the head, or be a `<marker>__shadow_` tail of this bucket marker.
func rgwNativeParseManifest(raw []byte, dataPool, marker, head string, wantSize uint64) (rgwNativeManifest, error) {
	var result rgwNativeManifest
	if len(raw) == 0 || len(raw) > rgwNativeManifestLimit {
		return result, errors.New("RGW manifest output outside its bound")
	}
	var document struct {
		Size    *uint64           `json:"size"`
		Objects []rgwNativeStripe `json:"objects"`
	}
	if err := json.Unmarshal(raw, &document); err != nil || document.Size == nil || len(document.Objects) == 0 || len(document.Objects) > rgwNativeStripeLimit+1 {
		return result, errors.New("invalid RGW manifest schema or stripe count")
	}
	result.Size = *document.Size
	if result.Size != wantSize {
		return result, fmt.Errorf("manifest logical size %d differs from input size %d", result.Size, wantSize)
	}
	tailPrefix := marker + "__shadow_"
	seen := map[string]bool{}
	var covered uint64
	for position, stripe := range document.Objects {
		identity := stripe.Raw
		if identity.Pool.Name != dataPool || identity.Pool.Namespace != "" || identity.Locator != "" || identity.OID == "" {
			return result, errors.New("manifest contains an unexpected pool, namespace or locator identity")
		}
		if stripe.Index == -1 {
			if position != 0 || stripe.Offset != 0 || stripe.Size != 0 || identity.OID != head || result.Sentinel != nil {
				return result, errors.New("invalid manifest head sentinel")
			}
			sentinel := identity
			result.Sentinel = &sentinel
			seen[identity.OID] = true
			continue
		}
		dataPosition := position
		if result.Sentinel != nil {
			dataPosition--
		}
		if stripe.Index != int64(dataPosition) || stripe.Offset != covered || stripe.Size == 0 {
			return result, errors.New("manifest has a gap, overlap, zero-length or out-of-order stripe")
		}
		if stripe.Offset+stripe.Size < stripe.Offset || stripe.Offset+stripe.Size > result.Size {
			return result, errors.New("manifest stripe exceeds the logical object")
		}
		first := dataPosition == 0
		switch {
		case first && result.Sentinel == nil:
			if identity.OID != head {
				return result, errors.New("first manifest stripe is not the head object")
			}
		case strings.HasPrefix(identity.OID, tailPrefix) && len(identity.OID) > len(tailPrefix):
		default:
			return result, errors.New("manifest contains an unexpected raw object identity")
		}
		if seen[identity.OID] {
			return result, errors.New("manifest repeats a raw object identity")
		}
		seen[identity.OID] = true
		covered += stripe.Size
		result.Stripes = append(result.Stripes, stripe)
	}
	if covered != result.Size || len(result.Stripes) < 2 {
		return result, fmt.Errorf("manifest is not a complete multi-object layout: covered=%d size=%d byte_objects=%d", covered, result.Size, len(result.Stripes))
	}
	return result, nil
}

// Tails returns the raw tail identities (every byte-bearing object that is not
// the head) in logical order.
func (m rgwNativeManifest) Tails(head string) []string {
	var tails []string
	for _, stripe := range m.Stripes {
		if stripe.Raw.OID != head {
			tails = append(tails, stripe.Raw.OID)
		}
	}
	return tails
}

// Objects returns every raw identity of the manifest, including a sentinel.
func (m rgwNativeManifest) Objects() []string {
	var objects []string
	if m.Sentinel != nil {
		objects = append(objects, m.Sentinel.OID)
	}
	for _, stripe := range m.Stripes {
		objects = append(objects, stripe.Raw.OID)
	}
	return objects
}

// rgwNativeReconstruct concatenates independently read raw objects in manifest
// order. Each read must have exactly the manifest stripe length.
func rgwNativeReconstruct(m rgwNativeManifest, read func(oid string) ([]byte, error)) ([]byte, error) {
	out := make([]byte, 0, m.Size)
	for _, stripe := range m.Stripes {
		part, err := read(stripe.Raw.OID)
		if err != nil {
			return nil, err
		}
		if uint64(len(part)) != stripe.Size {
			return nil, fmt.Errorf("native stripe %d has %d bytes, manifest has %d", stripe.Index, len(part), stripe.Size)
		}
		out = append(out, part...)
	}
	if uint64(len(out)) != m.Size {
		return nil, errors.New("reconstructed object size differs from the manifest")
	}
	return out, nil
}

// rgwNativeLargePayload is deterministic, incompressible and different in
// every 32-byte block, so reordered or repeated stripes cannot match.
func rgwNativeLargePayload(size int) []byte {
	out := make([]byte, 0, size+sha256.Size)
	var counter [8]byte
	for len(out) < size {
		binary.BigEndian.PutUint64(counter[:], uint64(len(out)/sha256.Size))
		block := sha256.Sum256(append([]byte("ceph-msgr-go rgw multistripe v1:"), counter[:]...))
		out = append(out, block[:]...)
	}
	return out[:size]
}

type rgwNativeGCObject struct {
	Pool     string `json:"pool"`
	OID      string `json:"oid"`
	Locator  string `json:"key"`
	Instance string `json:"instance"`
}

type rgwNativeGCEntry struct {
	Tag  string              `json:"tag"`
	Time string              `json:"time"`
	Objs []rgwNativeGCObject `json:"objs"`
}

func rgwNativeParseGCList(raw []byte) ([]rgwNativeGCEntry, error) {
	if len(raw) == 0 || len(raw) > rgwNativeGCListLimit {
		return nil, errors.New("RGW gc list output outside its bound")
	}
	var entries []rgwNativeGCEntry
	if err := json.Unmarshal(raw, &entries); err != nil || entries == nil || len(entries) > rgwNativeGCEntryLimit {
		return nil, errors.New("invalid RGW gc list schema or count")
	}
	return entries, nil
}

// rgwNativeGCOwned returns, for each requested raw object, the tags of the GC
// chains that carry it (in that pool). A chain object whose name carries this
// bucket marker but is not a manifest object is an unexpected identity.
func rgwNativeGCOwned(entries []rgwNativeGCEntry, dataPool, marker string, manifest []string) (map[string][]string, error) {
	owned := map[string][]string{}
	for _, entry := range entries {
		for _, object := range entry.Objs {
			if !strings.HasPrefix(object.OID, marker+"_") {
				continue
			}
			if !slices.Contains(manifest, object.OID) {
				return nil, errors.New("GC chain carries an unexpected object of this bucket")
			}
			if object.Pool != dataPool || object.Locator != "" || object.Instance != "" {
				return nil, errors.New("GC chain carries an unexpected pool, locator or instance identity")
			}
			owned[object.OID] = append(owned[object.OID], entry.Tag)
		}
	}
	return owned, nil
}

var rgwNativeWriteExtent = regexp.MustCompile(`(?:\[|,)(?:write|writefull) ([0-9]+)~([0-9]+)(?:[ ,\]]|$)`)

// rgwNativeWrittenBytes sums complete WRITE/WRITEFULL extents in received
// requests on one object. It reports request reception, not a result code.
func rgwNativeWrittenBytes(operations []rgwNativeOperation, pool int64, object string) (writes int, bytes uint64) {
	for _, operation := range operations {
		if operation.PoolID != pool || operation.Namespace != "" || operation.Object != object || !rgwNativeWrites(operation) {
			continue
		}
		for _, match := range rgwNativeWriteExtent.FindAllStringSubmatch(operation.Description, 8) {
			length, err := strconv.ParseUint(match[2], 10, 64)
			if err == nil {
				bytes += length
			}
		}
		writes++
	}
	return writes, bytes
}

var rgwNativeAnyCallPattern = regexp.MustCompile(`(?:\[|,)call ([a-z_]+\.[a-z_]+)(?:[ ,\]]|$)`)

// rgwNativeCallMethods lists complete class.method tokens of a request.
func rgwNativeCallMethods(operation rgwNativeOperation) []string {
	var methods []string
	for _, match := range rgwNativeAnyCallPattern.FindAllStringSubmatch(operation.Description, 8) {
		methods = append(methods, match[1])
	}
	return methods
}

// rgwNativeGCEnqueue reports whether a request carries a complete GC chain
// enqueue method token. Pinned Tentacle enqueues through the queue class
// (`rgw_gc.rgw_gc_queue_enqueue`) or, before a shard is transitioned, the
// legacy `rgw.gc_set_entry`; initialisation, lock and list calls do not count.
func rgwNativeGCEnqueue(operation rgwNativeOperation) bool {
	for _, method := range rgwNativeCallMethods(operation) {
		if method == "rgw_gc.rgw_gc_queue_enqueue" || method == "rgw.gc_set_entry" {
			return true
		}
	}
	return false
}

// rgwNativeGCObjectName selects the pinned GC shard objects (`gc.<N>`) without
// assuming the namespace RGW prints for them.
var rgwNativeGCObjectName = regexp.MustCompile(`^gc\.[0-9]+$`)

// rgwNativeMultiObjects discovers the object names that the gateway's PUT is
// expected to touch before the manifest exists: the head, every printed index
// shard of the bucket and every `<marker>__shadow_` tail. The strict join to
// the manifest happens only after the oracle-free reception snapshot.
func rgwNativeMultiObjects(output []byte, head, marker, bucketID string, dataPoolID, indexPoolID int64) map[string]int64 {
	objects := map[string]int64{head: dataPoolID}
	for _, match := range regexp.MustCompile(`:::(\.dir\.`+regexp.QuoteMeta(bucketID)+`(?:\.[0-9]+)?):head`).FindAllSubmatch(output, 4096) {
		objects[string(match[1])] = indexPoolID
	}
	for _, match := range regexp.MustCompile(`:::(`+regexp.QuoteMeta(marker)+`__shadow_[^:\s"\\]+):head`).FindAllSubmatch(output, 4096) {
		objects[string(match[1])] = dataPoolID
	}
	return objects
}

// rgwNativeMultiPutPublished is the multi-stripe analogue of
// rgwNativePutPublished: the head WRITE, at least one tail WRITE and both index
// class calls must be gateway-attributed before any backend oracle runs.
func rgwNativeMultiPutPublished(output []byte, clients []rgwNativeClientGeneration, head, marker, bucketID string, dataPoolID, indexPoolID int64) (bool, error) {
	objects := rgwNativeMultiObjects(output, head, marker, bucketID, dataPoolID, indexPoolID)
	write, tail, prepare, complete := false, false, false, false
	for _, client := range clients {
		operations, err := rgwNativeHistory(output, client.Client, objects)
		if err != nil {
			return false, err
		}
		for _, operation := range operations {
			isWrite, isPrepare, isComplete := rgwNativePutKinds(operation, head, bucketID, dataPoolID, indexPoolID)
			write, prepare, complete = write || isWrite, prepare || isPrepare, complete || isComplete
			if operation.PoolID == dataPoolID && strings.HasPrefix(operation.Object, marker+"__shadow_") && rgwNativeWrites(operation) {
				tail = true
			}
		}
	}
	return write && tail && prepare && complete, nil
}
