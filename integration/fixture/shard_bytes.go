package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// Shard-local object bytes for the disposable fixture. Test-only.
//
// The capture reuses the shard-history store copy (shard_history.go): each
// member OSD is paused only while its data directory is streamed into a
// fresh reader container without a network, and the reader runs on that
// private copy. Two offline operations of the pinned tool then read one
// object of the copy (Ceph v20.2.4, src/tools/ceph_objectstore_tool.cc):
//
//   - `--op list <name>` with `--pgid <pgid>s<i>` lists every object of that
//     shard's PG whose name matches (:2270-2284, lookup_ghobject :268-299),
//     one [pgid, collection, ghobject] array per object (:231-266). The
//     ghobject dump carries the shard ID (src/common/hobject.cc:472-495).
//   - `<[spg, ghobject]> get-bytes <file>` takes that identity back
//     (:4133-4168), stats the object in that one collection and reads all of
//     its bytes through ObjectStore (:2472-2504).
//
// Both are among the operations the tool mounts read-only (:3992-4011); they
// still run on the private copy only. Which listed entry is the object's
// head is selected here only to name it back to get-bytes; the owned-pool
// oracle (tools/osd_pool_mode_shardbytes_oracle.py) parses the listing
// again and judges the selection and the bytes. Nothing here says which
// chunks a primary used for a logical read.

const (
	shardBytesBeforeFile  = "osd-shard-bytes-before.json"
	shardBytesCaptureFile = "osd-shard-bytes-capture.json"
	// shardBytesListLimit bounds one listing; shardBytesLimit one shard's
	// bytes, which the oracle receipt carries.
	shardBytesListLimit = 16 << 10
	shardBytesLimit     = 64 << 10
	// shardBytesPath is where get-bytes writes in the reader. The tool
	// creates it exclusively (O_EXCL), so a stale file fails the read.
	shardBytesPath = "/tmp/shard-bytes"
	// crushItemNone is an empty position of the up vector.
	crushItemNone = 0x7fffffff
)

// shardBytesShard is what the capture did for one position of the up vector.
// The owned-pool oracle judges it.
type shardBytesShard struct {
	Index int    `json:"index"`
	OSD   int    `json:"osd"`
	SPG   string `json:"spg"`
	// Absent: the position is empty in the bracket's up vector and nothing
	// was copied.
	Absent bool `json:"absent"`
	shardStoreCopy
	ListCode   *int   `json:"list_exit_code"`
	ListBytes  int    `json:"list_bytes"`
	ListStderr string `json:"list_stderr"`
	ListFile   string `json:"list_file"`
	// Selected is the listed [spg, ghobject] identity given to get-bytes.
	Selected    string `json:"selected"`
	BytesCode   *int   `json:"bytes_exit_code"`
	BytesStderr string `json:"bytes_stderr"`
	BytesLength int    `json:"bytes_length"`
	BytesFile   string `json:"bytes_file"`
	Failure     string `json:"failure,omitempty"`
}

type shardBytesReceipt struct {
	Schema       int               `json:"schema_version"`
	Source       string            `json:"source_commit"`
	BeforeSHA256 string            `json:"before_sha256"`
	PGID         string            `json:"pgid"`
	Pool         int64             `json:"pool_id"`
	Object       string            `json:"object"`
	Up           []int             `json:"up"`
	Shards       []shardBytesShard `json:"shards"`
	Complete     bool              `json:"complete"`
}

// shardBytesTarget is what the capture takes from the stored bracket.
type shardBytesTarget struct {
	PGID      string `json:"pgid"`
	Pool      int64  `json:"pool_id"`
	Object    string `json:"object"`
	Namespace string `json:"namespace"`
	Up        []int  `json:"up"`
}

// shardBytesPlan reads the PG, the object and the up vector from the
// placement record the owned-pool oracle wrote as the capture's first
// bracket. The up vector names every shard the PG's OSDs hold, an async
// recovery target included; every member must be a distinct owned OSD and
// an empty position is recorded as absent. The object must be in the
// default namespace.
func shardBytesPlan(before []byte, owned []int) (shardBytesTarget, error) {
	var target shardBytesTarget
	if len(before) == 0 || len(before) > shardHistoryOutputLimit {
		return target, errors.New("shard-bytes bracket is empty or exceeds its bound")
	}
	if err := json.Unmarshal(before, &target); err != nil {
		return target, fmt.Errorf("shard-bytes bracket: %w", err)
	}
	if !shardHistoryPGID.MatchString(target.PGID) || !strings.HasPrefix(target.PGID, strconv.FormatInt(target.Pool, 10)+".") {
		return target, fmt.Errorf("shard-bytes bracket names PG %q of pool %d", target.PGID, target.Pool)
	}
	if target.Object == "" || len(target.Object) > 128 || strings.ContainsAny(target.Object, "\x00/\n") || target.Namespace != "" {
		return target, fmt.Errorf("shard-bytes bracket names object %q in namespace %q", target.Object, target.Namespace)
	}
	if len(target.Up) < 2 || len(target.Up) > 8 {
		return target, fmt.Errorf("shard-bytes bracket has %d up positions", len(target.Up))
	}
	seen, present := map[int]bool{}, 0
	for _, osd := range target.Up {
		if osd == crushItemNone {
			continue
		}
		known := false
		for _, id := range owned {
			known = known || id == osd
		}
		if !known || seen[osd] {
			return target, fmt.Errorf("up member %d is not a distinct owned OSD", osd)
		}
		seen[osd] = true
		present++
	}
	if present < 2 {
		return target, fmt.Errorf("shard-bytes bracket has %d present up members", present)
	}
	return target, nil
}

func shardBytesToolArgv(osd int, spg string, operation ...string) []string {
	return append([]string{"ceph-objectstore-tool", "--no-mon-config", "--data-path", shardHistoryReaderRoot + "/ceph-" + strconv.Itoa(osd),
		"--type", "bluestore", "--pgid", spg, "--format", "json"}, operation...)
}

// selectShardHead names the object's head among the listed entries of one
// shard's collection: the one entry of that spg and its head collection
// whose ghobject is the named object of the pool in the default namespace,
// at CEPH_NOSNAP, without a generation, on that shard. It returns the
// [spg, ghobject] identity get-bytes takes, with the ghobject exactly as
// listed.
func selectShardHead(listing []byte, spg string, pool int64, object string, shard int) (string, error) {
	var entries [][]json.RawMessage
	if err := json.Unmarshal(listing, &entries); err != nil {
		return "", fmt.Errorf("the listing is not an array of entries: %w", err)
	}
	var found []string
	for _, entry := range entries {
		if len(entry) != 3 {
			return "", fmt.Errorf("a listed entry has %d elements", len(entry))
		}
		var listedSPG, collection string
		if json.Unmarshal(entry[0], &listedSPG) != nil || json.Unmarshal(entry[1], &collection) != nil {
			return "", errors.New("a listed entry has no PG and collection")
		}
		var g struct {
			OID        *string `json:"oid"`
			Key        *string `json:"key"`
			Snap       *int64  `json:"snapid"`
			Pool       *int64  `json:"pool"`
			Namespace  *string `json:"namespace"`
			Generation *int64  `json:"generation"`
			Shard      *int    `json:"shard_id"`
		}
		if err := json.Unmarshal(entry[2], &g); err != nil || g.OID == nil || g.Key == nil || g.Snap == nil || g.Pool == nil || g.Namespace == nil || g.Shard == nil {
			return "", errors.New("a listed ghobject lacks its identity fields")
		}
		if listedSPG != spg || collection != spg+"_head" || *g.OID != object || *g.Key != "" || *g.Snap != -2 || *g.Pool != pool ||
			*g.Namespace != "" || g.Generation != nil || *g.Shard != shard {
			continue
		}
		var identity bytes.Buffer
		identity.WriteString(`["` + spg + `",`)
		identity.Write(entry[2])
		identity.WriteString("]")
		found = append(found, identity.String())
	}
	if len(found) != 1 {
		return "", fmt.Errorf("%d listed heads of %s on shard %d in %s", len(found), object, shard, spg)
	}
	return found[0], nil
}

// captureShardBytes copies one member's store and reads the object's bytes
// in its shard's collection.
func captureShardBytes(ctx context.Context, engine shardHistoryEngine, out string, target shardBytesTarget, index, osd int) (shard shardBytesShard) {
	shard = shardBytesShard{Index: index, OSD: osd, SPG: target.PGID + "s" + strconv.Itoa(index)}
	fail := func(step string, err error) {
		if shard.Failure == "" {
			shard.Failure = fmt.Sprintf("%s: %v", step, err)
		}
	}
	withShardStoreCopy(ctx, engine, osd, &shard.shardStoreCopy, fail, func(reader shardHistoryReader) {
		tool, stop := context.WithTimeout(ctx, shardHistoryToolBudget)
		code, listing, stderr, err := reader.run(tool, shardBytesToolArgv(osd, shard.SPG, "--op", "list", target.Object), shardBytesListLimit)
		stop()
		name := fmt.Sprintf("osd-shard-bytes-s%d-list.json", index)
		if err == nil {
			err = os.WriteFile(filepath.Join(out, name), listing, 0644)
		}
		if err != nil {
			fail("offline list of "+shard.SPG, err)
			return
		}
		shard.ListCode, shard.ListBytes, shard.ListStderr, shard.ListFile = &code, len(listing), string(stderr), name
		if code != 0 {
			fail("offline list of "+shard.SPG, fmt.Errorf("exit %d: %s", code, stderr))
			return
		}
		if shard.Selected, err = selectShardHead(listing, shard.SPG, target.Pool, target.Object, index); err != nil {
			fail("select the object's head", err)
			return
		}
		tool, stop = context.WithTimeout(ctx, shardHistoryToolBudget)
		code, _, stderr, err = reader.run(tool, shardBytesToolArgv(osd, shard.SPG, shard.Selected, "get-bytes", shardBytesPath), 4<<10)
		stop()
		if err != nil {
			fail("offline get-bytes of "+shard.SPG, err)
			return
		}
		shard.BytesCode, shard.BytesStderr = &code, string(stderr)
		if code != 0 {
			fail("offline get-bytes of "+shard.SPG, fmt.Errorf("exit %d: %s", code, stderr))
			return
		}
		code, data, err := shardBytesRead(ctx, reader)
		if err == nil && code != 0 {
			err = fmt.Errorf("exit %d", code)
		}
		name = fmt.Sprintf("osd-shard-bytes-s%d-data.bin", index)
		if err == nil {
			err = os.WriteFile(filepath.Join(out, name), data, 0644)
		}
		if err != nil {
			fail("read the bytes of "+shard.SPG, err)
			return
		}
		shard.BytesLength, shard.BytesFile = len(data), name
	})
	return shard
}

// shardBytesRead returns the file get-bytes wrote, within its bound.
func shardBytesRead(ctx context.Context, reader shardHistoryReader) (int, []byte, error) {
	code, data, _, err := reader.run(ctx, []string{"cat", shardBytesPath}, shardBytesLimit)
	return code, data, err
}

// captureShardBytesAll captures every present position of the up vector in
// order and stops at the first shard that failed.
func captureShardBytesAll(ctx context.Context, engine shardHistoryEngine, out string, before []byte, owned []int) (shardBytesReceipt, error) {
	digest := sha256.Sum256(before)
	receipt := shardBytesReceipt{Schema: 1, Source: nativeEncoderCommit, BeforeSHA256: hex.EncodeToString(digest[:]), Shards: []shardBytesShard{}}
	target, err := shardBytesPlan(before, owned)
	if err != nil {
		return receipt, err
	}
	receipt.PGID, receipt.Pool, receipt.Object, receipt.Up = target.PGID, target.Pool, target.Object, target.Up
	for index, osd := range target.Up {
		if osd == crushItemNone {
			receipt.Shards = append(receipt.Shards, shardBytesShard{Index: index, OSD: osd, SPG: target.PGID + "s" + strconv.Itoa(index), Absent: true})
			continue
		}
		shard := captureShardBytes(ctx, engine, out, target, index, osd)
		receipt.Shards = append(receipt.Shards, shard)
		if shard.Failure != "" || !shard.ReaderRemoved {
			return receipt, fmt.Errorf("shard %s on osd.%d: %s", shard.SPG, osd, shard.Failure)
		}
	}
	receipt.Complete = true
	return receipt, nil
}

// removeShardBytesOutputs deletes the capture receipt and every file of an
// earlier capture, so that a failed capture cannot leave readable evidence.
func removeShardBytesOutputs(out string) error {
	names, err := filepath.Glob(filepath.Join(out, "osd-shard-bytes-s*-*"))
	if err != nil {
		return err
	}
	for _, name := range append(names, filepath.Join(out, shardBytesCaptureFile)) {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlOSDShardBytesRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "capture-osd-shard-bytes")
	if err != nil || fields == nil {
		return err
	}
	if len(fields) != 1 || !c.profile.shardHistorySelected() {
		return errors.New("shard-bytes capture requires its exclusive erasure-capacity BlueStore selector and takes no argument")
	}
	if err := removeShardBytesOutputs(out); err != nil {
		return err
	}
	before, err := os.ReadFile(filepath.Join(out, shardBytesBeforeFile))
	if err != nil {
		return fmt.Errorf("shard-bytes capture needs the oracle's first bracket: %w", err)
	}
	osds, err := c.cluster.OSDs(ctx)
	if err != nil {
		return err
	}
	engine := &dockerShardHistory{cluster: c, containers: map[int]testcontainers.Container{}}
	var owned []int
	for _, osd := range osds {
		owned = append(owned, osd.ID)
		engine.containers[osd.ID] = osd.Container
	}
	if engine.docker, err = testcontainers.NewDockerClientWithOpts(ctx); err != nil {
		return fmt.Errorf("create shard-bytes Docker client: %w", err)
	}
	receipt, captureErr := captureShardBytesAll(ctx, engine, out, before, owned)
	closeErr := engine.docker.Close()
	raw, err := json.Marshal(receipt)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, shardBytesCaptureFile), raw, 0644)
	}
	if err = errors.Join(captureErr, closeErr, err); err != nil {
		return fmt.Errorf("owned shard-bytes capture: %w", err)
	}
	return publishAck(out, "osd-shard-bytes."+fields[0])
}

var _ = defineControl("osd-shard-bytes", controlAfterRanked, (*fixtureCluster).controlOSDShardBytesRequest)
