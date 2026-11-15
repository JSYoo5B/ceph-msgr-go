package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Shard-local PG-history capture for the disposable fixture. Test-only.
//
// The live `pg <pgid> log` command of the pinned source (Ceph v20.2.4,
// src/osd/OSD.cc:2775-2825) is answered by the PG's primary alone, so it
// cannot show what another acting shard holds. The offline reader
// (src/tools/ceph_objectstore_tool.cc:4410-4448 selects one exact spg_t
// collection, :439-480 decodes that collection's own log and missing set)
// can, but its info and log operations mount the store read-write
// (:3992-4011). It therefore never touches an owned OSD's store: each acting
// OSD is paused only while its data directory is streamed into a fresh
// reader container without a network, and the reader runs on that private
// copy. The copy lives in the reader's own filesystem and leaves with it.
//
// These are sequential captures of paused containers. They are not one
// atomic snapshot across stores and they do not drain device I/O that was
// already in flight; the placement brackets around the whole capture belong
// to the owned-pool oracle (tools/osd_pool_oracle.py mode shardhistory).

const (
	// shardHistoryMarker tells the selected test that the fixture admits the
	// capture. Without it the test skips instead of failing the controller.
	shardHistoryMarker = "CEPH_MSGR_NATIVE_SHARD_HISTORY"

	shardHistoryBeforeFile  = "osd-shard-history-before.json"
	shardHistoryCaptureFile = "osd-shard-history-capture.json"
	shardHistoryStoreRoot   = "/var/lib/ceph/osd"
	shardHistoryReaderRoot  = "/shard"
	shardHistoryReaderLabel = "ceph-msgr-go.shard-history-reader"

	// shardHistoryOutputLimit bounds each offline dump; four of them fit the
	// owned-pool oracle's receipt.
	shardHistoryOutputLimit = 64 << 10
	shardHistoryStderrLimit = 4 << 10
	// shardHistoryStoreLimit bounds the archive stream of one data directory.
	shardHistoryStoreLimit = 4 << 30
	// shardHistoryCopyBudget bounds the time one OSD stays paused. It is kept
	// below the 20 s default osd_heartbeat_grace so that a slow copy fails
	// the capture before its peers can report the paused OSD.
	shardHistoryCopyBudget = 12 * time.Second
	shardHistoryToolBudget = 60 * time.Second
)

// shardHistorySelectors are the only selections that admit the capture: the
// COPY_FROM2 control and the partial-overwrite control. Each is one exact
// test of the four-OSD topology's closed list.
var shardHistorySelectors = []string{
	"^TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration$",
}

var shardHistoryPGID = regexp.MustCompile(`^[1-9][0-9]{0,9}\.(0|[1-9a-f][0-9a-f]{0,7})$`)

func (p profile) shardHistorySelected() bool {
	return slices.Contains(shardHistorySelectors, p.testRun) && p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "4"
}

// shardHistoryShard is what the capture did for one index of the acting
// vector. The owned-pool oracle judges it; nothing here is a history claim.
type shardHistoryShard struct {
	Index int    `json:"index"`
	OSD   int    `json:"osd"`
	SPG   string `json:"spg"`
	// ReaderIsolated: the reader's only network mode is "none".
	ReaderIsolated bool `json:"reader_isolated"`
	Paused         bool `json:"paused"`
	// Resumed: the paused OSD container was resumed, also after a failed copy.
	Resumed      bool  `json:"resumed"`
	PausedMillis int64 `json:"paused_ms"`
	ArchiveBytes int64 `json:"store_archive_bytes"`
	// Whoami is the trimmed whoami file of the copy, read before any tool run.
	Whoami string `json:"whoami"`
	// Layout lists what the copy holds that the reader does not support: a
	// block, block.db or block.wal that is not a regular file. Empty is good.
	Layout     string `json:"unsupported_layout"`
	InfoCode   *int   `json:"info_exit_code"`
	InfoBytes  int    `json:"info_bytes"`
	InfoStderr string `json:"info_stderr"`
	InfoFile   string `json:"info_file"`
	LogCode    *int   `json:"log_exit_code"`
	LogBytes   int    `json:"log_bytes"`
	LogStderr  string `json:"log_stderr"`
	LogFile    string `json:"log_file"`
	// ReaderRemoved: Docker reports the reader, and the copy inside it, gone.
	ReaderRemoved bool   `json:"reader_removed"`
	Failure       string `json:"failure,omitempty"`
}

type shardHistoryReceipt struct {
	Schema       int                 `json:"schema_version"`
	Source       string              `json:"source_commit"`
	BeforeSHA256 string              `json:"before_sha256"`
	PGID         string              `json:"pgid"`
	Acting       []int               `json:"acting"`
	Shards       []shardHistoryShard `json:"shards"`
	Complete     bool                `json:"complete"`
}

// shardHistoryPlan reads the PG and the ordered acting vector from the
// placement record the owned-pool oracle wrote as the capture's first
// bracket. Every acting member must be a distinct owned OSD.
func shardHistoryPlan(before []byte, owned []int) (string, []int, error) {
	var placement struct {
		PGID   string `json:"pgid"`
		Acting []int  `json:"acting"`
	}
	if len(before) == 0 || len(before) > shardHistoryOutputLimit {
		return "", nil, errors.New("shard-history bracket is empty or exceeds its bound")
	}
	if err := json.Unmarshal(before, &placement); err != nil {
		return "", nil, fmt.Errorf("shard-history bracket: %w", err)
	}
	if !shardHistoryPGID.MatchString(placement.PGID) {
		return "", nil, fmt.Errorf("shard-history bracket names an invalid PG %q", placement.PGID)
	}
	if len(placement.Acting) < 2 {
		return "", nil, fmt.Errorf("shard-history bracket has %d acting members", len(placement.Acting))
	}
	seen := map[int]bool{}
	for _, osd := range placement.Acting {
		known := false
		for _, id := range owned {
			known = known || id == osd
		}
		if !known || seen[osd] {
			return "", nil, fmt.Errorf("acting member %d is not a distinct owned OSD", osd)
		}
		seen[osd] = true
	}
	return placement.PGID, placement.Acting, nil
}

// shardHistoryToolArgv is the one offline read of the private copy. The
// reader has no network, so the tool is told not to look for a MON.
func shardHistoryToolArgv(osd int, spg, op string) []string {
	return []string{"ceph-objectstore-tool", "--no-mon-config", "--data-path", shardHistoryReaderRoot + "/ceph-" + strconv.Itoa(osd),
		"--type", "bluestore", "--pgid", spg, "--format", "json", "--op", op}
}

// shardHistoryReader is one isolated reader container and the copy in it.
type shardHistoryReader interface {
	// run executes argv on the copy and returns its exit code with bounded
	// standard output and standard error, kept apart.
	run(ctx context.Context, argv []string, limit int) (code int, stdout, stderr []byte, err error)
	// remove deletes the reader with its copy and confirms it is gone.
	remove(ctx context.Context) error
}

// shardHistoryEngine is the Docker side of the capture.
type shardHistoryEngine interface {
	// reader starts a fresh reader and reports whether it has no network.
	reader(ctx context.Context) (shardHistoryReader, bool, error)
	pause(ctx context.Context, osd int) (resume func(context.Context) error, err error)
	// copyStore streams the OSD's data directory into the reader.
	copyStore(ctx context.Context, osd int, to shardHistoryReader) (int64, error)
}

// captureShardHistory copies and reads one acting shard. The OSD is resumed
// and the reader removed on every path; a step is never repeated.
func captureShardHistory(ctx context.Context, engine shardHistoryEngine, out, pgid string, index, osd int) (shard shardHistoryShard) {
	shard = shardHistoryShard{Index: index, OSD: osd, SPG: pgid + "s" + strconv.Itoa(index)}
	fail := func(step string, err error) {
		if shard.Failure == "" {
			shard.Failure = fmt.Sprintf("%s: %v", step, err)
		}
	}
	reader, isolated, err := engine.reader(ctx)
	if reader != nil {
		defer func() {
			// A fresh bound: the copy must go even after ctx expired.
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer stop()
			if err := reader.remove(cleanup); err != nil {
				fail("remove the reader and its copy", err)
			} else {
				shard.ReaderRemoved = true
			}
		}()
	}
	if err != nil {
		fail("start the isolated reader", err)
		return shard
	}
	shard.ReaderIsolated = isolated
	if !isolated {
		fail("start the isolated reader", errors.New("the reader has a network"))
		return shard
	}

	resume, err := engine.pause(ctx, osd)
	if err != nil {
		fail("pause the OSD", err)
		if resume == nil {
			return shard
		}
	}
	shard.Paused = true
	started := time.Now()
	if err == nil {
		copying, stop := context.WithTimeout(ctx, shardHistoryCopyBudget)
		shard.ArchiveBytes, err = engine.copyStore(copying, osd, reader)
		stop()
		if err != nil {
			fail("copy the paused OSD's data directory", err)
		}
	}
	resuming, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	err = resume(resuming)
	stop()
	shard.PausedMillis = time.Since(started).Milliseconds()
	if err != nil {
		fail("resume the OSD", err)
		return shard
	}
	shard.Resumed = true
	if shard.Failure != "" {
		return shard
	}

	store := shardHistoryReaderRoot + "/ceph-" + strconv.Itoa(osd)
	code, raw, _, err := reader.run(ctx, []string{"cat", store + "/whoami"}, 64)
	if err == nil && code != 0 {
		err = fmt.Errorf("exit %d", code)
	}
	if err != nil {
		fail("read the copy's whoami", err)
		return shard
	}
	shard.Whoami = strings.TrimSpace(string(raw))
	if shard.Whoami != strconv.Itoa(osd) {
		fail("attribute the copy", fmt.Errorf("its whoami is %q, not osd.%d", shard.Whoami, osd))
		return shard
	}
	// The reader opens one regular block file. A device or a separate DB or
	// WAL would not have been copied as a store.
	layout := `cd "$1" || exit 1; for f in block block.db block.wal; do if [ -e "$f" ] || [ -L "$f" ]; then if [ "$f" != block ] || [ -L "$f" ] || [ ! -f "$f" ]; then printf '%s ' "$f"; fi; elif [ "$f" = block ]; then printf 'no-block '; fi; done`
	code, raw, _, err = reader.run(ctx, []string{"sh", "-c", layout, "sh", store}, 256)
	if err == nil && code != 0 {
		err = fmt.Errorf("exit %d", code)
	}
	shard.Layout = strings.TrimSpace(string(raw))
	if err == nil && shard.Layout != "" {
		err = errors.New(shard.Layout)
	}
	if err != nil {
		fail("check the copy's block layout", err)
		return shard
	}

	for _, op := range []string{"info", "log"} {
		tool, stop := context.WithTimeout(ctx, shardHistoryToolBudget)
		code, stdout, stderr, err := reader.run(tool, shardHistoryToolArgv(osd, shard.SPG, op), shardHistoryOutputLimit)
		stop()
		name := fmt.Sprintf("osd-shard-history-s%d-%s.json", index, op)
		if err == nil {
			err = os.WriteFile(filepath.Join(out, name), stdout, 0644)
		}
		if err != nil {
			fail("offline "+op+" of "+shard.SPG, err)
			return shard
		}
		// Diagnostics beside a zero exit code are recorded, not judged here.
		if op == "info" {
			shard.InfoCode, shard.InfoBytes, shard.InfoStderr, shard.InfoFile = &code, len(stdout), string(stderr), name
		} else {
			shard.LogCode, shard.LogBytes, shard.LogStderr, shard.LogFile = &code, len(stdout), string(stderr), name
		}
		if code != 0 {
			fail("offline "+op+" of "+shard.SPG, fmt.Errorf("exit %d: %s", code, stderr))
			return shard
		}
	}
	return shard
}

// captureShardHistories captures every index of the acting vector in order
// and stops at the first shard that failed.
func captureShardHistories(ctx context.Context, engine shardHistoryEngine, out string, before []byte, owned []int) (shardHistoryReceipt, error) {
	digest := sha256.Sum256(before)
	receipt := shardHistoryReceipt{Schema: 1, Source: nativeEncoderCommit, BeforeSHA256: hex.EncodeToString(digest[:]), Shards: []shardHistoryShard{}}
	pgid, acting, err := shardHistoryPlan(before, owned)
	if err != nil {
		return receipt, err
	}
	receipt.PGID, receipt.Acting = pgid, acting
	for index, osd := range acting {
		shard := captureShardHistory(ctx, engine, out, pgid, index, osd)
		receipt.Shards = append(receipt.Shards, shard)
		if shard.Failure != "" || !shard.ReaderRemoved {
			return receipt, fmt.Errorf("shard %s on osd.%d: %s", shard.SPG, osd, shard.Failure)
		}
	}
	receipt.Complete = true
	return receipt, nil
}

// removeShardHistoryOutputs deletes the capture receipt and every dump of an
// earlier capture, so that a failed capture cannot leave readable evidence.
func removeShardHistoryOutputs(out string) error {
	names, err := filepath.Glob(filepath.Join(out, "osd-shard-history-s*-*.json"))
	if err != nil {
		return err
	}
	for _, name := range append(names, filepath.Join(out, shardHistoryCaptureFile)) {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlOSDShardHistoryRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "capture-osd-shard-history")
	if err != nil || fields == nil {
		return err
	}
	if len(fields) != 1 || !c.profile.shardHistorySelected() {
		return errors.New("shard-history capture requires its exclusive four-BlueStore selector and takes no argument")
	}
	if err := removeShardHistoryOutputs(out); err != nil {
		return err
	}
	before, err := os.ReadFile(filepath.Join(out, shardHistoryBeforeFile))
	if err != nil {
		return fmt.Errorf("shard-history capture needs the oracle's first bracket: %w", err)
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
		return fmt.Errorf("create shard-history Docker client: %w", err)
	}
	receipt, captureErr := captureShardHistories(ctx, engine, out, before, owned)
	closeErr := engine.docker.Close()
	raw, err := json.Marshal(receipt)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, shardHistoryCaptureFile), raw, 0644)
	}
	if err = errors.Join(captureErr, closeErr, err); err != nil {
		return fmt.Errorf("owned shard-history capture: %w", err)
	}
	return publishAck(out, "osd-shard-history."+fields[0])
}

// dockerShardHistory implements the capture with the fixture's own Docker
// client and the provider's pause lease, which the cluster also resumes
// during Terminate.
type dockerShardHistory struct {
	cluster    *fixtureCluster
	docker     *testcontainers.DockerClient
	containers map[int]testcontainers.Container
}

func (e *dockerShardHistory) reader(ctx context.Context) (shardHistoryReader, bool, error) {
	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e.cluster.profile.image, Entrypoint: []string{"/usr/bin/sleep"}, Cmd: []string{"infinity"},
			Labels:             map[string]string{shardHistoryReaderLabel: "true"},
			HostConfigModifier: func(host *container.HostConfig) { host.NetworkMode = "none" },
		}, Started: true,
	})
	if started == nil {
		return nil, false, err
	}
	reader := &dockerShardHistoryReader{docker: e.docker, container: started}
	if err != nil {
		return reader, false, err
	}
	inspected, err := e.docker.ContainerInspect(ctx, started.GetContainerID(), client.ContainerInspectOptions{})
	if err != nil {
		return reader, false, err
	}
	isolated := inspected.Container.HostConfig != nil && inspected.Container.HostConfig.NetworkMode == "none"
	if settings := inspected.Container.NetworkSettings; settings != nil {
		for name := range settings.Networks {
			isolated = isolated && name == "none"
		}
	}
	// An empty local configuration keeps the tool on its defaults without
	// the diagnostics it prints when no configuration file exists.
	prepare := `mkdir "$1" && mkdir -p /etc/ceph && : > /etc/ceph/ceph.conf`
	if code, _, _, err := reader.run(ctx, []string{"sh", "-c", prepare, "sh", shardHistoryReaderRoot}, 64); err != nil || code != 0 {
		return reader, isolated, errors.Join(err, fmt.Errorf("prepare the reader: exit %d", code))
	}
	return reader, isolated, nil
}

func (e *dockerShardHistory) pause(ctx context.Context, osd int) (func(context.Context) error, error) {
	lease, err := fault.Pause(ctx, e.cluster.cluster, e.containers[osd])
	if lease == nil {
		return nil, err
	}
	return lease.Resume, err
}

type countingReader struct {
	source io.Reader
	n      int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	r.n += int64(n)
	return n, err
}

func (e *dockerShardHistory) copyStore(ctx context.Context, osd int, to shardHistoryReader) (int64, error) {
	reader, ok := to.(*dockerShardHistoryReader)
	if !ok || e.containers[osd] == nil {
		return 0, errors.New("copy needs an owned OSD and a Docker reader")
	}
	source, err := e.docker.CopyFromContainer(ctx, e.containers[osd].GetContainerID(),
		client.CopyFromContainerOptions{SourcePath: shardHistoryStoreRoot + "/ceph-" + strconv.Itoa(osd)})
	if err != nil {
		return 0, err
	}
	defer source.Content.Close()
	if !source.Stat.Mode.IsDir() {
		return 0, errors.New("the OSD data path is not a directory")
	}
	counted := &countingReader{source: io.LimitReader(source.Content, shardHistoryStoreLimit+1)}
	_, err = e.docker.CopyToContainer(ctx, reader.container.GetContainerID(),
		client.CopyToContainerOptions{DestinationPath: shardHistoryReaderRoot, Content: counted})
	if err == nil && counted.n > shardHistoryStoreLimit {
		err = errors.New("the OSD data directory exceeds the copy bound")
	}
	return counted.n, err
}

type dockerShardHistoryReader struct {
	docker    *testcontainers.DockerClient
	container testcontainers.Container
}

func (r *dockerShardHistoryReader) exec(ctx context.Context, argv []string, limit int) (int, []byte, error) {
	code, stream, err := r.container.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil || stream == nil {
		return code, nil, errors.Join(err, errors.New("reader command produced no stream"))
	}
	data, err := io.ReadAll(io.LimitReader(stream, int64(limit)+1))
	if err == nil && len(data) > limit {
		err = fmt.Errorf("reader output exceeds %d bytes", limit)
	}
	return code, data, err
}

func (r *dockerShardHistoryReader) run(ctx context.Context, argv []string, limit int) (int, []byte, []byte, error) {
	// Standard error goes to a file of the reader so that it cannot enter
	// the JSON document on standard output.
	wrapped := append([]string{"sh", "-c", `"$@" 2>/tmp/shard-history-stderr`, "sh"}, argv...)
	code, stdout, err := r.exec(ctx, wrapped, limit)
	if err != nil {
		return code, nil, nil, err
	}
	_, stderr, err := r.exec(ctx, []string{"head", "-c", strconv.Itoa(shardHistoryStderrLimit), "/tmp/shard-history-stderr"}, shardHistoryStderrLimit)
	return code, stdout, stderr, err
}

func (r *dockerShardHistoryReader) remove(ctx context.Context) error {
	id := r.container.GetContainerID()
	if err := r.container.Terminate(ctx); err != nil {
		return err
	}
	_, err := r.docker.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return nil
	}
	return errors.Join(err, errors.New("the reader container still exists after removal"))
}
