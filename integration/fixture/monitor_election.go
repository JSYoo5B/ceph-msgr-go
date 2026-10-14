package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var errElectionObservationPending = errors.New("native election read did not complete")

type electionMember struct {
	Name string `json:"name"`
	Rank uint32 `json:"rank"`
}
type electionExpectation struct {
	FSID    string           `json:"fsid"`
	Epoch   uint32           `json:"epoch"`
	Members []electionMember `json:"members"`
	Quorum  []string         `json:"quorum"`
	Label   string           `json:"label"`
}
type electionObservation struct {
	ObservedAt    time.Time        `json:"observed_at"`
	FSID          string           `json:"fsid"`
	MapEpoch      uint32           `json:"map_epoch"`
	ElectionEpoch uint64           `json:"election_epoch"`
	Members       []electionMember `json:"members"`
	Quorum        []string         `json:"quorum"`
}
type electionSettled struct {
	SchemaVersion int                 `json:"schema_version"`
	RequestID     string              `json:"request_id"`
	Expected      electionExpectation `json:"expected"`
	First         electionObservation `json:"first"`
	Second        electionObservation `json:"second"`
}
type electionBackend struct {
	query   func(context.Context) ([]byte, error)
	observe func(*electionObservation, error) error
	now     func() time.Time
	wait    func(context.Context) error
}

func electionName(name string) bool {
	return name != "" && len(name) <= 128 && !slices.ContainsFunc([]rune(name), func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}
func electionMembers(members []electionMember) (map[string]uint32, error) {
	if len(members) == 0 || len(members) > 8 {
		return nil, errors.New("invalid native election membership count")
	}
	names := make(map[string]uint32)
	for i, member := range members {
		if !electionName(member.Name) || member.Rank != uint32(i) {
			return nil, errors.New("invalid native election member identity")
		}
		if _, duplicate := names[member.Name]; duplicate {
			return nil, errors.New("duplicate native election member")
		}
		names[member.Name] = member.Rank
	}
	return names, nil
}
func electionExpected(expected electionExpectation) error {
	if expected.FSID == "" || len(expected.FSID) > 128 || expected.Epoch == 0 || (expected.Label != "added" && expected.Label != "removed") {
		return errors.New("invalid native election expectation")
	}
	names, err := electionMembers(expected.Members)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, name := range expected.Quorum {
		if _, present := names[name]; !present || seen[name] {
			return errors.New("invalid expected native quorum identity")
		}
		seen[name] = true
	}
	if len(seen) <= len(names)/2 {
		return errors.New("expected native quorum is not a majority")
	}
	return nil
}

// Pinned Monitor.cc:2697-2737 exports these identities in quorum_status.
// Missing fields are rejected rather than treating them as an empty quorum.
func decodeElection(data []byte, expected electionExpectation) (electionObservation, bool, error) {
	var shape struct {
		Election *uint64   `json:"election_epoch"`
		Quorum   *[]uint32 `json:"quorum"`
		Names    *[]string `json:"quorum_names"`
		Leader   *string   `json:"quorum_leader_name"`
		Map      *struct {
			FSID  string  `json:"fsid"`
			Epoch *uint32 `json:"epoch"`
			Mons  *[]struct {
				Name *string `json:"name"`
				Rank *uint32 `json:"rank"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	var got electionObservation
	if len(data) > 1<<20 || json.Unmarshal(data, &shape) != nil || shape.Election == nil || *shape.Election == 0 || shape.Quorum == nil || shape.Names == nil || shape.Leader == nil || shape.Map == nil || shape.Map.Epoch == nil || *shape.Map.Epoch == 0 || shape.Map.Mons == nil {
		return got, false, errors.New("malformed native election observation")
	}
	got.FSID, got.MapEpoch, got.ElectionEpoch = shape.Map.FSID, *shape.Map.Epoch, *shape.Election
	for _, member := range *shape.Map.Mons {
		if member.Name == nil || member.Rank == nil {
			return got, false, errors.New("incomplete native election member identity")
		}
		got.Members = append(got.Members, electionMember{*member.Name, *member.Rank})
	}
	slices.SortFunc(got.Members, func(a, b electionMember) int { return int(a.Rank) - int(b.Rank) })
	names, err := electionMembers(got.Members)
	if err != nil {
		return got, false, err
	}
	if got.FSID != expected.FSID || got.MapEpoch > expected.Epoch {
		return got, false, errors.New("native election cluster or requested map epoch differs")
	}
	if got.MapEpoch == expected.Epoch && !reflect.DeepEqual(got.Members, expected.Members) {
		return got, false, errors.New("native election membership differs at requested map epoch")
	}
	if len(*shape.Quorum) != len(*shape.Names) || len(*shape.Quorum) > len(names) {
		return got, false, errors.New("native election quorum identities differ")
	}
	seen := make(map[string]bool)
	for i, name := range *shape.Names {
		rank, present := names[name]
		if !present || seen[name] || rank != (*shape.Quorum)[i] {
			return got, false, errors.New("native election quorum rank/name differs")
		}
		seen[name] = true
		got.Quorum = append(got.Quorum, name)
	}
	if len(seen) == 0 && *shape.Leader != "" || len(seen) != 0 && !seen[*shape.Leader] {
		return got, false, errors.New("native election leader is outside quorum")
	}
	slices.Sort(got.Quorum)
	wantQuorum := slices.Clone(expected.Quorum)
	slices.Sort(wantQuorum)
	ready := got.MapEpoch == expected.Epoch && reflect.DeepEqual(got.Members, expected.Members) && slices.Equal(got.Quorum, wantQuorum)
	return got, ready, nil
}

func waitSettledElection(parent context.Context, expected electionExpectation, backend electionBackend) (electionSettled, error) {
	var receipt electionSettled
	if err := electionExpected(expected); err != nil {
		return receipt, err
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	now, wait := backend.now, backend.wait
	if now == nil {
		now = time.Now
	}
	if wait == nil {
		wait = func(ctx context.Context) error {
			timer := time.NewTimer(250 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	var first *electionObservation
	var firstAt time.Time
	for attempts := 0; attempts < 128; attempts++ {
		if err := ctx.Err(); err != nil {
			return receipt, err
		}
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		data, err := backend.query(call)
		stop()
		if ctx.Err() != nil {
			return receipt, ctx.Err()
		}
		var got electionObservation
		var ready bool
		var observedAt time.Time
		if err == nil {
			got, ready, err = decodeElection(data, expected)
			observedAt = now()
			got.ObservedAt = observedAt.UTC()
		}
		if backend.observe != nil {
			var observation *electionObservation
			if data != nil {
				observation = &got
			}
			if recordErr := backend.observe(observation, err); recordErr != nil {
				return receipt, recordErr
			}
		}
		if err != nil && !errors.Is(err, errElectionObservationPending) {
			return receipt, err
		}
		if err != nil || !ready {
			first = nil
		} else if first == nil || first.ElectionEpoch != got.ElectionEpoch {
			copy := got
			first = &copy
			firstAt = observedAt
		} else if observedAt.Sub(firstAt) >= 2*time.Second && got.ObservedAt.Sub(first.ObservedAt) >= 2*time.Second {
			if ctx.Err() != nil {
				return receipt, ctx.Err()
			}
			return electionSettled{SchemaVersion: 1, Expected: expected, First: *first, Second: got}, nil
		}
		if err := wait(ctx); err != nil {
			return receipt, err
		}
	}
	return receipt, errors.New("native election did not settle within bounded observations")
}

func appendElectionHistory(out string, data []byte) error {
	file, err := os.OpenFile(filepath.Join(out, "monitor-election-history.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || info.Size() > 512<<10 || int64(len(data)+1) > (512<<10)-info.Size() {
		return errors.Join(err, file.Close(), errors.New("native election history exceeds 512 KiB bound"))
	}
	_, writeErr := file.Write(append(data, '\n'))
	return errors.Join(writeErr, file.Close())
}

func electionQueryResult(result ceph.NativeCommandResult, err error) ([]byte, error) {
	var unstopped *ceph.NativeCommandUnstoppedError
	if errors.As(err, &unstopped) {
		return nil, err
	}
	if result.StdoutTruncated || result.StderrTruncated {
		return nil, errors.New("native election query output was truncated")
	}
	if errors.Is(err, context.DeadlineExceeded) || result.TimedOut {
		return nil, errElectionObservationPending
	}
	if err != nil {
		return nil, err
	}
	if !result.Complete() || result.ExitCode != 0 {
		return nil, fmt.Errorf("native election query failed: exit=%d complete=%t", result.ExitCode, result.Complete())
	}
	return result.Stdout, nil
}

func (c *fixtureCluster) settleMonitorElection(ctx context.Context, out, id, input string) error {
	var expected electionExpectation
	if len(input) > 16<<10 || json.Unmarshal([]byte(input), &expected) != nil || expected.FSID != c.profile.fsid {
		return errors.New("invalid fixture election IPC identity")
	}
	var owned []string
	for _, daemon := range c.cluster.Monitors() {
		owned = append(owned, daemon.DaemonName)
	}
	quorum := slices.Clone(expected.Quorum)
	slices.Sort(owned)
	slices.Sort(quorum)
	if !slices.Equal(owned, quorum) {
		return errors.New("fixture election quorum differs from owned MON identities")
	}
	backend := electionBackend{
		query: func(call context.Context) ([]byte, error) {
			result, err := c.cluster.RunNativeCommand(call, c.cluster.ControlContainer(), ceph.NativeCommand{
				Argv:    []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"},
				Timeout: 5 * time.Second, MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "native settled election read",
			})
			return electionQueryResult(result, err)
		},
		observe: func(observation *electionObservation, readErr error) error {
			metadata := struct {
				RequestID string               `json:"request_id"`
				Label     string               `json:"label"`
				Observed  *electionObservation `json:"observed,omitempty"`
				Error     string               `json:"error,omitempty"`
			}{RequestID: id, Label: expected.Label, Observed: observation}
			if readErr != nil {
				metadata.Error = readErr.Error()
			}
			data, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			return appendElectionHistory(out, data)
		},
	}
	receipt, err := waitSettledElection(ctx, expected, backend)
	if err != nil {
		return err
	}
	receipt.RequestID = id
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	// Retain each topology phase as well as the current IPC response. The
	// remove observation must not overwrite the evidence for the add barrier.
	for _, name := range []string{"monitor-election-settled-" + expected.Label + ".json", "monitor-election-settled.json"} {
		temporary := filepath.Join(out, name+".tmp")
		if err := os.WriteFile(temporary, data, 0644); err != nil {
			return err
		}
		if err := os.Rename(temporary, filepath.Join(out, name)); err != nil {
			return err
		}
	}
	return nil
}
