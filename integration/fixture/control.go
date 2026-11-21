package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

func publishAck(out, name string) error {
	f, err := os.CreateTemp(out, "ack-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(out, name))
}

func (c *fixtureCluster) controlLoop(ctx context.Context, out string) {
	defer close(c.done)
	defer c.waitOwnedPool()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.controls(ctx, out); err != nil {
				if ctx.Err() == nil {
					err = c.classifyControlFailure(out, err)
				}
				c.mu.Lock()
				c.controlErr = err
				c.mu.Unlock()
				_ = os.WriteFile(filepath.Join(out, "provider.error"), []byte(err.Error()+"\n"), 0600)
				return
			}
		}
	}
}

// request claims the pending request named name and returns the fields of
// its first line: "<request ID> <arguments>". A request that carries input
// files has one more line "input <file name>" per input, each name once
// (internal/testcluster RequestControlInput): the test wrote each input as
// "<file name>.<request ID>", and the claim renames it to the file name the
// handler and its oracle read. The test never writes the plain file name,
// so a request the test abandoned and that is claimed later reads its own
// input, not the input of the request published after it; the fixture runs
// the requests of one control one at a time, so no later claim replaces the
// file while a handler reads it. A missing input fails the fixture, since
// the test writes it before it publishes the request.
func (c *fixtureCluster) request(out, name string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(out, name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	fields := strings.Fields(lines[0])
	if len(fields) == 0 {
		return nil, errors.New("empty fixture request")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 {
		return nil, errors.New("invalid fixture request identity")
	}
	var inputs []string
	for _, line := range lines[1:] {
		words := strings.Fields(line)
		if len(words) != 2 || words[0] != "input" || !controlInputName.MatchString(words[1]) || slices.Contains(inputs, words[1]) {
			return nil, errors.New("invalid fixture request input line")
		}
		inputs = append(inputs, words[1])
	}
	if err = os.Remove(filepath.Join(out, name)); err != nil {
		return nil, err
	}
	for _, input := range inputs {
		if err = os.Rename(filepath.Join(out, input+"."+fields[0]), filepath.Join(out, input)); err != nil {
			return nil, fmt.Errorf("claim the input of fixture request %s: %w", name, err)
		}
	}
	return fields, nil
}

// controlInputName is the closed shape of a request's input file name
// (internal/testcluster controlInputName).
var controlInputName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.json$`)

// controls polls every declared control once, in controlSequence order. It
// stops on the first failure; earlier mutations and acknowledgements are not
// replayed.
//
// While an owned-pool oracle request runs (controlOwnedPoolRequest), it
// polls nothing else but the requests that end a pause (pauseEndingActions):
// the other requests stay pending until the oracle request was settled, and
// a pass that starts one skips the controls after it.
func (c *fixtureCluster) controls(ctx context.Context, out string) error {
	if c.ownedPoolRunning() {
		return c.daemonRequests(ctx, out, pauseEndingActions)
	}
	for _, control := range controlSequence {
		if err := control.poll(c, ctx, out); err != nil {
			return err
		}
		if c.ownedPoolRunning() {
			return c.daemonRequests(ctx, out, pauseEndingActions)
		}
	}
	return nil
}

// controlPoll consumes the requests of one control from the output directory
// and acknowledges them there. It returns nil when none is pending.
type controlPoll func(c *fixtureCluster, ctx context.Context, out string) error

// controlOrder places a control in the polling sequence.
//
// Order among independent controls carries no meaning: each control consumes
// only its own request files and publishes only its own acknowledgements, and
// a test publishes a request and waits for its acknowledgement before it
// relies on the next. The order matters only when one poll finds several
// requests pending at once, and then only for which of them runs before the
// first failure stops the loop. The controls that existed when the
// hand-written sequence became declarations keep that sequence through a
// frozen rank (recordedControlOrder in control_order_test.go pins it), so no
// existing behaviour moved. A control added later has no earlier position any
// test could rely on: it declares controlAfterRanked and runs after every
// ranked control, ordered by its unique name, so adding one edits no list.
type controlOrder struct{ rank int }

// controlAfterRanked is the order of every control added after the frozen
// ranks were recorded.
var controlAfterRanked = controlOrder{}

// frozenControlRank is the recorded position of a control that existed when
// the sequence became declarations. A new control never takes a rank:
// TestDeclaredControlsKeepTheRecordedOrder refuses a ranked control that
// recordedControlOrder does not list.
func frozenControlRank(rank int) controlOrder {
	if rank <= 0 {
		panic("fixture control rank " + strconv.Itoa(rank) + " is not positive")
	}
	return controlOrder{rank: rank}
}

type fixtureControl struct {
	name  string
	order controlOrder
	poll  controlPoll
	// source is the function the declaration names, so one function is
	// declared as at most one control.
	source string
}

var controlName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// declaredControls collects the declarations; controlSequence orders them
// once every package-level declaration has run.
var declaredControls []*fixtureControl

// defineControl declares a control polled by the fixture's control loop. Call
// it in a package-level declaration beside the control, in the control's own
// file:
//
//	var _ = defineControl("osd-example", controlAfterRanked, (*fixtureCluster).controlOSDExampleRequest)
//
// The name is unique and lower-case words joined by hyphens. A duplicate
// name, a nil poll or a function declared twice stops the fixture at start.
func defineControl(name string, order controlOrder, poll controlPoll) bool {
	if poll == nil {
		panic("fixture control " + name + " has no poll function")
	}
	declaredControls = append(declaredControls, &fixtureControl{name: name, order: order, poll: poll, source: functionName(poll)})
	return true
}

// defineRequestControl declares a control of one request file named name:
// the poll consumes the request, runs handle with its fields and acknowledges
// it as "<name>-completed.<request ID>". It is declared like defineControl.
func defineRequestControl(name string, order controlOrder, handle func(c *fixtureCluster, ctx context.Context, out string, fields []string) error) bool {
	if handle == nil {
		panic("fixture control " + name + " has no request handler")
	}
	poll := func(c *fixtureCluster, ctx context.Context, out string) error {
		fields, err := c.request(out, name)
		if err != nil || fields == nil {
			return err
		}
		if err = handle(c, ctx, out, fields); err != nil {
			return err
		}
		return publishAck(out, name+"-completed."+fields[0])
	}
	declaredControls = append(declaredControls, &fixtureControl{name: name, order: order, poll: poll, source: functionName(handle)})
	return true
}

func functionName(fn any) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

// sequenceControls checks the declarations and orders them: ranked controls
// by their unique rank, then the others by name. Ranks need not be
// contiguous, so removing a ranked control renumbers no other one.
func sequenceControls(declared []*fixtureControl) ([]*fixtureControl, error) {
	names := map[string]bool{}
	sources := map[string]string{}
	ranks := map[int]string{}
	for _, control := range declared {
		if !controlName.MatchString(control.name) {
			return nil, fmt.Errorf("fixture control name %q is not lower-case words joined by hyphens", control.name)
		}
		if names[control.name] {
			return nil, fmt.Errorf("fixture control %q is declared twice", control.name)
		}
		names[control.name] = true
		if control.poll == nil || control.source == "" {
			return nil, fmt.Errorf("fixture control %q has no function", control.name)
		}
		if other, ok := sources[control.source]; ok {
			return nil, fmt.Errorf("function %s is declared as fixture controls %q and %q", control.source, other, control.name)
		}
		sources[control.source] = control.name
		if rank := control.order.rank; rank != 0 {
			if other, ok := ranks[rank]; ok {
				return nil, fmt.Errorf("fixture controls %q and %q share rank %d", other, control.name, rank)
			}
			ranks[rank] = control.name
		}
	}
	sequence := slices.Clone(declared)
	slices.SortFunc(sequence, func(a, b *fixtureControl) int {
		switch {
		case a.order.rank != 0 && b.order.rank != 0:
			return cmp.Compare(a.order.rank, b.order.rank)
		case a.order.rank != 0:
			return -1
		case b.order.rank != 0:
			return 1
		}
		return strings.Compare(a.name, b.name)
	})
	return sequence, nil
}

// controlSequence is the polling order of every declared control. It is built
// in init, which runs after every package-level declaration of the package.
var controlSequence []*fixtureControl

func init() {
	sequence, err := sequenceControls(declaredControls)
	if err != nil {
		panic(err)
	}
	controlSequence = sequence
}

// checkNativeOracleWorker stops the loop once the native oracle worker that
// provider.go launches has recorded a failure.
func (c *fixtureCluster) checkNativeOracleWorker(_ context.Context, out string) error {
	if fileExists(filepath.Join(out, "native-oracles.error")) {
		return errors.New("native oracle worker failed; inspect native-oracles.log")
	}
	return nil
}

var _ = defineControl("native-oracles-failure", frozenControlRank(3), (*fixtureCluster).checkNativeOracleWorker)

// saveMetadata (diagnostics.go) collects the control loop's failure when it
// exists.
var _ = collectDiagnosticFiles(
	"provider.error",
)
