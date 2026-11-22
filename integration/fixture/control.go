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

// recordControlFailure records the first failure of the control loop or the
// lease loop (leaseLoop) as the fixture's failure, in provider.error, which
// every later request reports (internal/testcluster RequestControl).
func (c *fixtureCluster) recordControlFailure(out string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.controlErr != nil {
		return
	}
	c.controlErr = err
	_ = os.WriteFile(filepath.Join(out, "provider.error"), []byte(err.Error()+"\n"), 0600)
}

func (c *fixtureCluster) controlLoop(ctx context.Context, out string) {
	defer close(c.done)
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
				c.recordControlFailure(out, err)
				return
			}
		}
	}
}

// pendingRequest is one published, unclaimed request envelope of a control:
// "<name>.<request ID>.request" (internal/testcluster RequestEnvelope).
type pendingRequest struct {
	// name is the control name the envelope was published under.
	name   string
	path   string
	id     int64
	fields []string
	inputs []string
}

// requestEnvelope is the shape of an envelope name after its control's name.
var requestEnvelope = regexp.MustCompile(`^\.([1-9][0-9]{0,18})\.request$`)

// readRequestDir reads the control directory. The tests replace it to
// publish requests between or during the fixture's reads.
var readRequestDir = os.ReadDir

// pendingRequests returns the pending envelopes of the controls names, all
// together in request-ID order, the order of their publication, from one
// snapshot of the directory (scanRequests). Each publication has its own
// envelope, so two publishers of one control never replace each other's
// request. An envelope's first line is "<request ID> <arguments>", with the
// ID of its file name; a request that carries input files has one more line
// "input <file name>" per input, each name once (internal/testcluster
// RequestControlInput). A malformed envelope fails the fixture.
func pendingRequests(out string, names ...string) ([]pendingRequest, error) {
	pending, rejected, err := scanRequests(out, names...)
	if err != nil {
		return nil, err
	}
	if len(rejected) != 0 {
		return nil, rejected[0].err
	}
	return pending, nil
}

// rejectedRequest is a pending envelope that is not a well-formed request:
// id is the request ID of its file name.
type rejectedRequest struct {
	name string
	path string
	id   string
	err  error
}

// requestEntry says whether the directory entry file is a request envelope
// of one of the controls names, and returns that name and the request ID of
// the file name.
func requestEntry(file string, names []string) (string, string, bool) {
	for _, name := range names {
		rest, ok := strings.CutPrefix(file, name)
		if match := requestEnvelope.FindStringSubmatch(rest); ok && match != nil {
			return name, match[1], true
		}
	}
	return "", "", false
}

// scanRequests returns the well-formed pending envelopes of the controls
// names in request-ID order, and every malformed one apart, so that a caller
// that must keep serving its other requests (the lease loop) can reject one
// envelope without stopping at it. err is set only when the directory
// itself cannot be read.
//
// All the names come from one snapshot, so a control that orders an action
// and its undo by request ID (a stop and its start, a pause and its end)
// sees both or neither of two requests published one after the other; one
// read per name would see only the later one when both are published
// between the reads, and run the undo first. One directory read is not a
// snapshot either: a request renamed into the directory while it is read
// may be missed while a later one is returned. So the directory is read
// twice. A test publishes a request after the publication of every request
// it has to follow has returned, and takes its ID only then, so the IDs of
// such requests increase in that order (internal/testcluster
// nextRequestID). When the first read returns a request, the publication of
// every request before it had returned before that read ended, and the
// second read, which starts later, returns them. The scan serves what the
// second read returns up to the highest ID of the first read; a later
// request is left for the next poll, since a request before it may still be
// missing from the second read.
func scanRequests(out string, names ...string) ([]pendingRequest, []rejectedRequest, error) {
	first, err := readRequestDir(out)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	var cutoff int64
	for _, entry := range first {
		if _, id, ok := requestEntry(entry.Name(), names); ok {
			seen[entry.Name()] = true
			if n, err := strconv.ParseInt(id, 10, 64); err == nil && n > cutoff {
				cutoff = n
			}
		}
	}
	if len(seen) == 0 {
		return nil, nil, nil
	}
	second, err := readRequestDir(out)
	if err != nil {
		return nil, nil, err
	}
	var pending []pendingRequest
	var rejected []rejectedRequest
	for _, entry := range second {
		name, id, ok := requestEntry(entry.Name(), names)
		if !ok {
			continue
		}
		if !seen[entry.Name()] {
			if n, err := strconv.ParseInt(id, 10, 64); err != nil || n > cutoff {
				continue
			}
		}
		path := filepath.Join(out, entry.Name())
		r, err := parseRequest(path, id)
		if err != nil {
			rejected = append(rejected, rejectedRequest{name: name, path: path, id: id, err: err})
			continue
		}
		r.name = name
		pending = append(pending, r)
	}
	slices.SortFunc(pending, func(a, b pendingRequest) int { return cmp.Compare(a.id, b.id) })
	return pending, rejected, nil
}

// parseRequest reads the envelope at path, whose file name carries the
// request ID id.
func parseRequest(path, id string) (pendingRequest, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return pendingRequest{}, errors.New("invalid fixture request envelope")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return pendingRequest{}, err
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	fields := strings.Fields(lines[0])
	if len(fields) == 0 {
		return pendingRequest{}, errors.New("empty fixture request")
	}
	if fields[0] != id {
		return pendingRequest{}, errors.New("invalid fixture request identity")
	}
	r := pendingRequest{path: path, id: n, fields: fields}
	for _, line := range lines[1:] {
		words := strings.Fields(line)
		if len(words) != 2 || words[0] != "input" || !controlInputName.MatchString(words[1]) || slices.Contains(r.inputs, words[1]) {
			return pendingRequest{}, errors.New("invalid fixture request input line")
		}
		r.inputs = append(r.inputs, words[1])
	}
	return r, nil
}

// claim removes one pending envelope, which only the fixture does, and
// renames each of its inputs from "<file name>.<request ID>" to the file
// name the handler and its oracle read. The test never writes the plain
// file name, so a request the test abandoned and that is claimed later
// reads its own input, not the input of a request published after it; the
// fixture runs the requests of one control one at a time, so no later claim
// replaces the file while a handler reads it. A missing input fails the
// fixture, since the test writes it before it publishes the request.
func claim(out string, r pendingRequest) ([]string, error) {
	if err := os.Remove(r.path); err != nil {
		return nil, err
	}
	for _, input := range r.inputs {
		if err := os.Rename(filepath.Join(out, input+"."+r.fields[0]), filepath.Join(out, input)); err != nil {
			return nil, fmt.Errorf("claim the input of fixture request %s: %w", filepath.Base(r.path), err)
		}
	}
	return r.fields, nil
}

// request claims the oldest pending request of the control name and
// returns its fields, or nil when none is pending.
func (c *fixtureCluster) request(out, name string) ([]string, error) {
	pending, err := pendingRequests(out, name)
	if err != nil || len(pending) == 0 {
		return nil, err
	}
	return claim(out, pending[0])
}

// controlInputName is the closed shape of a request's input file name
// (internal/testcluster controlInputName).
var controlInputName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.json$`)

// controls polls every declared control once, in controlSequence order. It
// stops on the first failure; earlier mutations and acknowledgements are not
// replayed.
func (c *fixtureCluster) controls(ctx context.Context, out string) error {
	for _, control := range controlSequence {
		if err := control.poll(c, ctx, out); err != nil {
			return err
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

// defineRequestControl declares a control of the requests named name: each
// poll claims the oldest pending envelope (request), runs handle with its
// fields and acknowledges it as "<name>-completed.<request ID>". It is
// declared like defineControl.
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
