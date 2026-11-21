package testcluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ControlInput is one input file of one control request. The fixture's
// handler and oracle read it under Name, but the test never writes Name
// itself: RequestControlInput writes the data as "<Name>.<request ID>" and
// names it on a line "input <Name>" after the request's first line, and the
// fixture renames that file to Name only when it claims this request
// (integration/fixture/control.go claim; integration/cluster.sh for the
// requests its loop serves). So a
// request the test abandoned at a deadline, and that the fixture claims or
// runs later, still reads its own input, never the input of the request
// published after it. Private writes the file 0600, as a few inputs were
// before; otherwise it is public evidence (WritePublicEvidence).
type ControlInput struct {
	Name    string
	Data    []byte
	Private bool
}

// controlInputName is the closed shape of an input file name: one plain file
// name of the control directory, never a path.
var controlInputName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.json$`)

var lastRequestID struct {
	sync.Mutex
	id int64
}

// nextRequestID returns a request identity unique within this process and
// increasing: the publication time in nanoseconds, or one more than the last
// identity when two requests are published within the same nanosecond. The
// identity names the acknowledgement and, with an input, the input file, so
// two concurrent requests never share either.
func nextRequestID() int64 {
	lastRequestID.Lock()
	defer lastRequestID.Unlock()
	id := time.Now().UnixNano()
	if id <= lastRequestID.id {
		id = lastRequestID.id + 1
	}
	lastRequestID.id = id
	return id
}

// RequestEnvelope is the file name of one publication of request: the
// request name, the request ID and ".request". Each publication has its own
// envelope, so two publishers of one control never replace each other's
// request; the fixture claims a control's pending envelopes in request-ID
// order, each atomically (integration/fixture/control.go pendingRequests, and
// integration/cluster.sh pending for the requests its loop serves).
func RequestEnvelope(request string, id int64) string {
	return fmt.Sprintf("%s.%d.request", request, id)
}

// RequestControl publishes non-secret control metadata atomically, in its own
// envelope (RequestEnvelope). The private fixture directory bounds access,
// while 0644 lets a non-root Linux host read files produced by the root
// container test process. Keys remain separate.
func RequestControl(ctx context.Context, control, request, response string, args ...string) error {
	return requestControl(ctx, control, request, response, nil, args)
}

// RequestControlInput is RequestControl for a request that carries input
// files (ControlInput), each named once; with none it is RequestControl. The
// inputs are written under their request-scoped names before the request is
// published, and they are left in place when the request is abandoned: the
// fixture may already have claimed the request and is the only one that
// renames them afterwards.
func RequestControlInput(ctx context.Context, control, request, response string, inputs []ControlInput, args ...string) error {
	return requestControl(ctx, control, request, response, inputs, args)
}

// ControlDaemonInput is ControlDaemon for a request that carries input files
// (RequestControlInput).
func ControlDaemonInput(ctx context.Context, control, action, daemon, name string, inputs ...ControlInput) error {
	return RequestControlInput(ctx, control, action+"-"+daemon, daemon+"-"+action, inputs, name)
}

func requestControl(ctx context.Context, control, request, response string, inputs []ControlInput, args []string) error {
	id, err := publishRequest(control, request, inputs, args)
	if err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("%s.%d", response, id))
	defer os.Remove(ack)
	_, err = waitControl(ctx, control, ack)
	return err
}

// publishRequest writes the request's inputs and then publishes its
// envelope, and returns its request ID. It publishes nothing once the
// fixture has recorded a failure.
func publishRequest(control, request string, inputs []ControlInput, args []string) (int64, error) {
	// A worker error recorded before this request was published is an earlier
	// failure: the request would never run, so say so instead of repeating the
	// originating text as if this request had caused it.
	if name, text, err := controlWorkerError(control); err != nil {
		return 0, err
	} else if name != "" {
		return 0, fmt.Errorf("fixture %s recorded before this %q request was published; the request was not executed and this is a cascading failure, not a new one: %s", name, request, text)
	}
	id := nextRequestID()
	content := fmt.Sprintf("%d %s\n", id, strings.Join(args, " "))
	named := map[string]bool{}
	for _, input := range inputs {
		if !controlInputName.MatchString(input.Name) || named[input.Name] {
			return 0, fmt.Errorf("control input name %q is not a plain JSON file name named once", input.Name)
		}
		named[input.Name] = true
		path := filepath.Join(control, fmt.Sprintf("%s.%d", input.Name, id))
		var err error
		if input.Private {
			err = os.WriteFile(path, input.Data, 0600)
		} else {
			err = WritePublicEvidence(path, input.Data)
		}
		if err != nil {
			return 0, err
		}
		content += "input " + input.Name + "\n"
	}
	file, err := os.CreateTemp(control, request+"-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(file.Name())
	if _, err := file.WriteString(content); err != nil {
		file.Close()
		return 0, err
	}
	if err := file.Chmod(0644); err != nil {
		file.Close()
		return 0, err
	}
	if err := file.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, RequestEnvelope(request, id))); err != nil {
		return 0, err
	}
	return id, nil
}

// waitControl waits until one of the acknowledgements acks exists and
// returns its path, or until ctx ends or the fixture records a failure.
func waitControl(ctx context.Context, control string, acks ...string) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, ack := range acks {
			if _, err := os.Stat(ack); err == nil {
				return ack, nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
		}
		// Preserve the first controller/oracle error instead of hiding a dead
		// worker behind repeated per-test context deadlines.
		if name, text, err := controlWorkerError(control); err != nil {
			return "", err
		} else if name != "" {
			return "", fmt.Errorf("fixture %s: %s", name, text)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func RestartMonitor(ctx context.Context, control, name string) error {
	return RequestControl(ctx, control, "restart-mon", "mon-restarted", name)
}

func ControlDaemon(ctx context.Context, control, action, daemon, name string) error {
	return RequestControl(ctx, control, action+"-"+daemon, daemon+"-"+action, name)
}

// controlWorkerError returns the first recorded controller/oracle error file.
func controlWorkerError(control string) (name, text string, err error) {
	for _, name := range []string{"provider.error", "native-oracles.error"} {
		data, err := os.ReadFile(filepath.Join(control, name))
		if err == nil {
			return name, strings.TrimSpace(string(data)), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}
	}
	return "", "", nil
}
