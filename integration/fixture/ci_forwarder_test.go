package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCIForwarder is a forwarder whose helper is serve, reading the requests
// and writing the answers over in-memory pipes. Stopping it closes both
// pipes, as stopping the real helper closes its input and ends its output.
func fakeCIForwarder(timeout time.Duration, in func(io.WriteCloser) io.WriteCloser, serve func(requests *bufio.Scanner, answers io.Writer)) (*ciForwarder, *atomic.Int32) {
	requestsOut, requestsIn := io.Pipe()
	answersOut, answersIn := io.Pipe()
	stops := new(atomic.Int32)
	stop := func() {
		stops.Add(1)
		requestsIn.Close()
		answersIn.Close()
	}
	go func() {
		serve(bufio.NewScanner(requestsOut), answersIn)
	}()
	var writer io.WriteCloser = requestsIn
	if in != nil {
		writer = in(requestsIn)
	}
	return newCIForwarder(writer, answersOut, stop, timeout), stops
}

// bounded runs one exchange and fails the test instead of hanging.
func bounded(t *testing.T, f *ciForwarder, matrices ...map[string]string) ([]map[string]string, []error, error) {
	t.Helper()
	type result struct {
		envs     []map[string]string
		refusals []error
		err      error
	}
	done := make(chan result, 1)
	go func() {
		envs, refusals, err := f.exchange(matrices)
		done <- result{envs, refusals, err}
	}()
	select {
	case r := <-done:
		return r.envs, r.refusals, r.err
	case <-time.After(20 * time.Second):
		t.Fatal("the exchange did not return")
		return nil, nil, nil
	}
}

// answering serves each request with the given lines, in order, and then
// keeps reading without answering.
func answering(lines ...string) func(*bufio.Scanner, io.Writer) {
	return func(requests *bufio.Scanner, answers io.Writer) {
		for requests.Scan() {
			if len(lines) == 0 {
				continue
			}
			if _, err := io.WriteString(answers, lines[0]+"\n"); err != nil {
				return
			}
			lines = lines[1:]
		}
	}
}

// requireRetired requires that f fails every later exchange with its first
// failure, without reading an answer, and that it stopped the helper once.
func requireRetired(t *testing.T, f *ciForwarder, stops *atomic.Int32, first error) {
	t.Helper()
	for range 2 {
		envs, refusals, err := bounded(t, f, map[string]string{"key": "next"})
		if err == nil || envs != nil || refusals != nil {
			t.Fatal("an exchange after the failed one was answered", envs, refusals, err)
		}
		if !strings.Contains(err.Error(), "retired by an earlier exchange") || !errors.Is(err, first) && !strings.Contains(err.Error(), first.Error()) {
			t.Fatal("the later exchange does not report the first failure:", err, first)
		}
	}
	if stops.Load() != 1 {
		t.Fatal("the helper was stopped", stops.Load(), "times, want once")
	}
}

func TestCIForwarderAnswersInOrderAndKeepsRefusalsPerMatrix(t *testing.T) {
	f, stops := fakeCIForwarder(time.Minute, nil, func(requests *bufio.Scanner, answers io.Writer) {
		for requests.Scan() {
			line := requests.Text()
			answer := fmt.Sprintf(`{"environment": {"REQUEST": %q}}`, line)
			if strings.Contains(line, "refuse") {
				answer = `{"error": "refused"}`
			}
			if _, err := io.WriteString(answers, answer+"\n"); err != nil {
				return
			}
		}
	})
	for round := range 2 {
		envs, refusals, err := bounded(t, f, map[string]string{"key": "a"}, map[string]string{"key": "refuse"}, map[string]string{"key": fmt.Sprint(round)})
		if err != nil {
			t.Fatal(err)
		}
		if envs[0]["REQUEST"] != `{"key":"a"}` || envs[1] != nil || refusals[1] == nil || refusals[1].Error() != "refused" ||
			envs[2]["REQUEST"] != fmt.Sprintf(`{"key":"%d"}`, round) || refusals[0] != nil || refusals[2] != nil {
			t.Fatal("answers out of order or refusals not kept per matrix", envs, refusals)
		}
	}
	if stops.Load() != 0 {
		t.Fatal("a refusal stopped the helper")
	}
}

// A failed exchange of every kind retires the forwarder before it is
// unlocked, so no later exchange reads an answer that belonged to it.
func TestCIForwarderRetiresAfterAnExchangeLosesItsFraming(t *testing.T) {
	failing := func(io.WriteCloser) io.WriteCloser { return failingWriter{} }
	silent := func(requests *bufio.Scanner, answers io.Writer) {
		for requests.Scan() {
		}
	}
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		in      func(io.WriteCloser) io.WriteCloser
		serve   func(*bufio.Scanner, io.Writer)
		want    string
	}{
		// The second answer is valid; a later exchange must not take it.
		{"undecodable answer", time.Minute, nil, answering("not json", `{"environment": {"STALE": "1"}}`, `{"environment": {"STALE": "2"}}`), "answer 1 of 2"},
		{"short read", time.Minute, nil, func(requests *bufio.Scanner, answers io.Writer) {
			requests.Scan()
			io.WriteString(answers, `{"environment": {"A": "1"}}`+"\n")
			answers.(io.Closer).Close()
		}, "helper ended after 1 of 2 answers"},
		// The helper ends without reading, so the write stays blocked until
		// the forwarder stops the helper.
		{"helper exit with a blocked writer", time.Minute, nil, func(_ *bufio.Scanner, answers io.Writer) {
			answers.(io.Closer).Close()
		}, "helper ended after 0 of 2 answers"},
		{"write error", time.Minute, failing, silent, "helper ended after 0 of 2 answers"},
		{"deadline", 100 * time.Millisecond, nil, silent, "no complete answer within 100ms"},
		{"answer without environment or refusal", time.Minute, nil, answering(`{}`, `{"environment": {"STALE": "1"}}`, `{"environment": {"STALE": "2"}}`),
			"answer 1 of 2 is neither"},
		{"answer with both", time.Minute, nil, answering(`{"environment": {"A": "1"}, "error": "x"}`, `{"environment": {"STALE": "1"}}`), "answer 1 of 2 is neither"},
		{"empty refusal", time.Minute, nil, answering(`{"error": ""}`, `{"environment": {"STALE": "1"}}`), "answer 1 of 2 is neither"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, stops := fakeCIForwarder(tc.timeout, tc.in, tc.serve)
			envs, refusals, err := bounded(t, f, map[string]string{"key": "a"}, map[string]string{"key": "b"})
			if err == nil || envs != nil || refusals != nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the failed exchange returned %v %v %v, want an error with %q", envs, refusals, err, tc.want)
			}
			requireRetired(t, f, stops, err)
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }
func (failingWriter) Close() error              { return nil }

// The real helper process: an exit before answering retires the forwarder,
// and stopping it reaps the process.
func TestCIForwarderRetiresWhenTheHelperProcessExits(t *testing.T) {
	command := exec.Command("sh", "-c", "read line; exit 3")
	f, err := startCIForwarder(command, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = bounded(t, f, map[string]string{"key": "a"}, map[string]string{"key": "b"})
	if err == nil || !strings.Contains(err.Error(), "helper ended after 0 of 2 answers") {
		t.Fatal("the exit of the helper was not reported", err)
	}
	if command.ProcessState == nil {
		t.Fatal("the helper process was not reaped")
	}
	_, _, later := bounded(t, f, map[string]string{"key": "c"})
	if later == nil || !strings.Contains(later.Error(), "retired by an earlier exchange") {
		t.Fatal("an exchange after the exit was attempted", later)
	}
}
