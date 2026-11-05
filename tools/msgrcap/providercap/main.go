// Command providercap runs one ceph-testcontainers-go capture.Start session
// for the msgrcap live recipe. It is development-only and a separate module,
// so that neither the product module nor the msgrcap analyzer depends on the
// capture provider.
//
//	providercap -container ID -network NET -interface eth0 -filter EXPR \
//	        -image IMAGE@sha256:DIGEST -output seg.pcap -report seg.report.json -ready seg.ready
//
// It starts the capture on the given running container, writes the -ready
// file once capture.Start has returned (tcpdump listens), waits for SIGTERM or
// SIGINT, then calls Session.Stop and writes the capture.Report as JSON with
// Output reduced to its base name. It exits nonzero when Start or Stop
// returned an error; the report is written in both cases. A session is bound
// to the container generation it joined, so a restarted target needs a new
// providercap run: one run is one capture segment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/testcontainers/testcontainers-go"
)

// result is the capture.Report with its fields at the top level, as
// msgrcap's capture manifest reads it, and the errors of the session.
type result struct {
	capture.Report
	// StartError and StopError are the errors of capture.Start and
	// Session.Stop, empty on success.
	StartError string `json:"start_error,omitempty"`
	StopError  string `json:"stop_error,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "providercap:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("providercap", flag.ContinueOnError)
	id := fs.String("container", "", "ID of the running target container")
	req := capture.Request{}
	fs.StringVar(&req.Network, "network", "", "user-defined bridge network of the target endpoint")
	fs.StringVar(&req.Interface, "interface", "eth0", "target interface on that network")
	fs.StringVar(&req.Filter, "filter", "", "pcap-filter expression")
	fs.StringVar(&req.HelperImage, "image", "", "capture helper image, pinned by digest")
	fs.IntVar(&req.SnapLen, "snaplen", capture.MaxSnapLen, "bytes kept per packet")
	fs.IntVar(&req.MaxPackets, "max-packets", 4000, "packet bound of the session")
	fs.StringVar(&req.Output, "output", "", "capture file to create")
	reportPath := fs.String("report", "", "JSON report output")
	readyPath := fs.String("ready", "", "file created once the capture is ready")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *reportPath == "" || *readyPath == "" {
		return errors.New("-container, -report and -ready are required")
	}
	if !strings.Contains(req.HelperImage, "@sha256:") {
		return errors.New("-image must be pinned by digest")
	}
	// Signals are observed from the start, so that a stop request during
	// Start is not lost; Start itself runs with its own bound.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	startCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	session, err := capture.Start(startCtx, &testcontainers.DockerContainer{ID: *id}, req)
	cancel()
	var res result
	if err != nil {
		res.StartError = err.Error()
		if session != nil {
			// Start returns a session with an error only when a probe or
			// helper could not be removed; Stop retries the removal.
			res.Report, err = session.Stop(context.Background())
			if err != nil {
				res.StopError = err.Error()
			}
		}
		return errors.Join(errors.New(res.StartError), write(*reportPath, res))
	}
	if err := os.WriteFile(*readyPath, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		_, stopErr := session.Stop(context.Background())
		return errors.Join(err, stopErr)
	}
	<-stop
	res.Report, err = session.Stop(context.Background())
	if err != nil {
		res.StopError = err.Error()
	}
	return errors.Join(err, write(*reportPath, res))
}

func write(path string, res result) error {
	// The archived report names no host path, as Session.Settle does.
	if res.Report.Output != "" {
		res.Report.Output = filepath.Base(res.Report.Output)
	}
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
