package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// mgrAcceptorOSDLogLimit bounds the complete osd.0 container log one
// mgr-acceptor-osd-log request scans. The MGR acceptor test raises osd.0's
// debug_ms to 5 only for its own run.
const mgrAcceptorOSDLogLimit = 256 << 20

// Pinned ceph/ceph/blob/v20.2.4/src/msg/async/ProtocolV2.cc logs both
// identification frames at debug_ms 5: send_client_ident ("sending
// identification: addrs=... target=... gid=... global_seq=... features_supported=...
// features_required=... flags=... cookie=...") and handle_server_ident
// ("received server identification: addrs=... gid=... global_seq=..."),
// global_seq in decimal and the masks, flags and cookie in hex.
var (
	osdClientIdentLine = regexp.MustCompile(`send_client_ident sending identification: addrs=(\S+) target=(\S+) gid=(\d+) global_seq=(\d+) features_supported=([0-9a-f]+) features_required=([0-9a-f]+) flags=([0-9a-f]+) cookie=([0-9a-f]+)`)
	osdServerIdentLine = regexp.MustCompile(`received server identification: addrs=(\S+) gid=(\d+) global_seq=(\d+) features_supported=([0-9a-f]+) features_required=([0-9a-f]+) flags=([0-9a-f]+) cookie=([0-9a-f]+)`)
)

// mgrAcceptorIdent is one identification frame the stock osd.0 logged:
// "client" for its CLIENT_IDENT to the MGR endpoint, "server" for the
// SERVER_IDENT it received from it. Values are the log's text.
type mgrAcceptorIdent struct {
	Kind      string `json:"kind"`
	Addrs     string `json:"addrs"`
	Target    string `json:"target,omitempty"`
	GID       string `json:"gid"`
	GlobalSeq string `json:"global_seq"`
	Supported string `json:"features_supported"`
	Required  string `json:"features_required"`
	Flags     string `json:"flags"`
	Cookie    string `json:"cookie"`
}

// mgrAcceptorOSDLogReceipt is the public receipt of one request. Admitted
// reports whether this run declared capMgrDaemonControl. Idents holds, in
// log order, every identification osd.0 logged whose target (client) or
// addresses (server) name the requested MGR port; it holds addresses,
// numeric names, sequences, masks and cookies, no key material.
type mgrAcceptorOSDLogReceipt struct {
	Label    string             `json:"label"`
	Admitted bool               `json:"admitted"`
	Idents   []mgrAcceptorIdent `json:"idents"`
}

// scanMgrAcceptorIdents reads one complete log within limit bytes and keeps
// the identification lines of the MGR endpoint on port.
func scanMgrAcceptorIdents(r io.Reader, port string, limit int) ([]mgrAcceptorIdent, error) {
	counted := &countedReader{r: io.LimitReader(r, int64(limit)+1)}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	needle := regexp.MustCompile(`:` + regexp.QuoteMeta(port) + `/`)
	var out []mgrAcceptorIdent
	for scanner.Scan() {
		line := scanner.Text()
		if m := osdClientIdentLine.FindStringSubmatch(line); m != nil && needle.MatchString(m[2]) {
			out = append(out, mgrAcceptorIdent{Kind: "client", Addrs: m[1], Target: m[2], GID: m[3], GlobalSeq: m[4], Supported: m[5], Required: m[6], Flags: m[7], Cookie: m[8]})
		} else if m := osdServerIdentLine.FindStringSubmatch(line); m != nil && needle.MatchString(m[1]) {
			out = append(out, mgrAcceptorIdent{Kind: "server", Addrs: m[1], GID: m[2], GlobalSeq: m[3], Supported: m[4], Required: m[5], Flags: m[6], Cookie: m[7]})
		}
	}
	if counted.n > int64(limit) {
		return nil, errContainerLogExceedsBound
	}
	return out, scanner.Err()
}

// mgrAcceptorOSDLog answers "<id> <label> <port>": it scans osd.0's
// complete container log and replaces mgr-acceptor-osd-log.json atomically.
func (c *fixtureCluster) mgrAcceptorOSDLog(ctx context.Context, out string, fields []string) error {
	if len(fields) != 3 {
		return errors.New("invalid MGR acceptor OSD log request")
	}
	if _, err := strconv.ParseUint(fields[2], 10, 16); err != nil {
		return fmt.Errorf("invalid MGR acceptor port %q", fields[2])
	}
	receipt := mgrAcceptorOSDLogReceipt{Label: fields[1], Admitted: c.profile.admits(capMgrDaemonControl)}
	if receipt.Admitted {
		daemon, err := c.daemon(ctx, "osd", "0")
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, time.Minute)
		err = withContainerLog(call, daemon.Logs, func(r io.Reader) error {
			var scanErr error
			receipt.Idents, scanErr = scanMgrAcceptorIdents(r, fields[2], mgrAcceptorOSDLogLimit)
			return scanErr
		})
		cancel()
		if err != nil {
			return fmt.Errorf("osd.0 log for %s: %w", fields[1], err)
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary := filepath.Join(out, "mgr-acceptor-osd-log.tmp")
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, "mgr-acceptor-osd-log.json"))
}

var _ = defineRequestControl("mgr-acceptor-osd-log", controlAfterRanked, (*fixtureCluster).mgrAcceptorOSDLog)

var _ = collectDiagnosticFiles("mgr-acceptor-osd-log.json")
