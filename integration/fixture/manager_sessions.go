package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

// A ceph-mgr process runs several MonClients. Its beacon client opens the MON
// session of con_type "mgr" whose global ID is the GID the MgrMap registers;
// the active MGR's modules open sessions of con_type "client" under the same
// entity name with their own global IDs. Only the owned MONs' local "sessions"
// admin socket command tells them apart. The observation and classification
// follow the ceph-testcontainers-go v0.3.0 recipe (MIT,
// internal/integration/manager_transition_integration_helpers_test.go
// decodeMgrSessions/beaconSources and docs/guides/messenger-client-fixtures.md
// "MGR MonClients"); the code here is written for this fixture.

// Statuses of one bounded native query. Only a complete query is an
// observation; an absence is observed only in a complete one.
const (
	queryComplete    = "complete"
	queryUnsupported = "unsupported"
	queryTimedOut    = "timed_out"
	queryTruncated   = "truncated"
	queryFailed      = "failed"
	queryUnavailable = "unavailable"
)

// queryErrorLimit caps the error text that a receipt keeps. MON session lists,
// MgrMaps and container states hold no key material; a native control's output
// is redacted before it is kept (auth_bundle.go).
const queryErrorLimit = 256

var errQueryMissing = errors.New("output without the selected fields")

func queryStatus(err error) string {
	if err == nil {
		return queryComplete
	}
	text := err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		return queryTimedOut
	case strings.Contains(text, "invalid command") || strings.Contains(text, "no valid command found") || strings.Contains(text, "unknown command"):
		return queryUnsupported
	}
	return queryFailed
}

// boundedQueryStatus reports a query whose own bound expired as timed out,
// also when the native error does not wrap the context's.
func boundedQueryStatus(err error, expired bool) string {
	if err != nil && expired {
		return queryTimedOut
	}
	return queryStatus(err)
}

func queryError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > queryErrorLimit {
		text = text[:queryErrorLimit]
	}
	return text
}

type managerSession struct {
	Entity   string `json:"entity"`
	GlobalID uint64 `json:"global_id"`
}

// monitorManagerSessions holds one MON's MGR sessions: beacon sessions
// (con_type "mgr") and every other session under a mgr.* entity.
type monitorManagerSessions struct {
	Monitor string           `json:"monitor"`
	Status  string           `json:"status"`
	Error   string           `json:"error,omitempty"`
	Beacons []managerSession `json:"beacons"`
	Modules []managerSession `json:"modules"`
}

func (m monitorManagerSessions) complete() bool { return m.Status == queryComplete }

// decodeManagerSessions decodes a MON's "sessions" output. Empty output, a
// JSON null and anything but a JSON list are errors, never an empty session
// list; an empty list is an answer.
func decodeManagerSessions(monitor string, data []byte) (monitorManagerSessions, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return monitorManagerSessions{}, errQueryMissing
	}
	var list []struct {
		EntityName string  `json:"entity_name"`
		ConType    string  `json:"con_type"`
		GlobalID   *uint64 `json:"global_id"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return monitorManagerSessions{}, err
	}
	if list == nil {
		return monitorManagerSessions{}, errQueryMissing
	}
	observed := monitorManagerSessions{Monitor: monitor, Status: queryComplete, Beacons: []managerSession{}, Modules: []managerSession{}}
	for _, session := range list {
		if !strings.HasPrefix(session.EntityName, "mgr.") {
			continue
		}
		if session.GlobalID == nil {
			return monitorManagerSessions{}, errors.New("MGR session without a global ID")
		}
		entry := managerSession{Entity: session.EntityName, GlobalID: *session.GlobalID}
		if session.ConType == "mgr" {
			observed.Beacons = append(observed.Beacons, entry)
		} else {
			observed.Modules = append(observed.Modules, entry)
		}
	}
	return observed, nil
}

// beaconMonitors returns the MONs holding the beacon session of mgr.<name>
// with global ID gid. A module session of the same entity, or a beacon with
// another global ID, is not it.
func beaconMonitors(monitors []monitorManagerSessions, name string, gid uint64) []string {
	var sources []string
	if gid == 0 {
		return nil
	}
	for _, monitor := range monitors {
		if slices.Contains(monitor.Beacons, managerSession{Entity: "mgr." + name, GlobalID: gid}) {
			sources = append(sources, monitor.Monitor)
		}
	}
	return sources
}

// moduleGlobalIDs returns the sorted global IDs of mgr.<name>'s non-beacon
// sessions on every observed MON.
func moduleGlobalIDs(monitors []monitorManagerSessions, name string) []uint64 {
	var ids []uint64
	for _, monitor := range monitors {
		for _, session := range monitor.Modules {
			if session.Entity == "mgr."+name {
				ids = append(ids, session.GlobalID)
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// observeManagerSessions reads every owned MON's sessions through its local
// admin socket, each within bound. A failed read stays recorded as such.
func (c *fixtureCluster) observeManagerSessions(ctx context.Context, bound time.Duration) []monitorManagerSessions {
	var observed []monitorManagerSessions
	for _, mon := range c.cluster.Monitors() {
		call, cancel := context.WithTimeout(ctx, bound)
		data, err := c.cluster.AdminSocket(call, "mon."+mon.DaemonName, "sessions")
		expired := call.Err() != nil
		cancel()
		entry := monitorManagerSessions{Monitor: mon.DaemonName}
		if err == nil {
			entry, err = decodeManagerSessions(mon.DaemonName, data)
		}
		if err != nil {
			entry = monitorManagerSessions{Monitor: mon.DaemonName, Status: boundedQueryStatus(err, expired), Error: queryError(err)}
		}
		observed = append(observed, entry)
	}
	return observed
}
