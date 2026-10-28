package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func managerFailoverReady(status ceph.ManagerStatus, names map[string]bool) bool {
	return len(names) == 2 && status.Available && status.ActiveGID != 0 && names[status.ActiveName] &&
		len(status.Standbys) == 1 && names[status.Standbys[0].Name] && status.Standbys[0].Name != status.ActiveName &&
		status.Standbys[0].GID != 0 && status.Standbys[0].GID != status.ActiveGID
}

// nativeManagerMapTimeout bounds one native `mgr dump`. A native CLI whose
// MGR peer crashed mid-activation can hang; one call must not consume a whole
// preparation budget silently.
const nativeManagerMapTimeout = 15 * time.Second

func (c *fixtureCluster) observeManagerMap(ctx context.Context) (managerPreparationMap, error) {
	call, cancel := context.WithTimeout(ctx, nativeManagerMapTimeout)
	defer cancel()
	data, err := c.cluster.Ceph(call, "mgr", "dump", "--format", "json")
	var state managerPreparationMap
	if err == nil {
		err = json.Unmarshal(data, &state)
	} else if call.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("native mgr dump exceeded %v: %w", nativeManagerMapTimeout, err)
	}
	return state, err
}

func (c *fixtureCluster) ownedManagers() (map[string]testcontainers.Container, []string, error) {
	daemons := make(map[string]testcontainers.Container)
	var names []string
	for _, d := range c.cluster.Managers() {
		if _, duplicate := daemons[d.DaemonName]; duplicate {
			return nil, nil, errors.New("duplicate owned MGR preparation identity")
		}
		daemons[d.DaemonName] = d.Container
		names = append(names, d.DaemonName)
	}
	return daemons, names, nil
}

func (c *fixtureCluster) prepareManagerFailover(ctx context.Context, out string) error {
	daemons, names, err := c.ownedManagers()
	if err != nil {
		return err
	}
	backend := managerPreparationBackend{
		observe: c.observeManagerMap,
		rejection: func(call context.Context, name string) (managerRefusal, error) {
			return c.nativeManagerRefusal(call, name, daemons[name], false)
		},
		stop:  func(call context.Context, name string) error { return daemons[name].Stop(call, nil) },
		start: func(call context.Context, name string) error { return daemons[name].Start(call) },
		record: func(receipt managerPreparationReceipt) error {
			data, err := json.Marshal(receipt)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(filepath.Join(out, "manager-preparation.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			_, writeErr := f.Write(append(data, '\n'))
			return errors.Join(writeErr, f.Close())
		},
	}
	return prepareManagerFailoverWithBackend(ctx, names, backend, 45*time.Second)
}

func (c *fixtureCluster) controlNamedManagerRequest(ctx context.Context, out string) error {
	if fields, err := c.request(out, "observe-named-mgr"); err != nil {
		return err
	} else if fields != nil {
		if err := c.namedManagerOracle(ctx, out, fields); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlManagerFailoverRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "prepare-mgr-failover")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "ready" || c.profile.mgrCount != "2" || c.profile.authEpoch == "1" || c.profile.idleSessions == "1" || c.profile.shortTickets == "1" || c.profile.expireTickets == "1" || c.profile.modeRejection != "" {
			return errors.New("invalid ordinary MGR failover preparation request")
		}
		if err = c.prepareManagerFailover(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-failover-prepare."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlManagerServiceConfigRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-mgr-service-config")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 {
			return errors.New("invalid MGR service configuration request")
		}
		if err = c.mgrServiceConfigControl(ctx, out, fields[1]); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-service-config-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlManagerStartRequest(ctx context.Context, out string) error {
	var err error
	if fileExists(filepath.Join(out, "start-mgrs")) && !fileExists(filepath.Join(out, "mgrs-started")) {
		if err = createManagersWithOwnership(ctx, c.cluster.AddManager, func(created *ceph.ManagerContainer) error {
			return recordOwnership(c, out, created)
		}); err != nil {
			return err
		}
		if err = c.waitReady(ctx, true); err != nil {
			return err
		}
		if err = publishAck(out, "mgrs-started"); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlManagerSecurityRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "secure-mgr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.modeRejection != "mgr" {
			return errors.New("invalid policy request")
		}
		if _, err = c.cluster.TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: []ceph.MessengerConnectionMode{ceph.MessengerSecure}}); err != nil {
			return err
		}
		// Start waits only for the admin socket, and a pre-stop registration
		// can outlive its process; acknowledge only an accepted MGR
		// transition receipt with new GIDs and beacon sessions.
		if err = c.restartManagersWithReceipt(ctx, out, fields[0]); err != nil {
			return err
		}
		if err = c.waitReady(ctx, true); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-secure."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

// AddManager may return an owned descriptor and an error after creating its
// container. Persist that attempt before returning, including partial failure.
// Neither manager creation nor an uncertain creation error is retried here.
func createManagersWithOwnership(ctx context.Context, add func(context.Context, string) (*ceph.ManagerContainer, error), persist func(*ceph.ManagerContainer) error) error {
	for _, name := range []string{"a", "b"} {
		created, creationErr := add(ctx, name)
		inventoryErr := persist(created)
		if err := errors.Join(creationErr, inventoryErr); err != nil {
			return err
		}
	}
	return nil
}
