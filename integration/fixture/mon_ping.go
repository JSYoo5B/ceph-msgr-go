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
)

// This is the native rados_ping_monitor path (ceph.in's special ping branch),
// not mon_command or tell. A native reset can return an empty successful result;
// require the selected daemon's nonempty metadata as well as process success.
func validateNativeMonPing(data []byte, name string, rank int, fsid string) error {
	var pong struct {
		Status *struct {
			Name string `json:"name"`
			Rank *int   `json:"rank"`
			Map  *struct {
				FSID  string  `json:"fsid"`
				Epoch *uint32 `json:"epoch"`
				Mons  []struct {
					Name string `json:"name"`
					Rank int    `json:"rank"`
				} `json:"mons"`
			} `json:"monmap"`
		} `json:"mon_status"`
	}
	if len(data) == 0 || len(data) > 1<<20 || json.Unmarshal(data, &pong) != nil || pong.Status == nil || pong.Status.Name != name || pong.Status.Rank == nil || *pong.Status.Rank != rank || pong.Status.Map == nil || pong.Status.Map.FSID != fsid || pong.Status.Map.Epoch == nil || *pong.Status.Map.Epoch == 0 || len(pong.Status.Map.Mons) != 3 {
		return errors.New("native MON ping lacks the selected daemon's cluster and rank metadata")
	}
	for i, member := range pong.Status.Map.Mons {
		if member.Rank != i || member.Name != []string{"a", "b", "c"}[i] {
			return errors.New("native MON ping membership differs from the owned fixture")
		}
	}
	return nil
}

func (c *fixtureCluster) prepareMonPing(ctx context.Context, out string) error {
	if c.profile.monPing != "1" {
		return errors.New("native MON ping requires its explicit fixture opt-in")
	}
	for rank, name := range []string{"a", "b", "c"} {
		result, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
			Argv:    []string{"ceph", "-c", "/etc/ceph/ceph.conf", "-n", "client.test", "-k", "/out/admin.keyring", "ping", "mon." + name},
			Timeout: 10 * time.Second, MaxStdout: 1 << 20, MaxStderr: 4096, Label: "native rados_ping_monitor mon." + name,
		})
		if err != nil || !result.Complete() || result.ExitCode != 0 || result.StdoutTruncated || result.StderrTruncated {
			return fmt.Errorf("native MON ping mon.%s exit=%d complete=%t: %w", name, result.ExitCode, result.Complete(), errors.Join(err, errors.New("native ping failed")))
		}
		if err := validateNativeMonPing(result.Stdout, name, rank, c.profile.fsid); err != nil {
			return fmt.Errorf("native MON ping mon.%s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(out, "mon-ping-native-"+name+".json"), result.Stdout, 0600); err != nil {
			return err
		}
	}
	return nil
}
