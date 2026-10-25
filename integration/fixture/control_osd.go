package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func osdAppliedEpoch(data []byte, selected int32, fsid string, expected uint32) (bool, error) {
	var applied struct {
		Who    *int32 `json:"whoami"`
		FSID   string `json:"cluster_fsid"`
		Newest string `json:"newest_map"`
		Lower  uint32 `json:"cluster_osdmap_trim_lower_bound"`
	}
	if err := json.Unmarshal(data, &applied); err != nil {
		return false, err
	}
	if applied.Who == nil || *applied.Who != selected || fsid == "" || applied.FSID != fsid {
		return false, errors.New("native OSD applied-map receipt belongs to a different daemon or cluster")
	}
	newest, err := strconv.ParseUint(applied.Newest, 10, 32)
	if err != nil || newest == 0 || uint64(applied.Lower) > newest {
		return false, errors.New("invalid native OSD applied-map epoch/trim receipt")
	}
	return newest >= uint64(expected) && applied.Lower > 1, nil
}

func (c *fixtureCluster) controlCopyFromErrorRequest(ctx context.Context, out string) error {
	if fields, err := c.request(out, "limit-osd-copyfrom-error"); err != nil {
		return err
	} else if fields != nil {
		if err := c.copyFromErrorControl(ctx, out, fields); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlCachePoolsRequest(ctx context.Context, out string) error {
	if fields, err := c.request(out, "osd-cache-pools"); err != nil {
		return err
	} else if fields != nil {
		if err := c.osdCachePoolsControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "osd-cache-pools-completed."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlOSDReadyRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-ready")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || (fields[1] != "clean" && fields[1] != "recovery") || c.profile.osd != "1" {
			return errors.New("invalid OSD clean readiness request")
		}
		if err := c.waitOSDReady(ctx, out, fields[1]); err != nil {
			return err
		}
		if err := publishAck(out, "osd-ready-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlOSDEpochRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-epoch")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 3 || fields[1] != "advance" || c.profile.osd != "1" {
			return errors.New("invalid OSD epoch request")
		}
		selected, err := strconv.ParseInt(fields[2], 10, 32)
		if err != nil || selected < 0 {
			return errors.New("invalid OSD epoch target")
		}
		if _, err = c.daemon("osd", fields[2]); err != nil {
			return err
		}
		dump, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(out, "osd-map-epoch.json"), dump, 0600); err != nil {
			return err
		}
		var expected struct {
			Epoch uint32 `json:"epoch"`
			FSID  string `json:"fsid"`
		}
		if err = json.Unmarshal(dump, &expected); err != nil {
			return err
		}
		if expected.Epoch == 0 || expected.FSID == "" || expected.FSID != c.profile.fsid {
			return errors.New("invalid native OSD epoch identity")
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			data, e := c.cluster.AdminSocket(ctx, "osd."+fields[2], "status")
			if e != nil {
				return e
			}
			ready, e := osdAppliedEpoch(data, int32(selected), expected.FSID, expected.Epoch)
			if e != nil {
				return e
			}
			if ready {
				if err = os.WriteFile(filepath.Join(out, "osd-map-applied.json"), data, 0600); err != nil {
					return err
				}
				break
			}
			if time.Now().After(deadline) {
				return errors.New("OSD did not apply/trim requested epoch")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if err = publishAck(out, "osd-epoch-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}
