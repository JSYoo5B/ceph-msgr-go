package main

import (
	"context"
)

func (c *fixtureCluster) controlRBDRequests(ctx context.Context, out string) error {
	if fields, err := c.request(out, "rbd-base"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdBaseControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-base-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-write"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdWriteControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-write-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-lock"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdLockControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-lock-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-clone"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdCloneControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-clone-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-journal"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdJournalControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-journal-completed."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}
