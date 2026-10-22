package main

import (
	"context"
	"errors"
	"time"
)

// The Go-originated cache-tier operation test reuses the live cache-pool
// fixture (owned base and writeback tier, no overlay) and its scoped
// identities. The product client sends the cache operations; this closed
// native observer only seeds, observes and removes its own three heads and
// never issues a cache operation. Each phase is one control; an uncertain
// observer call is retained and nothing is reissued.
const (
	cacheOpGoSelector = "^TestCephOSDCacheOpGoIntegration$"
	cacheOpGoAction   = "go-ops"
)

var cacheOpGoLabels = map[string]bool{"after-evict-dirty": true, "pinned": true, "unpinned": true, "flushed": true, "try-flushed": true, "evicted": true, "absent-final": true}

func cacheOpGoControl(fields []string) (phase, label string, ok bool) {
	switch {
	case len(fields) == 4 && (fields[3] == "prepare" || fields[3] == "finish"):
		return fields[3], "", true
	case len(fields) == 5 && fields[3] == "observe" && cacheOpGoLabels[fields[4]]:
		return fields[3], fields[4], true
	}
	return "", "", false
}

func (c *fixtureCluster) cacheOpGoNative(ctx context.Context, phase, label string) error {
	x := c.cachePools
	if x == nil || x.Resources == nil {
		return errors.New("Go cache-op observer requires prepared cache-pool resources")
	}
	argv := []string{"python3", "/out/osd_cache_op_go_oracle.py", "/tmp/ceph-msgr-test", "/out", phase}
	step := "oracle-" + cacheOpGoAction + "-" + phase
	if label != "" {
		argv = append(argv, label)
		step += "-" + label
	}
	return x.mutate(step, func() error {
		return c.runOracleCommandWithTimeout(ctx, x.Out, argv, "osd-cache-op-go-command.json", "closed Go cache-op native observer", 3*time.Minute)
	})
}
