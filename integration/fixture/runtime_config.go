package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// runtimeConfig is the fixture's consumer-side adapter for ordered
// daemon-local runtime configuration changes. It keeps the recipe's order,
// one effective readback per step and the caller's receipt fields, and
// delegates each step to the public ceph-testcontainers-go v0.5.0 primitive
// that performs it:
//
//   - A debug level of an owned OSD (debug_ms, debug_osd) or MDS (debug_ms,
//     debug_mds) is one override.SetDebugLevel window, opened by apply and
//     closed by restore through its own Restore. The window reads the prior
//     level, verifies the normalized readback, is bound to the daemon's
//     incarnation (the OSD container generation, or the MDS container and
//     the GID its admin socket reports) and leaves an outside edit
//     unrestored.
//   - Any other OSD option goes through the OSD daemon handle's AdminSocket
//     as one config set followed by one config get.
//   - Any other MDS option, such as mds_log_pause or ms_blackhole_client, goes
//     through the caller's native MDS admin socket path as the same set and
//     get. SetDebugLevel admits only debug levels, and the provider decided
//     against a generic fault-setting primitive
//     (JSYoo5B/ceph-testcontainers-go#539).
//
// A step that fails ends the call: no step is retried and no later step is
// issued, so an uncertain outcome stays with the caller's receipt. Restoring
// a debug option that has no open window observes it and sets nothing. A
// second window for an option that already has one is refused without a
// native call.
type runtimeConfig struct {
	// daemon is the daemon the adapter reaches, when it is bound to one.
	daemon string
	// report names the daemon in a readback mismatch, such as "DATA OSD".
	report string
	// scope lists the admitted options; nil admits every option.
	scope []string
	// scopeErr is returned for an option outside scope or an empty value.
	scopeErr error
	// guard, when set, runs before each step and after the last one.
	guard func(context.Context) error
	// set and get are the direct path to the daemon's runtime config.
	set func(ctx context.Context, name, value string) error
	get func(ctx context.Context, name string) (string, error)
	// debug opens a public debug window for one option of debugNames; nil
	// when the role has no public debug primitive.
	debug      func(ctx context.Context, name, level string) (runtimeDebugWindow, error)
	debugNames []string
	windows    map[string]runtimeDebugWindow
}

// runtimeDebugWindow is the part of *override.DebugLevelHandle that the
// adapter uses.
type runtimeDebugWindow interface {
	Receipt() override.DebugLevelReceipt
	Restore(context.Context) error
}

// osdRuntimeConfig reaches one owned OSD, like "osd.0", of cluster: debug
// levels through override.SetDebugLevel and other options through the OSD
// handle's AdminSocket, each direct call bounded by 15 seconds.
func osdRuntimeConfig(cluster *ceph.Container, daemon, report string, scope []string, scopeErr error) *runtimeConfig {
	return &runtimeConfig{daemon: daemon, report: report, scope: scope, scopeErr: scopeErr,
		set: func(ctx context.Context, name, value string) error {
			call, stop := context.WithTimeout(ctx, 15*time.Second)
			defer stop()
			_, err := adminSocket(call, cluster, daemon, "config", "set", name, value)
			return err
		},
		get: func(ctx context.Context, name string) (string, error) {
			call, stop := context.WithTimeout(ctx, 15*time.Second)
			defer stop()
			data, err := adminSocket(call, cluster, daemon, "config", "get", name)
			if err != nil {
				return "", err
			}
			return parseMDSCephFSCompletionConfig(data, name)
		},
		debug: func(ctx context.Context, name, level string) (runtimeDebugWindow, error) {
			handle, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: daemon, Name: name, Level: level})
			if handle == nil {
				return nil, err
			}
			return handle, err
		},
		debugNames: []string{"debug_ms", "debug_osd"},
	}
}

// mdsRuntimeConfig reaches one owned MDS of cluster. daemon resolves its
// current ID ("a" for mds.a) when a window opens. Options in windowed are
// debug levels set through override.SetDebugLevel; every other option goes
// through asok, which runs one admin socket command and returns its trimmed
// stdout. A caller leaves out a debug option that an outer window of the same
// daemon already holds: the provider admits one window per daemon and option,
// so a nested change is a direct set that the caller restores before the
// outer window.
func mdsRuntimeConfig(cluster *ceph.Container, daemon func() (string, error), report string, windowed []string, asok func(ctx context.Context, args ...string) ([]byte, error)) *runtimeConfig {
	r := &runtimeConfig{report: report,
		set: func(ctx context.Context, name, value string) error {
			_, err := asok(ctx, "config", "set", name, value)
			return err
		},
		get: func(ctx context.Context, name string) (string, error) {
			data, err := asok(ctx, "config", "get", name)
			if err != nil {
				return "", err
			}
			return parseMDSCephFSCompletionConfig(data, name)
		},
	}
	if len(windowed) != 0 {
		r.debugNames = windowed
		r.debug = func(ctx context.Context, name, level string) (runtimeDebugWindow, error) {
			id, err := daemon()
			if err != nil {
				return nil, err
			}
			handle, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{Daemon: "mds." + id, Name: name, Level: level})
			if handle == nil {
				return nil, err
			}
			return handle, err
		}
	}
	return r
}

// mdsDebugNames are the MDS debug levels that override.SetDebugLevel admits.
var mdsDebugNames = []string{"debug_mds", "debug_ms"}

// apply sets values in order, each read back, and requires every readback to
// equal its value. Readback receives each value read.
func (r *runtimeConfig) apply(ctx context.Context, order []string, values map[string]string, readback *map[string]string) error {
	return r.run(ctx, order, values, readback, false)
}

// restore returns options to values in order, each read back. An open debug
// window is restored through its own Restore.
func (r *runtimeConfig) restore(ctx context.Context, order []string, values map[string]string, readback *map[string]string) error {
	return r.run(ctx, order, values, readback, true)
}

func (r *runtimeConfig) run(ctx context.Context, order []string, values map[string]string, readback *map[string]string, restore bool) error {
	*readback = map[string]string{}
	for _, name := range order {
		value := values[name]
		if r.scope != nil && !slices.Contains(r.scope, name) || value == "" {
			if r.scopeErr != nil {
				return r.scopeErr
			}
			return fmt.Errorf("%s setting %s is outside closed scope", r.report, name)
		}
		if err := r.check(ctx); err != nil {
			return err
		}
		var got string
		var err error
		switch {
		case r.debug != nil && slices.Contains(r.debugNames, name) && restore:
			got, err = r.closeWindow(ctx, name)
		case r.debug != nil && slices.Contains(r.debugNames, name):
			got, err = r.openWindow(ctx, name, value)
		default:
			got, err = r.direct(ctx, name, value)
		}
		if got != "" {
			(*readback)[name] = got
		}
		if err != nil {
			return err
		}
		if got != value {
			return fmt.Errorf("%s reports %s=%s after setting %s", r.report, name, got, value)
		}
	}
	return r.check(ctx)
}

func (r *runtimeConfig) check(ctx context.Context) error {
	if r.guard == nil {
		return nil
	}
	return r.guard(ctx)
}

func (r *runtimeConfig) direct(ctx context.Context, name, value string) (string, error) {
	if err := r.set(ctx, name, value); err != nil {
		return "", err
	}
	return r.get(ctx, name)
}

// discard ends the open debug windows of an incarnation that the caller has
// already given up, such as an MDS that a restart replaced. Restore sets
// nothing on a replaced process (target-changed) and restores a process that
// is still the same one, and either releases the provider's admission, so a
// later window of the same daemon and option can open. Any other outcome is
// returned and its window kept.
func (r *runtimeConfig) discard(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(r.windows)) {
		window := r.windows[name]
		err := window.Restore(ctx)
		switch state := window.Receipt().State; state {
		case override.DebugLevelTargetChanged, override.DebugLevelRestored:
			delete(r.windows, name)
		default:
			errs = append(errs, errors.Join(err, fmt.Errorf("%s %s window of a discarded change ended %s", r.report, name, state)))
		}
	}
	return errors.Join(errs...)
}

// openWindow opens one debug window. A window that may have been applied is
// kept for restore whatever the error.
func (r *runtimeConfig) openWindow(ctx context.Context, name, level string) (string, error) {
	if r.windows[name] != nil {
		return "", fmt.Errorf("%s already has an open %s window", r.report, name)
	}
	window, err := r.debug(ctx, name, level)
	if window == nil {
		return "", errors.Join(err, fmt.Errorf("%s %s window is unavailable", r.report, name))
	}
	receipt := window.Receipt()
	if receipt.State != override.DebugLevelNotApplied {
		if r.windows == nil {
			r.windows = map[string]runtimeDebugWindow{}
		}
		r.windows[name] = window
	}
	return receipt.Applied, err
}

// closeWindow restores an open window. Without one nothing is owned, so the
// option is only observed. The window is forgotten once restored.
func (r *runtimeConfig) closeWindow(ctx context.Context, name string) (string, error) {
	window := r.windows[name]
	if window == nil {
		return r.get(ctx, name)
	}
	err := window.Restore(ctx)
	receipt := window.Receipt()
	if receipt.State == override.DebugLevelRestored {
		delete(r.windows, name)
	}
	if receipt.Restored != "" || len(receipt.Transitions) == 0 {
		return receipt.Restored, err
	}
	return receipt.Transitions[len(receipt.Transitions)-1].Readback, err
}
