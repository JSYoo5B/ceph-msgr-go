package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS charmap acceptance runs alone on a fresh completion fixture. It
// adds three closed native actions on one child of the owned directory, the
// charmap directory, and the MDS "p" flag (MAY_SET_VXATTR, which the
// ceph.dir.casesensitive and ceph.dir.normalization vxattrs need) to the
// native identity of that fixture only. charmap-set marks the empty charmap
// directory case-insensitive with the plan's normalization (one-shot,
// journaled); charmap-names lists it with the stock client and stats each
// planned other spelling; charmap-create creates the plan's native entry
// (one-shot, journaled). The stock client wraps every name it sends and
// unwraps every name it lists (Client::_wrap_name, Client::_unwrap_name), so
// these actions report names as the stock client presents them. Restarts use
// the completion fixture's own restart actions. Native behavior relied upon:
// mds/Server.cc (handle_set_vxattr charmap, handle_client_getvxattr,
// can_handle_charmap, handle_client_mkdir charmap inheritance), client/Client.cc
// (_wrap_name, _unwrap_name, readdir) and pybind/cephfs/cephfs.pyx at
// mdsFreshSource (LGPL-2.1 references; no code copied).

// capMDSCharmap admits one test alone on a fresh completion fixture with the
// charmap controls.
var capMDSCharmap = defineExclusiveCapability(fixtureMDSCephFSCompletion, "mds-charmap",
	"run one test alone on a fresh MDS CephFS completion fixture with the charmap controls",
	"MDS_FRESH=1 MDS_CEPHFS_COMPLETION=1", nil)

var mdsCephFSCharmapActions = []string{"charmap-set", "charmap-names", "charmap-create"}

// admitMDSCephFSCharmap refuses a charmap action without its capability.
func admitMDSCephFSCharmap(action string, p profile) error {
	if slices.Contains(mdsCephFSCharmapActions, action) && !p.admits(capMDSCharmap) {
		return errors.New("MDS CephFS completion charmap action requires capability " + capMDSCharmap.name)
	}
	return nil
}

// mdsCephFSCompletionNativeCapsFor adds "p" to the native identity of a
// charmap run; every other completion run keeps its caps.
func mdsCephFSCompletionNativeCapsFor(p profile, fsName string) ceph.ClientCaps {
	caps := mdsCephFSCompletionNativeCaps(fsName)
	if p.admits(capMDSCharmap) {
		caps.MDS = "allow rwps fsname=" + fsName
	}
	return caps
}

// mdsCephFSCharmapEntry is one planned entry: Name is its original spelling
// (the alternate name a client sends, and the name the stock client lists in
// a case-insensitive directory) and Probe another spelling the stock client
// resolves to the same entry.
type mdsCephFSCharmapEntry struct {
	Name  string `json:"name"`
	Probe string `json:"probe"`
}

// mdsCephFSCharmapPlan is the fixed charmap namespace of one run: the child
// directory, its normalization, the entries the test creates (in order) and
// the one the native oracle creates.
type mdsCephFSCharmapPlan struct {
	Dir           string                  `json:"dir"`
	Normalization string                  `json:"normalization"`
	Entries       []mdsCephFSCharmapEntry `json:"entries"`
	Native        mdsCephFSCharmapEntry   `json:"native"`
}

// mdsCephFSCharmapPlanFor is fixed. The originals differ from their NFKC
// normalized and case-folded form by fullwidth letters (compatibility
// decompositions), a combining acute accent (canonical composition) and
// letter case; each probe differs from its original in at least one of those
// and from the folded form in case.
func mdsCephFSCharmapPlanFor() mdsCephFSCharmapPlan {
	return mdsCephFSCharmapPlan{Dir: "charmap", Normalization: "nfkc", Entries: []mdsCephFSCharmapEntry{
		{"\uff33\uff55\uff42-\uff24\uff49\uff52", "SUB-DIR"},
		{"Cafe\u0301-File", "CAF\u00c9-FILE"},
		{"\uff2e\uff4f\uff44\uff45", "NODE"},
		{"\uff33\uff59\uff4d-\uff2c\uff49\uff4e\uff4b", "SYM-link"},
		{"\uff28\uff41\uff52\uff44-\uff2c\uff49\uff4e\uff4b", "HARD-LINK"},
		{"\uff32\uff45\uff4e\uff41\uff4d\uff45\uff44", "RENAMED"},
		{"\uff21\uff46\uff54\uff45\uff52-\uff32\uff45\uff53\uff54\uff41\uff52\uff54", "AFTER-RESTART"},
	}, Native: mdsCephFSCharmapEntry{"\uff2e\uff41\uff54\uff49\uff56\uff45-\uff25\uff4e\uff54\uff52\uff59", "NATIVE-entry"}}
}

// charmapPath is the charmap directory, a direct child of the owned directory.
func (p mdsCephFSCompletionPlan) charmapPath() (string, error) {
	root, err := p.path("dir")
	if err != nil || p.Charmap.Dir != "charmap" {
		return "", errors.Join(err, errors.New("MDS CephFS charmap directory is not owned"))
	}
	return root + "/" + p.Charmap.Dir, nil
}

// probes lists the closed spellings charmap-names stats: every planned
// original and probe.
func (p mdsCephFSCharmapPlan) probes() []string {
	var names []string
	for _, e := range append(slices.Clone(p.Entries), p.Native) {
		names = append(names, e.Name, e.Probe)
	}
	return names
}

// mdsCephFSCharmapNative is the output of one charmap action.
type mdsCephFSCharmapNative struct {
	Action         string                   `json:"action"`
	Path           string                   `json:"path"`
	NativeGlobalID uint64                   `json:"native_global_id"`
	Stat           *mdsCephFSStat           `json:"stat,omitempty"`
	Charmap        *mdsCephFSCharmap        `json:"charmap,omitempty"`
	Entries        []mdsCephFSCharmapListed `json:"entries,omitempty"`
	Probes         []mdsCephFSCharmapProbe  `json:"probes,omitempty"`
}

// mdsCephFSCharmapListed is one listed entry as the stock client presents
// it, with lstat, a subdirectory's charmap and a symlink's target.
type mdsCephFSCharmapListed struct {
	Name      string            `json:"name"`
	DIno      uint64            `json:"d_ino"`
	Stat      mdsCephFSStat     `json:"stat"`
	Charmap   *mdsCephFSCharmap `json:"charmap,omitempty"`
	TargetHex string            `json:"target_hex,omitempty"`
}

type mdsCephFSCharmapProbe struct {
	Name string        `json:"name"`
	Stat mdsCephFSStat `json:"stat"`
}

// The charmap driver. It admits only the charmap child of the owned
// directory and names without a separator; every stat is an lstat.
// Source: pybind/cephfs/cephfs.pyx at mdsFreshSource; independent driver.
const mdsCephFSCharmapNativePython = `
import cephfs, json, os, sys
identity, fsname, directory, child, action = sys.argv[1:6]
extra = sys.argv[6:]
if not directory.startswith('ceph-msgr-completion-') or any(c in directory for c in '/. ') or child != 'charmap':
    raise ValueError('path outside the owned directory')
def name_ok(n):
    return n not in ('', '.', '..') and '/' not in n and '\x00' not in n and len(n.encode('utf-8')) <= 255
base = '/' + directory + '/' + child
MASK = 0x30f
NOFOLLOW = 0x100
fs = cephfs.LibCephFS(conffile='/tmp/ceph-msgr-test/ceph.conf', auth_id=identity.removeprefix('client.'), conf={'key': NATIVE_KEY})
def stat(p):
    try:
        s = fs.statx(p, MASK, NOFOLLOW)
    except cephfs.ObjectNotFound as e:
        return {'exists': False, 'errno': abs(e.errno), 'ino': 0, 'mode': 0, 'uid': 0, 'gid': 0, 'nlink': 0, 'size': 0}
    return {'exists': True, 'errno': 0, 'ino': s['ino'], 'mode': s['mode'], 'uid': s['uid'], 'gid': s['gid'], 'nlink': s['nlink'], 'size': s['size']}
def charmap(p):
    try:
        return {name: fs.getxattr(p, 'ceph.dir.' + name, 64).decode('utf-8') for name in ('casesensitive', 'normalization', 'encoding')}
    except cephfs.NoData:
        return None
out = {'action': action, 'path': base}
try:
    fs.mount(filesystem_name=fsname)
    out['native_global_id'] = fs.get_instance_id()
    if action == 'set' and len(extra) == 1 and extra[0] in ('nfd', 'nfc', 'nfkd', 'nfkc'):
        fs.setxattr(base, 'ceph.dir.casesensitive', b'0', 0)
        fs.setxattr(base, 'ceph.dir.normalization', extra[0].encode(), 0)
        out['charmap'] = charmap(base)
        out['stat'] = stat(base)
    elif action == 'names' and extra and all(name_ok(n) for n in extra):
        out['charmap'] = charmap(base)
        out['stat'] = stat(base)
        out['entries'] = []
        handle = fs.opendir(base)
        try:
            while True:
                entry = fs.readdir(handle)
                if entry is None:
                    break
                name = entry.d_name.decode('utf-8')
                if name in ('.', '..'):
                    continue
                listed = {'name': name, 'd_ino': entry.d_ino, 'stat': stat(base + '/' + name)}
                kind = listed['stat']['mode'] & 0o170000
                if kind == 0o040000:
                    listed['charmap'] = charmap(base + '/' + name)
                elif kind == 0o120000:
                    listed['target_hex'] = fs.readlink(base + '/' + name, 4096).hex()
                out['entries'].append(listed)
        finally:
            fs.closedir(handle)
        out['probes'] = [{'name': n, 'stat': stat(base + '/' + n)} for n in extra]
    elif action == 'create' and len(extra) == 1 and name_ok(extra[0]):
        fd = fs.open(base + '/' + extra[0], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        fs.close(fd)
        out['stat'] = stat(base + '/' + extra[0])
    else:
        raise ValueError('native CephFS charmap action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSCharmapNative accepts exactly the shape of one charmap
// action. The values are evidence for the test to compare; here each is
// present, bounded and of the right kind.
func decodeMDSCephFSCharmapNative(action, path string, plan mdsCephFSCharmapPlan, data []byte) (mdsCephFSCharmapNative, error) {
	var result mdsCephFSCharmapNative
	if len(data) == 0 || len(data) > mdsCephFSJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS charmap output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSCharmapNative{}, errors.New("native CephFS charmap output identity differs")
	}
	charmap := func(c *mdsCephFSCharmap) bool {
		return c != nil && c.CaseSensitive != "" && c.Normalization != "" && c.Encoding != "" && len(c.CaseSensitive)+len(c.Normalization)+len(c.Encoding) <= 192
	}
	dir := func(s *mdsCephFSStat) bool {
		return s != nil && s.Exists && validMDSCephFSStat(*s) && s.Mode&0o170000 == 0o040000
	}
	ok := false
	switch action {
	case "set":
		ok = charmap(result.Charmap) && dir(result.Stat) && result.Entries == nil && result.Probes == nil
	case "names":
		probes := plan.probes()
		ok = charmap(result.Charmap) && dir(result.Stat) && len(result.Entries) <= len(plan.Entries)+1 && len(result.Probes) == len(probes)
		seen := map[string]bool{}
		for _, e := range result.Entries {
			ok = ok && utf8.ValidString(e.Name) && e.Name != "" && !seen[e.Name] && e.DIno != 0 && validMDSCephFSStat(e.Stat) && e.Stat.Exists &&
				(e.Charmap == nil || charmap(e.Charmap)) && len(e.TargetHex) <= 8192
			seen[e.Name] = true
		}
		for i, p := range result.Probes {
			ok = ok && p.Name == probes[i] && validMDSCephFSStat(p.Stat)
		}
	case "create":
		ok = result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.Stat.Mode&0o170000 == 0o100000 && result.Charmap == nil && result.Entries == nil
	}
	if !ok {
		return mdsCephFSCharmapNative{}, fmt.Errorf("native CephFS charmap %s output shape differs", action)
	}
	return result, nil
}

// mdsCephFSCharmapAct runs one charmap action. charmap-set and
// charmap-create are one-shot and journaled before the process starts; an
// uncertain one is never replayed.
func (c *fixtureCluster) mdsCephFSCharmapAct(ctx context.Context, id int64, action mdsCephFSCompletionAction, result *mdsCephFSCompletionResult) error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	if err := admitMDSCephFSCharmap(action.name, c.profile); err != nil {
		return err
	}
	if err := x.quiesced(x.current.GID); err != nil {
		return err
	}
	path, err := x.plan.charmapPath()
	if err != nil {
		return err
	}
	plan := x.plan.Charmap
	driver, _ := strings.CutPrefix(action.name, "charmap-")
	var extra []string
	var mutation *mdsCephFSMutation
	switch action.name {
	case "charmap-set":
		extra = []string{plan.Normalization}
	case "charmap-names":
		extra = plan.probes()
	case "charmap-create":
		extra = []string{plan.Native.Name}
	}
	if action.name != "charmap-names" {
		kind := "native-" + action.name
		if x.mutated(kind) {
			return fmt.Errorf("MDS CephFS completion %s was already issued", kind)
		}
		mutation = &mdsCephFSMutation{Kind: kind, RequestID: id, Target: plan.Dir, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
	}
	argv := append([]string{"python3", "-c", mdsCephFSCharmapNativePython, "client.mds-cephfs-completion-native-" + s.token, s.name, x.plan.Directory, plan.Dir, driver}, extra...)
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 45 * time.Second, MaxStdout: mdsCephFSJSONLimit, MaxStderr: 16 << 10,
		Label: "owned native CephFS charmap " + driver}, x.native)
	result.Process = &process
	var native mdsCephFSCharmapNative
	if err == nil && (!process.Complete() || process.ExitCode != 0) {
		err = fmt.Errorf("native CephFS charmap %s did not complete (exit %d, timed out %v)", driver, process.ExitCode, process.TimedOut)
	}
	if err == nil {
		native, err = decodeMDSCephFSCharmapNative(driver, path, plan, bytes.TrimSpace(process.Stdout))
	}
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		return err
	}
	result.Charmap = &native
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", native.NativeGlobalID
		return c.mdsCephFSCompletionJournal()
	}
	return nil
}

// saveMetadata (diagnostics.go) collects the public receipt the CephFS
// charmap test writes beside this file's native readback when it exists.
var _ = collectDiagnosticFiles(
	"mds-cephfs-charmap-results.json",
)
