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

// The multi-rank CHARMAP checkpoint of the CephFS multi-rank acceptance
// (two active ranks, not the standby-replay extension) adds one child of the
// owned directory, the charmap directory, which the test pins to rank 1, and
// three closed native actions on it. charmap-set marks the empty directory
// case-insensitive with the plan's normalization and reads the three values
// back; charmap-seed creates the plan's seed subdirectory, because the pinned
// MDBalancer::handle_export_pins exports only a nonempty fragment and a
// charmap can be set only on an empty directory (both one-shot, journaled);
// charmap-names lists it with the stock client, with each entry's lstat and a
// subdirectory's charmap. The stock client
// wraps every name it sends and unwraps every name it lists
// (Client::_wrap_name, Client::_unwrap_name), so these actions report names
// as the stock client presents them. The native identity already has the MDS
// "p" flag for export pins. Native behavior relied upon: mds/Server.cc
// (handle_set_vxattr charmap, handle_client_getvxattr, can_handle_charmap,
// handle_client_mkdir charmap inheritance), mds/CInode.cc (encode_inodestat
// optional metadata), client/Client.cc (_wrap_name, _unwrap_name, readdir)
// and pybind/cephfs/cephfs.pyx at mdsFreshSource (LGPL-2.1 references; no
// code copied).

// mdsCephFSRanksCharmapPlan is the fixed charmap namespace of one run: the
// child directory, its normalization, the original spelling of the seed
// subdirectory the stock client creates on rank 0 and of the entry the
// opt-in session creates on rank 1.
type mdsCephFSRanksCharmapPlan struct {
	Dir           string `json:"dir"`
	Normalization string `json:"normalization"`
	Seed          string `json:"seed"`
	Entry         string `json:"entry"`
}

// mdsCephFSRanksCharmapPlanFor is fixed. Both original spellings use
// fullwidth letters and upper case, so they differ from their NFKC
// case-folded wire names.
func mdsCephFSRanksCharmapPlanFor() mdsCephFSRanksCharmapPlan {
	return mdsCephFSRanksCharmapPlan{Dir: "cmap", Normalization: "nfkc", Seed: "\uff32\uff41\uff4e\uff4b-\uff3a\uff45\uff52\uff4f",
		Entry: "\uff32\uff41\uff4e\uff4b-\uff2f\uff4e\uff45"}
}

func (p mdsCephFSRanksCharmapPlan) valid() bool {
	name := func(n string) bool {
		return utf8.ValidString(n) && n != "" && !strings.ContainsAny(n, "/\x00") && len(n) <= 255
	}
	return p.Dir == "cmap" && p.Normalization == "nfkc" && name(p.Seed) && name(p.Entry) && p.Seed != p.Entry
}

// The ranks charmap driver. It admits only the charmap child of the owned
// directory; every stat is an lstat.
// Source: pybind/cephfs/cephfs.pyx at mdsFreshSource; independent driver.
const mdsCephFSRanksCharmapNativePython = `
import cephfs, json, sys
identity, fsname, directory, child, action = sys.argv[1:6]
extra = sys.argv[6:]
if not directory.startswith('ceph-msgr-ranks-') or any(c in directory for c in '/. ') or child != 'cmap':
    raise ValueError('path outside the owned directory')
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
    elif action == 'seed' and len(extra) == 1 and extra[0] not in ('', '.', '..') and '/' not in extra[0] and len(extra[0].encode('utf-8')) <= 255:
        fs.mkdir(base + '/' + extra[0], 0o755)
        out['charmap'] = charmap(base)
        out['stat'] = stat(base)
        out['entries'] = [{'name': extra[0], 'd_ino': 1, 'stat': stat(base + '/' + extra[0]), 'charmap': charmap(base + '/' + extra[0])}]
    elif action == 'names' and not extra:
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
                if len(out['entries']) == 8:
                    raise ValueError('charmap directory listing exceeds its bound')
                listed = {'name': name, 'd_ino': entry.d_ino, 'stat': stat(base + '/' + name)}
                if listed['stat']['mode'] & 0o170000 == 0o040000:
                    listed['charmap'] = charmap(base + '/' + name)
                out['entries'].append(listed)
        finally:
            fs.closedir(handle)
    else:
        raise ValueError('native CephFS ranks charmap action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSRanksCharmapNative accepts exactly the shape of one action.
func decodeMDSCephFSRanksCharmapNative(action, path string, data []byte) (mdsCephFSCharmapNative, error) {
	var result mdsCephFSCharmapNative
	if len(data) == 0 || len(data) > mdsCephFSRanksJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS ranks charmap output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSCharmapNative{}, errors.New("native CephFS ranks charmap output identity differs")
	}
	charmap := func(c *mdsCephFSCharmap) bool {
		return c != nil && c.CaseSensitive != "" && c.Normalization != "" && c.Encoding != "" && len(c.CaseSensitive)+len(c.Normalization)+len(c.Encoding) <= 192
	}
	ok := charmap(result.Charmap) && result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.Stat.Mode&0o170000 == 0o040000 && result.Probes == nil
	switch action {
	case "set":
		ok = ok && result.Entries == nil
	case "seed":
		ok = ok && len(result.Entries) == 1 && result.Entries[0].Stat.Exists && validMDSCephFSStat(result.Entries[0].Stat) &&
			result.Entries[0].Stat.Mode&0o170000 == 0o040000 && charmap(result.Entries[0].Charmap)
	case "names":
		ok = ok && len(result.Entries) <= 8
		seen := map[string]bool{}
		for _, e := range result.Entries {
			ok = ok && utf8.ValidString(e.Name) && e.Name != "" && !seen[e.Name] && e.DIno != 0 && validMDSCephFSStat(e.Stat) && e.Stat.Exists &&
				(e.Charmap == nil || charmap(e.Charmap)) && e.TargetHex == ""
			seen[e.Name] = true
		}
	default:
		ok = false
	}
	if !ok {
		return mdsCephFSCharmapNative{}, fmt.Errorf("native CephFS ranks charmap %s output shape differs", action)
	}
	return result, nil
}

// mdsCephFSRanksCharmapAct runs one charmap action. charmap-set is one-shot
// and journaled before the process starts; an uncertain one is never
// replayed.
func (c *fixtureCluster) mdsCephFSRanksCharmapAct(ctx context.Context, id int64, action mdsCephFSRanksAction, result *mdsCephFSRanksResult) error {
	s, x := c.mdsFresh, c.mdsCephFSRanks
	plan := x.plan.Charmap
	if plan == nil || !plan.valid() {
		return errors.New("MDS CephFS ranks charmap actions need the two-rank plan's charmap directory")
	}
	path, err := x.plan.path(plan.Dir)
	if err != nil {
		return err
	}
	driver, _ := strings.CutPrefix(action.name, "charmap-")
	var extra []string
	var mutation *mdsCephFSMutation
	switch action.name {
	case "charmap-set", "charmap-seed":
		kind := "native-" + action.name
		if slices.ContainsFunc(x.charmapMutations, func(m *mdsCephFSMutation) bool { return m.Kind == kind }) {
			return fmt.Errorf("MDS CephFS ranks %s was already issued", action.name)
		}
		extra = []string{plan.Normalization}
		if action.name == "charmap-seed" {
			if !slices.ContainsFunc(x.charmapMutations, func(m *mdsCephFSMutation) bool { return m.Kind == "native-charmap-set" && m.State == "applied" }) {
				return errors.New("MDS CephFS ranks charmap-seed requires an applied charmap-set")
			}
			extra = []string{plan.Seed}
		}
		mutation = &mdsCephFSMutation{Kind: kind, RequestID: id, Target: plan.Dir, State: "intended"}
		x.charmapMutations = append(x.charmapMutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSRanksJournal(); err != nil {
			return err
		}
	}
	argv := append([]string{"python3", "-c", mdsCephFSRanksCharmapNativePython, "client.mds-cephfs-ranks-native-" + s.token, s.name, x.plan.Directory, plan.Dir, driver}, extra...)
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 30 * time.Second, MaxStdout: mdsCephFSRanksJSONLimit, MaxStderr: 16 << 10,
		Label: "owned native CephFS ranks charmap " + driver}, x.native)
	result.Processes = append(result.Processes, &process)
	var native mdsCephFSCharmapNative
	if err == nil && (!process.Complete() || process.ExitCode != 0) {
		err = fmt.Errorf("native CephFS ranks charmap %s did not complete (exit %d, timed out %v)", driver, process.ExitCode, process.TimedOut)
	}
	if err == nil {
		native, err = decodeMDSCephFSRanksCharmapNative(driver, path, bytes.TrimSpace(process.Stdout))
	}
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSRanksJournal())
		}
		return err
	}
	result.Charmap = &native
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", native.NativeGlobalID
		return c.mdsCephFSRanksJournal()
	}
	return nil
}
