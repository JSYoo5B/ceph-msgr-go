package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

const mdsCommandVersionJSON = `{"prefix":"version","format":"json"}`
const mdsCommandInvalidJSON = `{"prefix":"ceph_msgr_unknown_mds_command","format":"json"}`
const mdsCommandInputHex = "00ff6d647300"

type mdsCommandsScope struct {
	client        *ceph.ClientConfig
	lastRequestID int64
}

func mdsCommandsCaps() ceph.ClientCaps {
	// MDSDaemon::handle_command checks allow_all(), independently of fsname.
	// This disposable command identity is deliberately not filesystem-bound.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow *"}
}

func (c *fixtureCluster) prepareMDSCommands(ctx context.Context, out string) error {
	if c.profile.mdsCommands != "1" || c.profile.mdsFresh != "1" || c.profile.osdProfile != "legacy-v6" || c.mdsFresh == nil || c.mdsCommands != nil {
		return errors.New("invalid MDS commands preparation scope")
	}
	s := c.mdsFresh
	x := &mdsCommandsScope{}
	c.mdsCommands = x
	name := "mds-commands-" + s.token
	var err error
	x.client, err = c.cluster.CreateClient(ctx, name, mdsCommandsCaps())
	if err != nil {
		return fmt.Errorf("create dedicated MDS command identity: %w", err)
	}
	_, keyring, err := x.client.ConnectionConfig()
	if err != nil {
		return err
	}
	var key string
	for _, line := range strings.Split(string(keyring), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == "key" {
			if key != "" {
				return errors.New("dedicated MDS command keyring has multiple keys")
			}
			key = strings.TrimSpace(v)
		}
	}
	if key == "" {
		return errors.New("dedicated MDS command key missing")
	}
	if err := os.WriteFile(filepath.Join(out, "mds-commands.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-commands.keyring"), keyring, 0600); err != nil {
		return err
	}
	c.profile.mdsCommandsIdentity = "client." + name
	return writeMDSFreshJSON(out, "mds-commands-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID,
		"fs_name": s.name, "mds_gid": s.oracle.MDS.GID, "client_identity": c.profile.mdsCommandsIdentity,
		"readonly_identity": s.oracle.ClientIdentity, "command_json": map[string]string{"version": mdsCommandVersionJSON, "invalid": mdsCommandInvalidJSON}, "input_hex": mdsCommandInputHex,
	})
}

func parseMDSCommandsObservation(fields []string, token string, previousID int64, previousFull, previousReadOnly uint64) (int64, []uint64, error) {
	if len(fields) != 4 {
		return 0, nil, errors.New("MDS command observer requires exactly two client GlobalIDs")
	}
	id, full, err := parseMDSFreshObservation(fields[:3], token, previousID, previousFull)
	other, otherErr := strconv.ParseUint(fields[3], 10, 64)
	if err != nil || otherErr != nil || other == 0 || other == full || strconv.FormatUint(other, 10) != fields[3] || previousReadOnly != 0 && other != previousReadOnly {
		return 0, nil, errors.New("MDS command observer client pair differs")
	}
	return id, []uint64{full, other}, nil
}

type mdsNativeCommandAction struct {
	command, identity, keyring string
}

func parseMDSNativeCommand(fields []string, token string, previousID int64) (int64, mdsNativeCommandAction, error) {
	var action mdsNativeCommandAction
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned native MDS command scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, action, errors.New("invalid native MDS command token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= previousID || id <= 0 || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid native MDS command request identity")
	}
	action.identity, action.keyring = "client.mds-commands-"+token, "/out/mds-commands.keyring"
	switch fields[2] {
	case "version":
		action.command = mdsCommandVersionJSON
	case "invalid":
		action.command = mdsCommandInvalidJSON
	case "readonly-version":
		action.command = mdsCommandVersionJSON
		action.identity, action.keyring = "client.mds-fresh-"+token, "/out/mds-fresh.keyring"
	default:
		return 0, mdsNativeCommandAction{}, errors.New("native MDS command action is not admitted")
	}
	return id, action, nil
}

// The pinned ceph_argparse mds route likewise initializes LibCephFS and calls
// mds_command without mounting. These fixed read-only commands preserve the
// signed code, raw data and ASCII status from the native API. They never send
// a CephFS OPEN. Source: pybind/{ceph_argparse.py,cephfs/cephfs.pyx} and
// client/Client.cc at mdsFreshSource; independent driver, no code copied.
const mdsNativeCommandPython = `
import cephfs, json, sys
identity, keyring, gid, command = sys.argv[1:]
allowed = ('{"prefix":"version","format":"json"}', '{"prefix":"ceph_msgr_unknown_mds_command","format":"json"}')
if command not in allowed or not gid.isdecimal() or int(gid) <= 0:
    raise ValueError('invalid fixed native MDS command')
filesystem = cephfs.LibCephFS(conffile='/tmp/ceph-msgr-test/ceph.conf', auth_id=identity.removeprefix('client.'), conf={'keyring':keyring})
try:
    filesystem.init()
    code, data, status = filesystem.mds_command(gid, command, bytes.fromhex('00ff6d647300'))
    print(json.dumps({'code':code, 'data_hex':data.hex(), 'status':status, 'status_utf8_hex':status.encode('utf-8').hex(), 'native_global_id':filesystem.get_instance_id()}))
finally:
    filesystem.shutdown()
`

type mdsNativeCommandResult struct {
	Source         string                    `json:"source_commit"`
	Token          string                    `json:"token"`
	RequestID      int64                     `json:"request_id"`
	FSID           string                    `json:"fsid"`
	FSCID          int64                     `json:"fscid"`
	FSName         string                    `json:"fs_name"`
	MDSGID         uint64                    `json:"mds_gid"`
	Identity       string                    `json:"identity"`
	Command        string                    `json:"command_json"`
	Input          string                    `json:"input_hex"`
	Code           int32                     `json:"code"`
	Data           string                    `json:"data_hex"`
	Status         string                    `json:"status"`
	StatusHex      string                    `json:"status_utf8_hex"`
	NativeGlobalID uint64                    `json:"native_global_id"`
	Complete       bool                      `json:"complete"`
	Process        *ceph.NativeCommandResult `json:"native_process,omitempty"`
	Error          string                    `json:"error,omitempty"`
}

func decodeMDSNativeCommandOutput(data []byte, result *mdsNativeCommandResult) error {
	if len(data) == 0 || len(data) > 64<<10 {
		return errors.New("native MDS command output exceeds its bound")
	}
	var output struct {
		Code           *int32  `json:"code"`
		Data           *string `json:"data_hex"`
		Status         *string `json:"status"`
		StatusHex      *string `json:"status_utf8_hex"`
		NativeGlobalID uint64  `json:"native_global_id"`
	}
	if json.Unmarshal(data, &output) != nil || output.Code == nil || output.Data == nil || output.Status == nil || output.StatusHex == nil || output.NativeGlobalID == 0 || len(*output.Data) > 32<<10 || len(*output.Status) > 4096 {
		return errors.New("native MDS command output identity or shape differs")
	}
	if _, err := hex.DecodeString(*output.Data); err != nil {
		return errors.New("native MDS command data is not binary-safe hexadecimal")
	}
	status, err := hex.DecodeString(*output.StatusHex)
	if err != nil || string(status) != *output.Status {
		return errors.New("native MDS command status bytes differ")
	}
	result.Code, result.Data, result.Status, result.StatusHex, result.NativeGlobalID = *output.Code, *output.Data, *output.Status, *output.StatusHex, output.NativeGlobalID
	return nil
}

func (c *fixtureCluster) nativeMDSCommands(ctx context.Context, out string, fields []string) (resultErr error) {
	s, commandScope := c.mdsFresh, c.mdsCommands
	if c.profile.mdsCommands != "1" || c.profile.mdsFresh != "1" || c.profile.osdProfile != "legacy-v6" || s == nil || s.fs == nil || commandScope == nil || commandScope.client == nil || s.out != out {
		return errors.New("invalid owned native MDS command fixture")
	}
	ctx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("native MDS command requested after cleanup began")
	}
	id, action, err := parseMDSNativeCommand(fields, s.token, commandScope.lastRequestID)
	if err != nil {
		return err
	}
	commandScope.lastRequestID = id
	receipt := mdsNativeCommandResult{Source: mdsFreshSource, Token: s.token, RequestID: id, FSID: s.oracle.FSID, FSCID: s.oracle.FSCID, FSName: s.name, MDSGID: s.oracle.MDS.GID, Identity: action.identity, Command: action.command, Input: mdsCommandInputHex}
	defer func() {
		if resultErr != nil {
			receipt.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-commands-native-result.json", receipt))
	}()
	current, err := c.readMDSFreshOracle(ctx)
	if err != nil {
		return err
	}
	if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || !reflect.DeepEqual(current.MDS, s.oracle.MDS) || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
		return errors.New("native MDS command target changed before submission")
	}
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: []string{"python3", "-c", mdsNativeCommandPython, action.identity, action.keyring, strconv.FormatUint(current.MDS.GID, 10), action.command}, Timeout: 10 * time.Second, MaxStdout: 64 << 10, MaxStderr: 4096, Label: "fixed read-only native MDS command"})
	receipt.Process = &process
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return errors.Join(err, errors.New("native MDS command process did not produce a complete receipt"))
	}
	if err := decodeMDSNativeCommandOutput(process.Stdout, &receipt); err != nil {
		return err
	}
	receipt.Complete = true
	return nil
}
