package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Helpers of the backend-fault checkpoint (TestRGWNativeBackendFaultIntegration).
// Everything here is cluster-free except the bounded native command wrapper;
// the parsers and predicates have unit tests in rgw_native_fault_unit_test.go.

// The outcome classes of one S3 request that overlapped a backend fault. The
// class records only what the HTTP exchange returned; the native postcondition
// is a separate observation, and success needs both.
const (
	rgwNativeSucceeded    = "succeeded"
	rgwNativeFailedStatus = "failed_status"
	rgwNativeUncertain    = "uncertain"
)

// rgwNativeFaultPayload is the deterministic 1,031-byte input of one object
// of the backend-fault checkpoint; tools/rgw_native_gate_backend_fault.py mirrors it.
func rgwNativeFaultPayload(multiplier, offset int) []byte {
	payload := make([]byte, 1031)
	for i := range payload {
		payload[i] = byte(i*multiplier + offset)
	}
	return payload
}

// rgwNativeOutcome is the classification of one overlapping request.
type rgwNativeOutcome struct {
	Request string `json:"request"`
	Class   string `json:"class"`
	Status  int    `json:"status,omitempty"`
	// Detail explains an uncertain outcome without any S3 error body.
	Detail string `json:"detail,omitempty"`
}

// rgwNativeClassify classifies one exchange. Any error, including a status
// whose body could not be read completely, leaves the server outcome unknown;
// only a complete response with the expected status is a success, and any
// other complete response is a failure with that status.
func rgwNativeClassify(request string, status int, err error, want int) rgwNativeOutcome {
	switch {
	case err != nil:
		detail := "no complete response; the server outcome is unknown and the request is not replayed"
		if status != 0 {
			detail = fmt.Sprintf("status %d without a complete response; the server outcome is unknown and the request is not replayed", status)
		}
		return rgwNativeOutcome{Request: request, Class: rgwNativeUncertain, Detail: detail}
	case status == want:
		return rgwNativeOutcome{Request: request, Class: rgwNativeSucceeded, Status: status}
	default:
		return rgwNativeOutcome{Request: request, Class: rgwNativeFailedStatus, Status: status}
	}
}

// rgwNativeInflight is one S3 request issued once in the background, so that
// it can be outstanding while a backend daemon is stopped.
type rgwNativeInflight struct {
	name, method, path string
	want               int
	done               chan struct{}
	status             int
	body               []byte
	err                error
}

func rgwNativeIssue(ctx context.Context, s3 rgwNativeS3, name, method, path string, payload []byte, want int) *rgwNativeInflight {
	request := &rgwNativeInflight{name: name, method: method, path: path, want: want, done: make(chan struct{})}
	go func() {
		defer close(request.done)
		request.status, request.body, request.err = s3.exchange(ctx, method, path, payload, rgwNativeDefaultResponseLimit)
	}()
	return request
}

// pending reports whether the request has not returned yet.
func (r *rgwNativeInflight) pending() bool {
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

// outcome waits for the request, at most until ctx ends, and classifies it.
// A wait that ends first is uncertain; the request is never reissued.
func (r *rgwNativeInflight) outcome(ctx context.Context) rgwNativeOutcome {
	select {
	case <-r.done:
		return rgwNativeClassify(r.name, r.status, r.err, r.want)
	case <-ctx.Done():
		return rgwNativeOutcome{Request: r.name, Class: rgwNativeUncertain, Detail: "the local wait ended before a response; the server outcome is unknown and the request is not replayed"}
	}
}

// rgwNativeDaemonState is one daemon's identity in a native map.
type rgwNativeDaemonState struct {
	Epoch     uint64 `json:"epoch"`
	Up        bool   `json:"up"`
	UpFrom    uint64 `json:"up_from,omitempty"`
	GID       uint64 `json:"gid,omitempty"`
	Nonce     uint64 `json:"nonce"`
	Available bool   `json:"available,omitempty"`
}

type rgwNativeAddrvec struct {
	Addrvec []struct {
		Nonce *uint64 `json:"nonce"`
	} `json:"addrvec"`
}

// nonce returns the one nonce every address of the vector carries.
func (v rgwNativeAddrvec) nonce() (uint64, error) {
	var nonce uint64
	if len(v.Addrvec) == 0 || len(v.Addrvec) > 4 {
		return 0, errors.New("address vector outside its bound")
	}
	for _, addr := range v.Addrvec {
		if addr.Nonce == nil || *addr.Nonce == 0 || (nonce != 0 && *addr.Nonce != nonce) {
			return 0, errors.New("address vector without one nonzero nonce")
		}
		nonce = *addr.Nonce
	}
	return nonce, nil
}

// rgwNativeOSDState reads osd.<id> from `ceph osd dump --format json`: the
// map epoch, whether the OSD is up, the epoch it came up in and the nonce of
// its public (client) Messenger. A down OSD keeps its last addresses.
func rgwNativeOSDState(raw []byte, id int) (rgwNativeDaemonState, error) {
	var dump struct {
		Epoch *uint64 `json:"epoch"`
		OSDs  []struct {
			OSD    *int             `json:"osd"`
			Up     *int             `json:"up"`
			UpFrom uint64           `json:"up_from"`
			Public rgwNativeAddrvec `json:"public_addrs"`
		} `json:"osds"`
	}
	if len(raw) > 4<<20 || json.Unmarshal(raw, &dump) != nil || dump.Epoch == nil || len(dump.OSDs) > 64 {
		return rgwNativeDaemonState{}, errors.New("invalid native OSD map dump")
	}
	for _, osd := range dump.OSDs {
		if osd.OSD == nil || *osd.OSD != id {
			continue
		}
		if osd.Up == nil || (*osd.Up != 0 && *osd.Up != 1) {
			return rgwNativeDaemonState{}, errors.New("native OSD map entry without an up state")
		}
		nonce, err := osd.Public.nonce()
		if err != nil {
			return rgwNativeDaemonState{}, fmt.Errorf("native OSD map entry: %w", err)
		}
		return rgwNativeDaemonState{Epoch: *dump.Epoch, Up: *osd.Up == 1, UpFrom: osd.UpFrom, Nonce: nonce}, nil
	}
	return rgwNativeDaemonState{}, fmt.Errorf("native OSD map has no osd.%d", id)
}

// rgwNativeMgrState reads the active manager from `ceph mgr dump --format
// json`: its GID, the nonce of its addresses and whether it is available.
func rgwNativeMgrState(raw []byte) (rgwNativeDaemonState, error) {
	var dump struct {
		Epoch     *uint64          `json:"epoch"`
		GID       uint64           `json:"active_gid"`
		Available *bool            `json:"available"`
		Addrs     rgwNativeAddrvec `json:"active_addrs"`
	}
	if len(raw) > 4<<20 || json.Unmarshal(raw, &dump) != nil || dump.Epoch == nil || dump.Available == nil {
		return rgwNativeDaemonState{}, errors.New("invalid native manager map dump")
	}
	state := rgwNativeDaemonState{Epoch: *dump.Epoch, Available: *dump.Available, GID: dump.GID}
	if !state.Available {
		return state, nil
	}
	nonce, err := dump.Addrs.nonce()
	if err != nil || dump.GID == 0 {
		return rgwNativeDaemonState{}, fmt.Errorf("available native manager without an identity: %v", err)
	}
	state.Nonce = nonce
	return state, nil
}

// rgwNativeServiceGIDs reads the GIDs of the rgw daemons of `ceph service
// dump --format json`. The daemons object also holds a summary string.
func rgwNativeServiceGIDs(raw []byte) ([]uint64, error) {
	var dump struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if len(raw) > 4<<20 || json.Unmarshal(raw, &dump) != nil || dump.Services == nil {
		return nil, errors.New("invalid native service map dump")
	}
	var gids []uint64
	for name, value := range dump.Services["rgw"].Daemons {
		if name == "summary" {
			continue
		}
		var daemon struct {
			GID uint64 `json:"gid"`
		}
		if json.Unmarshal(value, &daemon) != nil || daemon.GID == 0 {
			return nil, errors.New("native rgw service daemon without a GID")
		}
		gids = append(gids, daemon.GID)
	}
	slices.Sort(gids)
	return gids, nil
}

// rgwNativeServiceStamps reads, from `ceph service status --format json`,
// the status stamp of every rgw daemon the active manager has. Pinned
// DaemonServer::got_service_map gives a new manager process a daemon state
// with a zero stamp, printed as "0.000000"; only an MMgrReport that carries
// daemon status sets it (DaemonServer::handle_report). So a nonzero stamp
// from a restarted manager shows that the gateway reported to that process.
func rgwNativeServiceStamps(raw []byte) (map[string]string, error) {
	var dump map[string]map[string]struct {
		Stamp *string `json:"status_stamp"`
	}
	if len(raw) > 4<<20 || json.Unmarshal(raw, &dump) != nil || dump == nil {
		return nil, errors.New("invalid native service status")
	}
	stamps := map[string]string{}
	for name, daemon := range dump["rgw"] {
		if daemon.Stamp == nil {
			return nil, errors.New("native rgw service status without a stamp")
		}
		stamps[name] = *daemon.Stamp
	}
	return stamps, nil
}

var rgwNativeZeroStamp = regexp.MustCompile(`^0\.0+$`)

// rgwNativeReported reports whether a service status stamp is set.
func rgwNativeReported(stamp string) bool {
	return stamp != "" && !rgwNativeZeroStamp.MatchString(stamp)
}

// rgwNativePeer is one connection of a gateway radosclient Messenger: the
// local client identity, the peer's entity type and ID, the nonces of the
// peer's addresses and whether the connection is established. No address is
// retained.
type rgwNativePeer struct {
	Client    string   `json:"client"`
	Type      string   `json:"type"`
	ID        int64    `json:"id"`
	Nonces    []uint64 `json:"nonces"`
	State     string   `json:"state"`
	Connected bool     `json:"connected"`
}

// rgwNativeConnectedTo lists the gateway clients with an established
// connection to a peer of type kind whose addresses carry nonce.
func rgwNativeConnectedTo(peers []rgwNativePeer, kind string, nonce uint64) []string {
	var clients []string
	for _, peer := range peers {
		if peer.Type == kind && peer.Connected && peer.State == "STATE_CONNECTION_ESTABLISHED" && slices.Contains(peer.Nonces, nonce) && !slices.Contains(clients, peer.Client) {
			clients = append(clients, peer.Client)
		}
	}
	slices.Sort(clients)
	return clients
}

// rgwNativeParsePeers decodes the output of rgwNativeGatewayPeerInspector.
func rgwNativeParsePeers(raw []byte) ([]rgwNativePeer, error) {
	var dump struct {
		Candidates int             `json:"socket_candidates"`
		Peers      []rgwNativePeer `json:"peers"`
	}
	if len(raw) > 4<<20 || json.Unmarshal(raw, &dump) != nil || dump.Candidates == 0 || dump.Candidates > 8 || dump.Peers == nil || len(dump.Peers) > 1024 {
		return nil, errors.New("missing bounded process-owned gateway Messenger connections")
	}
	for _, peer := range dump.Peers {
		if !regexp.MustCompile(`^client\.[1-9][0-9]*$`).MatchString(peer.Client) || peer.Type == "" {
			return nil, errors.New("gateway Messenger connection without a client or peer identity")
		}
	}
	return dump.Peers, nil
}

var (
	rgwNativeReconnectPattern = regexp.MustCompile(`(?:\[|,)watch reconnect cookie [0-9]+ gen [1-9][0-9]*(?:[ ,\]]|$)`)
	rgwNativeRetryPattern     = regexp.MustCompile(` RETRY=([1-9][0-9]*) `)
	rgwNativeReadPattern      = regexp.MustCompile(`(?:\[|,)read 0~([0-9]+)(?:[ ,\]]|$)`)
)

// rgwNativeHeadRead matches a complete READ opcode token at offset 0 whose
// printed length covers an object of size bytes. Pinned RGWRados head reads
// (driver/rados/rgw_rados.cc) request 0~rgw_max_chunk_size, and
// PrimaryLogPG::do_read rewrites the extent length to the bytes it read, so
// the history of an executed read of the whole head prints 0~size; a request
// printed before execution prints the larger requested length. A class call,
// xattr or stat operation, a shorter read or a read at another offset does
// not match.
func rgwNativeHeadRead(operation rgwNativeOperation, size uint64) bool {
	for _, match := range rgwNativeReadPattern.FindAllStringSubmatch(operation.Description, 16) {
		if length, err := strconv.ParseUint(match[1], 10, 64); err == nil && length >= size {
			return true
		}
	}
	return false
}

// rgwNativeWatchReconnect matches a complete WATCH operation with the
// RECONNECT sub-operation (pinned Objecter::_send_linger for a registered
// watch; printed by OSDOp's operator<< and ceph_osd_watch_op_name).
func rgwNativeWatchReconnect(operation rgwNativeOperation) bool {
	return rgwNativeReconnectPattern.MatchString(operation.Description)
}

// rgwNativeRetryAttempt returns the retry attempt MOSDOp::print shows, or 0.
func rgwNativeRetryAttempt(operation rgwNativeOperation) uint64 {
	match := rgwNativeRetryPattern.FindStringSubmatch(operation.Description)
	if match == nil {
		return 0
	}
	attempt, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0
	}
	return attempt
}

// rgwNativeAfterReturnProblems checks operations a restarted OSD process
// received: each must carry a map epoch no older than the epoch the OSD came
// up in, and a TID above every TID its client had before the fault.
func rgwNativeAfterReturnProblems(operations []rgwNativeOperation, upFrom uint64, before map[string]uint64) []string {
	var problems []string
	for _, op := range operations {
		if op.MapEpoch < upFrom {
			problems = append(problems, fmt.Sprintf("%s TID %d carries map epoch %d before the OSD came up in %d", op.Client, op.TID, op.MapEpoch, upFrom))
		}
		if last, seen := before[op.Client]; seen && op.TID <= last {
			problems = append(problems, fmt.Sprintf("%s TID %d is not after its pre-fault TID %d", op.Client, op.TID, last))
		}
	}
	return problems
}

// rgwNativeMaxTIDs returns the largest TID of each client.
func rgwNativeMaxTIDs(operations []rgwNativeOperation) map[string]uint64 {
	result := map[string]uint64{}
	for _, op := range operations {
		result[op.Client] = max(result[op.Client], op.TID)
	}
	return result
}

// rgwNativeTryExec runs one bounded native command and reports a failure
// instead of failing the test, for polling a daemon that is starting.
func rgwNativeTryExec(parent context.Context, container testcontainers.Container, argv ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	code, reader, err := container.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("bounded native command: %w", err)
	}
	output, err := io.ReadAll(io.LimitReader(reader, (32<<20)+1))
	if err != nil || len(output) > 32<<20 || code != 0 {
		return nil, fmt.Errorf("bounded native command exit=%d read_error=%v bytes=%d", code, err, len(output))
	}
	return output, nil
}

// rgwNativeWaitFor polls ready every interval until it reports true, returns
// an error, or bound elapses. It only observes; it never repeats an input.
func rgwNativeWaitFor(parent context.Context, bound, interval time.Duration, what string, ready func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	for {
		done, err := ready(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not observed within %s: %w", what, bound, ctx.Err())
		case <-time.After(interval):
		}
	}
}

// The gateway inspectors select the UNIX admin sockets that the gateway's PID
// 1 holds, as rgwNativeGatewayInspector does, and read only those. They print
// no address, socket path or CLI error text.
const rgwNativeGatewaySockets = `
import glob,json,os,subprocess
held=set()
for fd in glob.glob('/proc/1/fd/*'):
    try:
        held.add(os.readlink(fd))
    except FileNotFoundError:
        pass
paths=[]
for line in open('/proc/net/unix'):
    fields=line.split()
    if len(fields)==8 and 'socket:['+fields[6]+']' in held and fields[7].startswith('/var/run/ceph/') and fields[7].endswith('.asok'):
        paths.append(fields[7])
paths=sorted(set(paths))
def query(path,*args):
    result=subprocess.run(['ceph','--admin-daemon',path,*args],capture_output=True,timeout=3)
    if result.returncode or len(result.stdout)>4*1024*1024:
        raise ValueError('invalid local admin socket response')
    value=json.loads(result.stdout)
    return value.get('status',value) if isinstance(value,dict) else value
`

// rgwNativeGatewayPeerInspector lists every connection of each authenticated
// radosclient Messenger (pinned AsyncMessenger::dump and AsyncConnection::dump):
// peer type and ID, the nonces of the peer's addresses and the connection
// state, keyed by the local client identity.
const rgwNativeGatewayPeerInspector = rgwNativeGatewaySockets + `
peers=[]
for path in paths[:8]:
    try:
        names=query(path,'messenger','dump')['messengers']
        if not isinstance(names,list) or len(names)>32:
            continue
        for name in names:
            if not isinstance(name,str) or not (name=='radosclient' or name.startswith('radosclient-')):
                continue
            value=query(path,'messenger','dump',name)['messenger']
            identity=value['my_name']
            if identity['type']!='client' or not isinstance(identity['num'],int) or not 0<identity['num']<2**63:
                continue
            for entry in value.get('connections',[])[:256]:
                conn=entry['async_connection']
                peer=conn['peer']
                addrs=list(peer.get('addr',{}).get('addrvec',[]))+list(entry.get('addrvec',[]))
                nonces=sorted({a['nonce'] for a in addrs if isinstance(a,dict) and isinstance(a.get('nonce'),int) and a['nonce']>0})
                peers.append(dict(client='client.'+str(identity['num']),type=str(peer['type']),id=int(peer['id']),nonces=nonces,state=str(conn['state']),connected=bool(conn['status']['connected'])))
    except (ValueError,KeyError,TypeError,subprocess.TimeoutExpired):
        pass
print(json.dumps(dict(socket_candidates=len(paths),peers=peers)))
`
