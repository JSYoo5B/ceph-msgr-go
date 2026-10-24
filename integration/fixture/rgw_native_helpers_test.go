package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// This fixture-only client accepts the fixed ASCII bucket/key paths below. It
// has no redirect or mutation retry policy and never includes credentials or an
// S3 error body in diagnostics. It is not an S3 API for product consumers.
type rgwNativeS3 struct {
	endpoint, access, secret, region string
	http                             *http.Client
}

// rgwNativeDefaultResponseLimit is the small-response bound of the original
// checkpoint. A larger-object case must request its own explicit allowance
// through requestBounded; the default is never raised globally.
const rgwNativeDefaultResponseLimit = 1 << 20

// rgwNativeMaxResponseLimit caps every explicit allowance.
const rgwNativeMaxResponseLimit = 16 << 20

func rgwNativeHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func (s rgwNativeS3) request(ctx context.Context, method, path string, payload []byte, status int) ([]byte, error) {
	return s.requestBounded(ctx, method, path, payload, status, rgwNativeDefaultResponseLimit)
}

func (s rgwNativeS3) requestBounded(ctx context.Context, method, path string, payload []byte, status int, limit int64) ([]byte, error) {
	if limit <= 0 || limit > rgwNativeMaxResponseLimit {
		return nil, errors.New("RGW checkpoint response allowance outside its fixed bound")
	}
	if !regexp.MustCompile(`^/[a-z0-9-]+(?:/[a-z0-9-]+)?$`).MatchString(path) {
		return nil, errors.New("RGW checkpoint accepts only fixed ASCII bucket/key paths")
	}
	request, err := http.NewRequestWithContext(ctx, method, s.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return nil, errors.New("construct RGW checkpoint request")
	}
	now := time.Now().UTC()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := rgwNativeHash(payload)
	request.Header.Set("X-Amz-Date", stamp)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	const headers = "host;x-amz-content-sha256;x-amz-date"
	canonical := strings.Join([]string{method, path, "", "host:" + request.URL.Host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + stamp + "\n", headers, payloadHash}, "\n")
	scope := day + "/" + s.region + "/s3/aws4_request"
	hmacSHA := func(key []byte, value string) []byte {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write([]byte(value))
		return mac.Sum(nil)
	}
	key := []byte("AWS4" + s.secret)
	for _, value := range []string{day, s.region, "s3", "aws4_request"} {
		key = hmacSHA(key, value)
	}
	signature := hex.EncodeToString(hmacSHA(key, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+rgwNativeHash([]byte(canonical))))
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.access+"/"+scope+", SignedHeaders="+headers+", Signature="+signature)
	response, err := s.http.Do(request)
	if err != nil {
		// A failed local wait can leave an unknown server-side outcome. The
		// caller fails this run and cleans its fixture; it does not replay PUT.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("RGW %s request failed (server outcome may be unknown): %w", method, ctx.Err())
		}
		return nil, fmt.Errorf("RGW %s request failed (server outcome may be unknown)", method)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("RGW %s response read was cancelled (server outcome may be unknown): %w", method, ctx.Err())
		}
		return nil, errors.New("RGW checkpoint response could not be read")
	}
	if int64(len(body)) > limit {
		return nil, errors.New("RGW checkpoint response exceeded its read bound")
	}
	if response.StatusCode != status {
		return nil, fmt.Errorf("RGW %s status %d, want %d", method, response.StatusCode, status)
	}
	return body, nil
}

type rgwNativeOperation struct {
	Client      string   `json:"client"`
	TID         uint64   `json:"tid"`
	PoolID      int64    `json:"pool_id"`
	Namespace   string   `json:"namespace,omitempty"`
	Object      string   `json:"object"`
	PGID        string   `json:"pgid"`
	MapEpoch    uint64   `json:"map_epoch"`
	Description string   `json:"description"`
	Events      []string `json:"events"`
}

// Match native MOSDOp descriptions only when the client identity, request TID,
// complete object name and reached_pg event agree. The observation is server
// reception, not an independent proof of the operation's result or durability.
func rgwNativeHistory(raw []byte, client string, objects map[string]int64) ([]rgwNativeOperation, error) {
	return rgwNativeHistoryMatch(raw, client, func(pool int64, namespace, locator, name string) bool {
		wanted, owned := objects[name]
		return owned && namespace == "" && locator == "" && pool == wanted
	})
}

// rgwNativeHistoryMatch applies the same client/TID/PG/reached_pg joins to any
// object selected by accept. The selector receives the PG pool-checked hobject
// pool, namespace, locator key and name exactly as the native request printed.
func rgwNativeHistoryMatch(raw []byte, client string, accept func(pool int64, namespace, locator, name string) bool) ([]rgwNativeOperation, error) {
	if len(raw) > 32<<20 || !regexp.MustCompile(`^client\.[1-9][0-9]*$`).MatchString(client) {
		return nil, errors.New("invalid RGW native history bound or identity")
	}
	var history struct {
		Ops []struct {
			Description string `json:"description"`
			Data        struct {
				Client struct {
					Client string `json:"client"`
					TID    uint64 `json:"tid"`
				} `json:"client_info"`
				Events []struct {
					Event string `json:"event"`
				} `json:"events"`
			} `json:"type_data"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(raw, &history); err != nil || history.Ops == nil || len(history.Ops) > 4096 {
		return nil, errors.New("invalid RGW native history schema or count")
	}
	var result []rgwNativeOperation
	targetPattern := regexp.MustCompile(`^([0-9]+):[0-9a-f]+:([^:]*):([^:]*):([^:]+):head$`)
	pgPattern := regexp.MustCompile(`^([0-9]+)\.[0-9a-f]+$`)
	epochPattern := regexp.MustCompile(` e([0-9]+)\)$`)
	for _, op := range history.Ops {
		if op.Data.Client.Client != client || op.Data.Client.TID == 0 || len(op.Description) > 16384 || len(op.Data.Events) > 128 ||
			!strings.HasPrefix(op.Description, "osd_op("+client+".") {
			continue
		}
		identity := client + ".0:" + strconv.FormatUint(op.Data.Client.TID, 10) + " "
		if !strings.HasPrefix(op.Description, "osd_op("+identity) {
			continue
		}
		parts := strings.SplitN(op.Description, " ", 4)
		if len(parts) != 4 {
			continue
		}
		target, pg, epoch := targetPattern.FindStringSubmatch(parts[2]), pgPattern.FindStringSubmatch(parts[1]), epochPattern.FindStringSubmatch(op.Description)
		if target == nil || pg == nil || epoch == nil {
			continue
		}
		pool, poolErr := strconv.ParseInt(target[1], 10, 64)
		pgPool, pgErr := strconv.ParseInt(pg[1], 10, 64)
		mapEpoch, epochErr := strconv.ParseUint(epoch[1], 10, 64)
		if poolErr != nil || pgErr != nil || epochErr != nil || pgPool != pool || !accept(pool, target[2], target[3], target[4]) {
			continue
		}
		entry := rgwNativeOperation{Client: client, TID: op.Data.Client.TID, PoolID: pool, Namespace: target[2], Object: target[4], PGID: parts[1], MapEpoch: mapEpoch, Description: op.Description}
		reached := false
		for _, event := range op.Data.Events {
			if event.Event == "reached_pg" {
				reached = true
			}
			entry.Events = append(entry.Events, event.Event)
		}
		if reached {
			result = append(result, entry)
		}
	}
	return result, nil
}

// Completed operation history is published by OpHistoryServiceThread after
// the S3 response can arrive. Poll this read-only observation with a deadline;
// never repeat the S3 mutation to make its reception appear in history.
func rgwNativePollHistory(parent context.Context, fetch func(context.Context) []byte, ready func([]byte) (bool, error)) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("native operation history was not published within its observation bound: %w", err)
		}
		output := fetch(ctx)
		matched, err := ready(output)
		if err != nil || matched {
			return output, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("native operation history was not published within its observation bound: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func rgwNativePutKinds(operation rgwNativeOperation, head, bucketID string, dataPoolID, indexPoolID int64) (write, prepare, complete bool) {
	isHead := operation.Object == head && operation.PoolID == dataPoolID
	isIndex := operation.PoolID == indexPoolID && (operation.Object == ".dir."+bucketID || strings.HasPrefix(operation.Object, ".dir."+bucketID+"."))
	return isHead && rgwNativeWrites(operation),
		isIndex && rgwNativeCall(operation, "bucket_prepare_op"),
		isIndex && rgwNativeCall(operation, "bucket_complete_op")
}

var rgwNativeWritePattern = regexp.MustCompile(`(?:\[|,)(?:write|writefull)(?:[ ,\]]|$)`)

// rgwNativeWrites matches complete WRITE/WRITEFULL opcode tokens, never a
// class-method or xattr name that merely ends with the same letters.
func rgwNativeWrites(operation rgwNativeOperation) bool {
	return rgwNativeWritePattern.MatchString(operation.Description)
}

func rgwNativeCall(operation rgwNativeOperation, method string) bool {
	return regexp.MustCompile(`(?:\[|,)call rgw\.` + regexp.QuoteMeta(method) + `(?:[ ,\]]|$)`).MatchString(operation.Description)
}

func rgwNativePutPublished(output []byte, clients []rgwNativeClientGeneration, head, bucketID string, dataPoolID, indexPoolID int64) (bool, error) {
	// The later oracle must independently confirm these exact observed shard
	// names. This extraction only lets the receiver observation precede every
	// native data/index readback, without assuming the bucket's shard count.
	objects := map[string]int64{head: dataPoolID}
	pattern := regexp.MustCompile(`:::(\.dir\.` + regexp.QuoteMeta(bucketID) + `(?:\.[0-9]+)?):head`)
	for _, match := range pattern.FindAllSubmatch(output, 4096) {
		objects[string(match[1])] = indexPoolID
	}
	write, prepare, complete := false, false, false
	for _, client := range clients {
		operations, err := rgwNativeHistory(output, client.Client, objects)
		if err != nil {
			return false, err
		}
		for _, operation := range operations {
			w, p, c := rgwNativePutKinds(operation, head, bucketID, dataPoolID, indexPoolID)
			write, prepare, complete = write || w, prepare || p, complete || c
		}
	}
	return write && prepare && complete, nil
}

// Names are discovered from the process-owned socket's catalog. Pinned
// AsyncMessenger appends the nonce to additional radosclient names in RGW.
// A shared CephX entity name or a generic shared-volume socket is insufficient.
const rgwNativeGatewayInspector = `
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
def query(path,*args):
    result=subprocess.run(['ceph','--admin-daemon',path,'messenger','dump',*args],capture_output=True,timeout=3)
    if result.returncode or len(result.stdout)>4*1024*1024:
        raise ValueError('invalid local messenger response')
    value=json.loads(result.stdout)
    return value.get('status',value)
found={}
for path in sorted(set(paths))[:8]:
    try:
        names=query(path)['messengers']
        if not isinstance(names,list) or len(names)>32:
            continue
        for name in names:
            if not isinstance(name,str) or not (name=='radosclient' or name.startswith('radosclient-')):
                continue
            value=query(path,name)['messenger']
            identity=value['my_name']
            if identity['type']=='client' and isinstance(identity['num'],int) and 0<identity['num']<2**63 and isinstance(value['nonce'],int) and value['nonce']>0:
                client='client.'+str(identity['num'])
                found[client]=dict(client=client,nonce=value['nonce'])
    except (ValueError,KeyError,TypeError):
        pass
print(json.dumps(dict(socket_candidates=len(paths),messengers=[found[key] for key in sorted(found)])))
`
