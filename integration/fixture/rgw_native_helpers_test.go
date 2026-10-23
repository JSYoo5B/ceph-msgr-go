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

func rgwNativeHash(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func (s rgwNativeS3) request(ctx context.Context, method, path string, payload []byte, status int) ([]byte, error) {
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
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
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
	Description string   `json:"description"`
	Events      []string `json:"events"`
}

// Match native MOSDOp descriptions only when the client identity, request TID,
// complete object name and reached_pg event agree. The observation is server
// reception, not an independent proof of the operation's result or durability.
func rgwNativeHistory(raw []byte, client string, objects map[string]bool) ([]rgwNativeOperation, error) {
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
	for _, op := range history.Ops {
		if op.Data.Client.Client != client || op.Data.Client.TID == 0 || len(op.Description) > 16384 || len(op.Data.Events) > 128 ||
			!strings.HasPrefix(op.Description, "osd_op("+client+".") {
			continue
		}
		identity := client + ".0:" + strconv.FormatUint(op.Data.Client.TID, 10) + " "
		if !strings.HasPrefix(op.Description, "osd_op("+identity) {
			continue
		}
		matched := false
		for object := range objects {
			if strings.Contains(op.Description, ":"+object+":") {
				matched = true
			}
		}
		if !matched {
			continue
		}
		entry := rgwNativeOperation{Client: client, TID: op.Data.Client.TID, Description: op.Description}
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
