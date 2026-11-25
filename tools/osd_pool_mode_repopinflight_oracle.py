"""Receiver-side replicated transactions in a bounded in-flight snapshot.

Pinned Ceph v20.2.4: src/messages/MOSDRepOp.h print names the client
identity, receiving PG and map epochs; only finish_decode adds object and
version. ReplicatedBackend.cc:1190-1288 decodes before mark_started and
queues the transaction, and :1293-1323 marks commit_sent only in its commit
callback. Presence without commit_sent is not by itself a persistence
barrier: the caller must also hold the receiver's commit path.
"""
import base64
import re

import osd_ops_in_flight_oracle as ops
from osd_pool_oracle import oracle_mode

REPOP = re.compile(r"osd_repop\((\S+) ((?:0|[1-9][0-9]*)\.[0-9a-f]+) e([0-9]+)/([0-9]+) (.+) v ([0-9]+'[0-9]+)(?:, has_updated_hit_set_history)?, pct=[0-9]+'[0-9]+\)")


def repops(parsed, num, inc):
    """All matching decoded repops; an undecoded match fails closed."""
    prefix = "osd_repop(client.%d.%d:" % (num, inc)
    found = []
    for op in parsed:
        if not op['description'].startswith(prefix):
            continue
        match = REPOP.fullmatch(op['description'])
        if not match:
            raise ops.OpsError('target replica transaction is not fully decoded')
        found.append(dict(reqid=match[1], pgid=match[2], epoch=int(match[3]), min_epoch=int(match[4]),
                          object=match[5], version=match[6], flag_point=op['flag_point'], events=op['events']))
    return found


@oracle_mode
def mode_repopinflight(rados, cluster, request, out):
    """The same placement brackets and byte/schema checks as opsinflight."""
    from osd_pool_oracle import mode_opsinflight
    if 'osd' not in request:
        raise ValueError('repopinflight requires an explicit receiver')
    receipt = mode_opsinflight(rados, cluster, request, out)['ops_in_flight']
    receipt['repops'] = []
    if receipt['complete']:
        try:
            num, inc = ops.client_entity(request['entity'])
            receipt['repops'] = repops(ops.parse_ops(base64.b64decode(receipt['raw'])), num, inc)
        except ops.OpsError as error:
            receipt['failures'].append(str(error))
            receipt['complete'] = False
    return {'repops_in_flight': receipt}
