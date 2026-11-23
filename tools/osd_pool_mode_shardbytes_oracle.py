"""Owned-pool oracle mode shardbytes: each shard's own bytes of one object.

Test-only; never product code. The fixture (integration/fixture
shard_bytes.go) copies the data directory of every OSD of the bracket's up
vector, one paused container at a time, lists the object in that shard's
collection of the private copy and reads its bytes there with the pinned
Ceph 20.2.4 tool (7f793731f1b39eb4f465e960113d2363c311b964,
src/tools/ceph_objectstore_tool.cc: list :2270-2284 and :231-266, the
identity read back :4133-4168, get-bytes :2472-2504). This mode brackets
that capture with two native placement records (phase before stores the
first; phase after reads the second) and judges it. It copies no Ceph code.

Expected bytes. The pool's native profile must be the isa plugin with the
default reed_sol_van technique and no chunk mapping, so shard i holds raw
chunk i (pinned OSDMonitor and ErasureCodeIsa: no mapping). The pinned isa
plugin encodes m=1 as the XOR of the data chunks and m>1 with ISA-L's
ec_encode_data over the Vandermonde rows of gf_gen_rs_matrix
(src/erasure-code/isa/ErasureCodeIsa.cc:288-300, :630-650; ISA-L
erasure_code/ec_base.c:73-90 of the vendored src/isa-l): parity row r has
coefficient (2^r)^j for data chunk j in GF(2^8) with the polynomial 0x11d,
and row 0 is the XOR. Every stripe of stripe_width bytes is k chunks of
stripe_width/k bytes; a shard holds its chunk of each stripe in order
(src/osd/ECTransaction.cc:97-114 writes each shard's own extent). The
expected chunks are computed here from logical bytes: the object as the
native client reads it now, and each reference the caller passes (contents
it read natively earlier). Only an object of whole stripes is judged; the
padding of a partial stripe is not modelled.

A complete receipt states which of those contents each captured shard's
bytes equal, at that bracketed placement, for a shard identity taken from
the up position, the collection and the listed shard_id together. It does
not say which chunks a primary read or decoded for any logical read, and a
position empty in the up vector is reported absent, not judged.
"""
import base64
import hashlib
import json
import os
import pathlib
import re

import osd_mutation_oracle as mutation
import osd_pg_history_oracle as history
from osd_pool_oracle import b64, ec_profile_get, find_pool, object_request, oracle_mode, pool_name

BEFORE = "osd-shard-bytes-before.json"
CAPTURE = "osd-shard-bytes-capture.json"
LIST_FILE = re.compile(r"osd-shard-bytes-s([0-7])-list\.json")
DATA_FILE = re.compile(r"osd-shard-bytes-s([0-7])-data\.bin")
SOURCE_COMMIT = "7f793731f1b39eb4f465e960113d2363c311b964"
MAX_RECORD = 64 << 10
MAX_LIST = 16 << 10
MAX_SHARD = 64 << 10
MAX_OBJECT = 256 << 10
MAX_REFERENCES = 3
CRUSH_ITEM_NONE = 0x7fffffff
LABEL = re.compile(r"[a-z][a-z0-9-]{0,31}")
STORE_KEYS = {"reader_isolated", "paused", "resumed", "paused_ms", "store_archive_bytes", "whoami", "unsupported_layout", "reader_removed"}
SHARD_KEYS = STORE_KEYS | {"index", "osd", "spg", "absent", "list_exit_code", "list_bytes", "list_stderr", "list_file", "selected",
                           "bytes_exit_code", "bytes_stderr", "bytes_length", "bytes_file"}
# The pinned ghobject_t::dump of a head on a shard: hobject_t::dump, then
# shard_id and its own max (src/common/hobject.cc:186-195, :488-495); a head
# has no generation.
GHOBJECT_KEYS = ("oid", "key", "snapid", "hash", "max", "pool", "namespace", "shard_id", "max")


class BytesError(ValueError):
    pass


def scrub_store_diagnostics(stderr, spg, pool, shard):
    """The listing's diagnostics when they name only the shard's scrub stores, as lines; None otherwise.

    The tool reads every object's object_info while it walks the shard's
    collection for the listing and prints a line for one without it, then
    goes on (src/tools/ceph_objectstore_tool.cc:110-131). A primary's scrub
    keeps its error stores as two temporary objects of that collection,
    scrub_<spg> and deep_scrub_<spg> (src/osd/scrubber/ScrubStore.cc:105-117),
    in the PG's temporary pool -2 - pool (pg_t::make_temp_ghobject), which
    have none. Exactly those lines, each at most once, are admitted; any other
    diagnostic is not.
    """
    if not isinstance(stderr, str):
        return None
    if stderr == "":
        return []
    lines = stderr.split("\n")
    if lines[-1] != "":
        return None
    lines = lines[:-1]
    pattern = re.compile(r"Error getting attr on : %s_head,%d#%d:[0-9a-f]{8}:::(deep_)?scrub_%s:head#, \(61\) No data available"
                         % (re.escape(spg), shard, -2 - pool, re.escape(spg)))
    kinds = [pattern.fullmatch(line) for line in lines]
    if any(m is None for m in kinds) or len({m.group(1) for m in kinds}) != len(kinds):
        return None
    return lines


def gf_mul(a, b):
    """Multiplication in GF(2^8) with the polynomial 0x11d (ISA-L gf_mul)."""
    product = 0
    while b:
        if b & 1:
            product ^= a
        a <<= 1
        if a & 0x100:
            a ^= 0x11d
        b >>= 1
    return product


def parity_rows(k, m):
    """The parity rows of ISA-L gf_gen_rs_matrix(a, k + m, k)."""
    rows, gen = [], 1
    for _ in range(m):
        row, p = [], 1
        for _ in range(k):
            row.append(p)
            p = gf_mul(p, gen)
        rows.append(row)
        gen = gf_mul(gen, 2)
    return rows


def _xor(a, b):
    return (int.from_bytes(a, "big") ^ int.from_bytes(b, "big")).to_bytes(len(a), "big")


def shard_chunks(data, k, m, stripe_width):
    """The k + m shard contents of logical bytes data, each its chunks in stripe order."""
    if type(k) is not int or type(m) is not int or not 1 <= k <= 8 or not 1 <= m <= 4:
        raise BytesError("unsupported width k=%r m=%r" % (k, m))
    if type(stripe_width) is not int or stripe_width <= 0 or stripe_width % k:
        raise BytesError("stripe_width %r is not a multiple of k=%d" % (stripe_width, k))
    if not data or len(data) % stripe_width:
        raise BytesError("the object is %d bytes, not whole stripes of %d" % (len(data), stripe_width))
    chunk = stripe_width // k
    rows = parity_rows(k, m)
    tables = {c: bytes(gf_mul(c, x) for x in range(256)) for row in rows for c in row}
    shards = [bytearray() for _ in range(k + m)]
    for start in range(0, len(data), stripe_width):
        chunks = [bytes(data[start + j * chunk:start + (j + 1) * chunk]) for j in range(k)]
        for j, part in enumerate(chunks):
            shards[j] += part
        for r, row in enumerate(rows):
            parity = bytes(chunk)
            for j, part in enumerate(chunks):
                parity = _xor(parity, part.translate(tables[row[j]]))
            shards[k + r] += parity
    return [bytes(shard) for shard in shards]


def _ghobject_pairs(pairs):
    return pairs


def parse_listing(raw):
    """The [spg, collection, ghobject pairs] entries of one pinned `--op list` dump.

    The dump repeats the key max in a head's ghobject; the pairs keep both.
    Any other object in the document must have unique keys.
    """
    if not isinstance(raw, (bytes, bytearray)) or not raw or len(raw) > MAX_LIST:
        raise BytesError("the listing is empty or exceeds its bound")
    try:
        document = json.loads(bytes(raw).decode("utf-8"), object_pairs_hook=_ghobject_pairs)
    except (UnicodeDecodeError, ValueError) as error:
        raise BytesError("the listing is not one complete JSON document: " + str(error)[:120]) from None
    if not isinstance(document, list):
        raise BytesError("the listing is not an array")
    entries = []
    for entry in document:
        if (not isinstance(entry, list) or len(entry) != 3 or not isinstance(entry[0], str) or not isinstance(entry[1], str)
                or not isinstance(entry[2], list)):
            raise BytesError("a listed entry is not [pgid, collection, ghobject]")
        entries.append((entry[0], entry[1], entry[2]))
    return entries


def head_identity(pairs, pool, name, shard):
    """Whether listed ghobject pairs are exactly the head of name in pool on shard."""
    keys = tuple(key for key, _ in pairs)
    if keys != GHOBJECT_KEYS:
        return False
    values = dict(pairs[:4] + pairs[5:8])
    return (values["oid"] == name and values["key"] == "" and values["snapid"] == -2 and type(values["hash"]) is int
            and values["pool"] == pool and values["namespace"] == "" and values["shard_id"] == shard
            and pairs[4][1] == 0 and pairs[8][1] == 0)


def select_head(raw, spg, pool, name, shard):
    """The one listed head of name in spg's head collection on shard, as [spg, pairs]."""
    found = [pairs for listed, collection, pairs in parse_listing(raw)
             if listed == spg and collection == spg + "_head" and head_identity(pairs, pool, name, shard)]
    if len(found) != 1:
        raise BytesError("%d listed heads of %s on shard %d in %s" % (len(found), name, shard, spg))
    return [spg, found[0]]


def parse_selected(text):
    """The [spg, ghobject pairs] identity the fixture gave get-bytes."""
    if not isinstance(text, str) or not text or len(text) > 2048:
        raise BytesError("the capture names no selected identity")
    try:
        value = json.loads(text, object_pairs_hook=_ghobject_pairs)
    except ValueError:
        raise BytesError("the selected identity is not JSON") from None
    if not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], str) or not isinstance(value[1], list):
        raise BytesError("the selected identity is not [spg, ghobject]")
    return value


def _shard(index, osd, pgid, pool, name, record, files, expected):
    """One shard's receipt; expected maps each label to its k + m shard contents."""
    spg = "%ss%d" % (pgid, index)
    receipt = {"index": index, "osd": osd, "spg": spg}
    if not isinstance(record, dict) or set(record) - {"failure"} != SHARD_KEYS:
        return dict(receipt, complete=False, failures=["capture record has an unexpected shape"])
    if record["index"] != index or record["osd"] != osd or record["spg"] != spg:
        return dict(receipt, complete=False, failures=["capture is for index %r, osd %r, %r" % (record["index"], record["osd"], record["spg"])])
    if osd == CRUSH_ITEM_NONE:
        absent = record["absent"] is True and not record["selected"] and not record["bytes_file"] and not record["list_file"]
        return dict(receipt, absent=True, complete=absent, failures=[] if absent else ["an empty position has a capture"])
    failures = []
    if record["absent"] is not False:
        failures.append("a present member is recorded absent")
    if record.get("failure"):
        failures.append("capture failed: " + str(record["failure"])[:200])
    if record["whoami"] != str(osd):
        failures.append("the copy's whoami is %r, not osd.%d" % (str(record["whoami"])[:20], osd))
    for flag, text in (("reader_isolated", "the reader was not isolated from every network"), ("paused", "the OSD was not paused for the copy"),
                       ("resumed", "the OSD was not resumed"), ("reader_removed", "the reader and its copy were not removed")):
        if record[flag] is not True:
            failures.append(text)
    if record["unsupported_layout"] != "":
        failures.append("unsupported block layout: " + str(record["unsupported_layout"])[:80])
    for op in ("list", "bytes"):
        if type(record[op + "_exit_code"]) is not int or record[op + "_exit_code"] != 0:
            failures.append("offline %s exited %r" % (op, record[op + "_exit_code"]))
    if record["bytes_stderr"] != "":
        failures.append("offline bytes wrote diagnostics: %s" % str(record["bytes_stderr"])[:200])
    admitted = scrub_store_diagnostics(record["list_stderr"], spg, pool, index) if type(pool) is int else None
    if admitted is None:
        failures.append("offline list wrote diagnostics: %s" % str(record["list_stderr"])[:300])
    else:
        receipt["list_scrub_store_diagnostics"] = admitted
    if record["list_file"] != "osd-shard-bytes-s%d-list.json" % index or record["bytes_file"] != "osd-shard-bytes-s%d-data.bin" % index:
        failures.append("the capture names %r and %r as this shard's files" % (str(record["list_file"])[:60], str(record["bytes_file"])[:60]))
    receipt.update(whoami=record["whoami"], paused_ms=record["paused_ms"], store_archive_bytes=record["store_archive_bytes"])
    if failures:
        return dict(receipt, complete=False, failures=failures)
    try:
        listing = files(record["list_file"], MAX_LIST)
        data = files(record["bytes_file"], MAX_SHARD)
    except (OSError, ValueError) as error:
        return dict(receipt, complete=False, failures=["a capture file is unreadable: %s" % str(error)[:120]])
    if len(listing) != record["list_bytes"] or len(data) != record["bytes_length"]:
        failures.append("the files have %d and %d bytes; the capture recorded %r and %r" % (len(listing), len(data), record["list_bytes"], record["bytes_length"]))
    receipt.update(listing=base64.b64encode(listing).decode(), length=len(data), sha256=hashlib.sha256(data).hexdigest(),
                   raw=base64.b64encode(data).decode())
    try:
        head = select_head(listing, spg, pool, name, index)
        if parse_selected(record["selected"]) != head:
            failures.append("the capture read another identity than the listing's one head")
        receipt["shard_id"] = dict(head[1][:8])["shard_id"]
    except BytesError as error:
        failures.append(str(error))
    receipt["matches"] = [label for label, shards in expected.items() if shards[index] == data]
    receipt["failures"] = failures
    receipt["complete"] = not failures
    return receipt


def judge(before_raw, after, capture_raw, files, descriptor, profile, native, references):
    """One fail-closed receipt over every position of the bracket's up vector.

    ``native`` is the native client's snapshot of the object, ``references``
    {label: logical bytes}; ``descriptor`` the pool's map entry and
    ``profile`` its native erasure-code profile.
    """
    receipt = {"after": after, "shards": [], "absence_established": False, "chunks_used_by_reads": "not established"}
    failures = []

    def finish():
        receipt["failures"] = failures
        receipt["complete"] = not failures
        return receipt

    try:
        before = history.strict_document(before_raw, MAX_RECORD)
        capture = history.strict_document(capture_raw, MAX_RECORD)
    except history.HistoryError as error:
        failures.append("bracket or capture record: " + str(error))
        return finish()
    receipt["before"] = before
    if not isinstance(before, dict) or not isinstance(before.get("pgid"), str) or not isinstance(before.get("up"), list):
        failures.append("the first bracket is not a placement record")
        return finish()
    if before != after:
        failures.append("placement or epoch changed between the before and after brackets")
    pgid, up, pool, name = before["pgid"], before["up"], before.get("pool_id"), before.get("object")
    receipt.update(pgid=pgid, up=up, acting=before.get("acting"))
    if (not isinstance(capture, dict) or capture.get("schema_version") != 1 or capture.get("source_commit") != SOURCE_COMMIT
            or not isinstance(capture.get("shards"), list)):
        failures.append("the capture record has an unexpected shape or provenance")
        return finish()
    if capture.get("before_sha256") != hashlib.sha256(before_raw).hexdigest():
        failures.append("the capture was not taken under this first bracket")
    if capture.get("complete") is not True:
        failures.append("the capture is incomplete")
    if capture.get("pgid") != pgid or capture.get("up") != up or capture.get("pool_id") != pool or capture.get("object") != name:
        failures.append("the capture is for %r up %r" % (capture.get("pgid"), capture.get("up")))
    if not 2 <= len(up) <= 8 or any(type(osd) is not int or not 0 <= osd <= CRUSH_ITEM_NONE for osd in up):
        failures.append("the up vector is not between 2 and 8 positions")
        return finish()
    if (profile.get("plugin") != "isa" or profile.get("technique", "reed_sol_van") != "reed_sol_van" or "mapping" in profile
            or not str(profile.get("k", "")).isdigit() or not str(profile.get("m", "")).isdigit()):
        failures.append("the native profile %r is not an unmapped isa reed_sol_van profile" % (profile,))
        return finish()
    k, m, width = int(profile["k"]), int(profile["m"]), descriptor.get("stripe_width")
    receipt.update(k=k, m=m, plugin="isa", technique="reed_sol_van", stripe_width=width)
    if k + m != len(up) or descriptor.get("size") != len(up):
        failures.append("k=%d m=%d does not match the pool size %r and %d up positions" % (k, m, descriptor.get("size"), len(up)))
        return finish()
    if native.get("code") != 0:
        failures.append("the native read of the object returned %r" % native.get("code"))
        return finish()
    logical = {"native": base64.b64decode(native["data"])}
    logical.update(references)
    receipt["native"] = {"version": native.get("version"), "length": len(logical["native"]), "sha256": hashlib.sha256(logical["native"]).hexdigest()}
    receipt["references"] = {label: {"length": len(data), "sha256": hashlib.sha256(data).hexdigest()} for label, data in references.items()}
    try:
        expected = {label: shard_chunks(data, k, m, width) for label, data in logical.items()}
    except BytesError as error:
        failures.append(str(error))
        return finish()
    receipt["expected"] = {label: [hashlib.sha256(s).hexdigest() for s in shards] for label, shards in expected.items()}
    if len(capture["shards"]) != len(up):
        failures.append("the capture has %d shards for %d up positions" % (len(capture["shards"]), len(up)))
    for index, osd in enumerate(up):
        record = capture["shards"][index] if index < len(capture["shards"]) else None
        shard = _shard(index, osd, pgid, pool, name, record, files, expected)
        receipt["shards"].append(shard)
        failures.extend("shard %d (osd.%d): %s" % (index, osd, text) for text in shard["failures"])
    return finish()


def read_bounded(path, limit):
    with path.open("rb") as file:
        data = file.read(limit + 1)
    if len(data) > limit:
        raise ValueError("%s exceeds %d bytes" % (path.name, limit))
    return data


def parse_references(value):
    """{label: bytes} from the request's references."""
    if value is None:
        return {}
    if not isinstance(value, list) or len(value) > MAX_REFERENCES:
        raise ValueError("shardbytes takes at most %d references" % MAX_REFERENCES)
    references = {}
    for item in value:
        if not isinstance(item, dict) or set(item) != {"label", "data"} or not isinstance(item["label"], str) or not LABEL.fullmatch(item["label"]):
            raise ValueError("a reference is a label and base64 data")
        if item["label"] in references or item["label"] == "native":
            raise ValueError("reference labels are distinct and not native")
        references[item["label"]] = b64(item["data"], MAX_OBJECT)
    return references


@oracle_mode
def mode_shardbytes(rados, cluster, request, out):
    """Placement brackets and judgement of one shard-local byte capture.

    Phase before stores the native placement of the object; the fixture's
    capture takes its PG, object and up vector from that stored record.
    Phase after reads the placement again, requires the two complete
    records to be identical, reads the object and the pool's profile
    natively and judges each captured shard's bytes against the chunks of
    the native object and of each reference. Neither phase copies a store or
    changes the cluster.
    """
    phase = request.get("phase")
    if phase not in ("before", "after") or set(request) - {"phase", "slot", "object", "references"}:
        raise ValueError("shardbytes needs phase before or after and no unknown field")
    pool = pool_name(request["slot"])
    name, namespace = object_request(request)
    if namespace:
        raise ValueError("shardbytes reads objects of the default namespace")
    if phase == "before" and "references" in request:
        raise ValueError("shardbytes references belong to phase after")
    references = parse_references(request.get("references"))
    dump = rados.dump(cluster)
    descriptor = find_pool(dump, pool)
    if descriptor is None:
        raise RuntimeError("owned pool is missing")

    def command(*arguments, timeout):
        return mutation.native_mon_query(rados.lib, cluster, *arguments, timeout=timeout)

    stored = pathlib.Path(out) / BEFORE
    if phase == "before":
        for stale in (stored, pathlib.Path(out) / CAPTURE):
            stale.unlink(missing_ok=True)
        placement = mutation.collect_placement(command, pool, request["object"], descriptor["pool"])
        temporary = pathlib.Path(out) / (BEFORE + ".tmp")
        temporary.write_bytes(json.dumps(placement).encode())
        os.replace(temporary, stored)
        return {"placement": placement}
    before = read_bounded(stored, MAX_RECORD)
    capture = read_bounded(pathlib.Path(out) / CAPTURE, MAX_RECORD)
    after = mutation.collect_placement(command, pool, request["object"], descriptor["pool"])
    code, profile = ec_profile_get(rados, cluster, descriptor.get("erasure_code_profile", ""))
    if code < 0 or profile is None:
        raise RuntimeError("the owned pool's erasure-code profile is unreadable: %d" % code)
    io = rados.ioctx(cluster, pool, "")
    try:
        native = rados.native.snapshot(io, name, [])
    finally:
        rados.lib.rados_ioctx_destroy(io)

    def files(file_name, limit):
        if not isinstance(file_name, str) or not (LIST_FILE.fullmatch(file_name) or DATA_FILE.fullmatch(file_name)):
            raise ValueError("invalid shard-bytes file name")
        return read_bounded(pathlib.Path(out) / file_name, limit)

    receipt = judge(before, after, capture, files, descriptor, profile, native, references)
    # One bracket admits one judgement.
    stored.unlink()
    return {"shard_bytes": receipt}
