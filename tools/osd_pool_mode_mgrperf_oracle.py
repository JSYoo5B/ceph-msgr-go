"""Bounded native workload for stock OSD dynamic perf-metric reports.

The independently connected librados handle supplies its global ID and exactly
three WRITEFULL/READ pairs. No MGR counter is read by this oracle. The owned
pool's cleanup removes the unique namespace. Ceph v20.2.4
src/osd/DynamicPerfStats.h counts OPS per request and BYTES as inb + outb;
src/osd/PrimaryLogPG.cc swaps these counters out at each report.
"""
import ctypes as C
import hashlib
import uuid

from osd_pool_oracle import find_pool, oracle_mode, pool_name


@oracle_mode
def mode_mgrperf(rados, cluster, request, out):
    if set(request) != {"slot"}:
        raise ValueError("mgrperf takes only an owned pool slot")
    pool = pool_name(request["slot"])
    if find_pool(rados.dump(cluster), pool) is None:
        raise ValueError("owned pool is missing")
    lib = rados.lib
    lib.rados_get_instance_id.argtypes, lib.rados_get_instance_id.restype = [C.c_void_p], C.c_uint64
    lib.rados_read.argtypes = [C.c_void_p, C.c_char_p, C.c_void_p, C.c_size_t, C.c_uint64]
    lib.rados_read.restype = C.c_int
    native = rados.connect(timeout=5)
    io = None
    namespace = "mgr-perf-" + uuid.uuid4().hex
    rows = []
    try:
        global_id = lib.rados_get_instance_id(native)
        if global_id == 0:
            raise RuntimeError("native client has no global ID")
        io = rados.ioctx(native, pool, namespace)
        for index, size in enumerate((257, 1025, 4097)):
            name = ("object-%d" % index).encode()
            data = bytes((i + index) % 251 for i in range(size))
            written = lib.rados_write_full(io, name, data, size)
            rados.native.check(written)
            buffer = C.create_string_buffer(size + 1)
            read = lib.rados_read(io, name, buffer, size + 1, 0)
            if written != 0 or read != size or buffer.raw[:read] != data:
                raise RuntimeError("native WRITEFULL/READ verification failed")
            rows.append({"object": name.decode(), "size": size, "write_return": written,
                         "read_return": read, "sha256": hashlib.sha256(data).hexdigest()})
    finally:
        if io is not None:
            lib.rados_ioctx_destroy(io)
        lib.rados_shutdown(native)
    return {"workload": {"global_id": global_id, "namespace": namespace, "objects": rows,
                         "ops": 2 * len(rows), "bytes": 2 * sum(r["size"] for r in rows)}}
