#!/usr/bin/env python3
"""Closed native observer for the Go copy source cache-flag test.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964, LGPL-2.1 librados;
never a product dependency. The Go test sends COPY_FROM and COPY_FROM2 with
and without the source-read flags IGNORE_CACHE and IGNORE_OVERLAY itself.
This oracle only seeds owned heads, observes them independently and removes
them; it issues no copy and no cache operation. Every read selects
IGNORE_CACHE and IGNORE_OVERLAY, so it names exactly the pool it reads, with
or without the base pool's overlay, and promotes nothing. Each phase is a
separate fixture control:

  prepare        record placements, seed base A and tier B (dirty) for the
                 dirty head, base A alone for the two miss heads, and observe
                 every head; the destinations start absent in both pools
  observe NAME   observe one owned head in both pools
  finish         remove every owned head in both pools and confirm absence

Every call publishes its intent first; an uncertain call stops the phase and
nothing is reissued, including by a later phase.
"""
import json
from pathlib import Path
import sys
import time

import osd_cache_tier_ops_oracle as ops

COMMIT, BASE, CHANGED = ops.COMMIT, ops.BASE, ops.CHANGED
READ_FLAGS = ops.IGNORE_CACHE | ops.IGNORE_OVERLAY
RECEIPT = "osd-cache-copy-flags-native.json"
CHECKPOINT = "cache-copy-flags-observer-v1"
DIRTY = "go-copy-dirty"
MISSES = ("go-copy-miss-0", "go-copy-miss-1")
DESTINATIONS = ("go-copy-dst-0", "go-copy-dst-1")
OBJECTS = (DIRTY,) + MISSES + DESTINATIONS
PHASES = ("prepare", "observe", "finish")
MAX_OBSERVATIONS = 96


class Native(ops.Native):
    objects = OBJECTS

    def execute(self, role, pool, name, action, flags, data=CHANGED):
        # The observer seeds, reads and removes; it never issues cache operations.
        if role != "observer" or action in ops.CACHE_ACTIONS or action == "is-dirty" and name != DIRTY:
            raise ValueError("copy cache-flag observer issues no cache operation")
        return super().execute(role, pool, name, action, flags, data)


class Controller(ops.Controller):
    def observe_head(self, label, name):
        states = {}
        for pool in ("base", "tier"):
            state = self.call(label + "-" + pool, "observer", self.plan[pool], name, "snapshot", READ_FLAGS, gate=(0, -2))
            if state["code"] == 0 and (state["stat_prval"] != 0 or state["read_prval"] != 0):
                raise ops.Stop("snapshot per-operation result differs: " + label)
            states[pool] = state
        self.receipt["observations"][label] = states
        return states

    def prepare(self):
        if self.receipt["phase"] != "new":
            raise ops.Stop("copy cache-flag observer was already prepared")
        base, tier = self.plan["base"], self.plan["tier"]
        for pool in (base, tier):
            for name in OBJECTS:
                placed = self.native.placement(pool, name)
                placed.update(pool_name=pool, object=name)
                self.receipt["placements"].append(placed)
                self.persist()
        self.call("seed-base-" + DIRTY, "observer", base, DIRTY, "writefull", READ_FLAGS, gate=(0,), data=BASE)
        self.call("seed-tier-" + DIRTY, "observer", tier, DIRTY, "writefull", READ_FLAGS, gate=(0,), data=CHANGED)
        for name in MISSES:
            self.call("seed-base-" + name, "observer", base, name, "writefull", READ_FLAGS, gate=(0,), data=BASE)
        for name in OBJECTS:
            seeded = self.observe_head("seeded-" + name, name)
            want = {DIRTY: (BASE, CHANGED)}.get(name, (BASE, None) if name in MISSES else (None, None))
            for pool, data in zip(("base", "tier"), want):
                held = self.holds(seeded[pool], data) if data is not None else seeded[pool]["code"] == -2
                self.require(held, "seeded state differs: %s %s" % (pool, name))
            self.receipt["seeded"][name] = seeded
        dirty = self.dirty("seeded-dirty-" + DIRTY, DIRTY)
        self.require(dirty.get("dirty") is True, "seeded tier head is not dirty")
        self.receipt["seeded_dirty"] = dirty
        self.receipt["phase"] = "prepared"
        self.persist()

    def observe(self, name):
        if name not in OBJECTS:
            raise ValueError("foreign observed head")
        if self.receipt["phase"] not in ("prepared", "observed") or len(self.receipt["labels"]) >= MAX_OBSERVATIONS:
            raise ops.Stop("observation outside the prepared sequence or beyond its bound")
        label = "observe-%d-%s" % (len(self.receipt["labels"]) + 1, name)
        self.receipt["labels"].append({"label": label, "object": name, "states": self.observe_head(label, name)})
        self.receipt["phase"] = "observed"
        self.persist()

    def finish(self):
        if self.receipt["phase"] == "finished":
            raise ops.Stop("copy cache-flag observer was already finished")
        record = self.receipt["cleanup"]
        record["attempted"] = True
        self.persist()
        for pool in ("tier", "base"):
            for name in OBJECTS:
                self.call("cleanup-" + pool + "-" + name, "observer", self.plan[pool], name, "delete", READ_FLAGS, gate=(0, -2))
                gone = self.call("cleanup-absent-" + pool + "-" + name, "observer", self.plan[pool], name, "snapshot", READ_FLAGS, gate=(-2,))
                self.require(gone["code"] == -2, "owned head remains after cleanup")
        record["complete"] = True
        self.receipt["phase"] = "finished"
        self.persist()


def new_receipt(plan):
    return {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "plan": plan,
            "issues_copy_or_cache_operations": False, "read_flags": READ_FLAGS, "phase": "new", "steps": [],
            "placements": [], "observations": {}, "seeded": {}, "labels": [], "cleanup": {}, "native_identities": {}}


def main(args, native_factory=Native, sleep=time.sleep):
    if len(args) not in (4, 5) or args[3] not in PHASES or (args[3] == "observe") != (len(args) == 5):
        raise ValueError("usage: osd_cache_copy_flags_oracle.py ROOT OUT prepare|observe NAME|finish")
    root, out, phase = Path(args[1]), Path(args[2]), args[3]
    raw = (out / "osd-ignore-cache-plan.json").read_bytes()
    if len(raw) > 16384:
        raise ValueError("plan exceeds bound")
    plan = ops.validate_plan(json.loads(raw))
    path = out / RECEIPT
    if path.exists():
        raw = path.read_bytes()
        if len(raw) > 1 << 20:
            raise ValueError("receipt exceeds bound")
        receipt = json.loads(raw)
        if receipt.get("source_commit") != COMMIT or receipt.get("checkpoint") != CHECKPOINT or receipt.get("plan") != plan:
            raise ValueError("foreign copy cache-flag observer receipt")
        if any(step["state"] != "completed" for step in receipt["steps"]):
            raise ValueError("an earlier observer call is uncertain; nothing is reissued")
    elif phase == "prepare":
        receipt = new_receipt(plan)
    else:
        raise ValueError("copy cache-flag observer receipt is absent")
    save = lambda: ops.publish(path, receipt)
    save()
    native = None
    try:
        native = native_factory(root, out, plan)
        controller = Controller(native, plan, receipt, save, sleep=sleep)
        getattr(controller, phase)(*args[4:])
    except BaseException as exc:
        receipt["error"] = str(exc)[:2048]
        save()
        raise
    finally:
        if native is not None:
            native.close()
    print(json.dumps({"phase": receipt["phase"], "steps": len(receipt["steps"])}))


if __name__ == "__main__":
    main(sys.argv)
