#!/usr/bin/env python3
"""Closed native observer for the Go-originated cache-tier operation test.

Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964, LGPL-2.1 librados;
never a product dependency. The Go test sends CACHE_FLUSH, CACHE_TRY_FLUSH,
CACHE_EVICT, CACHE_PIN and CACHE_UNPIN itself. This oracle only seeds owned
heads, observes them independently (IGNORE_CACHE STAT+READ in both pools and
the C++ is_dirty read used by the native-only checkpoint) and removes them. It
issues no cache operation. Each phase is a separate fixture control:

  prepare        record placements, seed base A and tier B for two heads and
                 observe them; observe an absent control head
  observe LABEL  observe the head that a closed label names
  finish         remove every owned head and confirm absence

Every call publishes its intent first; an uncertain call stops the phase and
nothing is reissued, including by a later phase.
"""
import json
import os
from pathlib import Path
import sys
import time

import osd_cache_tier_ops_oracle as ops

COMMIT, NAMESPACE, BASE, CHANGED = ops.COMMIT, ops.NAMESPACE, ops.BASE, ops.CHANGED
IGNORE_CACHE = ops.IGNORE_CACHE
RECEIPT = "osd-cache-op-go-native.json"
CHECKPOINT = "cache-op-go-observer-v1"
FLUSH, TRY, ABSENT = "go-cache-flush", "go-cache-try", "go-cache-absent"
OBJECTS = (FLUSH, TRY, ABSENT)
# Closed observation labels and the head each one reads. try-flushed also
# polls is_dirty within a bounded budget, as the native-only checkpoint does.
LABELS = {"after-evict-dirty": FLUSH, "pinned": FLUSH, "unpinned": FLUSH, "flushed": FLUSH,
          "try-flushed": TRY, "evicted": FLUSH, "absent-final": ABSENT,
          "clean-flush": FLUSH, "clean-try-flush": FLUSH,
          "absent-flush": ABSENT, "absent-try-flush": ABSENT, "absent-evict": ABSENT}
TRY_POLLS, TRY_INTERVAL = ops.TRY_POLLS, ops.TRY_INTERVAL
PHASES = ("prepare", "observe", "finish")


class Native(ops.Native):
    objects = OBJECTS

    def execute(self, role, pool, name, action, flags, data=CHANGED):
        # The observer seeds, reads and removes; it never issues cache operations.
        if role != "observer" or action in ops.CACHE_ACTIONS:
            raise ValueError("Go cache-op observer issues no cache operation")
        return super().execute(role, pool, name, action, flags, data)


class Controller(ops.Controller):
    def observe_head(self, label, name):
        entry = {"snapshot": self.snapshot(label, name)}
        if name != ABSENT:
            entry["dirty"] = self.dirty(label + "-dirty", name)
        return entry

    def prepare(self):
        if self.receipt["phase"] != "new":
            raise ops.Stop("Go cache-op observer was already prepared")
        plan, base, tier = self.plan, self.plan["base"], self.plan["tier"]
        for pool in (base, tier):
            for name in OBJECTS:
                placed = self.native.placement(pool, name)
                placed.update(pool_name=pool, object=name)
                self.receipt["placements"].append(placed)
                self.persist()
        for name in (FLUSH, TRY):
            self.call("seed-base-" + name, "observer", base, name, "writefull", IGNORE_CACHE, gate=(0,), data=BASE)
            self.call("seed-tier-" + name, "observer", tier, name, "writefull", IGNORE_CACHE, gate=(0,), data=CHANGED)
            seeded = self.observe_head("seeded-" + name, name)
            self.require(self.holds(seeded["snapshot"]["base"], BASE) and self.holds(seeded["snapshot"]["tier"], CHANGED),
                         "seeded bytes differ: " + name)
            self.require(seeded["dirty"].get("dirty") is True, "seeded tier head is not dirty: " + name)
            self.receipt["seeded"][name] = seeded
        absent = self.observe_head("absent-initial", ABSENT)["snapshot"]
        self.require(absent["base"]["code"] == -2 and absent["tier"]["code"] == -2, "absent control head exists")
        self.receipt["phase"] = "prepared"
        self.persist()

    def observe(self, label):
        if label not in LABELS:
            raise ValueError("foreign observation label")
        if self.receipt["phase"] not in ("prepared", "observed") or label in self.receipt["labels"]:
            raise ops.Stop("observation outside the prepared sequence or repeated: " + label)
        name = LABELS[label]
        entry = {}
        if label == "try-flushed":
            polls = []
            for index in range(TRY_POLLS):
                state = self.dirty("try-flushed-poll-%d" % index, name)
                polls.append(state.get("dirty"))
                if state.get("dirty") is False:
                    break
                self.sleep(TRY_INTERVAL)
            entry["dirty_polls"] = polls
            entry["poll_budget"] = [TRY_POLLS, TRY_INTERVAL]
        entry.update(self.observe_head(label, name))
        self.receipt["labels"][label] = entry
        self.receipt["phase"] = "observed"
        self.persist()

    def finish(self):
        if self.receipt["phase"] == "finished":
            raise ops.Stop("Go cache-op observer was already finished")
        record = self.receipt["cleanup"]
        record["attempted"] = True
        self.persist()
        for pool in ("tier", "base"):
            for name in OBJECTS:
                self.call("cleanup-" + pool + "-" + name, "observer", self.plan[pool], name, "delete", IGNORE_CACHE, gate=(0, -2))
                gone = self.call("cleanup-absent-" + pool + "-" + name, "observer", self.plan[pool], name, "snapshot", IGNORE_CACHE, gate=(-2,))
                self.require(gone["code"] == -2, "owned head remains after cleanup")
        record["complete"] = True
        self.receipt["phase"] = "finished"
        self.persist()


def new_receipt(plan):
    return {"schema_version": 1, "source_commit": COMMIT, "checkpoint": CHECKPOINT, "plan": plan,
            "issues_cache_operations": False, "phase": "new", "steps": [], "placements": [],
            "observations": {}, "seeded": {}, "labels": {}, "cleanup": {}, "native_identities": {}}


def main(args, native_factory=Native, sleep=time.sleep):
    if len(args) not in (4, 5) or args[3] not in PHASES or (args[3] == "observe") != (len(args) == 5):
        raise ValueError("usage: osd_cache_op_go_oracle.py ROOT OUT prepare|observe LABEL|finish")
    root, out, phase = Path(args[1]), Path(args[2]), args[3]
    raw = (out / "osd-ignore-cache-plan.json").read_bytes()
    if len(raw) > 16384:
        raise ValueError("plan exceeds bound")
    plan = ops.validate_plan(json.loads(raw))
    path = out / RECEIPT
    if path.exists():
        raw = path.read_bytes()
        if len(raw) > 512 << 10:
            raise ValueError("receipt exceeds bound")
        receipt = json.loads(raw)
        if receipt.get("source_commit") != COMMIT or receipt.get("checkpoint") != CHECKPOINT or receipt.get("plan") != plan:
            raise ValueError("foreign Go cache-op observer receipt")
        if any(step["state"] != "completed" for step in receipt["steps"]):
            raise ValueError("an earlier observer call is uncertain; nothing is reissued")
    elif phase == "prepare":
        receipt = new_receipt(plan)
    else:
        raise ValueError("Go cache-op observer receipt is absent")
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
