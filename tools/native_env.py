#!/usr/bin/env python3
"""Forward one native job's matrix to the fixture runner's environment.

The native workflow (.github/workflows/native-scenarios.yml) copies this file
from its own trusted revision before it checks out the tested commit, and runs

  MATRIX_JSON='<the job's matrix>' native_env.py -- sh integration/run.sh

for the runner only: it execs the command with its own environment plus the
job's fixture inputs, so a row's parameters reach the tested runner and never
the environment of a trusted step (the result gate, the evidence collector
or the terminal recorder). This module is the one reading of a matrix into
that environment: scenario_plan.py validate refuses the row parameters it
cannot forward (parameter_problem), scenario_plan.py env prints its result for
a documented reproduction, and the fixture module's catalog tests ask it for
every row they pass through loadProfile (`serve`).

A row parameter p (a lower-case name) reaches the fixture as
CEPH_MSGR_TEST_<P>, its value unchanged, and only when the matrix sets it;
the fixture owns its default. Which parameters exist is the fixture's own
decision: loadEnvironmentProfile refuses a CEPH_MSGR_TEST_ name that no input
of loadProfile reads, and validate passes every row through that admission.
A new fixture input is declared with defineInput
(integration/fixture/input.go) beside the code that reads it, and the rows
set it; it edits nothing here, no workflow line and no field of profile.

The parameters that the workflow bound before this rule (LEGACY) keep their
names and fallbacks exactly; that set is frozen and a new parameter never
joins it (tools/test_native_env.py).

  native_env.py -- COMMAND [ARG ...]   exec COMMAND with MATRIX_JSON's fixture inputs
  native_env.py serve                  one JSON matrix per input line, one JSON answer per output line
"""
import json
import os
import re
import subprocess
import sys

PREFIX = "CEPH_MSGR_TEST_"
PARAMETER = re.compile(r"[a-z][a-z0-9_]{0,63}")

IMAGE = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

# Frozen: the matrix parameters the workflow bound one line each before
# forwarding became a rule, in that order, with their variable and fallback.
# These variables are always set, as those lines set them. A new parameter
# never joins this table; it is forwarded by the rule above.
LEGACY = {
    "image": ("CEPH_MSGR_TEST_IMAGE", IMAGE),
    "key": ("CEPH_MSGR_TEST_KEY_TYPE", ""),
    "service": ("CEPH_MSGR_TEST_SERVICE_CIPHER", ""),
    "family": ("CEPH_MSGR_TEST_IP_FAMILY", ""),
    "stress": ("CEPH_MSGR_STRESS_DURATION", ""),
    "managers": ("CEPH_MSGR_TEST_MGR_COUNT", "2"),
    "osd": ("CEPH_MSGR_TEST_OSD", "0"),
    "osd_profile": ("CEPH_MSGR_TEST_OSD_PROFILE", "legacy-v6"),
    "osd_cache_pools": ("CEPH_MSGR_TEST_OSD_CACHE_POOLS", "0"),
    "mgr_service": ("CEPH_MSGR_TEST_MGR_SERVICE", "0"),
    "mds_fresh": ("CEPH_MSGR_TEST_MDS_FRESH", "0"),
    "mds_commands": ("CEPH_MSGR_TEST_MDS_COMMANDS", "0"),
    "mds_sessions": ("CEPH_MSGR_TEST_MDS_SESSIONS", "0"),
    "mds_cephfs": ("CEPH_MSGR_TEST_MDS_CEPHFS", "0"),
    "mds_cephfs_ranks": ("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS", "0"),
    "mds_cephfs_ranks_replay": ("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY", "0"),
    "mds_cephfs_completion": ("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION", "0"),
    "mds_cephfs_data": ("CEPH_MSGR_TEST_MDS_CEPHFS_DATA", "0"),
    "rbd": ("CEPH_MSGR_TEST_RBD", "0"),
    "mon_ping": ("CEPH_MSGR_TEST_MON_PING", "0"),
    "rgw_native": ("CEPH_MSGR_TEST_RGW_NATIVE", "0"),
    "test_run": ("CEPH_MSGR_TEST_RUN", ""),
    "test_sequence": ("CEPH_MSGR_TEST_SEQUENCE", ""),
    "capabilities": ("CEPH_MSGR_TEST_CAPABILITIES", ""),
    "osd_store": ("CEPH_MSGR_TEST_OSD_STORE", "memstore"),
    "osd_replicas": ("CEPH_MSGR_TEST_OSD_REPLICAS", "1"),
    "osd_soak": ("CEPH_MSGR_TEST_OSD_SOAK", ""),
    "runtime": ("CEPH_MSGR_TEST_RUNTIME", "container"),
    "race": ("CEPH_MSGR_TEST_RACE", "0"),
    "expiry": ("CEPH_MSGR_TEST_EXPIRE_TICKETS", "0"),
    "idle": ("CEPH_MSGR_TEST_IDLE_SESSIONS", "0"),
    "auth_epoch": ("CEPH_MSGR_TEST_AUTH_EPOCH", "0"),
    "native_mgr_preparation": ("CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", "0"),
    "short_tickets": ("CEPH_MSGR_TEST_SHORT_TICKETS", "0"),
    "mode_rejection": ("CEPH_MSGR_TEST_MODE_REJECTION", ""),
    "mode": ("CEPH_MSGR_TEST_CONNECTION_MODE", "secure"),
    "mapped": ("CEPH_MSGR_TEST_MAPPED_IPV6", "0"),
}
LEGACY_NAMES = {name: param for param, (name, _) in LEGACY.items()}

# Matrix keys that describe the job, never a fixture input: the planner's own
# (a fixed check's marker, the first row, the rows and the row count of a
# fixture sequence) and the display release (the image selects the daemon).
# A row may set release; it may not set a planner key.
PLANNER_KEYS = frozenset({"check", "scenario", "scenarios", "sequence_rows"})
UNFORWARDED = PLANNER_KEYS | {"release"}

# The fixture inputs the workflow's run step sets itself. No row parameter
# may name one; the step's own value reaches the runner unchanged.
RESERVED = frozenset({"CEPH_MSGR_TEST_DIAGNOSTICS"})


class ForwardingError(Exception):
    pass


def variable(param):
    """The variable that carries a row parameter, or None if it is not one."""
    if param in LEGACY:
        return LEGACY[param][0]
    if param in UNFORWARDED or not isinstance(param, str) or not PARAMETER.fullmatch(param):
        return None
    return PREFIX + param.upper()


def parameter_problem(param):
    """Why a catalog row cannot set param, or None when it can."""
    if not isinstance(param, str) or not PARAMETER.fullmatch(param):
        return "a parameter is a lower-case name of letters, digits and underscores"
    if param in PLANNER_KEYS:
        return "the planner sets it"
    if param in LEGACY or param in UNFORWARDED:
        return None
    name = variable(param)
    if name in RESERVED:
        return "%s is set by the native workflow itself" % name
    if name in LEGACY_NAMES:
        return "%s is the variable of parameter %s" % (name, LEGACY_NAMES[name])
    return None


def environment(matrix):
    """{variable: value} that the runner receives for one job's matrix: the
    frozen LEGACY variables first, in their order, then every other
    forwarded parameter in name order."""
    if not isinstance(matrix, dict):
        raise ForwardingError("a job's matrix is a JSON object")
    for param, value in matrix.items():
        if param in UNFORWARDED:
            continue
        problem = parameter_problem(param)
        if problem:
            raise ForwardingError("matrix parameter %r cannot be forwarded: %s" % (param, problem))
        if not isinstance(value, str):
            raise ForwardingError("matrix parameter %s is not a string" % param)
    result = {name: matrix.get(param) or fallback for param, (name, fallback) in LEGACY.items()}
    for param in sorted(matrix.keys() - LEGACY.keys() - UNFORWARDED):
        result[variable(param)] = matrix[param]
    return result


def runner_environment(base, matrix):
    """base with the job's fixture inputs added; a variable base already sets
    (the workflow's own, RESERVED) is never replaced."""
    forwarded = environment(matrix)
    present = sorted(set(forwarded) & set(base))
    if present:
        raise ForwardingError("the step already sets %s" % ", ".join(present))
    return dict(base, **forwarded)


def serve(lines, output):
    for line in lines:
        try:
            answer = {"environment": environment(json.loads(line))}
        except (ValueError, ForwardingError) as error:
            answer = {"error": str(error)}
        output.write(json.dumps(answer) + "\n")
        output.flush()


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["serve"]:
        serve(sys.stdin, sys.stdout)
        return 0
    if len(argv) < 2 or argv[0] != "--":
        print(__doc__, file=sys.stderr)
        return 2
    try:
        env = runner_environment(dict(os.environ), json.loads(os.environ["MATRIX_JSON"]))
    except KeyError:
        print("native_env: MATRIX_JSON is not set", file=sys.stderr)
        return 2
    except (ValueError, ForwardingError) as error:
        print("native_env: %s" % error, file=sys.stderr)
        return 2
    sys.stdout.flush()
    if os.name == "nt":
        # Windows has no exec that replaces the process: os.execvpe starts a
        # new one and ends this one at once, so the caller would not see the
        # runner's status. Run it and return its status instead.
        return subprocess.call(argv[1:], env=env)
    os.execvpe(argv[1], argv[1:], env)


if __name__ == "__main__":
    sys.exit(main())
