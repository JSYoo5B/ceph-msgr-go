#!/usr/bin/env python3
"""Record one native attempt even when its test step was interrupted.

The trusted workflow runs this in an always() step. It re-evaluates the
original required-test snapshot against whatever log exists, including one
whose test command ended before writing native-tests.json, and writes
scenario-result/tests.json and result.json (tested and controller SHA,
scenarios, run ID and attempt, job and step outcomes):

- test_evidence is missing without a snapshot or log, partial when required
  results are missing, completed when every required parent has a terminal
  result; a completed FAIL or SKIP still fails the gate. The required tests
  are the snapshot's expected tests and the tests the fixture announced as
  selected (native_test_results.py), so a broad run cut off before its last
  announced test is partial.
- test_result=passed needs both the reconstructed gate and a successful test
  step, so a cancelled step can keep a completed PASS while its attempt stays
  unaccepted, and missing evidence never yields acceptance.
- evidence_collection and evidence_upload keep their own step outcomes; a
  requested bundle needs both. native_cleanup is verified only when
  collection validated the family's cleanup fields, never for an interrupted
  job whose containers were removed later.
- result_artifact_upload is unobserved: this result precedes its own upload,
  and only the Actions API or artifact listing establishes publication.

The recorder refuses an existing result directory and exits unsuccessfully
for an unaccepted attempt. always() cannot run when GitHub terminates the
runner or cancels before the trusted tools are copied; an absent result then
establishes nothing about whether the test ran or cleaned up, and a later
attempt's artifacts never fill it.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil

import native_test_results


def record(env, directory):
    root = Path(directory)
    output = root / "scenario-result"
    output.mkdir(exist_ok=False)
    for key in ("TESTED_SHA", "CONTROLLER_SHA"):
        if not re.fullmatch(r"[0-9a-f]{40}", env[key]):
            raise ValueError("invalid commit identity")
    run_id, attempt = int(env["RUN_ID"]), int(env["RUN_ATTEMPT"])
    if run_id < 1 or attempt < 1:
        raise ValueError("invalid run identity")
    status = root / "evidence-status.json"
    if status.is_file():
        shutil.copyfile(status, output / "evidence-collection.json")
    gate = None
    expected = root / "expected-tests.json"
    log = root / "ceph-tests.log"
    if expected.is_file() and log.is_file():
        gate = native_test_results.verify(json.loads(expected.read_text()),
                                          log.read_text(errors="replace"))
        (output / "tests.json").write_text(json.dumps(gate, indent=2) + "\n")
    completeness = "missing"
    if gate is not None:
        required = set(map(tuple, gate["expected"])) | set(map(tuple, gate.get("selected", [])))
        observed = {(item["suite"], item["test"]) for item in gate["observed"]}
        completeness = "completed" if observed and required <= observed else "partial"
    outcome = env.get("TEST_OUTCOME", "")
    test_result = "unknown"
    if outcome in ("cancelled", "skipped"):
        test_result = outcome
    elif gate is not None and gate["passed"] and outcome == "success":
        test_result = "passed"
    elif outcome == "failure":
        test_result = "failed"
    pilot = env.get("EVIDENCE_PILOT") == "true"
    collection = env.get("EVIDENCE_COLLECTION", "") or "unobserved"
    upload = env.get("EVIDENCE_UPLOAD", "") or "unobserved"
    evidence = "not_requested"
    if pilot:
        evidence = "complete" if collection == upload == "success" else "incomplete"
    accepted = (env["RESULT"] == "success" and test_result == "passed"
                and evidence in ("complete", "not_requested"))
    result = {
        "schema_version": 1,
        "execution_source": env.get("EXECUTION_SOURCE", "github-actions"),
        "scenario": env["SCENARIO"],
        "scenarios": env.get("SCENARIOS", env["SCENARIO"]).split(),
        "commit": env["TESTED_SHA"],
        "controller_sha": env["CONTROLLER_SHA"],
        "run_id": run_id,
        "workflow_run": env.get("RUN_URL", ""),
        "run_attempt": attempt,
        "result": "failure" if env["RESULT"] == "success" and not accepted else env["RESULT"],
        "job_result_before_record": env["RESULT"],
        "image": env["IMAGE"],
        "test_result": test_result,
        "test_evidence": completeness,
        "test_gate_passed": gate["passed"] if gate is not None else False,
        "evidence_result": evidence,
        "evidence_collection": collection,
        "evidence_upload": upload,
        # A validated success bundle includes reviewed native cleanup flags.
        # An interrupted job or missing bundle cannot establish cleanup.
        "native_cleanup": "verified" if collection == "success" else "unverified",
        "result_artifact_upload": "unobserved",
        "accepted": accepted,
    }
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()
    result = record(os.environ, args.directory)
    if not result["accepted"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
