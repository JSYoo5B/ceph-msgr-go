"""Receipt check of TestRGWNativeCheckpointIntegration for tools/rgw_native_gate.py.

The small-object checkpoint logs one text receipt line; it must name the
pinned source and confirm the object, index and user cleanup.
"""
TEST = "TestRGWNativeCheckpointIntegration"
MARKER = "RGW_NATIVE_CHECKPOINT "


def check(line, gate):
    """(problems, summary fields) of the one receipt line."""
    problems = []
    if "object_index_user_cleanup=confirmed" not in line or gate.SOURCE not in line:
        problems.append("checkpoint receipt lacks its confirmed cleanup or pinned source")
    return problems, {}
