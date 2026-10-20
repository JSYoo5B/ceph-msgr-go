#!/usr/bin/env python3
"""Strict parser and authorization for the `/ci run` pull-request command.

Run by .github/workflows/ci-command.yml from the default branch. Every input
arrives through environment variables, never through shell interpolation of the
comment. The tool decides only whether a request is acceptable and which
selectors it names; resolving the names against the tested revision's catalog is
tools/scenario_plan.py's job.

Grammar (one command per comment, on its own line):

    /ci run <selector> [<selector> ...]

A selector is `all`, `group:<group>` or a scenario identifier made of
[A-Za-z0-9._-]. Lines inside fenced code blocks or block quotes are ignored, so
quoting an earlier command never runs it. A comment without a `/ci` line is not
a command and is ignored silently; a malformed or ambiguous command is refused
with an explanation.

  ci_command.py parse    env COMMENT_BODY AUTHOR_ASSOCIATION IS_PULL_REQUEST
  ci_command.py pull     env HEAD_REPO BASE_REPO HEAD_SHA PR_STATE
"""
import os
import re
import sys

TRUSTED_ASSOCIATIONS = ("OWNER", "MEMBER", "COLLABORATOR")
MAX_SELECTORS = 20
SELECTOR = re.compile(r"^(all|group:[a-z0-9][a-z0-9-]*|[a-z0-9][a-z0-9._-]*)$")
SHA = re.compile(r"^[0-9a-f]{40}$")
USAGE = "usage: `/ci run <scenario-id|group:<group>|all> ...` on its own line"


class CommandError(Exception):
    """The comment is a command, but it is refused."""


def command_lines(body):
    """Lines that start a command, outside code fences and block quotes."""
    found, fence = [], None
    for raw in body.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw.rstrip()
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence is not None:
            if (marker and marker[1][0] == fence[0]
                    and len(marker[1]) >= len(fence) and not marker[2].strip()):
                fence = None
            continue
        if marker:
            fence = marker[1]
            continue
        if line.startswith(">"):
            continue
        if line == "/ci" or line.startswith("/ci ") or line.startswith("/ci\t"):
            found.append(line)
    return found


def parse_comment(body):
    """Return the ordered, de-duplicated selectors, or None if not a command."""
    lines = command_lines(body)
    if not lines:
        return None
    if len(lines) > 1:
        raise CommandError("more than one /ci command in one comment; " + USAGE)
    parts = lines[0].split()
    if len(parts) < 3 or parts[0] != "/ci" or parts[1] != "run":
        raise CommandError("unsupported command; " + USAGE)
    selectors = []
    for selector in parts[2:]:
        if not SELECTOR.match(selector) or len(selector) > 160:
            raise CommandError("invalid selector %r; %s" % (selector[:40], USAGE))
        if selector not in selectors:
            selectors.append(selector)
    if len(selectors) > MAX_SELECTORS:
        raise CommandError("at most %d selectors per command" % MAX_SELECTORS)
    return selectors


def authorize(association, is_pull_request):
    """Return a refusal reason, or None when the commenter may request a run."""
    if not is_pull_request:
        return "commands are accepted only on pull requests"
    if association not in TRUSTED_ASSOCIATIONS:
        return "only repository owners, members and collaborators can request runs"
    return None


def check_pull(head_repo, base_repo, head_sha, state):
    """Return a refusal reason for a pull request that cannot be tested, else None."""
    if state != "open":
        return "the pull request is %s, not open" % (state or "in an unknown state")
    if not head_repo or head_repo != base_repo:
        return "pull requests from forks are not supported"
    if not SHA.match(head_sha or ""):
        return "the pull request head is not a full commit identifier"
    return None


def write_output(**values):
    path = os.environ.get("GITHUB_OUTPUT")
    lines = []
    for name, value in values.items():
        value = str(value)
        if "\n" in value or "\r" in value:
            raise ValueError("output %s must be a single line" % name)
        lines.append("%s=%s\n" % (name, value))
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.writelines(lines)
    else:
        sys.stdout.writelines(lines)


def run_parse(env):
    try:
        selectors = parse_comment(env.get("COMMENT_BODY", ""))
    except CommandError as error:
        write_output(command="refused", selectors="", reason=str(error))
        return 0
    if selectors is None:
        write_output(command="none", selectors="", reason="")
        return 0
    reason = authorize(env.get("AUTHOR_ASSOCIATION", ""), env.get("IS_PULL_REQUEST") == "true")
    if reason:
        write_output(command="refused", selectors="", reason=reason)
        return 0
    write_output(command="accepted", selectors=" ".join(selectors), reason="")
    return 0


def run_pull(env):
    reason = check_pull(env.get("HEAD_REPO"), env.get("BASE_REPO"), env.get("HEAD_SHA"), env.get("PR_STATE"))
    write_output(pull="refused" if reason else "accepted", reason=reason or "")
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1 or argv[0] not in ("parse", "pull"):
        print(__doc__, file=sys.stderr)
        return 2
    return {"parse": run_parse, "pull": run_pull}[argv[0]](os.environ)


if __name__ == "__main__":
    sys.exit(main())
