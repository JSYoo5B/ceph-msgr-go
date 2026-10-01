#!/usr/bin/env python3
"""Select bounded JSON crash metadata from Docker's streamed directory archive.

No archive member is extracted to disk. Logs, links and process dumps are
ignored; only regular files named meta containing a JSON object are emitted.
This development tool works with stopped containers and uses Python's stdlib.
"""

import json
from pathlib import PurePosixPath
import sys
import tarfile


MAX_METADATA_BYTES = 1024 * 1024


def collect(stream, output):
    try:
        with tarfile.open(fileobj=stream, mode="r|") as archive:
            for member in archive:
                if (not member.isfile() or PurePosixPath(member.name).name != "meta"
                        or not 0 < member.size <= MAX_METADATA_BYTES):
                    continue
                with archive.extractfile(member) as source:
                    try:
                        metadata = json.load(source)
                    except (ValueError, UnicodeError):
                        continue
                if isinstance(metadata, dict):
                    output.write(json.dumps(metadata, separators=(",", ":")) + "\n")
    except (tarfile.TarError, OSError):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(collect(sys.stdin.buffer, sys.stdout))
