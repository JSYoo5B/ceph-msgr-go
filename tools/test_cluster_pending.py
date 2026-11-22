"""Run the request scanner of integration/cluster.sh against controlled listings."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


def pending_function():
    text = (ROOT / "integration/cluster.sh").read_text()
    start = text.index("pending() {\n")
    return text[start:text.index("\n}\n", start) + 3]


@unittest.skipUnless(os.name == "posix", "cluster.sh is a POSIX shell script")
class ClusterPendingTests(unittest.TestCase):
    def pending(self, listings, name="observe-service-holder"):
        """Run pending NAME where the n-th listing of /out returns listings[n]
        (the last one again after the end), and return REQ_ID, or None."""
        with tempfile.TemporaryDirectory() as tmp:
            for n, listing in enumerate(listings):
                Path(tmp, "listing.%d" % n).write_text("".join(entry + "\n" for entry in listing))
            Path(tmp, "calls").write_text("")
            script = (
                'ls() { test "$1" = /out || exit 9; echo >> "$TMP/calls"; '
                'n=$(($(wc -l < "$TMP/calls") - 1)); test "$n" -lt "$LAST" || n=$LAST; '
                'cat "$TMP/listing.$n"; }\n' + pending_function() +
                'if pending "$NAME"; then echo "$REQ_ID $REQ"; else echo none; fi\n')
            result = subprocess.run(["sh", "-eu", "-c", script], check=True, capture_output=True, text=True,
                                    env=dict(os.environ, TMP=tmp, LAST=str(len(listings) - 1), NAME=name))
            output = result.stdout.strip()
            if output == "none":
                return None
            request_id, path = output.split(" ")
            self.assertEqual(path, "/out/%s.%s.request" % (name, request_id))
            return request_id

    def test_oldest_envelope_of_its_own_name(self):
        listing = ["observe-service-holder.20.request", "observe-service-holder.3.request",
                   "observe-service-holder-x.1.request", "observe-service-holder.2.rejected",
                   "service-holder-observed.1"]
        self.assertEqual(self.pending([listing]), "3")

    def test_no_envelope(self):
        self.assertIsNone(self.pending([["service-holder-observed.1"]]))

    def test_listing_that_misses_the_earlier_request(self):
        # The first listing missed the start published before the cleanup;
        # the second one returns both, and the start is served first.
        first = ["observe-service-holder.11.request"]
        second = ["observe-service-holder.10.request", "observe-service-holder.11.request"]
        self.assertEqual(self.pending([first, second]), "10")


if __name__ == "__main__":
    unittest.main()
