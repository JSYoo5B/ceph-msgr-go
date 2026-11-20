import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import protocol_evidence as evidence
from test_go_test_list import REPEATED, SHAPES, WRONG_FILE, module, tests_file


class ProtocolEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.text = (evidence.ROOT / evidence.EVIDENCE).read_text()
        self.receipts = evidence.receipt_texts(evidence.ROOT)

    def check(self, text=None, receipts=None):
        return evidence.check(text=self.text if text is None else text,
                              receipts=self.receipts if receipts is None else receipts)

    def with_receipt(self, name, text):
        receipts = dict(self.receipts)
        if text is None:
            del receipts[name]
        else:
            receipts[name] = text
        return receipts

    def receipt(self, name, category="exact-commit", boundary="One profile; one boundary",
                record="[Record](../../.github/scenarios/README.md)"):
        lines = ["# Native receipt " + name, ""]
        for field, value in (("Attribution", category), ("Boundary", boundary), ("Record", record)):
            if value is not None:
                lines += [field + ": " + value, ""]
        return "\n".join(lines)

    def table_row(self, start):
        return next(line for line in self.text.splitlines() if line.startswith(start))

    def swapped(self, first, second):
        a, b = self.table_row(first), self.table_row(second)
        return self.text.replace(a, "\0").replace(b, a).replace("\0", b)

    def test_current_join_covers_messages_roles_and_real_unit_definitions(self):
        self.assertEqual(self.check(), [])
        self.assertEqual(len(evidence.message_rows(evidence.ROOT)), 50)
        self.assertEqual(len(evidence.feature_rows(evidence.ROOT)), 11)
        self.assertEqual(len(evidence.frame_rows(evidence.ROOT)), 22)
        catalog = evidence.opcode_rows(evidence.ROOT)
        self.assertEqual(len(catalog), 80)
        self.assertEqual(sum(row["status"] == "implemented" for row in catalog.values()), 56)
        self.assertLessEqual(evidence.NUMBERED_RECEIPTS, set(self.receipts))

    def test_every_implemented_opcode_needs_its_own_join(self):
        row = next(line for line in self.text.splitlines() if line.startswith("| READ |"))
        self.assertIn("opcodes: missing row READ", self.check(self.text.replace(row, "")))
        self.assertIn("opcodes: duplicate row READ", self.check(self.text.replace(row, row + "\n" + row)))
        text = self.text.replace(row, row.replace("| READ |", "| MASKTRUNC |"))
        self.assertIn("opcodes: undeclared row MASKTRUNC", self.check(text))

    def test_opcode_requires_the_catalog_wire_value_and_a_contract(self):
        row = next(line for line in self.text.splitlines() if line.startswith("| READ |"))
        text = self.text.replace(row, row.replace("`0x1201`", "`0x2201`"))
        self.assertIn("opcode READ: wrong wire value", self.check(text))
        text = self.text.replace(row, row.replace("[CONTRACT.md](../internal/osd/CONTRACT.md)", "none"))
        self.assertIn("opcode READ: missing contract link", self.check(text))

    def test_malformed_opcode_catalog_cannot_silently_omit_rows(self):
        documents = [
            "name,opcode\nREAD,0x1201\n",
            "name,opcode,status\nREAD,0x1201,unknown\n",
            "name,opcode,status\nREAD,0x1201,implemented\nREAD,0x1202,implemented\n",
            "name,opcode,status\nREAD,0x1201,implemented\nSTAT,0x1201,implemented\n",
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "internal/osd/opcodes-tentacle.csv"
            path.parent.mkdir(parents=True)
            for document in documents:
                with self.subTest(document=document):
                    path.write_text(document)
                    with self.assertRaises(ValueError):
                        evidence.opcode_rows(root)

    def test_every_frame_needs_a_distinct_join(self):
        row = next(line for line in self.text.splitlines() if line.startswith("| 12 |"))
        self.assertIn("frames: missing row 12", self.check(self.text.replace(row, "")))
        self.assertIn("frames: duplicate row 12", self.check(self.text.replace(row, row + "\n" + row)))
        self.assertIn("frames: undeclared row 99", self.check(self.text.replace(row, row.replace("| 12 |", "| 99 |"))))

    def test_frame_observation_needs_both_attribution_and_observation_kind(self):
        receipts = {"R1": ["R1", "exact-commit", "profile", "record"],
                    "U": ["U", "unjoined", "gap", "none"]}
        for value in ("R1:inferred", "R1:reported", "R1:captured", "U:unobserved",
                      "R1:inferred, U:unobserved"):
            with self.subTest(value=value):
                self.assertEqual(evidence.frame_receipt_problems(value, receipts), [])
        for value in ("R1", "R1:PASS", "U:captured", "R1:unobserved", "missing:reported"):
            with self.subTest(value=value):
                self.assertTrue(evidence.frame_receipt_problems(value, receipts))

    def test_frame_join_rejects_unknown_receipt_and_invalid_observation(self):
        text = self.text.replace("| R9:reported |", "| missing:reported |")
        self.assertIn("frame 12: unknown receipt missing", self.check(text))
        text = self.text.replace("| R9:reported |", "| R9:passed |")
        self.assertIn("frame 12: invalid observation R9:passed", self.check(text))

    def test_frame_unit_selector_is_not_an_integration_receipt(self):
        text = self.text.replace("[TestHandshakeMissingFeaturesPreservesPeerMask](../internal/session/handshake_refusal_test.go)",
                                 "[TestCephMonPingIntegration](../integration/mon_ping_test.go)")
        self.assertIn("frame 10: integration selector is not unit evidence", self.check(text))

    def test_deleted_or_duplicated_message_cannot_hide_in_another_row(self):
        row = next(line for line in self.text.splitlines() if line.startswith("| 42 |"))
        self.assertIn("messages: missing row 42", self.check(self.text.replace(row, "")))
        self.assertIn("messages: duplicate row 42", self.check(self.text.replace(row, row + "\n" + row)))
        self.assertIn("messages: undeclared row 999", self.check(self.text.replace("| 42 |", "| 999 |")))

    def test_every_feature_profile_and_opt_in_needs_its_own_join(self):
        row = next(line for line in self.text.splitlines() if line.startswith("| MON MDSENC opt-in |"))
        self.assertIn("features: missing row MON MDSENC opt-in", self.check(self.text.replace(row, "")))
        self.assertIn("features: duplicate row MON MDSENC opt-in",
                      self.check(self.text.replace(row, row + "\n" + row)))

    def test_defined_only_constant_cannot_borrow_implemented_evidence(self):
        text = self.text.replace("| none | none |", "| none | R2 |", 1)
        self.assertTrue(any("defined-only row must refuse support/evidence" in p for p in self.check(text)))

    def test_nonexistent_or_renamed_test_is_not_a_selector_receipt(self):
        text = self.text.replace("[TestMonPingRejectsMalformedReply]", "[TestInventedPass]")
        self.assertIn("message 2: missing top-level Go test TestInventedPass", self.check(text))
        text = self.text.replace("[TestMonPingRejectsMalformedReply](mon_ping_test.go)", "none")
        self.assertIn("message 2: missing unit test link", self.check(text))

    def test_linked_test_must_be_one_go_lists_for_that_file(self):
        root = module(self, dict(SHAPES, **WRONG_FILE))
        package = root / "integration"
        # Literal expectation: the bare Test, the aliased-import test and the
        # blank parameter are tests; TestMain, the method, the lowercase helper
        # and the build-constrained test are not.
        for name, file in (("Test", "shapes_test.go"), ("TestX", "shapes_test.go"), ("TestAlias", "alias_test.go")):
            self.assertTrue(evidence.listed_test(root, package / file, name), name)
        for name, file in (("TestMain", "shapes_test.go"), ("TestY", "shapes_test.go"), ("Testable", "shapes_test.go"),
                           ("TestIgnored", "ignored_test.go"), ("TestAlias", "shapes_test.go"),
                           ("TestActual", "wrong_test.go")):
            self.assertFalse(evidence.listed_test(root, package / file, name), name)
        self.assertTrue(evidence.listed_test(root, package / "right_test.go", "TestActual"))

    def test_linked_test_of_a_nested_module_is_listed_by_that_module(self):
        root = module(self, {})
        nested = root / "fixture"
        (nested / "sub").mkdir(parents=True)
        (nested / "go.mod").write_text("module example.com/nested\n\ngo 1.24\n", encoding="utf-8")
        (nested / "sub" / "a_test.go").write_text(tests_file("func TestNested(t *testing.T) {}", package="sub"),
                                                  encoding="utf-8")
        self.assertEqual(evidence.module_dir(root, nested / "sub" / "a_test.go"), nested.resolve())
        self.assertTrue(evidence.listed_test(root, nested / "sub" / "a_test.go", "TestNested"))
        self.assertFalse(evidence.listed_test(root, nested / "sub" / "a_test.go", "TestOther"))

    def test_repeated_init_functions_do_not_hide_a_linked_test(self):
        root = module(self, REPEATED)
        package = root / "integration"
        self.assertTrue(evidence.listed_test(root, package / "right_test.go", "TestActual"))
        self.assertFalse(evidence.listed_test(root, package / "wrong_test.go", "TestActual"))

    def test_product_source_or_integration_selector_is_not_unit_evidence(self):
        text = self.text.replace("(mon_ping_test.go)", "(mon_ping.go)")
        self.assertIn("message 2: missing top-level Go test TestMonPingRejectsMalformedReply", self.check(text))
        text = self.text.replace("[TestMonPingRejectsMalformedReply](mon_ping_test.go)",
                                 "[TestCephMonPingIntegration](../integration/mon_ping_test.go)")
        self.assertIn("message 2: integration selector is not unit evidence", self.check(text))

    def test_receipt_ids_are_not_free_text_or_silent_fallbacks(self):
        text = self.text.replace("| R1 |\n", "| R999 |\n", 1)
        self.assertIn("message 2: unknown receipt R999", self.check(text))
        receipts = self.with_receipt("R1", self.receipts["R1"].replace(
            "Attribution: exact-commit", "Attribution: all-native-passed"))
        self.assertIn("receipt R1: unknown attribution category", self.check(receipts=receipts))

    def test_native_receipt_needs_a_checked_in_record_not_a_run_url_alone(self):
        record = "[Manual request receipt](../../.github/scenarios/README.md)"
        self.assertIn(record, self.receipts["R1"])
        receipts = self.with_receipt("R1", self.receipts["R1"].replace(record, "none"))
        self.assertIn("receipt R1: missing local record", self.check(receipts=receipts))
        receipts = self.with_receipt("R1", self.receipts["R1"].replace(
            "../../.github/scenarios/README.md", "https://example.invalid/run"))
        self.assertTrue(any("missing or non-repository file" in p for p in self.check(receipts=receipts)))

    def test_receipt_record_links_resolve_from_the_receipt_file(self):
        # The pre-directory spelling, relative to the ledger, points outside
        # the repository from cephmsgr/receipts/ and is refused.
        receipts = self.with_receipt("R1", self.receipts["R1"].replace(
            "../../.github/scenarios/README.md", "../.github/scenarios/README.md"))
        self.assertTrue(any("missing or non-repository file" in p for p in self.check(receipts=receipts)))

    def test_new_receipt_takes_a_name_not_the_next_number(self):
        for name in ("R41", "R36", "R0"):
            with self.subTest(name=name):
                problems = self.check(receipts=self.with_receipt(name, self.receipt(name)))
                self.assertIn("receipt " + name + ": numbered IDs are frozen; name a new receipt instead", problems)
        for name in ("Osd-tier", "osd_tier", "osd tier", "osd:tier", "-osd"):
            with self.subTest(name=name):
                problems = self.check(receipts=self.with_receipt(name, self.receipt(name)))
                self.assertIn("receipt " + name + ": ID must be lowercase words joined by hyphens", problems)
        receipts = self.with_receipt("osd-tier-go", self.receipt("osd-tier-go"))
        row = self.table_row("| READ |")
        text = self.text.replace(row, row.replace("| R12 |", "| R12, osd-tier-go |"))
        self.assertEqual(self.check(text, receipts), [])
        self.assertIn("opcode READ: unknown receipt osd-tier-go", self.check(text))

    def test_frozen_numbered_receipt_cannot_disappear(self):
        problems = self.check(receipts=self.with_receipt("R27", None))
        self.assertIn("receipt R27: cited numbered ID has no file", problems)

    def test_receipt_file_needs_heading_category_boundary_and_record(self):
        cases = (
            (self.receipt("osd-x", category=None), "receipt osd-x: missing Attribution"),
            (self.receipt("osd-x", boundary=None), "receipt osd-x: missing Boundary"),
            (self.receipt("osd-x", record=None), "receipt osd-x: missing Record"),
            (self.receipt("osd-x", category=""), "receipt osd-x: missing Attribution"),
            (self.receipt("osd-y"), "receipt osd-x: first line must be '# Native receipt osd-x'"),
            (self.receipt("osd-x") + "Record: none\n", "receipt osd-x: duplicate Record"),
            (self.receipt("osd-x") + "Note: more\n", "receipt osd-x: unexpected line Note: more"),
        )
        for text, problem in cases:
            with self.subTest(problem=problem):
                self.assertIn(problem, self.check(receipts=self.with_receipt("osd-x", text)))
        self.assertEqual(self.check(receipts=self.with_receipt("osd-x", self.receipt("osd-x"))), [])

    def test_rows_follow_their_source_order(self):
        cases = (
            ("| READ |", "| STAT |", "opcodes: row READ is out of opcode catalog order (after STAT)"),
            ("| SET_CHUNK |", "| TIER_PROMOTE |", "opcodes: row SET_CHUNK is out of opcode catalog order (after TIER_PROMOTE)"),
            ("| 2 | PingMonitor;", "| 4 | Snapshot MON", "messages: row 2 is out of message type order (after 4)"),
            ("| 1 | Dial", "| 2 |", "frames: row 1 is out of frame tag order (after 2)"),
            ("| MON (entity type 1) |", "| OSD (entity type 4) |",
             "features: row MON (entity type 1) is out of feature ledger order (after OSD (entity type 4))"),
        )
        for first, second, problem in cases:
            with self.subTest(problem=problem):
                self.assertIn(problem, self.check(self.swapped(first, second)))
        # Appending a moved row at the end of the table is refused as well.
        row = self.table_row("| READ |")
        last = self.table_row("| SCRUBLS |")
        text = self.text.replace(row + "\n", "").replace(last, last + "\n" + row)
        self.assertIn("opcodes: row READ is out of opcode catalog order (after SCRUBLS)", self.check(text))

    def test_missing_or_escaping_links_cannot_pass(self):
        for target in ("missing_test.go", "../../../outside_test.go"):
            with self.subTest(target=target):
                text = self.text.replace("(mon_ping_test.go)", "(" + target + ")")
                self.assertTrue(any("missing or non-repository file" in p for p in self.check(text)))

    def test_missing_section_empty_table_and_malformed_rows_fail(self):
        with self.assertRaisesRegex(ValueError, "missing table section"):
            self.check(self.text.replace("## Message evidence", "## Missing"))
        with self.assertRaisesRegex(ValueError, "empty table section"):
            evidence.table("## Missing\n", "Missing")
        text = self.text.replace("| 2 | PingMonitor;", "| 2 | | PingMonitor;")
        self.assertIn("messages: expected four nonempty columns", self.check(text))

    def test_cli_returns_failure_for_unreadable_or_invalid_join(self):
        with patch.object(evidence, "check", side_effect=OSError("unavailable")):
            with patch("sys.stderr"):
                self.assertEqual(evidence.main(), 1)
        with patch.object(evidence, "check", return_value=[]):
            with patch("sys.stdout"):
                self.assertEqual(evidence.main(), 0)


if __name__ == "__main__":
    unittest.main()
