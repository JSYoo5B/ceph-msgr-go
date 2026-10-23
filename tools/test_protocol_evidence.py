import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import protocol_evidence as evidence


class ProtocolEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.text = (evidence.ROOT / evidence.EVIDENCE).read_text()

    def check(self, text):
        return evidence.check(text=text)

    def test_current_join_covers_messages_roles_and_real_unit_definitions(self):
        self.assertEqual(self.check(self.text), [])
        self.assertEqual(len(evidence.message_rows(evidence.ROOT)), 50)
        self.assertEqual(len(evidence.feature_rows(evidence.ROOT)), 11)
        self.assertEqual(len(evidence.frame_rows(evidence.ROOT)), 22)
        catalog = evidence.opcode_rows(evidence.ROOT)
        self.assertEqual(len(catalog), 80)
        self.assertEqual(sum(row["status"] == "implemented" for row in catalog.values()), 53)

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

    def test_product_source_or_integration_selector_is_not_unit_evidence(self):
        text = self.text.replace("(mon_ping_test.go)", "(mon_ping.go)")
        self.assertIn("message 2: missing top-level Go test TestMonPingRejectsMalformedReply", self.check(text))
        text = self.text.replace("[TestMonPingRejectsMalformedReply](mon_ping_test.go)",
                                 "[TestCephMonPingIntegration](../integration/mon_ping_test.go)")
        self.assertIn("message 2: integration selector is not unit evidence", self.check(text))

    def test_receipt_ids_are_not_free_text_or_silent_fallbacks(self):
        text = self.text.replace("| R1 |\n", "| R999 |\n", 1)
        self.assertIn("message 2: unknown receipt R999", self.check(text))
        text = self.text.replace("| R1 | exact-commit |", "| R1 | all-native-passed |")
        self.assertIn("receipt R1: unknown attribution category", self.check(text))

    def test_native_receipt_needs_a_checked_in_record_not_a_run_url_alone(self):
        text = self.text.replace("[Manual request receipt](../.github/scenarios/README.md)", "none")
        self.assertIn("receipt R1: missing local record", self.check(text))
        text = self.text.replace("../.github/scenarios/README.md", "https://example.invalid/run")
        self.assertTrue(any("missing or non-repository file" in p for p in self.check(text)))

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
