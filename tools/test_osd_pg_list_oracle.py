#!/usr/bin/env python3
import json
import unittest
import osd_pg_list_oracle as oracle

class PGListOracleTests(unittest.TestCase):
    def test_raw_identity_and_locator_preserved(self):
        data = json.dumps([dict(namespace="tenant", name="x\0z", locator="route"), dict(namespace="tenant",name="a")]).encode()
        self.assertEqual(oracle.parse_listing(data,"tenant"), [dict(namespace="tenant",object="x\0z",key="route"), dict(namespace="tenant",object="a",key="")])

    def test_wrong_inventory_and_scope_rejected(self):
        for raw in ({}, [dict(namespace="else",name="a")], [dict(namespace="tenant",name=3)], [dict(namespace="tenant",name="a",extra=1)], [dict(namespace="tenant",name="a")]*2):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                oracle.parse_listing(json.dumps(raw).encode(),"tenant")

    def test_explicit_pg_input(self):
        self.assertEqual(oracle.request(dict(pg_seed=15,namespace="")),(15,""))
        for raw in (dict(pg_seed=True,namespace=""),dict(pg_seed=-1,namespace=""),dict(pg_seed=0,namespace="\x01"),dict(pg_seed=0,namespace="",pool="other")):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                oracle.request(raw)

if __name__ == "__main__":
    unittest.main()
