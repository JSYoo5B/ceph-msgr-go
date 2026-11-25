import unittest

import osd_ops_in_flight_oracle as ops
from osd_pool_mode_repopinflight_oracle import repops


class ReplicaTransactions(unittest.TestCase):
    def test_decoded_identity_version_and_events(self):
        event = {'description': "osd_repop(client.42.3:9 2.a e17/15 2:abc:::dst:head v 17'8, pct=17'6)",
                 'flag_point': 'started', 'events': ['initiated', 'started']}
        got = repops([event], 42, 3)
        self.assertEqual(len(got), 1)
        self.assertEqual((got[0]['reqid'], got[0]['version'], got[0]['epoch']), ('client.42.3:9', "17'8", 17))
        self.assertEqual(got[0]['events'], event['events'])
        self.assertEqual(repops([event], 42, 4), [])
        event['events'].append('commit_sent')
        self.assertIn('commit_sent', repops([event], 42, 3)[0]['events'])

    def test_undecoded_or_malformed_target_refused(self):
        for description in ['osd_repop(client.42.3:9 2.a e17/15)',
                            "osd_repop(client.42.3:9 2.a e17/15 dst v broken, pct=17'6)"]:
            with self.assertRaises(ops.OpsError):
                repops([dict(description=description)], 42, 3)
