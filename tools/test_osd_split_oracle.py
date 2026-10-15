#!/usr/bin/env python3
"""Mocks validate the recipe boundary; they are not native split evidence."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import osd_split_oracle as oracle

FSID="12345678-1234-1234-1234-123456789abc"
TOKEN="1"*32
NAME="fixture-native-omapw-main-"+"a"*32
META=dict(pool="ceph-msgr-osd-fixture",pool_id=7,object="fixture",profile=oracle.PROFILE,replicas=1,pool_pg_num=8,osd_id=0)
OWNED=dict(schema_version=1,source_commit=oracle.xattr.SOURCE_COMMIT,pool=META["pool"],pool_id=7,fixture_object="fixture",namespace="",objects=[NAME])
STATE=dict(code=0,version=3,data="AP8=",seconds=1,nanos=2,attributes=[],gets=[])


def pool(n=8,pgp=None):
    pgp=n if pgp is None else pgp
    return dict(pool=7,pool_name=META["pool"],type=1,size=1,min_size=1,flags=1,crush_rule=0,pg_autoscale_mode="off",pg_num=n,pg_num_target=n,pg_num_pending=n,pg_placement_num=pgp,pg_placement_num_target=pgp)


def topology(descriptor=None):
    return dict(fsid=FSID,epoch=11,osds=[dict(osd=0,up=1)],pools=[descriptor or pool()])


def observation(n=8,pgp=None):
    seed=3 if n==8 else 11
    pgid=f"7.{seed:x}"
    result=dict(pool=pool(n,pgp),epoch=11 if n==8 else 20,raw_hash=0xaabbccdb,all_active_clean=True,
                placement=dict(pool=META["pool"],pool_id=7,objname=NAME,pgid=pgid,raw_pgid="7.aabbccdb",acting_primary=0,acting=[0],epoch=11 if n==8 else 20),
                pg=dict(info=dict(pgid=pgid,history=dict(last_epoch_split=20 if n==16 else 0)),epoch=11 if n==8 else 20,acting=[0],state="active+clean"))
    if n==16:
        result["parent_pg"]=dict(info=dict(pgid="7.3",history=dict(last_epoch_split=20)),epoch=20,acting=[0],state="active+clean")
    return result


def scope():
    before=observation();before["object_state"]=copy.deepcopy(STATE);before["fixture_state"]=copy.deepcopy(STATE)
    return dict(schema_version=1,source_commit=oracle.xattr.SOURCE_COMMIT,token=TOKEN,fsid=FSID,ownership=copy.deepcopy(OWNED),object=NAME,before=before,ready=False,done=False,restored=False)


class SplitOracleTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.out=Path(self.tmp.name);self.root=self.out/"root"

    def seed(self,status=None):
        status=scope() if status is None else status
        oracle.omap.publish(self.out,oracle.NAME+"-status",status)
        oracle.omap.publish(self.out,oracle.token_name(status["token"],"status"),status)
        return status

    def test_explicit_input_and_token_boundaries(self):
        self.assertEqual(oracle.request({"object":NAME}),NAME)
        for raw in ({},None,{"object":""},{"object":"bad\0name"},{"object":NAME,"pool":"foreign"}):
            with self.subTest(raw=raw),self.assertRaises(ValueError):oracle.request(raw)
        for token in (None,"../foreign","f"*31,"z"*32):
            with self.assertRaises(ValueError):oracle.token_name(token,"status")

    def test_ownership_requires_optin_single_osd_and_owned_uuid(self):
        manifest=dict(osd_profile=oracle.PROFILE,osd_replicas="1",osd="1",fsid=FSID)
        (self.out/"daemon-version").write_text("20.2.4 "+oracle.xattr.SOURCE_COMMIT)
        for name,value in (("osd-oracle",META),("fixture-manifest",manifest),(oracle.writes.OWNED,OWNED),(oracle.NAME+"-input",dict(object=NAME))):oracle.omap.publish(self.out,name,value)
        self.assertEqual(oracle.owned(self.root,self.out)[3],NAME)
        for changed in (dict(META,profile="legacy-v6"),dict(META,replicas=3),dict(META,pool="foreign"),dict(META,pool_pg_num=16)):
            oracle.omap.publish(self.out,"osd-oracle",changed)
            with self.assertRaises(ValueError):oracle.owned(self.root,self.out)
        oracle.omap.publish(self.out,"osd-oracle",META)
        oracle.omap.publish(self.out,oracle.NAME+"-input",dict(object="unowned"))
        with self.assertRaises(ValueError):oracle.owned(self.root,self.out)

    def test_live_pool_checks_all_targets_pending_and_policy(self):
        self.assertEqual(oracle.checked_pool(topology(),META,FSID),pool())
        for raw in (dict(topology(),fsid="foreign"),dict(topology(),osds=[dict(osd=1,up=1)]),topology(dict(pool(),pg_num_target=32)),topology(dict(pool(),pg_num_pending=7)),topology(dict(pool(),pg_placement_num=9)),topology(dict(pool(),pg_autoscale_mode="on")),topology(dict(pool(),size=3)),topology(dict(pool(),flags=0)),topology(dict(pool(),flags=33))):
            with self.subTest(raw=raw),self.assertRaises(ValueError):oracle.checked_pool(raw,META,FSID)
        self.assertTrue(oracle.stable(pool(16),16));self.assertFalse(oracle.stable(dict(pool(),pg_num_target=16),8))

    def test_all_pg_stats_require_exact_seed_set_and_clean(self):
        stats=[dict(pgid=f"7.{n:x}",state="active+clean") for n in range(16)]
        self.assertTrue(oracle.pg_stats(stats,META,16));self.assertTrue(oracle.pg_stats(dict(pg_stats=stats),META,16))
        self.assertFalse(oracle.pg_stats(stats[:8],META,16))
        stats[0]["state"]="active+degraded";self.assertFalse(oracle.pg_stats(stats,META,16))
        with self.assertRaises(ValueError):oracle.pg_stats([dict(pgid="8.0",state="active+clean")],META,1)
        with self.assertRaises(ValueError):oracle.pg_stats([dict(pgid="7.0",state="active+clean")]*2,META,2)

    def test_read_observation_binds_applied_epoch_and_parent_split_history(self):
        split=observation(16);top=dict(topology(pool(16)),epoch=20)
        stats=[dict(pgid=f"7.{n:x}",state="active+clean") for n in range(16)]
        with patch.object(oracle,"ceph",side_effect=[top,split["placement"],split["pg"],stats,split["parent_pg"]]):
            result=oracle.observe(self.root,META,FSID,NAME,"7.3")
        self.assertEqual(result["parent_pg"]["info"]["history"]["last_epoch_split"],20)
        with patch.object(oracle,"ceph",side_effect=[top,dict(split["placement"],epoch=21)]),self.assertRaises(oracle.ObservationChanged):oracle.observe(self.root,META,FSID,NAME)

    def test_transition_wait_repeats_only_native_read_observations(self):
        with patch.object(oracle,"observe",side_effect=[oracle.ObservationChanged("map race"),observation(16)]),patch.object(oracle.time,"sleep") as sleep:
            result=oracle.wait_state(self.root,META,FSID,NAME,16)
        self.assertEqual(result["pool"]["pg_num"],16);sleep.assert_called_once()
        with patch.object(oracle,"observe",return_value=observation()),patch.object(oracle.time,"monotonic",side_effect=[0,100]),self.assertRaises(TimeoutError):oracle.wait_state(self.root,META,FSID,NAME,16)

    def test_mutation_allowlist_checks_live_owner_before_each_command(self):
        with patch.object(oracle,"ceph",side_effect=[topology(),None]) as command:
            oracle.set_pool(self.root,META,FSID,"pg_num",16)
        self.assertEqual(command.call_args_list[1].args[1],["osd","pool","set",META["pool"],"pg_num","16"])
        for field,value in (("size",2),("pg_num_actual",16),("pg_num",32),("pgp_num",1)):
            with patch.object(oracle,"ceph") as command,self.assertRaises(ValueError):oracle.set_pool(self.root,META,FSID,field,value)
            command.assert_not_called()

    def test_restore_pgp_before_merge_and_preserves_current_caller_state(self):
        status=scope();current=dict(STATE,version=4,data="bmV3")
        with patch.object(oracle,"object_state",side_effect=[current,current,STATE]),patch.object(oracle,"set_pool") as set_pool,patch.object(oracle,"wait_pgp_eight") as pgp,patch.object(oracle,"wait_state",return_value=observation()):
            oracle.restore(self.root,self.out,META,FSID,status)
        self.assertTrue(status["restored"]);self.assertEqual(status["pre_restore_object"],current)
        self.assertEqual([c.args[-2:] for c in set_pool.call_args_list],[("pgp_num",8),("pg_num",8)]);pgp.assert_called_once()

    def test_snapshot_or_pgp_failure_cannot_skip_original_pg_target_restore(self):
        for fail in ("snapshot","pgp"):
            status=scope()
            state_side=[RuntimeError("snapshot failed"),STATE,STATE] if fail=="snapshot" else [STATE,STATE,STATE]
            set_side=[RuntimeError("uncertain pgp command"),None] if fail=="pgp" else None
            with patch.object(oracle,"object_state",side_effect=state_side),patch.object(oracle,"set_pool",side_effect=set_side) as set_pool,patch.object(oracle,"wait_pgp_eight"),patch.object(oracle,"wait_state",return_value=observation()),self.assertRaises(RuntimeError):
                oracle.restore(self.root,self.out,META,FSID,status)
            self.assertEqual(set_pool.call_args_list[-1].args[-2:],("pg_num",8))

    def test_worker_finally_restores_after_uncertain_first_mutation(self):
        status=self.seed()
        with patch.object(oracle,"owned",return_value=(META,FSID,OWNED,NAME)),patch.object(oracle,"observe",return_value=observation()),patch.object(oracle,"set_pool",side_effect=RuntimeError("unknown first config result")),patch.object(oracle,"restore") as restore,self.assertRaises(RuntimeError):
            oracle.worker(self.root,self.out,TOKEN)
        restore.assert_called_once();saved=oracle.current(self.out,META,FSID,OWNED,NAME)
        self.assertTrue(saved["done"]);self.assertIn("unknown first",saved["error"])

    def test_worker_requires_real_child_and_restores_on_wrong_native_mapping(self):
        self.seed();wrong=observation(16);wrong["placement"]["pgid"]="7.3"
        with patch.object(oracle,"owned",return_value=(META,FSID,OWNED,NAME)),patch.object(oracle,"observe",return_value=observation()),patch.object(oracle,"set_pool"),patch.object(oracle,"wait_state",return_value=wrong),patch.object(oracle,"restore") as restore,self.assertRaises(ValueError):oracle.worker(self.root,self.out,TOKEN)
        restore.assert_called_once()

    def test_worker_auto_deadline_is_finite_and_restores(self):
        status=self.seed();split=observation(16)
        with patch.object(oracle,"owned",return_value=(META,FSID,OWNED,NAME)),patch.object(oracle,"observe",return_value=observation()),patch.object(oracle,"set_pool"),patch.object(oracle,"wait_state",return_value=split),patch.object(oracle,"object_state",return_value=STATE),patch.object(oracle.time,"monotonic",side_effect=[0,oracle.HOLD_SECONDS+1]),patch.object(oracle,"restore") as restore:
            oracle.worker(self.root,self.out,TOKEN)
        restore.assert_called_once();saved=oracle.current(self.out,META,FSID,OWNED,NAME)
        self.assertTrue(saved["automatic_restore"]);self.assertTrue(saved["done"])

    def test_stale_history_proves_only_exact_native_envelope_reception(self):
        status=scope();status.update(ready=True,split=observation(16))
        expected=dict(global_id=42,transaction=17,client_incarnation=95,map_epoch=11,pool=7,pg_seed=3,raw_hash=0xaabbccdb)
        self.assertEqual(oracle.stale_request(expected,status,META),expected)
        entry=dict(description="osd_op(client.42.95:17 7.3 7.aabbccdb (undecoded) ondisk+write+returnvec e11)",type_data=dict(client_info=dict(client="client.42",tid=17),events=[dict(event="reached_pg")]))
        result=oracle.parse_stale_history(dict(ops=[entry]),expected,TOKEN)
        self.assertTrue(result["reached_pg"]);self.assertFalse(result["body_decoded"]);self.assertIn("execution remains unknown",result["evidence"])
        for changed in (dict(expected,map_epoch=21),dict(expected,pg_seed=4),dict(expected,raw_hash=1),dict(expected,global_id=False)):
            with self.assertRaises(ValueError):oracle.stale_request(changed,status,META)
        for description in (entry["description"].replace("client.42","client.43"),entry["description"].replace(":17",":18"),entry["description"].replace("e11)","e20)")):
            self.assertIsNone(oracle.parse_stale_history(dict(ops=[dict(entry,description=description)]),expected,TOKEN))
        with self.assertRaises(ValueError):oracle.parse_stale_history(dict(ops=[entry,entry]),expected,TOKEN)

    def test_start_retires_only_validated_prior_scope_input(self):
        prior=scope();prior.update(ready=True,done=True,restored=True,split=observation(16))
        expected=dict(global_id=42,transaction=17,client_incarnation=95,map_epoch=11,pool=7,pg_seed=3,raw_hash=0xaabbccdb)
        path=self.out/(oracle.NAME+"-stale-input.json")
        oracle.omap.publish(self.out,oracle.NAME+"-stale-input",expected)
        with self.assertRaises(ValueError):oracle.archive_previous_stale(self.out,None,META)
        self.assertTrue(path.exists())
        with self.assertRaises(ValueError):oracle.archive_previous_stale(self.out,dict(prior,before=observation(16)),META)
        self.assertTrue(path.exists())
        oracle.archive_previous_stale(self.out,prior,META)
        self.assertFalse(path.exists())
        self.assertEqual(oracle.omap.read_json(self.out/(oracle.token_name(TOKEN,"stale-input")+".json")),expected)
        new=dict(prior,token="2"*32)
        # Even if exact old history is retained, a new scope has no input to
        # request or synthesize its own receipt from that old envelope.
        with patch.object(oracle,"ceph") as ceph:
            self.assertIsNone(oracle.observe_stale(self.root,self.out,META,new))
        ceph.assert_not_called()
        self.assertFalse((self.out/(oracle.token_name(new["token"],"stale-receipt")+".json")).exists())

    def test_orphan_input_stops_start_before_observation_or_mutation(self):
        oracle.omap.publish(self.out,oracle.NAME+"-stale-input",dict(global_id=42))
        with patch.object(oracle,"owned",return_value=(META,FSID,OWNED,NAME)),patch.object(oracle,"observe") as observe,patch.object(oracle,"set_pool") as mutate,patch.object(oracle.subprocess,"Popen") as spawn:
            with self.assertRaisesRegex(ValueError,"no previous owned scope"):
                oracle.run("start",self.root,self.out)
        observe.assert_not_called();mutate.assert_not_called();spawn.assert_not_called()
        self.assertTrue((self.out/(oracle.NAME+"-stale-input.json")).exists())
        self.assertFalse((self.out/(oracle.NAME+"-control.lock")).exists())

    def test_native_snapshot_has_process_timeout_and_never_executes_in_unit_test(self):
        process=SimpleNamespace(returncode=0,stdout=b'{"code":0}',stderr=b"")
        with patch.object(oracle.subprocess,"run",return_value=process) as run:
            self.assertEqual(oracle.object_state(self.root,self.out,NAME),dict(code=0))
        self.assertEqual(run.call_args.kwargs["timeout"],oracle.COMMAND_SECONDS)
        self.assertIn("snapshot",run.call_args.args[0])

    def test_current_scope_rejects_view_token_or_ownership_drift(self):
        status=self.seed();self.assertEqual(oracle.current(self.out,META,FSID,OWNED,NAME),status)
        status["object"]="foreign";oracle.omap.publish(self.out,oracle.NAME+"-status",status)
        with self.assertRaises(ValueError):oracle.current(self.out,META,FSID,OWNED,NAME)

if __name__=="__main__":unittest.main()
