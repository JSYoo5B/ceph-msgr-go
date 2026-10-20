import base64
import ctypes as C
import struct
import unittest

import osd_checksum_compound_oracle as oracle


class ScriptedCAPI:
    """Pointer/buffer plumbing fake, not evidence of Ceph checksum behavior."""
    def __init__(self, suffix_stat=False):
        self.actions, self.calls, self.released = [], 0, 0
        self.suffix_stat = suffix_stat
    def rados_create_read_op(self): return 17
    def rados_release_read_op(self, operation): self.released += 1
    def rados_read_op_checksum(self, operation, kind, seed, width, offset, length, chunk, output, size, prval):
        self.actions.append(('checksum', kind, C.string_at(seed, width), offset, length, chunk, output, size, prval))
    def rados_read_op_stat2(self, operation, size, stamp, prval):
        self.actions.append(('stat', size, stamp, prval))
    def rados_read_op_read(self, operation, offset, length, output, count, prval):
        self.actions.append(('read', offset, length, output, count, prval))
    def rados_read_op_assert_version(self, operation, version):
        self.actions.append(('assert', version))
    def rados_read_op_cmpext(self, operation, data, length, offset, prval):
        self.actions.append(('cmpext', C.string_at(data, length), offset, prval))
    def rados_get_last_version(self, io): return 23
    def rados_read_op_operate(self, operation, io, name, flags):
        self.calls += 1
        for action in self.actions:
            if action[0] == 'checksum':
                _,kind,seed,offset,length,chunk,output,size,prval = action
                C.cast(prval, C.POINTER(C.c_int))[0] = 0
                count = length // chunk if chunk else 1
                data = struct.pack('<I', count) + b''.join((100+i).to_bytes(8 if kind == 1 else 4, 'little') for i in range(count))
                C.memmove(output, data, len(data))
            elif action[0] == 'stat':
                C.cast(action[3], C.POINTER(C.c_int))[0] = -5 if self.suffix_stat else 0
                if not self.suffix_stat:
                    C.cast(action[1], C.POINTER(C.c_uint64))[0] = 16
                    stamp = C.cast(action[2], C.POINTER(oracle.xattr.Timespec))[0]
                    stamp.tv_sec, stamp.tv_nsec = 1700000500, 123456789
            elif action[0] == 'read':
                _,offset,length,output,count,prval = action
                data = oracle.DATA[offset:offset+length]
                C.memmove(output, data, len(data))
                C.cast(count,C.POINTER(C.c_size_t))[0] = len(data)
                C.cast(prval,C.POINTER(C.c_int))[0] = 0
            elif action[0] == 'cmpext':
                C.cast(action[-1],C.POINTER(C.c_int))[0] = 0
        return -75 if self.suffix_stat else 0


class CompoundOracleTests(unittest.TestCase):
    def test_closed_input_refuses_extra_flags_operations_and_foreign_case(self):
        self.assertEqual(oracle.validate({'case':'mixed','object':'owned'})['object'],b'owned')
        for value in ({'case':'foreign','object':'owned'}, {'case':'mixed','object':'owned','flags':0},
                      {'case':'mixed','object':'owned','operations':[]}, {'case':'mixed','object':'x\0bad'},
                      {'case':True,'object':'owned'}):
            with self.subTest(value=value), self.assertRaises(ValueError): oracle.validate(value)

    def test_fixed_cases_include_separate_width_seeds_guards_and_missing(self):
        self.assertEqual(len(oracle.CASES),10)
        mixed=oracle.vector('mixed',23)
        self.assertEqual([a['op'] for a in mixed],['checksum','stat','read','checksum'])
        self.assertEqual((mixed[0]['seed'],mixed[3]['seed']),(0x1234,0xfedcba9876543210))
        self.assertEqual(oracle.vector('checksum-before-assert',23)[1]['version'],24)
        self.assertEqual(oracle.vector('assert-first',23)[0]['version'],24)
        comparison=base64.b64decode(oracle.vector('checksum-before-cmpext',23)[1]['data'])
        self.assertEqual([i for i,(a,b) in enumerate(zip(comparison,oracle.DATA[:4])) if a!=b],[2])
        for name in oracle.CASES:
            self.assertTrue(all(a['op'] in ('checksum','stat','read','assert','cmpext') for a in oracle.vector(name,23)))
        for version in (0,True,(1<<64)-1):
            with self.assertRaises(ValueError): oracle.vector('mixed',version)

    def test_one_native_operate_keeps_binary_buffers_and_independent_checksum_outputs(self):
        native=oracle.Native.__new__(oracle.Native);native.lib=ScriptedCAPI()
        result=native.compound(5,b'owned',oracle.vector('mixed',23))
        self.assertEqual((native.lib.calls,native.lib.released,result['operate_calls']),(1,1,1))
        self.assertEqual([a[0] for a in native.lib.actions],['checksum','stat','read','checksum'])
        self.assertEqual(native.lib.actions[0][2],struct.pack('<I',0x1234))
        self.assertEqual(native.lib.actions[3][2],struct.pack('<Q',0xfedcba9876543210))
        self.assertEqual(result['operations'][0]['values'],[100,101])
        self.assertEqual(result['operations'][3]['values'],[100])
        self.assertEqual(base64.b64decode(result['operations'][2]['data']),oracle.DATA[4:12])
        self.assertEqual(result['operations'][1]['nanos'],123456789)
        self.assertFalse(result['raw_wire_per_op_results_available'])
        self.assertFalse(result['returned_extent_union_available'])

    def test_assert_has_no_prval_and_stat_callback_error_remains_separate(self):
        native=oracle.Native.__new__(oracle.Native);native.lib=ScriptedCAPI(suffix_stat=True)
        result=native.compound(5,b'owned',oracle.vector('assert-first',23))
        self.assertEqual(result['aggregate'],-75)
        self.assertIsNone(result['operations'][0]['callback_prval'])
        self.assertFalse(result['operations'][0]['prval_available'])
        stat=result['operations'][2]
        self.assertEqual((stat['callback_prval'],stat['size'],stat['seconds'],stat['nanos']),(-5,(1<<63)-1,-7,-11))
        self.assertNotIn('raw_result',stat)
        self.assertEqual(native.lib.calls,1)

    def test_native_call_exception_releases_once_without_repeating(self):
        class Broken(ScriptedCAPI):
            def rados_read_op_operate(self, *args):
                self.calls += 1
                raise RuntimeError("native read result unavailable")
        native=oracle.Native.__new__(oracle.Native);native.lib=Broken()
        with self.assertRaisesRegex(RuntimeError,"unavailable"):
            native.compound(5,b'owned',oracle.vector('mixed',23))
        self.assertEqual((native.lib.calls,native.lib.released),(1,1))

    def test_native_count_cannot_escape_its_own_checksum_buffer(self):
        class Overflow(ScriptedCAPI):
            def rados_read_op_operate(self, *args):
                result=super().rados_read_op_operate(*args)
                checksum=self.actions[0]
                C.memmove(checksum[6], struct.pack('<I',99),4)
                return result
        native=oracle.Native.__new__(oracle.Native);native.lib=Overflow()
        with self.assertRaisesRegex(ValueError,"count exceeds bound"):
            native.compound(5,b'owned',oracle.vector('distinct-seeds',23))
        self.assertEqual((native.lib.calls,native.lib.released),(1,1))


if __name__=='__main__':unittest.main()
