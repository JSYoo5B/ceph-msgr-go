// Development-only Ceph 20.2.4 native encoder; never a product dependency.
// Pinned commit: 7f793731f1b39eb4f465e960113d2363c311b964.
// Ceph headers/library are LGPL-2.1. This wrapper constructs public objects
// and invokes unchanged native codecs; no Ceph implementation is copied.
//
// Each request carries one cache-tier operation built by the native
// ObjectOperation helpers (osdc/Objecter.h cache_flush, cache_try_flush,
// cache_evict, cache_pin, cache_unpin). Request flags follow the native
// callers: the rados CLI (tools/rados/rados.cc do_cache_flush,
// do_cache_try_flush, do_cache_evict) submits ObjectReadOperations with
// OPERATION_IGNORE_CACHE|OPERATION_IGNORE_OVERLAY, adding OPERATION_SKIPRWLOCKS
// for try-flush and evict; librados translate_flags maps those to the wire
// flags and Objecter::get_read_flags adds READ. Pin and unpin are
// ObjectWriteOperations (Objecter prepare_mutate_op adds WRITE), encoded with
// and without IGNORE_CACHE. ONDISK|RETURNVEC are the fixed header flags of
// this repository's encoder and are set explicitly for byte comparison.
#include "messages/MOSDOp.h"
#include "msg/async/frames_v2.h"
#include "osdc/Objecter.h"
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
#ifdef HAVE_JAEGER
#error "Closed cache-operation oracle requires official WITH_JAEGER=OFF"
#endif
static_assert(CEPH_OSD_OP_CACHE_FLUSH == 0x821f);
static_assert(CEPH_OSD_OP_CACHE_EVICT == 0x8220);
static_assert(CEPH_OSD_OP_CACHE_TRY_FLUSH == 0x8221);
static_assert(CEPH_OSD_OP_CACHE_PIN == 0x2224);
static_assert(CEPH_OSD_OP_CACHE_UNPIN == 0x2225);
static_assert(CEPH_OSD_FLAG_IGNORE_CACHE == 0x8000);
static_assert(CEPH_OSD_FLAG_SKIPRWLOCKS == 0x10000);
static_assert(CEPH_OSD_FLAG_IGNORE_OVERLAY == 0x20000);
uint64_t features(unsigned version) {
  if (version == 6) return 3026423347916342070ULL;
  if (version == 8) return 3386711319181820726ULL;
  if (version == 9) return 3387274269135242038ULL;
  throw std::runtime_error("unsupported oracle request version");
}
std::string hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  for (unsigned char byte : bytes) {
    result += digits[byte >> 4]; result += digits[byte & 15];
  }
  return result;
}
void write(const std::filesystem::path& path, const ceph::buffer::list& bytes) {
  std::ofstream out(path, std::ios::binary);
  out.exceptions(std::ios::failbit | std::ios::badbit);
  const auto value = bytes.to_str(); out.write(value.data(), value.size());
}
struct Shape { int flags; bool write; };
// These are request layouts, not assertions about tier state, dirty or pin
// flags, lock modes, or any server result.
ceph::ref_t<MOSDOp> request(unsigned version, const std::string& shape) {
  ObjectOperation op;
  constexpr int tier = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;
  Shape s{};
  if (shape == "flush") { op.cache_flush(); s = {CEPH_OSD_FLAG_READ | tier, false}; }
  else if (shape == "try-flush") { op.cache_try_flush(); s = {CEPH_OSD_FLAG_READ | tier | CEPH_OSD_FLAG_SKIPRWLOCKS, false}; }
  else if (shape == "evict") { op.cache_evict(); s = {CEPH_OSD_FLAG_READ | tier | CEPH_OSD_FLAG_SKIPRWLOCKS, false}; }
  else if (shape == "pin") { op.cache_pin(); s = {CEPH_OSD_FLAG_WRITE, true}; }
  else if (shape == "pin-ignore-cache") { op.cache_pin(); s = {CEPH_OSD_FLAG_WRITE | CEPH_OSD_FLAG_IGNORE_CACHE, true}; }
  else if (shape == "unpin") { op.cache_unpin(); s = {CEPH_OSD_FLAG_WRITE, true}; }
  else if (shape == "unpin-ignore-cache") { op.cache_unpin(); s = {CEPH_OSD_FLAG_WRITE | CEPH_OSD_FLAG_IGNORE_CACHE, true}; }
  else throw std::runtime_error("unknown closed cache-operation shape");
  if (op.ops.size() != 1 || op.ops[0].indata.length() ||
      (ceph_osd_op_mode_modify(op.ops[0].op.op) != 0) != s.write)
    throw std::runtime_error("native cache builder changed its single operation");
  const hobject_t object(object_t("cache-op-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  // v6 transmits raw hash pg_t; modern versions transmit the caller's actual PG.
  spg_t pg(pg_t(version == 6 ? 0x11223344 : 0x44, 7), shard_id_t(-1));
  const int flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC | s.flags;
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11, flags, features(version));
  req->set_retry_attempt(0);
  req->set_mtime(s.write ? utime_t(1700000000, 123456789) : utime_t());
  req->set_snap_seq(snapid_t(0)); req->set_snaps({});
  req->ops.assign(op.ops.begin(), op.ops.end());
  for (auto& item : req->ops) item.rval = 0;
  return req;
}
void emit(const std::filesystem::path& root, const std::string& name,
          unsigned version, MOSDOp& source) {
  if (!source.empty_payload() || source.get_middle().length() ||
      source.get_data().length() || source.otel_trace.IsValid())
    throw std::runtime_error("native encoder input must be fresh and untraced");
  source.set_seq(1); source.set_priority(CEPH_MSG_PRIO_DEFAULT);
  // Exactly one canonical encode selects the version from native features.
  source.encode(features(version), MSG_CRC_ALL, true);
  const auto& header = source.get_header();
  const auto& footer = source.get_footer();
  if (header.type != 42 || header.version != version || header.compat_version != 3 ||
      header.front_len != source.get_payload().length() || header.middle_len ||
      header.data_len != source.get_data().length())
    throw std::runtime_error("canonical native message envelope differs");
  if (version >= 8 && source.get_payload().to_str().substr(63,24) != std::string(24,'\0'))
    throw std::runtime_error("native legacy trace is not empty");
  if (version == 9 && source.get_payload().to_str().substr(87,7) != std::string("\x01\x01\x01\0\0\0\0",7))
    throw std::runtime_error("native v9 OTEL envelope is not empty");
  // Offline projection into the real native v2 MessageFrame, not a live
  // ProtocolV2 session. Sequence/ACK/priority are explicit test inputs.
  ceph_msg_header2 h{};
  h.seq=header.seq; h.tid=header.tid; h.type=header.type;
  h.priority=header.priority; h.version=header.version;
  h.data_off=header.data_off; h.ack_seq=3; h.flags=footer.flags;
  h.compat_version=header.compat_version; h.reserved=header.reserved;
  auto frame=ceph::msgr::v2::MessageFrame::Encode(h, source.get_payload(),
                                               source.get_middle(), source.get_data());
  ceph::buffer::list encoded;
  encoded.append(reinterpret_cast<const char*>(&frame.header()), sizeof(frame.header()));
  if (encoded.length()!=41) throw std::runtime_error("native header2 size changed");
  write(root/(name+".header.bin"),encoded);
  write(root/(name+".front.bin"),source.get_payload());
  write(root/(name+".middle.bin"),source.get_middle());
  write(root/(name+".data.bin"),source.get_data());

  auto decoded=ceph::make_message<MOSDOp>();
  decoded->set_header(header);
  ceph::buffer::list payload(source.get_payload()); decoded->set_payload(payload);
  decoded->set_data(source.get_data()); decoded->decode_payload();
  const bool finished=decoded->finish_decode();
  if (finished != (version >= 8) || decoded->finish_decode() || decoded->otel_trace.IsValid())
    throw std::runtime_error("native partial/final decode or invalid trace differs");
  const auto locator=decoded->get_object_locator();
  if (decoded->get_spg()!=source.get_spg() || decoded->get_hobj()!=source.get_hobj() ||
      decoded->get_client_inc()!=1 || decoded->get_reqid().inc!=1 ||
      decoded->get_reqid().tid!=77 || decoded->get_map_epoch()!=11 ||
      decoded->get_flags()!=source.get_flags() || decoded->get_features()!=features(version) ||
      decoded->get_mtime()!=source.get_mtime() || decoded->get_snapid().val!=CEPH_NOSNAP ||
      decoded->get_snap_seq()!=source.get_snap_seq() || decoded->get_snaps()!=source.get_snaps() ||
      locator.pool!=7 || locator.hash!=-1 || locator.key!="route-key" ||
      locator.nspace!="tenant" || decoded->ops.size()!=source.ops.size() ||
      decoded->get_retry_attempt()!=0)
    throw std::runtime_error("fresh native decoder changed cache-operation identity/flags");
  for (size_t i=0;i<source.ops.size();++i) {
    const auto& a=source.ops[i]; const auto& b=decoded->ops[i];
    if (std::memcmp(&a.op,&b.op,sizeof(ceph_osd_op)) || a.indata.length() ||
        b.indata.length() || b.rval!=0 || b.outdata.length())
      throw std::runtime_error("fresh native decoder changed cache op union/flags/input/result");
  }
  std::ofstream out(root/(name+".decoded.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "{\"features\":" << decoded->get_features()
      << ",\"flags\":" << decoded->get_flags()
      << ",\"pool\":" << decoded->get_spg().pool()
      << ",\"seed\":" << decoded->get_spg().ps()
      << ",\"shard\":" << static_cast<int>(decoded->get_spg().shard)
      << ",\"raw_hash\":" << decoded->get_hobj().get_hash()
      << ",\"client_inc\":1,\"reqid_inc\":1,\"reqid_tid\":77,\"epoch\":11"
      << ",\"locator_hash\":-1,\"object\":\"cache-op-codec\",\"key\":\"route-key\",\"namespace\":\"tenant\""
      << ",\"snap\":" << decoded->get_snapid().val
      << ",\"snap_seq\":" << decoded->get_snap_seq().val << ",\"snaps\":[";
  for (size_t i=0;i<decoded->get_snaps().size();++i) {
    if(i)out<<","; out<<decoded->get_snaps()[i].val;
  }
  out << "],\"retry_attempt\":" << decoded->get_retry_attempt()
      << ",\"mtime_seconds\":" << decoded->get_mtime().sec()
      << ",\"mtime_nanoseconds\":" << decoded->get_mtime().nsec()
      << ",\"otel_valid\":false,\"finish_decode_performed\":" << (finished?"true":"false")
      << ",\"second_finish_decode_needed\":false,\"operations\":[";
  for (size_t i=0;i<decoded->ops.size();++i) {
    if (i) out<<",";
    const auto& op=decoded->ops[i];
    const auto* packed=reinterpret_cast<const char*>(&op.op);
    out << "{\"code\":" << uint16_t(op.op.op)
        << ",\"flags\":" << uint32_t(op.op.flags)
        << ",\"offset\":" << uint64_t(op.op.extent.offset)
        << ",\"length\":" << uint64_t(op.op.extent.length)
        << ",\"payload_len\":" << uint32_t(op.op.payload_len)
        << ",\"union_hex\":\"" << hex(std::string(packed+6,28))
        << "\",\"indata_hex\":\"" << hex(op.indata.to_str())
        << "\",\"outdata_hex\":\"" << hex(op.outdata.to_str())
        << "\",\"result\":" << op.rval << "}";
  }
  out << "]}\n";
  // The independently decoded object is never encoded again.
}
} // namespace
int main(int argc,char** argv) {
  try {
    if (argc!=2) throw std::runtime_error("usage: native-cache-op-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    if (!std::filesystem::create_directory(root))
      throw std::runtime_error("output directory must not exist");
    for (unsigned version : {6U,8U,9U})
      for (const std::string shape : {"flush", "try-flush", "evict", "pin", "pin-ignore-cache",
                                      "unpin", "unpin-ignore-cache"}) {
        const auto name="cache-op-native-v"+std::to_string(version)+"-"+shape;
        auto req=request(version,shape); emit(root,name,version,*req);
      }
    return 0;
  } catch(const std::exception& error) { std::cerr<<error.what()<<"\n";return 1; }
}
