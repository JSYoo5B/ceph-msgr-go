// Development-only Ceph 20.2.4 native encoder; never a product dependency.
// Pinned commit: 7f793731f1b39eb4f465e960113d2363c311b964.
// Ceph headers/library are LGPL-2.1. This wrapper constructs public objects
// and invokes unchanged native codecs; no Ceph implementation is copied.
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
#error "Closed CHECKSUM compound oracle requires official WITH_JAEGER=OFF"
#endif
static_assert(CEPH_OSD_OP_CHECKSUM == 0x121f);
static_assert(CEPH_OSD_OP_CMPEXT == 0x1220);
static_assert(CEPH_OSD_OP_ASSERT_VER == 0x1208);
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
void checksum(ObjectOperation& op, uint8_t type, uint64_t offset,
              uint64_t length, uint32_t chunk, uint64_t initial) {
  ceph::buffer::list seed;
  if (type == CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64) encode(initial, seed);
  else encode(static_cast<uint32_t>(initial), seed);
  op.checksum(type, seed, offset, length, chunk,
              static_cast<ceph::buffer::list*>(nullptr), nullptr, nullptr);
}
void read(ObjectOperation& op, uint64_t offset, uint64_t length) {
  op.read(offset, length, static_cast<ceph::buffer::list*>(nullptr), nullptr, nullptr);
}
// These are request layouts, not assertions about a server's object state,
// checksum values, EOF, execution boundary or successful/failed operations.
ceph::ref_t<MOSDOp> request(unsigned version, const std::string& shape) {
  ObjectOperation op;
  if (shape == "mixed") {
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C, 0, 8, 4, 0x1234);
    op.stat(nullptr, nullptr, nullptr); read(op, 4, 8);
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64, 0, 16, 0, 0xfedcba9876543210ULL);
  } else if (shape == "compare-boundary") {
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32, 0, 8, 0, 0xffffffffULL);
    ceph::buffer::list compare; compare.append(std::string("\x00\xff" "A\n", 4));
    op.cmpext(0, compare, nullptr);
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64, 8, 8, 4, 0x8000000000000001ULL);
    op.stat(nullptr, nullptr, nullptr);
  } else if (shape == "eof-layout") {
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C, 16, 8, 4, 1);
    read(op, 0, 8); op.stat(nullptr, nullptr, nullptr);
  } else if (shape == "assert-boundary") {
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C, 0, 8, 4, 1);
    op.assert_version(9);
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64, 0, 8, 0, 2);
    read(op, 0, 4);
  } else if (shape == "checksum-boundary") {
    read(op, 0, 4);
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C, 12, 8, 8, 3);
    op.stat(nullptr, nullptr, nullptr);
    checksum(op, CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32, 0, 8, 0, 4);
  } else throw std::runtime_error("unknown closed compound shape");
  const hobject_t object(object_t("checksum-compound-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  // v6 transmits raw hash pg_t; modern versions transmit the caller's actual PG.
  spg_t pg(pg_t(version == 6 ? 0x11223344 : 0x44, 7), shard_id_t(-1));
  int flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
  for (const auto& item : op.ops) {
    if (ceph_osd_op_mode_read(item.op.op)) flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(item.op.op)) flags |= CEPH_OSD_FLAG_WRITE;
  }
  if (!(flags & CEPH_OSD_FLAG_READ) || (flags & CEPH_OSD_FLAG_WRITE))
    throw std::runtime_error("native compound must select READ alone");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11, flags, features(version));
  req->set_retry_attempt(0); req->set_mtime(utime_t());
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
    throw std::runtime_error("fresh native decoder changed compound identity/flags");
  for (size_t i=0;i<source.ops.size();++i) {
    const auto& a=source.ops[i]; const auto& b=decoded->ops[i];
    if (std::memcmp(&a.op,&b.op,sizeof(ceph_osd_op)) || a.indata.to_str()!=b.indata.to_str() ||
        b.rval!=0 || b.outdata.length() || b.op.flags)
      throw std::runtime_error("fresh native decoder changed compound op union/input/result");
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
      << ",\"locator_hash\":-1,\"object\":\"checksum-compound-codec\",\"key\":\"route-key\",\"namespace\":\"tenant\""
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
    if (argc!=2) throw std::runtime_error("usage: native-checksum-compound-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    if (!std::filesystem::create_directory(root))
      throw std::runtime_error("output directory must not exist");
    for (unsigned version : {6U,8U,9U})
      for (const std::string shape : {"mixed", "compare-boundary", "eof-layout",
                                      "assert-boundary", "checksum-boundary"}) {
        const auto name="checksum-compound-native-v"+std::to_string(version)+"-"+shape;
        auto req=request(version,shape); emit(root,name,version,*req);
      }
    return 0;
  } catch(const std::exception& error) { std::cerr<<error.what()<<"\n";return 1; }
}
