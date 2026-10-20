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

namespace {
#ifdef HAVE_JAEGER
#error "Closed IgnoreCache oracle requires official WITH_JAEGER=OFF"
#endif
constexpr uint64_t cache_feature = 1ULL << 35;
static_assert(cache_feature == CEPH_FEATURE_OSD_CACHEPOOL);
static_assert(CEPH_OSD_FLAG_IGNORE_CACHE == 0x8000);
uint64_t features(unsigned version) {
  if (version == 6) return 3026423347916342070ULL | cache_feature;
  if (version == 8) return 3386711319181820726ULL | cache_feature;
  if (version == 9) return 3387274269135242038ULL | cache_feature;
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
ceph::ref_t<MOSDOp> request(unsigned version, const std::string& kind, bool ignore) {
  ObjectOperation op;
  const bool mutation = kind == "writefull" || kind == "delete";
  if (kind == "stat") op.stat(nullptr, nullptr, nullptr);
  else if (kind == "read") op.read(2, 4, nullptr, nullptr, nullptr);
  else if (kind == "writefull") {
    ceph::buffer::list data; data.append(std::string("\x00\xff" "A\n", 4));
    op.write_full(data);
  } else if (kind == "delete") op.remove();
  else throw std::runtime_error("unknown closed operation");
  const hobject_t object(object_t("cache-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  // v6 transmits the raw hash pg_t; modern versions transmit actual PG.
  spg_t pg(pg_t(version == 6 ? 0x11223344 : 0x44, 7), shard_id_t(-1));
  int flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC |
      (mutation ? CEPH_OSD_FLAG_WRITE : CEPH_OSD_FLAG_READ) |
      (ignore ? CEPH_OSD_FLAG_IGNORE_CACHE : 0);
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11, flags, features(version));
  req->set_retry_attempt(0);
  req->set_mtime(mutation ? utime_t(1700000000, 5) : utime_t());
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
      decoded->get_snap_seq().val || !decoded->get_snaps().empty() ||
      locator.pool!=7 || locator.hash!=-1 || locator.key!="route-key" ||
      locator.nspace!="tenant" || decoded->ops.size()!=1 ||
      std::memcmp(&decoded->ops[0].op,&source.ops[0].op,sizeof(ceph_osd_op)) ||
      decoded->ops[0].indata.to_str()!=source.ops[0].indata.to_str())
    throw std::runtime_error("fresh native decoder changed request identity/flags/op/data");
  const auto& op=decoded->ops[0];
  const auto* packed=reinterpret_cast<const char*>(&op.op);
  std::ofstream out(root/(name+".decoded.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "{\"features\":" << decoded->get_features()
      << ",\"flags\":" << decoded->get_flags()
      << ",\"pool\":" << decoded->get_spg().pool()
      << ",\"seed\":" << decoded->get_spg().ps()
      << ",\"shard\":" << static_cast<int>(decoded->get_spg().shard)
      << ",\"raw_hash\":" << decoded->get_hobj().get_hash()
      << ",\"client_inc\":1,\"reqid_inc\":1,\"reqid_tid\":77,\"epoch\":11"
      << ",\"locator_hash\":-1,\"object\":\"cache-codec\",\"key\":\"route-key\",\"namespace\":\"tenant\""
      << ",\"snap\":" << decoded->get_snapid().val
      << ",\"snap_seq\":0,\"snaps\":[],\"retry_attempt\":" << decoded->get_retry_attempt()
      << ",\"mtime_seconds\":" << decoded->get_mtime().sec()
      << ",\"mtime_nanoseconds\":" << decoded->get_mtime().nsec()
      << ",\"otel_valid\":false,\"finish_decode_performed\":" << (finished?"true":"false")
      << ",\"second_finish_decode_needed\":false,\"operation\":{\"code\":" << uint16_t(op.op.op)
      << ",\"flags\":" << uint32_t(op.op.flags)
      << ",\"offset\":" << uint64_t(op.op.extent.offset)
      << ",\"length\":" << uint64_t(op.op.extent.length)
      << ",\"payload_len\":" << uint32_t(op.op.payload_len)
      << ",\"union_hex\":\"" << hex(std::string(packed+6,28))
      << "\",\"indata_hex\":\"" << hex(op.indata.to_str()) << "\"}}\n";
  // The independently decoded object is never encoded again.
}
} // namespace
int main(int argc,char** argv) {
  try {
    if (argc!=2) throw std::runtime_error("usage: native-ignore-cache-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    if (!std::filesystem::create_directory(root))
      throw std::runtime_error("output directory must not exist");
    for (unsigned version : {6U,8U,9U})
      for (const std::string kind : {"stat","read","writefull","delete"})
        for (bool ignore : {false,true}) {
          const auto name="ignore-cache-native-v"+std::to_string(version)+"-"+kind+(ignore?"-ignore":"-normal");
          auto req=request(version,kind,ignore); emit(root,name,version,*req);
        }
    return 0;
  } catch(const std::exception& error) { std::cerr<<error.what()<<"\n";return 1; }
}
