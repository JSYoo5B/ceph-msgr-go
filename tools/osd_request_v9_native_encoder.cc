// Development-only direct native MOSDOp v9 payload encoder; never a product dependency.
// Compile against Ceph 20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// Ceph headers/library carry LGPL-2.1 notices. This wrapper only constructs
// public native objects and invokes their encoders; it copies no implementation.
// No decoded front is imported and no socket or network capture is used.
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "msg/async/frames_v2.h"
#include "osdc/Objecter.h"
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
#ifdef HAVE_JAEGER
#error "This closed native oracle requires the official SDK WITH_JAEGER=OFF configuration"
#endif
constexpr uint64_t features = 3386711319181820726ULL | CEPH_FEATUREMASK_SERVER_SQUID;
static_assert(features == 3387274269135242038ULL);
constexpr size_t otel_offset = 87;
const std::string empty_otel("\x01\x01\x01\x00\x00\x00\x00",7);

void write_segment(const std::filesystem::path& path,
                   const ceph::buffer::list& segment) {
  const auto bytes = segment.to_str();
  std::ofstream out(path, std::ios::binary);
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out.write(bytes.data(), bytes.size());
}

ceph::buffer::list value(const std::string& bytes) {
  ceph::buffer::list result;
  result.append(bytes);
  return result;
}

std::string hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  for (unsigned char byte : bytes) {
    result += digits[byte >> 4];
    result += digits[byte & 15];
  }
  return result;
}

void verify_operations(const std::vector<OSDOp>& encoded,
                       const std::vector<OSDOp>& decoded, bool reply) {
  if (encoded.size() != decoded.size())
    throw std::runtime_error("native decoder changed operation count");
  for (size_t i = 0; i != encoded.size(); ++i) {
    if (std::memcmp(&encoded[i].op, &decoded[i].op, sizeof(ceph_osd_op)) != 0 ||
        (reply && (encoded[i].rval != decoded[i].rval ||
                   encoded[i].outdata.to_str() != decoded[i].outdata.to_str())) ||
        (!reply && encoded[i].indata.to_str() != decoded[i].indata.to_str()))
      throw std::runtime_error("native decoder changed packed union/result/data");
  }
}

void write_decoder_receipt(const std::filesystem::path& root,
                           const std::string& name,
                           const std::vector<OSDOp>& ops, bool reply) {
  std::ofstream out(root / (name + ".decoded-ops.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "[";
  for (size_t i = 0; i != ops.size(); ++i) {
    const auto& op = ops[i];
    if (i) out << ",";
    const auto* packed = reinterpret_cast<const char*>(&op.op);
    out << "{\"code\":" << uint16_t(op.op.op)
        << ",\"flags\":" << uint32_t(op.op.flags);
    out << ",\"offset\":" << uint64_t(op.op.extent.offset)
        << ",\"length\":" << uint64_t(op.op.extent.length);
    out << ",\"payload_len\":" << uint32_t(op.op.payload_len)
        << ",\"union_hex\":\"" << hex(std::string(packed + 6, 28))
        << "\",\"indata_hex\":\"" << hex(op.indata.to_str())
        << "\",\"outdata_hex\":\"" << hex(op.outdata.to_str()) << "\"";
    if (reply) out << ",\"result\":" << int(op.rval);
    out << "}";
  }
  out << "]\n";
}


ceph::ref_t<MOSDOp> request(const std::string& kind) {
 ObjectOperation op;
 const bool writes = kind == "mutation" || kind == "snapshot";
 if (kind == "read") { op.stat(nullptr, nullptr, nullptr); op.read(2, 4, nullptr, nullptr, nullptr); }
 else if (kind == "mutation") { op.create(true); auto data = value(std::string("\x00\xff" "A\n",4)); op.write(2, data); }
 else if (kind == "snapshot") { auto data = value("snap"); op.write_full(data); }
 else if (kind == "checksum") { ceph::buffer::list seed; encode(uint32_t(0xffffffffU),seed); op.checksum(CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C,seed,2,8,4,static_cast<ceph::buffer::list*>(nullptr),nullptr,nullptr); }
 else if (kind == "pgnls") { ceph::buffer::list filter; op.pg_nls(2,filter,hobject_t(),9); }
 else throw std::runtime_error("unknown closed v9 vector");
 const bool listing=kind=="pgnls";
 const hobject_t object(object_t(listing?"":"v9-codec"),listing?"":"route-key",CEPH_NOSNAP,
                        listing?0x44:0x11223344,7,"tenant");
 spg_t pg(pg_t(0x44,7),shard_id_t(-1));
 int flags=CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
 flags |= writes ? CEPH_OSD_FLAG_WRITE : CEPH_OSD_FLAG_READ;
 if (kind == "snapshot") flags |= CEPH_OSD_FLAG_ORDERSNAP;
 if (listing) flags |= CEPH_OSD_FLAG_PGOP;
 auto req=ceph::make_message<MOSDOp>(kind == "snapshot" ? 99 : 1,77,object,pg,11,flags,features);
 req->set_retry_attempt(0);
 req->set_mtime(writes ? utime_t(1700000000,123456789) : utime_t());
 req->set_snap_seq(snapid_t(kind == "snapshot" ? 7 : 0));
 req->set_snaps(kind == "snapshot" ? std::vector<snapid_t>{7,5,2} : std::vector<snapid_t>{});
 req->ops.assign(op.ops.begin(),op.ops.end());
 for (auto& item : req->ops) item.rval=0;
 return req;
}

void emit(const std::filesystem::path& root, const std::string& name,
          Message& message) {
  if (message.get_payload().length() != 0 || message.get_data().length() != 0)
    throw std::runtime_error("encoder input must have empty front and data");
  // Canonical Message::encode starts with a fresh empty native object and
  // dispatches MOSDOp::encode_payload using the actual feature mask itself.
  // No decoded payload, direct pre-encoding or header-version override is used.
  if (message.otel_trace.IsValid()) throw std::runtime_error("native trace unexpectedly valid");
  message.set_seq(1);
  message.set_priority(CEPH_MSG_PRIO_DEFAULT);
  message.encode(features, MSG_CRC_ALL, true);
  if (message.get_header().type!=42 || message.get_header().version!=9 || message.get_header().compat_version!=3)
    throw std::runtime_error("canonical native encoder did not select Type42/v9/compat3");
  const auto front=message.get_payload().to_str();
  if(front.size()<otel_offset+7 || front.substr(otel_offset,7)!=empty_otel || front.substr(otel_offset-24,24)!=std::string(24,'\0'))
    throw std::runtime_error("fresh native v9 did not encode empty trace and seven-byte invalid OTEL");
  const auto& header = message.get_header();
  const auto& footer = message.get_footer();
  if (header.front_len != message.get_payload().length() ||
      header.middle_len != message.get_middle().length() ||
      header.data_len != message.get_data().length())
    throw std::runtime_error("native envelope lengths disagree with payloads");
  // Project the fresh native envelope into the native v2 struct, as required
  // by its public MessageFrame API. This does not execute ProtocolV2's live
  // write_message/session path. ACK sequence 3 is another explicit test input.
  ceph_msg_header2 header2{};
  header2.seq = header.seq;
  header2.tid = header.tid;
  header2.type = header.type;
  header2.priority = header.priority;
  header2.version = header.version;
  header2.data_off = header.data_off;
  header2.ack_seq = 3;
  header2.flags = footer.flags;
  header2.compat_version = header.compat_version;
  header2.reserved = header.reserved;
  auto frame = ceph::msgr::v2::MessageFrame::Encode(
      header2, message.get_payload(), message.get_middle(), message.get_data());
  ceph::buffer::list encoded_header;
  encoded_header.append(reinterpret_cast<const char*>(&frame.header()),
                        sizeof(frame.header()));
  if (encoded_header.length() != 41)
    throw std::runtime_error("native v2 message header size changed");
  write_segment(root / (name + ".header.bin"), encoded_header);
  std::ofstream metadata(root / (name + ".native-header.json"));
  metadata.exceptions(std::ios::failbit | std::ios::badbit);
  metadata << "{\"type\":" << uint16_t(header.type)
           << ",\"version\":" << uint16_t(header.version)
           << ",\"compat\":" << uint16_t(header.compat_version)
           << ",\"tid\":" << uint64_t(header.tid)
           << ",\"seq\":" << uint64_t(header.seq)
           << ",\"priority\":" << uint16_t(header.priority)
           << ",\"front_len\":" << uint32_t(header.front_len)
           << ",\"middle_len\":" << uint32_t(header.middle_len)
           << ",\"data_len\":" << uint32_t(header.data_len)
           << ",\"data_off\":" << uint16_t(header.data_off)
           << ",\"flags\":" << unsigned(footer.flags)
           << ",\"ack_seq\":" << uint64_t(frame.header().ack_seq)
           << ",\"reserved\":" << uint16_t(header.reserved) << "}\n";
  write_segment(root / (name + ".front.bin"), message.get_payload());
  write_segment(root / (name + ".data.bin"), message.get_data());
  std::cout << name << "\t" << message.get_payload().length() << "\t"
            << message.get_data().length() << "\n";
}

void decode_request(const std::filesystem::path& root,const std::string& name,const MOSDOp& source) {
 auto decoded=ceph::make_message<MOSDOp>();
 decoded->set_header(source.get_header());
 ceph::buffer::list payload(source.get_payload());decoded->set_payload(payload);decoded->set_data(source.get_data());
 decoded->decode_payload();
 if(decoded->otel_trace.IsValid())throw std::runtime_error("fresh partial native decoder made empty OTEL valid");
 if(!decoded->finish_decode() || decoded->finish_decode())throw std::runtime_error("native two-stage decode state differs");
 verify_operations(source.ops,decoded->ops,false);
 const auto expected=source.get_object_locator(),actual=decoded->get_object_locator();
 if(source.get_spg()!=decoded->get_spg() || source.get_hobj()!=decoded->get_hobj() || source.get_client_inc()!=decoded->get_client_inc() || decoded->get_reqid().inc!=static_cast<int32_t>(source.get_client_inc()) || decoded->get_reqid().tid!=77 || source.get_mtime()!=decoded->get_mtime() || source.get_snap_seq()!=decoded->get_snap_seq() || source.get_snaps()!=decoded->get_snaps() || decoded->get_features()!=features || source.get_flags()!=decoded->get_flags() || expected.pool!=actual.pool || expected.hash!=actual.hash || expected.key!=actual.key || expected.nspace!=actual.nspace)
  throw std::runtime_error("v9 native decoder changed request/routing/snapshot/mtime/features");
 write_decoder_receipt(root,name,decoded->ops,false);
 std::ofstream out(root/(name+".decoded-routing.json"));out.exceptions(std::ios::failbit|std::ios::badbit);
 out<<"{\"pool\":"<<decoded->get_spg().pool()<<",\"seed\":"<<decoded->get_spg().ps()
    <<",\"shard\":"<<static_cast<int>(decoded->get_spg().shard)<<",\"raw_hash\":"<<decoded->get_hobj().get_hash()
    <<",\"client_inc\":"<<decoded->get_client_inc()<<",\"reqid_inc\":"<<decoded->get_reqid().inc
    <<",\"reqid_tid\":"<<decoded->get_reqid().tid<<",\"features\":"<<decoded->get_features()
    <<",\"epoch\":"<<decoded->get_map_epoch()<<",\"flags\":"<<decoded->get_flags()
    <<",\"locator_hash\":"<<actual.hash<<",\"object_hex\":\""<<hex(decoded->get_oid().name)<<"\",\"key_hex\":\""<<hex(actual.key)
    <<"\",\"namespace_hex\":\""<<hex(actual.nspace)<<"\",\"snap\":"<<decoded->get_snapid().val
    <<",\"snap_seq\":"<<decoded->get_snap_seq().val<<",\"snaps\":[";
 for(size_t i=0;i<decoded->get_snaps().size();++i){if(i)out<<",";out<<decoded->get_snaps()[i].val;}
 out<<"],\"mtime_seconds\":"<<decoded->get_mtime().sec()<<",\"mtime_nanoseconds\":"<<decoded->get_mtime().nsec()
    <<",\"otel_valid\":false,\"empty_otel_hex\":\""<<hex(empty_otel)<<"\",\"legacy_trace_zero\":true,\"finish_decode_performed\":true,\"second_finish_decode_needed\":false}\n";
 // This separately decoded object is never passed to any encoder.
}

void verify_feature_selection(const std::filesystem::path& root) {
 auto control=request("read");
 control->encode(features & ~CEPH_FEATURE_SERVER_SQUID,MSG_CRC_ALL,true);
 if(control->get_header().version!=8||control->empty_payload()||control->otel_trace.IsValid())throw std::runtime_error("canonical SERVER_SQUID selection control failed");
 std::ofstream out(root/"native-feature-selection.json");out.exceptions(std::ios::failbit|std::ios::badbit);
 out<<"{\"canonical_message_encode\":true,\"features\":"<<features<<",\"without_server_squid_features\":"<<(features & ~CEPH_FEATURE_SERVER_SQUID)
    <<",\"without_server_squid_version\":8,\"with_server_squid_version\":9,\"have_jaeger\":false,\"empty_otel_hex\":\""<<hex(empty_otel)<<"\"}\n";
}
} // namespace

int main(int argc,char** argv) {
 try {
  if (argc != 2) throw std::runtime_error("usage: native-v9-encoder OUTPUT_DIR");
  const std::filesystem::path root(argv[1]); std::filesystem::create_directories(root);
  verify_feature_selection(root);
  for (const auto& kind : {"read","mutation","snapshot","checksum","pgnls"}) {
   const std::string name=std::string("request-v9-native-")+kind;
   auto req=request(kind); emit(root,name,*req); decode_request(root,name,*req);
  }
  return 0;
 } catch (const std::exception& error) { std::cerr << error.what() << "\n"; return 1; }
}
