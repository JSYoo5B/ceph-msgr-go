// Development-only native MOSDBackoff encoder and independent decoder.
// Pinned Ceph v20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// Native headers/library are LGPL-2.1. This wrapper invokes unmodified native
// encoders on fresh objects; no Ceph implementation or captured bytes copied.
#include "messages/MOSDBackoff.h"
#include "msg/async/frames_v2.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

namespace {
constexpr uint64_t features = 3026423347916342070ULL;
void write_segment(const std::filesystem::path& path,const ceph::buffer::list& segment) {
 const auto bytes=segment.to_str();
 std::ofstream out(path,std::ios::binary);out.exceptions(std::ios::failbit|std::ios::badbit);
 out.write(bytes.data(),bytes.size());
}
void emit(const std::filesystem::path& root, const std::string& name,
          Message& message) {
  if (message.get_payload().length() != 0 || message.get_data().length() != 0)
    throw std::runtime_error("encoder input must have empty front and data");
  // Direct dispatch invokes the actual native feature-selected payload encoder;
  // Message::encode's decoded-payload reuse path is deliberately not involved.
  message.encode_payload(features);
  if (message.get_header().version != 1)
    throw std::runtime_error("native encoder did not select version 1");
  // Message::encode now only updates the native envelope and footer after
  // the direct fresh-object payload encode above. No decoded bytes are used.
  message.set_seq(1);  // Explicit offline transport input, not live session state.
  message.set_priority(CEPH_MSG_PRIO_DEFAULT);
  message.encode(features, MSG_CRC_ALL, true);
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

void emit_backoff(const std::filesystem::path& root,const std::string& name,
 const spg_t& pg,uint32_t epoch,uint8_t op,uint64_t id,const hobject_t& begin,const hobject_t& end) {
 if (begin > end) throw std::runtime_error("native range is reversed");
 auto source=ceph::make_message<MOSDBackoff>(pg,epoch,op,id,begin,end);
 emit(root,name,*source);
 auto decoded=ceph::make_message<MOSDBackoff>();
 decoded->set_header(source->get_header());
 ceph::buffer::list payload(source->get_payload());decoded->set_payload(payload);
 decoded->set_data(source->get_data());decoded->decode_payload();
 if(decoded->pgid!=pg||decoded->map_epoch!=epoch||decoded->op!=op||decoded->id!=id||decoded->begin!=begin||decoded->end!=end)
  throw std::runtime_error("native backoff decoder changed identity or bounds");
 // Decoder comparison also checks C++ canonical hobject order independently
 // of Go, including the effective key and bit-reversed object hash.
 std::ofstream out(root/(name+".decoded-ops.json"));
 out << "[{\"code\":" << unsigned(decoded->op) << ",\"id\":" << decoded->id
     << ",\"map_epoch\":" << decoded->map_epoch << ",\"single\":" << (decoded->begin==decoded->end?"true":"false") << "}]\n";
}
} // namespace
int main(int argc,char** argv) {
 try {
  if(argc!=2)throw std::runtime_error("usage: native-backoff-encoder OUTPUT_DIR");
  const std::filesystem::path root(argv[1]);std::filesystem::create_directories(root);
  const spg_t pg(pg_t(0x12,7));
  const hobject_t single(object_t(std::string("bin\0\xff",5)),"route-key",CEPH_NOSNAP,0x11223344,7,"tenant");
  const hobject_t begin(object_t("a"),"k",CEPH_NOSNAP,0x11223344,7,"tenant");
  const hobject_t end(object_t("z"),"route",CEPH_NOSNAP,0x11223344,7,"tenant");
  emit_backoff(root,"backoff-native-event-v1-block-single",pg,11,CEPH_OSD_BACKOFF_OP_BLOCK,17,single,single);
  emit_backoff(root,"backoff-native-event-v1-block-range",pg,11,CEPH_OSD_BACKOFF_OP_BLOCK,std::numeric_limits<uint64_t>::max(),begin,end);
  emit_backoff(root,"backoff-native-event-v1-ack-range",pg,11,CEPH_OSD_BACKOFF_OP_ACK_BLOCK,std::numeric_limits<uint64_t>::max(),begin,end);
  emit_backoff(root,"backoff-native-event-v1-unblock-range",pg,12,CEPH_OSD_BACKOFF_OP_UNBLOCK,std::numeric_limits<uint64_t>::max(),begin,end);
  emit_backoff(root,"backoff-native-event-v1-block-sharded",spg_t(pg_t(2,9),shard_id_t(3)),19,CEPH_OSD_BACKOFF_OP_BLOCK,0,hobject_t(),hobject_t::get_max());
  return 0;
 }catch(const std::exception& error){std::cerr<<error.what()<<"\n";return 1;}
}
