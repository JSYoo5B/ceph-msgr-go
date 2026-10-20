// Development-only direct native WRITESAME payload encoder; never a product dependency.
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
#include <map>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
constexpr uint64_t features = 3026423347916342070ULL;

void write_segment(const std::filesystem::path& path,
                   const ceph::buffer::list& segment) {
  const auto bytes = segment.to_str();
  std::ofstream out(path, std::ios::binary);
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out.write(bytes.data(), bytes.size());
}


ceph::buffer::list value(const std::string& bytes) {
 ceph::buffer::list out; out.append(bytes); return out;
}
std::vector<OSDOp> pattern_operations() {
 ObjectOperation op;
 op.writesame(17,12,value(std::string("\0\xffx",3)));
 op.writesame(7,0,value(std::string("\xaa",1)));
 op.append(value(std::string("tail\0\xff",6)));
 std::vector<OSDOp> result(op.ops.begin(),op.ops.end());
 for(auto& item: result) item.rval=0;
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
        << ",\"flags\":" << uint32_t(op.op.flags)
        << ",\"offset\":" << uint64_t(op.op.writesame.offset)
        << ",\"logical_length\":" << uint64_t(op.op.writesame.length)
        << ",\"pattern_length\":" << uint64_t(op.op.writesame.data_length)
        << ",\"payload_len\":" << uint32_t(op.op.payload_len)
        << ",\"union_hex\":\"" << hex(std::string(packed + 6, 28))
        << "\",\"indata_hex\":\"" << hex(op.indata.to_str())
        << "\",\"outdata_hex\":\"" << hex(op.outdata.to_str()) << "\"";
    if (reply) out << ",\"result\":" << int(op.rval);
    out << "}";
  }
  out << "]\n";
}

ceph::ref_t<MOSDOp> request(std::vector<OSDOp> ops, bool write) {
  const hobject_t object(object_t("writesame-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  int request_flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
  for (const auto& op : ops) {
    if (ceph_osd_op_mode_read(op.op.op)) request_flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) request_flags |= CEPH_OSD_FLAG_WRITE;
  }
  if (bool(request_flags & CEPH_OSD_FLAG_WRITE) != write)
    throw std::runtime_error("native opcode mode differs from declared fixture kind");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
                                      request_flags, features);
  req->set_retry_attempt(0);
  req->set_mtime(write ? utime_t(1700000000, 123456789) : utime_t());
  req->set_snap_seq(snapid_t(0));
  req->set_snaps({});
  req->ops = std::move(ops);
  return req;
}

void emit(const std::filesystem::path& root, const std::string& name,
          Message& message) {
  if (message.get_payload().length() != 0 || message.get_data().length() != 0)
    throw std::runtime_error("encoder input must have empty front and data");
  // Direct dispatch invokes the actual native feature-selected payload encoder;
  // Message::encode's decoded-payload reuse path is deliberately not involved.
  message.encode_payload(features);
  if (message.get_header().version != 6)
    throw std::runtime_error("native encoder did not select version 6");
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


void decode_request(const std::filesystem::path& root, const std::string& name,
                    const MOSDOp& source) {
  auto decoded = ceph::make_message<MOSDOp>();
  decoded->set_header(source.get_header());
  ceph::buffer::list payload(source.get_payload());
  decoded->set_payload(payload);
  decoded->set_data(source.get_data());
  decoded->decode_payload();
  verify_operations(source.ops, decoded->ops, false);
  write_decoder_receipt(root, name, decoded->ops, false);
  // This separately decoded object is never passed to an encoder.
}

void reply(const std::filesystem::path& root, const std::string& name,
           std::vector<OSDOp> input, std::vector<OSDOp> outputs,
           bool write, int aggregate_result) {
  auto req = request(std::move(input), write);
  req->encode_payload(features);
  auto response = ceph::make_message<MOSDOpReply>(req.get(), aggregate_result, 11,
      aggregate_result < 0 ? 0 : CEPH_OSD_FLAG_ACK, false);
  response->set_reply_versions(eversion_t(11, 20), 19);
  // Public native claim_ops supplies explicit server-shaped logical replies.
  // GET value_len and individual results below are inputs, not a live handler
  // execution. merge_osd_op_vector_out_data derives payload_len and data order.
  for (auto& op : outputs) op.indata.clear();
  response->claim_ops(outputs);
  emit(root, name, *response);
  std::vector<OSDOp> encoded_ops;
  response->claim_ops(encoded_ops);  // Extract encoded ops; never re-encode.
  auto decoded = ceph::make_message<MOSDOpReply>();
  decoded->set_header(response->get_header());
  ceph::buffer::list payload(response->get_payload());
  decoded->set_payload(payload);
  decoded->set_data(response->get_data());
  decoded->decode_payload();
  if (decoded->get_result() != aggregate_result || decoded->get_tid() != 77)
    throw std::runtime_error("native reply decoder changed result or TID");
  std::vector<OSDOp> decoded_ops;
  decoded->claim_ops(decoded_ops);
  verify_operations(encoded_ops, decoded_ops, true);
  write_decoder_receipt(root, name, decoded_ops, true);
}
}  // namespace

int main(int argc,char** argv) {
 try {
  if(argc!=2)throw std::runtime_error("usage: native-writesame-encoder OUTPUT_DIR");
  const std::filesystem::path root(argv[1]);std::filesystem::create_directories(root);
  auto input=pattern_operations();
  auto req=request(input,true);emit(root,"writesame-native-request-v6-all",*req);decode_request(root,"writesame-native-request-v6-all",*req);
  reply(root,"writesame-native-reply-v6-success",input,input,true,0);
  auto refused=input;refused[0].rval=-EFBIG;
  reply(root,"writesame-native-reply-v6-refused",input,refused,true,-EFBIG);
  auto failok=input;failok[0].op.flags=CEPH_OSD_OP_FLAG_FAILOK;
  auto continued=failok;continued[0].rval=-EFBIG;
  reply(root,"writesame-native-reply-v6-failok",failok,continued,true,0);
  std::vector<OSDOp> zero{input[1]};reply(root,"writesame-native-reply-v6-zero",zero,zero,true,0);
  return 0;
 }catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
