// Development-only direct native PGNLS v8 request/v6 reply payload encoder; never a product dependency.
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
constexpr uint64_t features = 3098480941956367158ULL;

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

std::vector<OSDOp> all_operations() {
 ObjectOperation op;
 ceph::buffer::list filter;
 op.pg_nls(2, filter, hobject_t(), 9);
 std::vector<OSDOp> result(op.ops.begin(), op.ops.end());
 for (auto& item : result) item.rval = 0;
 return result;
}

ceph::buffer::list listing(bool populated) {
 pg_nls_response_t response;
 response.handle = hobject_t(object_t("last"), "route", CEPH_NOSNAP, 0x11223344, 7, "tenant");
 if (populated) {
  response.entries.emplace_back("tenant", "one", "");
  response.entries.emplace_back("tenant", "two", "route");
 }
 ceph::buffer::list encoded;
 response.encode(encoded);
 pg_nls_response_t decoded;
 auto p = encoded.cbegin(); decoded.decode(p);
 if (!p.end() || decoded.entries.size() != response.entries.size() || decoded.handle != response.handle) throw std::runtime_error("native PGNLS decode");
 for (size_t i=0;i<decoded.entries.size();++i) {
  if (decoded.entries[i].nspace != response.entries[i].nspace || decoded.entries[i].oid != response.entries[i].oid || decoded.entries[i].locator != response.entries[i].locator) throw std::runtime_error("native PGNLS entry differs");
 }
 return encoded;
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

// PGNLS derives READ mode from the native opcode.
ceph::ref_t<MOSDOp> request(std::vector<OSDOp> ops) {
  const hobject_t object(object_t(""), "", CEPH_NOSNAP,
                         0x44, 7, "tenant");
  spg_t pg(pg_t(0x44, 7));
  int request_flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC | CEPH_OSD_FLAG_PGOP;
  for (const auto& op : ops) {
    if (ceph_osd_op_mode_read(op.op.op)) request_flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) request_flags |= CEPH_OSD_FLAG_WRITE;
  }
  if (!(request_flags & CEPH_OSD_FLAG_READ) || (request_flags & CEPH_OSD_FLAG_WRITE))
    throw std::runtime_error("native op modes no longer select READ alone");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
                                      request_flags, features);
  req->set_retry_attempt(0);
  // Read-only observation has no caller mtime.
  req->set_mtime(utime_t());
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
  if (message.get_header().version != (message.get_type() == 42 ? 8 : 6))
    throw std::runtime_error("native encoder selected an unexpected PGNLS request/reply version");
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
  if (!decoded->finish_decode()) throw std::runtime_error("PGNLS v8 final decode missing");
  if (source.get_spg() != decoded->get_spg() || source.get_hobj() != decoded->get_hobj() || decoded->get_client_inc() != 1 || decoded->get_reqid().inc != 1 || decoded->get_reqid().tid != 77 || decoded->get_object_locator().hash != 0x44) throw std::runtime_error("PGNLS v8 identity or locator changed");
  std::ofstream routing(root / (name + ".decoded-routing.json"));
  routing << "{\"pool\":" << decoded->get_spg().pool() << ",\"seed\":" << decoded->get_spg().ps()
          << ",\"shard\":" << static_cast<int>(decoded->get_spg().shard) << ",\"raw_hash\":" << decoded->get_hobj().get_hash()
          << ",\"client_inc\":" << decoded->get_client_inc() << ",\"reqid_inc\":" << decoded->get_reqid().inc << "}\n";
  verify_operations(source.ops, decoded->ops, false);
  write_decoder_receipt(root, name, decoded->ops, false);
  // This separately decoded object is never passed to an encoder.
}

void reply(const std::filesystem::path& root, const std::string& name,
           std::vector<OSDOp> input, std::vector<OSDOp> outputs,
           int aggregate_result, int acktype, eversion_t replay,
           version_t user) {
  auto req = request(std::move(input));
  req->encode_payload(features);
  // Public constructor keeps request mode bits and replaces only completion
  // flags. The acktype is an explicit logical input of each vector.
  auto response = ceph::make_message<MOSDOpReply>(req.get(), aggregate_result, 11,
      acktype, false);
  response->set_reply_versions(replay, user);
  // Explicit native response values are wire oracles, not live OSD evidence.
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

int main(int argc, char** argv) {
 try {
  if (argc != 2) throw std::runtime_error("usage: native-pg-list-encoder OUTPUT_DIR");
  const std::filesystem::path root(argv[1]);
  std::filesystem::create_directories(root);
  auto req = request(all_operations());
  emit(root, "pg-list-v8-native-request-v8-list", *req);
  decode_request(root, "pg-list-v8-native-request-v8-list", *req);
  for (const auto& name : {"populated", "empty-end", "einval", "eacces"}) {
   auto input = all_operations(), output = input;
   int result = 0;
   if (std::string(name) == "einval") result = -EINVAL;
   else if (std::string(name) == "eacces") result = -EACCES;
   else {
    output[0].outdata = listing(std::string(name) == "populated");
    if (std::string(name) == "empty-end") result = 1;
   }
   output[0].rval = result < 0 ? result : 0;
   reply(root, std::string("pg-list-v8-native-reply-v6-") + name, input, output,
         result, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(), 19);
  }
  return 0;
 } catch (const std::exception& error) { std::cerr << error.what() << "\n"; return 1; }
}
