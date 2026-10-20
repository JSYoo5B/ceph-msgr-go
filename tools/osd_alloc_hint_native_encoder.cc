// Development-only direct native SETALLOCHINT payload encoder; never a product dependency.
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
constexpr uint64_t features = 3026423347916342070ULL;

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

// Pinned include/rados.h 558-568 CEPH_OSD_ALLOC_HINT_FLAG_* values.
constexpr uint32_t hint_flags = CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE |
                                CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE;
constexpr uint32_t all_hint_flags =
    CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE | CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_WRITE |
    CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_READ | CEPH_OSD_ALLOC_HINT_FLAG_RANDOM_READ |
    CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY | CEPH_OSD_ALLOC_HINT_FLAG_IMMUTABLE |
    CEPH_OSD_ALLOC_HINT_FLAG_SHORTLIVED | CEPH_OSD_ALLOC_HINT_FLAG_LONGLIVED |
    CEPH_OSD_ALLOC_HINT_FLAG_COMPRESSIBLE | CEPH_OSD_ALLOC_HINT_FLAG_INCOMPRESSIBLE |
    CEPH_OSD_ALLOC_HINT_FLAG_LOG;
static_assert(hint_flags == 257, "pinned alloc hint flag values changed");
static_assert(all_hint_flags == 2047, "pinned alloc hint flag set changed");

// The pinned public builder set_alloc_hint (Objecter.h 1631-1641) calls
// add_alloc_hint (303-310), which stores expected_object_size,
// expected_write_size and flags in the packed alloc_hint union, and then
// always sets the per-op FAILOK flag. No other builder flag is involved.
void alloc_hint(ObjectOperation& operation, uint64_t object_size,
                uint64_t write_size, uint32_t flags) {
  operation.set_alloc_hint(object_size, write_size, flags);
}

std::vector<OSDOp> operations(ObjectOperation&& operation) {
  std::vector<OSDOp> result(operation.ops.begin(), operation.ops.end());
  for (auto& op : result) op.rval = 0;
  return result;
}

// SETALLOCHINT(4 MiB, 64 KiB, SEQUENTIAL_WRITE|COMPRESSIBLE), a WRITEFULL of
// "abc" through write_full(buffer::list&&) (584-589) and an all-zero
// SETALLOCHINT. Every input is an explicit native value, not a Go encoding.
std::vector<OSDOp> all_operations() {
  ObjectOperation operation;
  alloc_hint(operation, 4194304, 65536, hint_flags);
  operation.write_full(value("abc"));
  alloc_hint(operation, 0, 0, 0);
  return operations(std::move(operation));
}

std::vector<OSDOp> single_hint(uint64_t object_size, uint64_t write_size,
                               uint32_t flags) {
  ObjectOperation operation;
  alloc_hint(operation, object_size, write_size, flags);
  return operations(std::move(operation));
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
    if (op.op.op == CEPH_OSD_OP_SETALLOCHINT) {
      out << ",\"expected_object_size\":" << uint64_t(op.op.alloc_hint.expected_object_size)
          << ",\"expected_write_size\":" << uint64_t(op.op.alloc_hint.expected_write_size)
          << ",\"alloc_hint_flags\":" << uint32_t(op.op.alloc_hint.flags);
    } else {
      out << ",\"offset\":" << uint64_t(op.op.extent.offset)
          << ",\"length\":" << uint64_t(op.op.extent.length)
          << ",\"truncate_size\":" << uint64_t(op.op.extent.truncate_size)
          << ",\"truncate_seq\":" << uint32_t(op.op.extent.truncate_seq);
    }
    out << ",\"payload_len\":" << uint32_t(op.op.payload_len)
        << ",\"union_hex\":\"" << hex(std::string(packed + 6, 28))
        << "\",\"indata_hex\":\"" << hex(op.indata.to_str())
        << "\",\"outdata_hex\":\"" << hex(op.outdata.to_str()) << "\"";
    if (reply) out << ",\"result\":" << int(op.rval);
    out << "}";
  }
  out << "]\n";
}

// SETALLOCHINT and WRITEFULL are both WR in rados.h (321 and 274), so
// native ceph_osd_op_mode_modify derives only the WRITE bit.
ceph::ref_t<MOSDOp> request(std::vector<OSDOp> ops) {
  const hobject_t object(object_t("alloc-hint-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  int request_flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
  for (const auto& op : ops) {
    if (ceph_osd_op_mode_read(op.op.op)) request_flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) request_flags |= CEPH_OSD_FLAG_WRITE;
  }
  if (!(request_flags & CEPH_OSD_FLAG_WRITE) || (request_flags & CEPH_OSD_FLAG_READ))
    throw std::runtime_error("native op modes no longer select WRITE alone");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
                                      request_flags, features);
  req->set_retry_attempt(0);
  // Every vector writes, so each carries the explicit caller mtime.
  req->set_mtime(utime_t(1700000000, 5));
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
           int aggregate_result, int acktype, eversion_t replay,
           version_t user) {
  auto req = request(std::move(input));
  req->encode_payload(features);
  // Public constructor keeps request mode bits and replaces only completion
  // flags. The acktype is an explicit logical input of each vector.
  auto response = ceph::make_message<MOSDOpReply>(req.get(), aggregate_result, 11,
      acktype, false);
  response->set_reply_versions(replay, user);
  // Public claim_ops supplies the input union echoes and explicit logical
  // per-op results. SETALLOCHINT and WRITEFULL produce no output. No live
  // hint recording, object store hint, staging or durability is established.
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
    if (argc != 2) throw std::runtime_error("usage: native-alloc-hint-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    auto req = request(all_operations());
    emit(root, "alloc-hint-native-request-v6-all", *req);
    decode_request(root, "alloc-hint-native-request-v6-all", *req);

    // The SETALLOCHINT handler (PrimaryLogPG.cc 6717-6730) sets result 0 and
    // stages the hint; the vector commits: set_reply_versions(at_version,
    // user_at_version) (4325) and the on-commit callback adds ACK|ONDISK
    // (4387). The logical versions are 11'20 and 20.
    auto input = all_operations();
    auto output = input;
    reply(root, "alloc-hint-native-reply-v6-success", input, output, 0,
          CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(11, 20), 20);

    // Logical older-OSD shape: an unrecognized opcode takes the do_osd_ops
    // default -EOPNOTSUPP (8197-8202); the builder's FAILOK keeps the per-op
    // result and resets the running result (8205-8210), so the WRITEFULL
    // commits through the same ACK|ONDISK on-commit completion (4387).
    ObjectOperation failok;
    alloc_hint(failok, 4194304, 65536, hint_flags);
    failok.write_full(value("abc"));
    auto failok_input = operations(std::move(failok));
    auto failok_output = failok_input;
    failok_output[0].rval = -EOPNOTSUPP;
    reply(root, "alloc-hint-native-reply-v6-failok-refused", failok_input,
          failok_output, 0, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
          eversion_t(11, 20), 20);

    // Logical refusal shape on the update_log_only path: a failed may_write
    // do_osd_ops sets update_log_only (8991-8998), execute_ctx skips
    // complete_read_ctx (4316), calls set_reply_versions(at_version,
    // user_at_version) (4325) with the unchanged object user version
    // (4247-4248), adds ACK|ONDISK (4366) and records the error (4370).
    // Versions are 11'20 and 19.
    auto enospc_input = single_hint(4194304, 65536, hint_flags);
    auto enospc_output = enospc_input;
    enospc_output[0].rval = -ENOSPC;
    reply(root, "alloc-hint-native-reply-v6-enospc", enospc_input, enospc_output,
          -ENOSPC, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(11, 20), 19);

    // Maximum sizes and all eleven pinned flag bits in the echoed union, on
    // the same commit path as success.
    constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
    auto max_input = single_hint(max, max, all_hint_flags);
    auto max_output = max_input;
    reply(root, "alloc-hint-native-reply-v6-max", max_input, max_output, 0,
          CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(11, 20), 20);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
