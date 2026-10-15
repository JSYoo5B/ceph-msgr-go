// Development-only direct native CALL payload encoder; never a product dependency.
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

// Binary method input with an embedded NUL and a high byte.
const std::string binary_input("T\x00\xff", 3);
// pinned cls_hello.cc write_too_much_return_data appends this 52-byte
// sentence ten times (lines 183-185): 520 output bytes.
const std::string too_long("you should not see this because it is toooooo long. ");

ceph::buffer::list value(const std::string& bytes) {
  ceph::buffer::list result;
  result.append(bytes);
  return result;
}

// The pinned call(const char*, const char*, ceph::buffer::list&) overload
// (Objecter.h 1404-1406) is the only three-argument call(); a non-const
// lvalue buffer::list selects it without the string_view error_code or
// completion overloads. add_call (227-242) stores u8 class_len/method_len and
// u32 indata_len, leaves argc zero and appends class, method, then input.
void call(ObjectOperation& operation, const char* cls, const char* method,
          const std::string& input) {
  ceph::buffer::list indata = value(input);
  operation.call(cls, method, indata);
}

std::vector<OSDOp> operations(ObjectOperation&& operation) {
  std::vector<OSDOp> result(operation.ops.begin(), operation.ops.end());
  for (auto& op : result) op.rval = 0;
  return result;
}

// A CMPXATTR guard on "a" EQ "v" (STRING mode) before a WR class method with
// binary input. Every input is an explicit native string, not a Go encoding.
std::vector<OSDOp> all_operations() {
  ObjectOperation operation;
  operation.cmpxattr(std::string_view("a"), CEPH_OSD_CMPXATTR_OP_EQ,
                     CEPH_OSD_CMPXATTR_MODE_STRING, value("v"));
  call(operation, "hello", "write_return_data", binary_input);
  return operations(std::move(operation));
}

std::vector<OSDOp> single_call(const char* method, const std::string& input) {
  ObjectOperation operation;
  call(operation, "hello", method, input);
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
    if (op.op.op == CEPH_OSD_OP_CALL) {
      // The cls fields overlap extent.offset; the OSD output length is
      // recorded in the overlapping extent.length (PrimaryLogPG.cc 6257).
      out << ",\"class_len\":" << unsigned(op.op.cls.class_len)
          << ",\"method_len\":" << unsigned(op.op.cls.method_len)
          << ",\"argc\":" << unsigned(op.op.cls.argc)
          << ",\"indata_len\":" << uint32_t(op.op.cls.indata_len)
          << ",\"output_len\":" << uint64_t(op.op.extent.length);
    } else {
      out << ",\"name_len\":" << uint32_t(op.op.xattr.name_len)
          << ",\"value_len\":" << uint32_t(op.op.xattr.value_len)
          << ",\"cmp_op\":" << unsigned(op.op.xattr.cmp_op)
          << ",\"cmp_mode\":" << unsigned(op.op.xattr.cmp_mode);
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

// The CALL request READ/WRITE bit is an explicit caller mode: the OSD derives
// read/write from the registered method flags (osd_op_util.cc 189-204), and
// CALL itself is RD|EXEC, so ceph_osd_op_mode_modify cannot select WRITE.
// Other operations keep the native op-mode derivation used earlier.
ceph::ref_t<MOSDOp> request(std::vector<OSDOp> ops, bool write) {
  const hobject_t object(object_t("call-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  int request_flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
  for (const auto& op : ops) {
    if (op.op.op == CEPH_OSD_OP_CALL) {
      request_flags |= write ? CEPH_OSD_FLAG_WRITE : CEPH_OSD_FLAG_READ;
      continue;
    }
    if (ceph_osd_op_mode_read(op.op.op)) request_flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) request_flags |= CEPH_OSD_FLAG_WRITE;
  }
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
                                      request_flags, features);
  req->set_retry_attempt(0);
  // Explicit caller mtime for write-mode vectors; read-mode vectors use zero.
  req->set_mtime((request_flags & CEPH_OSD_FLAG_WRITE) ? utime_t(1700000000, 5) : utime_t());
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

// Record a method result like pinned PrimaryLogPG.cc 6255-6258: the OSD sets
// extent.length to the method output length, then appends the output.
void method_output(OSDOp& op, int rval, const std::string& output) {
  op.rval = rval;
  op.op.extent.length = output.size();
  op.outdata = value(output);
}

void reply(const std::filesystem::path& root, const std::string& name,
           std::vector<OSDOp> input, std::vector<OSDOp> outputs, bool write,
           int aggregate_result, int acktype, eversion_t replay,
           version_t user) {
  auto req = request(std::move(input), write);
  req->encode_payload(features);
  // Public constructor keeps request mode bits and replaces only completion
  // flags. The acktype is an explicit logical input of each vector.
  auto response = ceph::make_message<MOSDOpReply>(req.get(), aggregate_result, 11,
      acktype, false);
  response->set_reply_versions(replay, user);
  // Public claim_ops supplies explicit logical per-op results, the recorded
  // output length and output bytes. No live class loading, method execution,
  // staging, durability or write-reply bound is established here.
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
    if (argc != 2) throw std::runtime_error("usage: native-call-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    auto req = request(all_operations(), true);
    emit(root, "call-native-request-v6-all", *req);
    decode_request(root, "call-native-request-v6-all", *req);

    // RD method say_hello (cls_hello.cc 57-75) without a write: execute_ctx
    // calls complete_read_ctx, which adds ACK|ONDISK (PrimaryLogPG.cc 9220)
    // and reports eversion_t() with the object user version (9208-9213).
    auto read_input = single_call("say_hello", "Tester");
    auto read_output = read_input;
    method_output(read_output[0], 0, "Hello, Tester!");
    reply(root, "call-native-reply-v6-read-output", read_input, read_output,
          false, 0, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(), 19);

    // WR method write_return_data with empty input (cls_hello.cc 146-170)
    // stages a setxattr, returns 42 and 18 output bytes. do_osd_ops keeps 42
    // (8206-8213), prepare_transaction returns it (9042), the RETURNVEC bound
    // admits 18 bytes (4288-4301) and the commit callback adds ACK|ONDISK
    // (4387) after set_reply_versions(at_version, user_at_version) (4325).
    auto positive_input = single_call("write_return_data", "");
    auto positive_output = positive_input;
    method_output(positive_output[0], 42, "you might see this");
    reply(root, "call-native-reply-v6-positive", positive_input, positive_output,
          true, 42, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(11, 20), 20);

    // write_too_much_return_data (cls_hello.cc 172-188) returns 42 with 520
    // bytes. 6257 records extent.length 520; the RETURNVEC bound then sets the
    // aggregate and per-op result to -EOVERFLOW and clears outdata
    // (4294-4300). The negative result takes complete_read_ctx (4316-4322),
    // which adds ACK|ONDISK and sets no reply versions for this error.
    std::string overflow_output;
    for (unsigned i = 0; i < 10; ++i) overflow_output += too_long;
    if (overflow_output.size() != 520)
      throw std::runtime_error("hello overflow output length changed");
    auto overflow_input = single_call("write_too_much_return_data", "");
    auto overflow_ops = overflow_input;
    method_output(overflow_ops[0], 42, overflow_output);
    overflow_ops[0].rval = -EOVERFLOW;
    overflow_ops[0].outdata.clear();
    reply(root, "call-native-reply-v6-eoverflow", overflow_input, overflow_ops,
          true, -EOVERFLOW, CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK, eversion_t(), 0);

    // An unregistered method is refused before execution: OpInfo::set_from_op
    // maps get_method_flags -ENOENT to -EOPNOTSUPP (osd_op_util.cc 189-195),
    // do_op calls reply_op_error (PrimaryLogPG.cc 2051-2054), and
    // OSDService::reply_op_error (OSD.cc 1635-1648) keeps only the request's
    // ACK|ONDISK bits (ONDISK here), zero versions and unexecuted op echoes.
    auto unsupported_input = single_call("nope", "");
    auto unsupported_output = unsupported_input;
    reply(root, "call-native-reply-v6-eopnotsupp", unsupported_input,
          unsupported_output, false, -EOPNOTSUPP, CEPH_OSD_FLAG_ONDISK,
          eversion_t(), 0);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
