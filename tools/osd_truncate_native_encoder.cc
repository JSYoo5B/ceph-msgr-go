// Development-only direct native extent truncate-field (READ, WRITE and
// TRUNCATE with truncate_size/truncate_seq) payload encoder; never a product
// dependency.
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
// Pinned include/rados.h opcodes of the three extent operations.
static_assert(CEPH_OSD_OP_READ == 0x1201, "pinned READ opcode changed");
static_assert(CEPH_OSD_OP_WRITE == 0x2201, "pinned WRITE opcode changed");
static_assert(CEPH_OSD_OP_TRUNCATE == 0x2203, "pinned TRUNCATE opcode changed");
static_assert(sizeof(ceph_osd_op) == 38, "pinned packed ceph_osd_op size changed");

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

std::vector<OSDOp> operations(ObjectOperation&& operation) {
  std::vector<OSDOp> result(operation.ops.begin(), operation.ops.end());
  for (auto& op : result) op.rval = 0;
  return result;
}

// One logical request: its ops plus the request-level fields that
// Objecter::_prepare_osd_op passes through the public set_snapid,
// set_snap_seq and set_snaps setters (Objecter.cc 3394-3396).
struct Vector {
  std::vector<OSDOp> ops;
  snapid_t snap;
  snapid_t snap_seq;
  std::vector<snapid_t> snaps;
  int extra_flags;
  utime_t mtime;
};

// The CephFS data path (Client::_read -> Filer::read_trunc ->
// Objecter::sg_read_trunc -> Objecter::read_trunc, Objecter.h 3383-3402)
// builds one READ per object extent: CEPH_OSD_OP_READ with the extent
// offset/length, the inode's truncate_size and truncate_seq and op flags 0.
// The same fields are set here on the op added by the public
// ObjectOperation::add_data (Objecter.h 176-181): read 4~12 with
// truncate_size 10 and truncate_seq 3 on the head.
Vector read_vector() {
  ObjectOperation operation;
  ceph::buffer::list none;
  operation.add_data(CEPH_OSD_OP_READ, 4, 12, none);
  OSDOp& op = operation.ops.back();
  op.op.extent.truncate_size = 10;
  op.op.extent.truncate_seq = 3;
  op.op.flags = 0;
  return {operations(std::move(operation)), snapid_t(CEPH_NOSNAP), snapid_t(0),
          {}, 0, utime_t()};
}

// The write path (Client::_write -> Filer::write_trunc ->
// Objecter::sg_write_trunc -> Objecter::write_trunc, Objecter.h 3550-3573)
// sends WRITE with truncate_size/truncate_seq; the public
// ObjectOperation::write(off, bl, truncate_size, truncate_seq) builder
// (570-577) sets exactly those fields: write "truncate" at 8 with
// truncate_size 10 and truncate_seq 2, snap context {1, []}.
Vector write_vector() {
  ObjectOperation operation;
  auto data = value("truncate");
  operation.write(8, data, 10, 2);
  return {operations(std::move(operation)), snapid_t(CEPH_NOSNAP), snapid_t(1),
          {}, 0, utime_t(1739999999, 5)};
}

// Objecter::trunc (Objecter.h 3643-3661) sends CEPH_OSD_OP_TRUNCATE with
// extent.offset = truncate_size = trunc_size and the sequence, length 0.
// The public ObjectOperation::truncate(off) builder (606-609) adds the
// TRUNCATE with offset 10 and length 0; the sequence fields are set as
// Objecter::trunc does: truncate_size 10, truncate_seq 3, snap context {1, []}.
Vector trunc_vector() {
  ObjectOperation operation;
  operation.truncate(10);
  OSDOp& op = operation.ops.back();
  op.op.extent.truncate_size = 10;
  op.op.extent.truncate_seq = 3;
  return {operations(std::move(operation)), snapid_t(CEPH_NOSNAP), snapid_t(1),
          {}, 0, utime_t(1739999999, 6)};
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

void write_snaps(std::ostream& out, const std::vector<snapid_t>& snaps) {
  out << "[";
  for (size_t i = 0; i != snaps.size(); ++i) {
    if (i) out << ",";
    out << uint64_t(snaps[i]);
  }
  out << "]";
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
    if (op.op.op == CEPH_OSD_OP_ROLLBACK) {
      out << ",\"snapid\":" << uint64_t(op.op.snap.snapid);
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
    if (reply) {
      out << ",\"result\":" << int(op.rval);
    }
    out << "}";
  }
  out << "]\n";
}

// Native op modes derive READ or WRITE; ORDERSNAP is an explicit request
// flag, and ONDISK and RETURNVEC are always requested.
ceph::ref_t<MOSDOp> request(const Vector& source) {
  const hobject_t object(object_t("trunc-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  int request_flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC |
                      source.extra_flags;
  for (const auto& op : source.ops) {
    if (ceph_osd_op_mode_read(op.op.op)) request_flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) request_flags |= CEPH_OSD_FLAG_WRITE;
  }
  const bool write = request_flags & CEPH_OSD_FLAG_WRITE;
  if (write == bool(request_flags & CEPH_OSD_FLAG_READ))
    throw std::runtime_error("native op modes no longer select READ or WRITE alone");
  if (!write && (source.mtime != utime_t() || source.snap_seq != snapid_t(0) ||
                 !source.snaps.empty()))
    throw std::runtime_error("read vector carries a write-only input");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
                                      request_flags, features);
  req->set_snapid(source.snap);
  req->set_snap_seq(source.snap_seq);
  req->set_snaps(source.snaps);
  req->ops = source.ops;
  req->set_mtime(source.mtime);
  req->set_retry_attempt(0);
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

void emit_request(const std::filesystem::path& root, const std::string& name,
                  const Vector& source) {
  auto req = request(source);
  emit(root, name, *req);
  auto decoded = ceph::make_message<MOSDOp>();
  decoded->set_header(req->get_header());
  ceph::buffer::list payload(req->get_payload());
  decoded->set_payload(payload);
  decoded->set_data(req->get_data());
  decoded->decode_payload();
  verify_operations(req->ops, decoded->ops, false);
  // The v6 decoder completes the final decode, so the snapshot fields are
  // directly comparable with the fresh encoder inputs.
  if (decoded->get_snapid() != source.snap ||
      decoded->get_snap_seq() != source.snap_seq ||
      decoded->get_snaps() != source.snaps ||
      decoded->get_flags() != req->get_flags() ||
      decoded->get_mtime() != source.mtime || decoded->get_tid() != 77)
    throw std::runtime_error("native request decoder changed snapshot fields");
  write_decoder_receipt(root, name, decoded->ops, false);
  std::ofstream out(root / (name + ".decoded-message.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "{\"flags\":" << decoded->get_flags()
      << ",\"snapid\":" << uint64_t(decoded->get_snapid())
      << ",\"snap_seq\":" << uint64_t(decoded->get_snap_seq())
      << ",\"snaps\":";
  write_snaps(out, decoded->get_snaps());
  out << ",\"mtime_seconds\":" << decoded->get_mtime().sec()
      << ",\"mtime_nanoseconds\":" << decoded->get_mtime().nsec()
      << ",\"map_epoch\":" << decoded->get_map_epoch()
      << ",\"tid\":" << decoded->get_tid()
      << ",\"object\":\"" << decoded->get_oid().name << "\"}\n";
  // This separately decoded object is never passed to an encoder.
}

void finish_reply(const std::filesystem::path& root, const std::string& name,
                  MOSDOpReply& response, int aggregate_result, int flags) {
  emit(root, name, response);
  std::vector<OSDOp> encoded_ops;
  response.claim_ops(encoded_ops);  // Extract encoded ops; never re-encode.
  auto decoded = ceph::make_message<MOSDOpReply>();
  decoded->set_header(response.get_header());
  ceph::buffer::list payload(response.get_payload());
  decoded->set_payload(payload);
  decoded->set_data(response.get_data());
  decoded->decode_payload();
  if (decoded->get_result() != aggregate_result || decoded->get_tid() != 77 ||
      decoded->get_flags() != flags)
    throw std::runtime_error("native reply decoder changed result, flags or TID");
  std::vector<OSDOp> decoded_ops;
  decoded->claim_ops(decoded_ops);
  verify_operations(encoded_ops, decoded_ops, true);
  write_decoder_receipt(root, name, decoded_ops, true);
  std::ofstream out(root / (name + ".decoded-message.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "{\"flags\":" << decoded->get_flags()
      << ",\"result\":" << decoded->get_result()
      << ",\"replay_epoch\":" << decoded->get_replay_version().epoch
      << ",\"replay_version\":" << decoded->get_replay_version().version
      << ",\"user_version\":" << decoded->get_user_version()
      << ",\"map_epoch\":" << decoded->get_map_epoch()
      << ",\"tid\":" << decoded->get_tid()
      << ",\"object\":\"" << decoded->get_oid().name << "\"}\n";
}

// Completion through the execute_ctx reply (PrimaryLogPG.cc 4309-4311),
// which the read or commit path finishes with ACK|ONDISK. The explicit
// acktype, versions and per-op outputs are logical inputs of each vector.
void reply(const std::filesystem::path& root, const std::string& name,
           const Vector& source, std::vector<OSDOp> outputs,
           int aggregate_result, int acktype, eversion_t replay,
           version_t user) {
  auto req = request(source);
  req->encode_payload(features);
  // Public constructor keeps request mode bits and replaces only completion
  // flags. RETURNVEC is set, so output data is not ignored.
  auto response = ceph::make_message<MOSDOpReply>(req.get(), aggregate_result, 11,
      acktype, false);
  response->set_reply_versions(replay, user);
  // Public claim_ops supplies the input union echoes, explicit outputs and
  // logical per-op results with input data cleared.
  for (auto& op : outputs) op.indata.clear();
  response->claim_ops(outputs);
  const int flags = (req->get_flags() & ~(CEPH_OSD_FLAG_ONDISK |
      CEPH_OSD_FLAG_ONNVRAM | CEPH_OSD_FLAG_ACK)) | acktype;
  finish_reply(root, name, *response, aggregate_result, flags);
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: native-truncate-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    emit_request(root, "truncate-native-request-v6-read", read_vector());
    emit_request(root, "truncate-native-request-v6-write", write_vector());
    emit_request(root, "truncate-native-request-v6-trunc", trunc_vector());

    constexpr int completed = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

    // do_read (PrimaryLogPG.cc 5849-5861): the object's truncate_seq (here
    // logically 2) is older than the op's 3 and 4+12 > truncate_size 10 < the
    // object size (logically 16), so the size becomes 10 and the extent
    // length is cut to 10 - 4 = 6, which the reply echoes with six bytes of
    // output; truncate_size and truncate_seq are echoed unchanged.
    // complete_read_ctx sets versions eversion_t() and user version 21
    // (9209-9210) and adds ACK|ONDISK (9220).
    auto read_source = read_vector();
    auto read_outputs = read_source.ops;
    read_outputs[0].op.extent.length = 6;
    read_outputs[0].outdata = value("ABCDEF");
    reply(root, "truncate-native-reply-v6-read-clipped", read_source,
          read_outputs, 0, completed, eversion_t(), 21);

    // An old WRITE that arrives after the trim (6768-6777): the object's
    // truncate_seq (logically 3) is newer than the op's 2 and 8+8 exceeds the
    // object size (logically 10), so the extent length becomes 10 - 8 = 2 and
    // the input is cut to two bytes. The commit path replies with the
    // trimmed extent, no output, result 0 and the commit versions (11'22,
    // user 22; 4325, 4387).
    auto write_source = write_vector();
    auto write_outputs = write_source.ops;
    write_outputs[0].op.extent.length = 2;
    reply(root, "truncate-native-reply-v6-write-trimmed", write_source,
          write_outputs, 0, completed, eversion_t(11, 22), 22);

    // A sequenced TRUNCATE whose truncate_seq 3 is not newer than the
    // object's (logically 3) is a no-op (6986-6992) that still commits as a
    // write: the extent is echoed unchanged with result 0 and versions 11'23,
    // user 23.
    auto trunc_source = trunc_vector();
    reply(root, "truncate-native-reply-v6-trunc-noop", trunc_source,
          trunc_source.ops, 0, completed, eversion_t(11, 23), 23);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
