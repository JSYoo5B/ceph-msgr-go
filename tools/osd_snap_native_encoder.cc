// Development-only direct native snapshot context, ROLLBACK and LIST_SNAPS
// payload encoder; never a product dependency.
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

// Pinned include/rados.h 38-39, 267, 290, 460 and 498.
static_assert(CEPH_NOSNAP == 0xfffffffffffffffeULL, "pinned CEPH_NOSNAP changed");
static_assert(CEPH_SNAPDIR == 0xffffffffffffffffULL, "pinned CEPH_SNAPDIR changed");
static_assert(CEPH_OSD_OP_LIST_SNAPS == 0x120a, "pinned LIST_SNAPS opcode changed");
static_assert(CEPH_OSD_OP_ROLLBACK == 0x220e, "pinned ROLLBACK opcode changed");
static_assert(CEPH_OSD_FLAG_ORDERSNAP == 0x40, "pinned ORDERSNAP flag changed");
static_assert(EOLDSNAPC == 85, "pinned EOLDSNAPC changed");

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

// One logical request: its ops plus the request-level snapshot fields that
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

// READ 0~4 through read(off, len, bufferlist*, int*, Context*) (Objecter.h
// 497-505, add_data 176-181) and STAT through stat(uint64_t*, nullptr,
// nullptr) (427-431), read at clone snap 4 with no snap context.
Vector read_vector() {
  ObjectOperation operation;
  operation.read(0, 4, static_cast<ceph::buffer::list*>(nullptr),
                 static_cast<int*>(nullptr), static_cast<Context*>(nullptr));
  operation.stat(static_cast<uint64_t*>(nullptr), nullptr, nullptr);
  return {operations(std::move(operation)), snapid_t(4), snapid_t(0), {}, 0,
          utime_t()};
}

// WRITEFULL "v2" through write_full(buffer::list&&) (584-589) on the head
// with snap context seq 7, snaps [7,4,2] (descending, as SnapContext keeps
// them) and the explicit ORDERSNAP request flag.
Vector write_vector() {
  ObjectOperation operation;
  operation.write_full(value("v2"));
  return {operations(std::move(operation)), snapid_t(CEPH_NOSNAP), snapid_t(7),
          {snapid_t(7), snapid_t(4), snapid_t(2)}, CEPH_OSD_FLAG_ORDERSNAP,
          utime_t(1739999999, 5)};
}

// ROLLBACK to snap 4 through the public rollback(uint64_t) builder
// (Objecter.h 1515-1518): add_op(CEPH_OSD_OP_ROLLBACK) and le64 snap.snapid.
Vector rollback_vector() {
  ObjectOperation operation;
  operation.rollback(4);
  return {operations(std::move(operation)), snapid_t(CEPH_NOSNAP), snapid_t(7),
          {snapid_t(7), snapid_t(4), snapid_t(2)}, 0, utime_t(1739999999, 5)};
}

// LIST_SNAPS through list_snaps(librados::snap_set_t*, int*) (1482-1490)
// with null outputs, so the builder adds only the op. librados requires the
// read snap CEPH_SNAPDIR (librados_cxx.cc 2224-2238) and the OSD refuses
// LIST_SNAPS at any other snap (PrimaryLogPG.cc 2321-2338).
Vector list_vector() {
  ObjectOperation operation;
  operation.list_snaps(static_cast<librados::snap_set_t*>(nullptr),
                       static_cast<int*>(nullptr));
  return {operations(std::move(operation)), snapid_t(CEPH_SNAPDIR), snapid_t(0),
          {}, 0, utime_t()};
}

// The STAT handler encodes object_info size (u64) then mtime (utime_t)
// (PrimaryLogPG.cc 6270-6272); the native encoders construct these bytes.
ceph::buffer::list stat_output(uint64_t size, utime_t mtime) {
  using ceph::encode;
  ceph::buffer::list result;
  encode(size, result);
  encode(mtime, result);
  if (result.length() != 16)
    throw std::runtime_error("native STAT output is no longer 16 bytes");
  return result;
}

clone_info clone(snapid_t id, std::vector<snapid_t> snaps,
                 std::vector<std::pair<uint64_t, uint64_t>> overlap,
                 uint64_t size) {
  clone_info result;
  result.cloneid = id;
  result.snaps = std::move(snaps);
  result.overlap = std::move(overlap);
  result.size = size;
  return result;
}

obj_list_snap_response_t listing() {
  obj_list_snap_response_t response;
  response.clones = {
      clone(snapid_t(2), {snapid_t(2)}, {{0, 3}}, 3),
      clone(snapid_t(7), {snapid_t(4), snapid_t(7)}, {{0, 2}, {4, 4}}, 9),
      clone(snapid_t(CEPH_NOSNAP), {}, {}, 11)};
  response.seq = snapid_t(7);
  return response;
}

// Native obj_list_snap_response_t::encode (osd_types.h 6852-6900) with its
// clone_info entries (6786-6846); no byte of the listing is hand-written.
ceph::buffer::list list_output() {
  ceph::buffer::list result;
  listing().encode(result);
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

void write_snaps(std::ostream& out, const std::vector<snapid_t>& snaps) {
  out << "[";
  for (size_t i = 0; i != snaps.size(); ++i) {
    if (i) out << ",";
    out << uint64_t(snaps[i]);
  }
  out << "]";
}

// Separately decode reply outputs with the native decoders: the listing via
// obj_list_snap_response_t::decode and STAT via u64 plus utime_t.
void write_output_receipt(std::ostream& out, const OSDOp& op) {
  using ceph::decode;
  if (op.op.op == CEPH_OSD_OP_LIST_SNAPS && op.outdata.length()) {
    obj_list_snap_response_t decoded;
    auto p = op.outdata.cbegin();
    decoded.decode(p);
    if (!p.end()) throw std::runtime_error("native listing decoder left bytes");
    // Compare decoded fields with the logical inputs; never re-encode.
    const auto expected = listing();
    bool same = decoded.seq == expected.seq &&
                decoded.clones.size() == expected.clones.size();
    for (size_t i = 0; same && i != decoded.clones.size(); ++i) {
      const auto& got = decoded.clones[i];
      const auto& want = expected.clones[i];
      same = got.cloneid == want.cloneid && got.snaps == want.snaps &&
             got.overlap == want.overlap && got.size == want.size;
    }
    if (!same) throw std::runtime_error("native listing decoder changed the listing");
    out << ",\"listing\":{\"seq\":" << uint64_t(decoded.seq) << ",\"clones\":[";
    for (size_t i = 0; i != decoded.clones.size(); ++i) {
      const auto& c = decoded.clones[i];
      if (i) out << ",";
      out << "{\"cloneid\":" << uint64_t(c.cloneid) << ",\"snaps\":";
      write_snaps(out, c.snaps);
      out << ",\"overlap\":[";
      for (size_t j = 0; j != c.overlap.size(); ++j) {
        if (j) out << ",";
        out << "[" << c.overlap[j].first << "," << c.overlap[j].second << "]";
      }
      out << "],\"size\":" << c.size << "}";
    }
    out << "]}";
  }
  if (op.op.op == CEPH_OSD_OP_STAT && op.outdata.length()) {
    uint64_t size;
    utime_t mtime;
    auto p = op.outdata.cbegin();
    decode(size, p);
    decode(mtime, p);
    if (!p.end()) throw std::runtime_error("native STAT decoder left bytes");
    out << ",\"stat_size\":" << size << ",\"stat_mtime_seconds\":" << mtime.sec()
        << ",\"stat_mtime_nanoseconds\":" << mtime.nsec();
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
      write_output_receipt(out, op);
    }
    out << "}";
  }
  out << "]\n";
}

// Native op modes derive READ or WRITE; ORDERSNAP is an explicit request
// flag, and ONDISK and RETURNVEC are always requested.
ceph::ref_t<MOSDOp> request(const Vector& source) {
  const hobject_t object(object_t("snap-codec"), "route-key", CEPH_NOSNAP,
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

// The public calls of OSDService::reply_op_error(op, err) (OSD.cc 1630-1649):
// the completion bits are the request's own ACK|ONDISK bits, output is kept
// only with RETURNVEC, versions are eversion_t()/0 and op_returns is empty.
// The constructor copies the request ops, whose decoded results are zero.
void reply_op_error(const std::filesystem::path& root, const std::string& name,
                    const Vector& source, int err) {
  auto req = request(source);
  req->encode_payload(features);
  const int acktype = req->get_flags() & (CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  auto response = ceph::make_message<MOSDOpReply>(req.get(), err, 11, acktype,
      !req->has_flag(CEPH_OSD_FLAG_RETURNVEC));
  response->set_reply_versions(eversion_t(), 0);
  response->set_op_returns({});
  const int flags = (req->get_flags() & ~(CEPH_OSD_FLAG_ONDISK |
      CEPH_OSD_FLAG_ONNVRAM | CEPH_OSD_FLAG_ACK)) | acktype;
  finish_reply(root, name, *response, err, flags);
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: native-snap-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    emit_request(root, "snap-native-request-v6-read", read_vector());
    emit_request(root, "snap-native-request-v6-write", write_vector());
    emit_request(root, "snap-native-request-v6-rollback", rollback_vector());
    emit_request(root, "snap-native-request-v6-list", list_vector());

    constexpr int completed = CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK;

    // LIST_SNAPS at CEPH_SNAPDIR resolves to the head object context
    // (PrimaryLogPG.cc 2317-2319); the handler encodes the native listing
    // with the head last (6571-6659). The read path complete_read_ctx sets
    // versions eversion_t() and the head user version (9209-9210) and adds
    // ACK|ONDISK (9220). The logical head user version is 20.
    auto list_source = list_vector();
    auto list_outputs = list_source.ops;
    list_outputs[0].outdata = list_output();
    reply(root, "snap-native-reply-v6-list", list_source, list_outputs, 0,
          completed, eversion_t(), 20);

    // ROLLBACK (6901-6905, _rollback_to 8373) leaves the union unchanged and
    // produces no output; the commit path runs set_reply_versions(at_version,
    // user_at_version) (4325) and the on-commit callback adds ACK|ONDISK
    // (4387). The logical versions are 11'21 and 21.
    auto rollback_source = rollback_vector();
    reply(root, "snap-native-reply-v6-rollback", rollback_source,
          rollback_source.ops, 0, completed, eversion_t(11, 21), 21);

    // ORDERSNAP with snapc seq below the object snapset seq replies through
    // reply_ctx(ctx, -EOLDSNAPC) (4223-4230, 4428-4433). EOLDSNAPC is the
    // Ceph-defined 85 from rados.h 498, not a host errno.
    reply_op_error(root, "snap-native-reply-v6-eoldsnapc", write_vector(), -EOLDSNAPC);

    // A clone read: do_read keeps extent length 4 for four bytes read
    // (5835-5929), READ at offset 0 leaves data_off 0 (6142-6144, 9196), STAT
    // encodes size 4 and mtime 1739990000.000000003 (6270-6272), and
    // complete_read_ctx sets the clone user version 3 (9209-9210) and adds
    // ACK|ONDISK (9220).
    auto read_source = read_vector();
    auto read_outputs = read_source.ops;
    read_outputs[0].outdata = value("v1v1");
    read_outputs[1].outdata = stat_output(4, utime_t(1739990000, 3));
    reply(root, "snap-native-reply-v6-read", read_source, read_outputs, 0,
          completed, eversion_t(), 3);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
