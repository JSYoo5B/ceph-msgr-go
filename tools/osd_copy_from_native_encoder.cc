// Development-only direct native COPY_FROM request encoder; never a product dependency.
// Compile against Ceph 20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// Ceph headers/library carry LGPL-2.1 notices. This wrapper only constructs
// public native objects and invokes their encoders; it copies no implementation.
// No decoded front is imported and no socket or network capture is used.
#include "messages/MOSDOp.h"
#include "msg/async/frames_v2.h"
#include "osdc/Objecter.h"
#include <cstring>
#include <utility>

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
                       const std::vector<OSDOp>& decoded) {
  if (encoded.size() != decoded.size())
    throw std::runtime_error("native decoder changed operation count");
  for (size_t i = 0; i != encoded.size(); ++i) {
    if (std::memcmp(&encoded[i].op, &decoded[i].op, sizeof(ceph_osd_op)) != 0 ||
        encoded[i].indata.to_str() != decoded[i].indata.to_str())
      throw std::runtime_error("native decoder changed packed union/result/data");
  }
}

void write_decoder_receipt(const std::filesystem::path& root,
                           const std::string& name,
                           const std::vector<OSDOp>& ops) {
  std::ofstream out(root / (name + ".decoded-ops.json"));
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out << "[";
  for (size_t i = 0; i != ops.size(); ++i) {
    const auto& op = ops[i];
    if (i) out << ",";
    const auto* packed = reinterpret_cast<const char*>(&op.op);
    out << "{\"code\":" << uint16_t(op.op.op)
        << ",\"flags\":" << uint32_t(op.op.flags);
    out << ",\"payload_len\":" << uint32_t(op.op.payload_len)
        << ",\"union_hex\":\"" << hex(std::string(packed + 6, 28))
        << "\",\"indata_hex\":\"" << hex(op.indata.to_str())
        << "\",\"outdata_hex\":\"" << hex(op.outdata.to_str()) << "\"";
    out << "}";
  }
  out << "]\n";
}

// These are explicit native inputs; no Go front or data is imported.
std::vector<OSDOp> operations(const std::string& shape) {
  ObjectOperation op;
  object_locator_t source(8);
  source.key = "source-key";
  source.nspace = "source-ns";
  const object_t object("src\x01\xc3\xa9");
  const bool snapshot = shape.find("snapshot") != std::string::npos;
  const snapid_t snap(snapshot ? 6 : CEPH_NOSNAP);
  const version_t version = snapshot ? 0x0102030405060708ULL : 0;
  const unsigned advice = snapshot ? CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL : 0;
  if (shape == "copy-head" || shape == "copy-snapshot") {
    op.copy_from(object, snap, source, version, 0, advice);
  } else {
    const bool truncate = shape == "copy2-truncate";
    op.copy_from2(object, snap, source, version,
                 truncate ? CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ : 0,
                 truncate ? 9 : 0, truncate ? 0xfedcba98ULL : 0, advice);
  }
  return std::vector<OSDOp>(op.ops.begin(), op.ops.end());
}

ceph::ref_t<MOSDOp> request(const std::string& shape) {
  const hobject_t object(object_t("copy-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  auto ops = operations(shape);
  int flags = CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_RETURNVEC;
  for (const auto& op : ops) {
    if (ceph_osd_op_mode_read(op.op.op)) flags |= CEPH_OSD_FLAG_READ;
    if (ceph_osd_op_mode_modify(op.op.op)) flags |= CEPH_OSD_FLAG_WRITE;
  }
  if ((flags & CEPH_OSD_FLAG_READ) || !(flags & CEPH_OSD_FLAG_WRITE))
    throw std::runtime_error("copy must derive WRITE alone");
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11, flags, features);
  req->set_retry_attempt(0);
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
  verify_operations(source.ops, decoded->ops);
  write_decoder_receipt(root, name, decoded->ops);
  // This separately decoded object is never passed to an encoder.
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: native-copy-from-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    if (std::filesystem::exists(root))
      throw std::runtime_error("output must be fresh");
    std::filesystem::create_directories(root);
    for (const auto& shape : {"copy-head", "copy2-head", "copy2-truncate",
                              "copy-snapshot", "copy2-snapshot"}) {
      const auto name = std::string("copy-from-native-request-v6-") + shape;
      auto req = request(shape);
      emit(root, name, *req);
      decode_request(root, name, *req);
      // Decode the operation inputs separately from MOSDOp's opaque indata.
      const auto& op = req->ops.at(0);
      auto p = op.indata.cbegin();
      object_t object;
      object_locator_t locator;
      decode(object, p);
      decode(locator, p);
      uint32_t sequence = 0;
      uint64_t size = 0;
      if (op.op.op == CEPH_OSD_OP_COPY_FROM2) {
        decode(sequence, p);
        decode(size, p);
      }
      const bool truncate = std::string(shape) == "copy2-truncate";
      if (!p.end() || object.name != "src\x01\xc3\xa9" || locator.pool != 8 ||
          locator.key != "source-key" || locator.nspace != "source-ns" ||
          locator.hash != -1 || sequence != (truncate ? 9U : 0U) ||
          size != (truncate ? 0xfedcba98ULL : 0ULL))
        throw std::runtime_error("native source input decoder differs");
      std::ofstream receipt(root / (name + ".decoded-source.json"));
      receipt.exceptions(std::ios::failbit | std::ios::badbit);
      receipt << "{\"object_hex\":\"" << hex(object.name)
              << "\",\"pool\":" << locator.pool
              << ",\"key_hex\":\"" << hex(locator.key)
              << "\",\"namespace_hex\":\"" << hex(locator.nspace)
              << "\",\"hash\":" << locator.hash
              << ",\"truncate_sequence\":" << sequence
              << ",\"truncate_size\":" << size << "}\n";
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
