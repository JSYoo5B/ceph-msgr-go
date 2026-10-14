// Development-only direct native payload encoder; never a product dependency.
// Compile against Ceph 20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// Ceph headers/library carry LGPL-2.1 notices. This wrapper only constructs
// public native objects and invokes their encoders; it copies no implementation.
// No decoded front is imported and no socket or network capture is used.
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

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

void add_extent(std::vector<OSDOp>& ops, uint16_t code, uint32_t flags,
                uint64_t offset, uint64_t length,
                const std::string& input = {}) {
  ops.emplace_back(code);
  auto& op = ops.back();
  op.op.flags = flags;
  op.op.extent.offset = offset;
  op.op.extent.length = length;
  if (!input.empty()) op.indata.append(input);
  op.rval = 0;
}

std::vector<OSDOp> all_operations() {
  std::vector<OSDOp> ops;
  add_extent(ops, CEPH_OSD_OP_CREATE, CEPH_OSD_OP_FLAG_EXCL, 0, 0);
  add_extent(ops, CEPH_OSD_OP_WRITE, 0, 2, 4,
             std::string("\x00\xff" "A\n", 4));
  add_extent(ops, CEPH_OSD_OP_WRITEFULL, 0, 0, 3, "new");
  add_extent(ops, CEPH_OSD_OP_APPEND, 0, 0, 2, std::string("\x80!", 2));
  add_extent(ops, CEPH_OSD_OP_TRUNCATE, 0, 7, 0);
  add_extent(ops, CEPH_OSD_OP_ZERO, 0, 1, 2);
  ops.emplace_back(CEPH_OSD_OP_ASSERT_VER);
  ops.back().op.assert_ver.ver = 19;
  ops.back().rval = 0;
  add_extent(ops, CEPH_OSD_OP_CMPEXT, CEPH_OSD_OP_FLAG_FAILOK, 1, 2,
             std::string("\x00\xff", 2));
  add_extent(ops, CEPH_OSD_OP_DELETE, 0, 0, 0);
  return ops;
}

ceph::ref_t<MOSDOp> request(std::vector<OSDOp> ops) {
  const hobject_t object(object_t("mutation-codec"), "route-key", CEPH_NOSNAP,
                         0x11223344, 7, "tenant");
  spg_t pg(pg_t(0x11223344, 7));
  auto req = ceph::make_message<MOSDOp>(1, 77, object, pg, 11,
      CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_WRITE |
      CEPH_OSD_FLAG_RETURNVEC, features);
  req->set_retry_attempt(0);
  req->set_mtime(utime_t(1700000000, 123456789));
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
  write_segment(root / (name + ".front.bin"), message.get_payload());
  write_segment(root / (name + ".data.bin"), message.get_data());
  std::cout << name << "\t" << message.get_payload().length() << "\t"
            << message.get_data().length() << "\n";
}

void reply(const std::filesystem::path& root, const std::string& name,
           std::vector<OSDOp> ops, int result) {
  auto req = request(std::move(ops));
  // Encode the newly constructed request to give native payload_len semantics
  // before creating a reply. Its front is not imported or reused as reply data.
  req->encode_payload(features);
  auto response = ceph::make_message<MOSDOpReply>(req.get(), result, 11,
                                                 CEPH_OSD_FLAG_ONDISK, true);
  response->set_reply_versions(eversion_t(11, 20), 19);
  emit(root, name, *response);
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: native-encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    auto req = request(all_operations());
    emit(root, "mutation-native-request-v6-all", *req);
    reply(root, "mutation-native-reply-v6-all-success", all_operations(), 0);
    auto create = all_operations();
    create.resize(1);
    create[0].rval = -17;
    reply(root, "mutation-native-reply-v6-exclusive-eexist", std::move(create), -17);
    auto comparison = all_operations();
    auto compare = comparison[7];
    compare.op.flags = 0;
    compare.rval = -4097;
    reply(root, "mutation-native-reply-v6-cmpext-mismatch", {compare}, -4097);
    std::vector<OSDOp> normalized;
    add_extent(normalized, CEPH_OSD_OP_TRUNCATE, 0, 1, 0);
    // This is a freshly constructed truncate reply, not a server ZERO
    // normalization capture. Live normalization evidence is separate.
    reply(root, "mutation-native-reply-v6-truncate", std::move(normalized), 0);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << "\n";
    return 1;
  }
}
