// Test-only independent native MAPEXT observer, Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados/librados.hpp:882,
// librados/librados_cxx.cc:1309 and librados/IoCtxImpl.cc:1408-1440.
// Source licensing: COPYING defaults to LGPL-2.1 or LGPL-3.0; IoCtxImpl.cc
// explicitly names LGPL-2.1. This wrapper invokes APIs; no implementation copied.
// Caller supplies owned fixture identity/range and optionally a read snap id.
#include "include/rados/librados.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>

namespace {
constexpr size_t max_extents = 4096;

uint64_t number(const char* text) {
  const std::string value(text);
  if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("invalid unsigned decimal");
  size_t used = 0;
  const auto result = std::stoull(value, &used);
  if (used != value.size()) throw std::runtime_error("invalid decimal suffix");
  return result;
}

void trace(const char* stage) {
  if (std::getenv("CEPH_MSGR_NATIVE_MAPEXT_TRACE"))
    std::cerr << "native mapext stage: " << stage << '\n';
}
} // namespace

int main(int argc, char** argv) {
  try {
    static_assert(sizeof(size_t) >= sizeof(uint64_t));
    if (argc != 7 && argc != 8)
      throw std::runtime_error("usage: oracle CONF KEYRING POOL OBJECT OFFSET LENGTH [SNAP]");
    const auto offset = number(argv[5]), length = number(argv[6]);
    const bool snapshot = argc == 8;
    const auto snap = snapshot ? number(argv[7]) : 0;
    if (snapshot && (snap == 0 || snap >= std::numeric_limits<uint64_t>::max() - 1))
      throw std::runtime_error("invalid read snap id");
    if (!length || offset > std::numeric_limits<uint64_t>::max() - length)
      throw std::runtime_error("invalid extent map range");
    librados::Rados client;
    trace("init2");
    int code = client.init2("client.test", "ceph", 0);
    if (code >= 0) { trace("conf_read_file"); code = client.conf_read_file(argv[1]); }
    if (code >= 0) { trace("conf_set_keyring"); code = client.conf_set("keyring", argv[2]); }
    if (code >= 0) { trace("connect"); code = client.connect(); }
    if (code < 0) throw std::runtime_error("native connect " + std::to_string(code));
    librados::IoCtx io;
    trace("ioctx_create");
    code = client.ioctx_create(argv[3], io);
    if (code < 0) throw std::runtime_error("native pool " + std::to_string(code));
    if (snapshot) { trace("snap_set_read"); io.snap_set_read(snap); }
    std::map<uint64_t, uint64_t> extents;
    trace("mapext");
    code = io.mapext(argv[4], offset, static_cast<size_t>(length), extents);
    if (extents.size() > max_extents ||
        (code >= 0 && static_cast<size_t>(code) != extents.size()) ||
        (code < 0 && !extents.empty()))
      throw std::runtime_error("native extent map count/result exceeds bound");
    const auto range_end = offset + length;
    auto previous_end = offset;
    for (const auto& [off, len] : extents) {
      if (!len || off < previous_end || off > range_end || len > range_end - off)
        throw std::runtime_error("native extent map outside supplied range");
      previous_end = off + len;
    }

    // IoCtxImpl::mapext does not request an object version from Objecter.
    // The Python oracle separately observes selected-snapshot STAT2; this
    // helper does not claim a MAPEXT reply version.
    std::cout << "{\"code\":" << code << ",\"extents\":[";
    bool first = true;
    for (const auto& [off, len] : extents) {
      if (!first) std::cout << ',';
      first = false;
      std::cout << "{\"offset\":" << off << ",\"length\":" << len << '}';
    }
    std::cout << "]}\n";
    trace("close");
    io.close();
    client.shutdown();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
