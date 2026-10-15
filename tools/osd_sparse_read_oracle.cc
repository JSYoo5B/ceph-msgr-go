// Test-only native oracle. Ceph v20.2.4 public LGPL-2.1 librados SDK API;
// no implementation copied. Caller supplies owned fixture identity/range.
#include "include/rados/librados.hpp"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>

uint64_t number(const char* text) {
 std::string value(text);
 if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
  throw std::runtime_error("invalid unsigned decimal");
 size_t used = 0;
 auto result = std::stoull(value, &used);
 if (used != value.size()) throw std::runtime_error("invalid decimal suffix");
 return result;
}
void trace(const char* stage) {
 if (std::getenv("CEPH_MSGR_NATIVE_SPARSE_READ_TRACE"))
  std::cerr << "native sparse-read stage: " << stage << '\n';
}
int main(int argc, char** argv) {
 try {
  if (argc != 7) throw std::runtime_error("usage: oracle CONF KEYRING POOL OBJECT OFFSET LENGTH");
  const auto offset = number(argv[5]), length = number(argv[6]);
  if (!length || length > (1U << 20) || offset > std::numeric_limits<uint64_t>::max() - length)
   throw std::runtime_error("invalid bounded range");
  librados::Rados client;
  trace("init2");
  int r = client.init2("client.test", "ceph", 0);
  if (r >= 0) { trace("conf_read_file"); r = client.conf_read_file(argv[1]); }
  if (r >= 0) { trace("conf_set_keyring"); r = client.conf_set("keyring", argv[2]); }
  if (r >= 0) { trace("connect"); r = client.connect(); }
  if (r < 0) throw std::runtime_error("native connect " + std::to_string(r));
  librados::IoCtx io;
  trace("ioctx_create");
  r = client.ioctx_create(argv[3], io);
  if (r < 0) throw std::runtime_error("native pool " + std::to_string(r));
  std::map<uint64_t,uint64_t> extents;
  librados::bufferlist data;
  trace("sparse_read");
  r = io.sparse_read(argv[4], extents, data, length, offset);
  std::cout << "{\"code\":" << r << ",\"extents\":[";
  bool first = true;
  for (auto [off, len] : extents) {
   if (!first) std::cout << ',';
   first = false;
   std::cout << "{\"offset\":" << off << ",\"length\":" << len << '}';
  }
  std::cout << "],\"data_hex\":\"";
  const char* digits = "0123456789abcdef";
  for (unsigned char b : data.to_str()) std::cout << digits[b >> 4] << digits[b & 15];
  std::cout << "\",\"version\":" << io.get_last_version() << "}\n";
  trace("close");
  io.close();
  client.shutdown();
  return 0;
 } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
