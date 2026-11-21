// Development-only wrapper around unchanged Ceph Tentacle v20.2.4 headers
// and ceph-common, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// LGPL-2.1 native implementation is invoked, not copied. No Go wire bytes
// are inputs. Every vector is produced by a fresh native message through the
// canonical Message::encode, and decoded again by a different fresh native
// object whose fields are checked before the vector is written.
//
// Vectors: MMgrBeacon (standby, populated active), MMgrMap (populated,
// empty, MON null map, pre-Nautilus feature variant), MMgrDigest (populated,
// empty), MAuth carrying CEPHX_GET_ROTATING_KEY, MAuthReply carrying an
// encrypted RotatingSecrets for an aes256k and an aes principal, and a
// refused MAuthReply. Two hand-cut beacon fronts record native refusal.
// All keys are synthetic constants; none belongs to a cluster.
#include "auth/Auth.h"
#include "auth/Crypto.h"
#include "auth/cephx/CephxProtocol.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "include/stringify.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMgrBeacon.h"
#include "messages/MMgrDigest.h"
#include "messages/MMgrMap.h"
#include "mon/MonCommand.h"
#include "msg/async/frames_v2.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
using Map = std::map<std::string, std::string>;
CephContext* context = nullptr;
const std::string binary("\0\xff", 2);
const char* fsid_text = "00112233-4455-6677-8899-aabbccddeeff";

void require(bool condition, const char* reason) {
  if (!condition) throw std::runtime_error(reason);
}
std::string hex(const std::string& input) {
  const char* digits = "0123456789abcdef";
  std::string out;
  for (unsigned char b : input) { out += digits[b >> 4]; out += digits[b & 15]; }
  return out;
}
void write(const std::filesystem::path& path, const std::string& value) {
  std::ofstream out(path, std::ios::binary);
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out.write(value.data(), value.size());
}
uuid_d fsid() {
  uuid_d u;
  require(u.parse(fsid_text), "fsid parse");
  return u;
}
entity_addrvec_t addrs(const char* text) {
  entity_addrvec_t v;
  require(v.parse(text), "addrvec parse");
  return v;
}
std::string addrvec_json(const entity_addrvec_t& v) {
  std::string out = "[";
  for (const auto& a : v.v) {
    if (out.size() > 1) out += ",";
    out += "\"" + stringify(a) + "\"";
  }
  return out + "]";
}
std::string map_json(const Map& input) {
  std::string out = "{";
  for (const auto& [key, value] : input) {
    if (out.size() > 1) out += ",";
    out += "\"" + hex(key) + "\":\"" + hex(value) + "\"";
  }
  return out + "}";
}

// The fixed populated module shapes: a module without options and one
// with every ModuleOption collection populated, in a deliberately unsorted
// vector order so the legacy sorted name set differs from it.
std::vector<MgrMap::ModuleInfo> modules() {
  MgrMap::ModuleOption option;
  option.name = "opt"; option.type = 4; option.level = 2; option.flags = 1;
  option.default_value = "on"; option.min = "0"; option.max = "9";
  option.enum_allowed = {"on", "off"}; option.desc = "d"; option.long_desc = "long";
  option.tags = {"t2", "t1"}; option.see_also = {"x"};
  MgrMap::ModuleInfo zeta;
  zeta.name = "zeta"; zeta.can_run = true;
  MgrMap::ModuleInfo alpha;
  alpha.name = "alpha"; alpha.can_run = false; alpha.error_string = "missing dep";
  alpha.module_options["opt"] = option;
  return {zeta, alpha};
}
std::vector<MonCommand> commands() {
  MonCommand first;
  first.cmdstring = "gotest status name=x,type=CephString,req=false";
  first.helpstring = "show"; first.module = "mgr"; first.req_perms = "r";
  first.flags = MonCommand::FLAG_MGR | MonCommand::FLAG_POLL;
  MonCommand second;
  second.cmdstring = "gotest reset"; second.helpstring = ""; second.module = "mgr";
  second.req_perms = "rw"; second.flags = 0;
  return {first, second};
}

std::string emit(const std::filesystem::path& root, const std::string& name,
                 Message* message, uint64_t features, unsigned type,
                 unsigned version, unsigned compat) {
  require(message->empty_payload(), "producer must start with an empty payload");
  message->set_seq(1);
  message->set_priority(CEPH_MSG_PRIO_DEFAULT);
  message->encode(features, MSG_CRC_ALL, true);
  const auto& h = message->get_header();
  const auto& footer = message->get_footer();
  require(h.type == type && h.version == version && h.compat_version == compat &&
          h.tid == 0 && h.middle_len == 0 && h.data_len == 0,
          "native envelope differs");
  ceph_msg_header2 h2{};
  h2.seq = h.seq; h2.tid = h.tid; h2.type = h.type;
  h2.priority = h.priority; h2.version = h.version; h2.data_off = h.data_off;
  h2.ack_seq = 3; h2.flags = footer.flags;
  h2.compat_version = h.compat_version; h2.reserved = h.reserved;
  auto frame = ceph::msgr::v2::MessageFrame::Encode(
      h2, message->get_payload(), message->get_middle(), message->get_data());
  write(root / (name + ".header.bin"),
        std::string(reinterpret_cast<const char*>(&frame.header()), sizeof(frame.header())));
  write(root / (name + ".front.bin"), message->get_payload().to_str());
  write(root / (name + ".middle.bin"), message->get_middle().to_str());
  write(root / (name + ".data.bin"), message->get_data().to_str());
  std::cout << name << " type=" << type << " v=" << version << " compat=" << compat
            << " front=" << uint32_t(h.front_len) << "\n";
  return message->get_payload().to_str();
}

template<class T>
boost::intrusive_ptr<T> redecode(const Message& source, const std::string& front) {
  auto decoded = ceph::make_message<T>();
  decoded->set_header(source.get_header());
  ceph::buffer::list copy;
  copy.append(front);
  decoded->set_payload(copy);
  decoded->decode_payload();
  return decoded;
}

void decoded(const std::filesystem::path& root, const std::string& name, const std::string& fields) {
  write(root / (name + ".decoded.json"), "{" + fields + "}\n");
}

std::string beacon_json(MMgrBeacon& d) {
  std::string out = "\"gid\":" + std::to_string(d.get_gid()) + ",\"name\":\"" + d.get_name() +
                    "\",\"fsid\":\"" + stringify(d.get_fsid()) + "\",\"available\":" +
                    (d.get_available() ? "true" : "false") + ",\"server_addrs\":" +
                    addrvec_json(d.get_server_addrs()) + ",\"modules\":[";
  bool first = true;
  for (const auto& m : d.get_available_modules()) {
    out += std::string(first ? "" : ",") + "\"" + m.name + "\"";
    first = false;
  }
  out += "],\"commands\":[";
  first = true;
  for (const auto& c : d.get_command_descs()) {
    out += std::string(first ? "" : ",") + "{\"cmd\":\"" + hex(c.cmdstring) + "\",\"flags\":" + std::to_string(c.flags) + "}";
    first = false;
  }
  out += "],\"metadata_hex\":" + map_json(d.get_metadata()) + ",\"services_hex\":" +
         map_json(d.get_services()) + ",\"mgr_features\":" + std::to_string(d.get_mgr_features()) +
         ",\"clients\":[";
  first = true;
  for (const auto& [client, a] : d.get_clients()) {
    out += std::string(first ? "" : ",") + "{\"name\":\"" + client + "\",\"addrs\":" + addrvec_json(a) + "}";
    first = false;
  }
  return out + "]";
}

void beacons(const std::filesystem::path& root) {
  auto standby = ceph::make_message<MMgrBeacon>(
      fsid(), 4242, "x", entity_addrvec_t(), false, std::vector<MgrMap::ModuleInfo>{}, Map{},
      std::multimap<std::string, entity_addrvec_t>{}, 0);
  auto front = emit(root, "beacon-standby", standby.get(), CEPH_FEATURES_ALL, MSG_MGR_BEACON, 11, 8);
  auto d = redecode<MMgrBeacon>(*standby, front);
  require(d->get_gid() == 4242 && d->get_name() == "x" && !d->get_available() &&
          d->get_server_addrs().empty() && d->get_available_modules().empty() &&
          d->get_metadata().empty() && d->get_services().empty() && d->get_clients().empty() &&
          d->get_mgr_features() == 0 && d->get_fsid() == fsid(), "native standby beacon decode differs");
  decoded(root, "beacon-standby", beacon_json(*d));

  // Without clients, the two trailing vectors are empty: front minus the
  // last eight bytes is the prefix before clients_addrs.
  std::string prefix = front.substr(0, front.size() - 8);

  std::multimap<std::string, entity_addrvec_t> clients;
  clients.emplace("rbd_support", entity_addrvec_t());
  clients.emplace("libcephsqlite", addrs("v2:10.0.0.6:0/77"));
  clients.emplace("libcephsqlite", addrs("v2:10.0.0.6:0/78"));
  Map metadata{{"addr", "10.0.0.5"}, {"binary", binary}};
  auto active = ceph::make_message<MMgrBeacon>(
      fsid(), 4242, "x", addrs("[v2:10.0.0.5:6800/1234,v1:10.0.0.5:6801/1234,v2:[fd00::5]:6802/1234]"),
      true, modules(), Map(metadata), std::move(clients), 0x3f);
  active->set_command_descs(commands());
  active->set_services(Map{{"dashboard", "https://10.0.0.5:8443/"}});
  front = emit(root, "beacon-active", active.get(), CEPH_FEATURES_ALL, MSG_MGR_BEACON, 11, 8);
  d = redecode<MMgrBeacon>(*active, front);
  require(d->get_gid() == 4242 && d->get_available() && d->get_server_addrs().size() == 3 &&
          d->get_available_modules() == modules() && d->get_metadata() == metadata &&
          d->get_command_descs().size() == 2 && d->get_clients().size() == 3 &&
          d->get_mgr_features() == 0x3f, "native active beacon decode differs");
  decoded(root, "beacon-active", beacon_json(*d));

  // Native refusals: one byte short, and a clients name count that differs
  // from the address count (MMgrBeacon::decode_payload malformed_input).
  ceph::buffer::list addrs_bl, names_bl;
  std::vector<entity_addrvec_t> client_addrs{addrs("v2:10.0.0.6:0/77"), addrs("v2:10.0.0.6:0/78")};
  std::vector<std::string> client_names{"libcephsqlite"};
  ceph::encode(client_addrs, addrs_bl, CEPH_FEATURES_ALL);
  ceph::encode(client_names, names_bl);
  std::string mismatch = prefix + addrs_bl.to_str() + names_bl.to_str();
  const std::vector<std::pair<std::string, std::string>> refusals{
      {"beacon-truncated", front.substr(0, front.size() - 1)}, {"beacon-count-mismatch", mismatch}};
  for (const auto& [label, bytes] : refusals) {
    bool refused = false;
    try {
      redecode<MMgrBeacon>(*active, bytes);
    } catch (const ceph::buffer::error&) {
      refused = true;
    }
    require(refused, "native beacon decoder accepted a malformed front");
    write(root / (label + ".front.bin"), bytes);
    decoded(root, label, "\"native_refused\":true");
  }
}

MgrMap populated_map() {
  MgrMap map;
  map.epoch = 12;
  map.active_addrs = addrs("[v2:10.0.0.5:6800/1234,v1:10.0.0.5:6801/1234]");
  map.active_gid = 4242;
  map.available = true;
  map.active_name = "x";
  map.standbys[4300] = MgrMap::StandbyInfo(4300, "y", {modules()[0]}, 0x3f);
  map.standbys[4400] = MgrMap::StandbyInfo(4400, "z", {}, 0);
  map.modules = {"pg_autoscaler", "balancer"};
  map.services = {{"dashboard", "https://10.0.0.5:8443/"}};
  map.available_modules = modules();
  map.active_change = utime_t(1700000000, 5);
  map.always_on_modules = {{20, {"crash", "balancer"}}};
  map.active_mgr_features = 0x3f;
  map.last_failure_osd_epoch = 9;
  map.clients.emplace("libcephsqlite", addrs("v2:10.0.0.6:0/77"));
  map.flags = MgrMap::FLAG_DOWN;
  map.force_disabled_modules = {"telemetry"};
  return map;
}

std::string map_fields(const MgrMap& m) {
  std::string out = "\"epoch\":" + std::to_string(m.epoch) + ",\"active_gid\":" + std::to_string(m.active_gid) +
                    ",\"available\":" + (m.available ? "true" : "false") + ",\"active_name\":\"" + m.active_name +
                    "\",\"active_addrs\":" + addrvec_json(m.active_addrs) + ",\"standbys\":[";
  bool first = true;
  for (const auto& [gid, s] : m.standbys) {
    out += std::string(first ? "" : ",") + "{\"gid\":" + std::to_string(s.gid) + ",\"name\":\"" + s.name +
           "\",\"modules\":" + std::to_string(s.available_modules.size()) + ",\"features\":" +
           std::to_string(s.mgr_features) + "}";
    first = false;
  }
  out += "],\"modules\":" + std::to_string(m.modules.size()) + ",\"available_modules\":" +
         std::to_string(m.available_modules.size()) + ",\"active_change\":[" +
         std::to_string(m.active_change.sec()) + "," + std::to_string(m.active_change.nsec()) +
         "],\"active_mgr_features\":" + std::to_string(m.active_mgr_features) +
         ",\"last_failure_osd_epoch\":" + std::to_string(m.last_failure_osd_epoch) +
         ",\"clients\":" + std::to_string(m.clients.size()) + ",\"flags\":" + std::to_string(m.flags) +
         ",\"force_disabled\":" + std::to_string(m.force_disabled_modules.size());
  return out;
}

void maps(const std::filesystem::path& root) {
  struct Case { const char* name; MgrMap map; uint64_t features; };
  MgrMap empty;
  empty.epoch = 1;
  const std::vector<Case> cases{{"mgrmap-populated", populated_map(), CEPH_FEATURES_ALL},
                                {"mgrmap-empty", empty, CEPH_FEATURES_ALL},
                                {"mgrmap-null", MgrMap::create_null_mgrmap(), CEPH_FEATURES_ALL},
                                {"mgrmap-legacy", populated_map(), 0}};
  for (const auto& c : cases) {
    auto message = ceph::make_message<MMgrMap>(c.map);
    auto front = emit(root, c.name, message.get(), c.features, MSG_MGR_MAP, 1, 1);
    auto d = redecode<MMgrMap>(*message, front);
    const MgrMap& got = d->get_map();
    if (c.features) {
      require(map_fields(got) == map_fields(c.map) && got.services == c.map.services &&
              got.available_modules == c.map.available_modules && got.modules == c.map.modules &&
              got.always_on_modules == c.map.always_on_modules &&
              got.force_disabled_modules == c.map.force_disabled_modules,
              "native MgrMap decode differs");
    } else {
      // The pre-Nautilus shape keeps only the legacy address and v5 fields.
      require(got.epoch == c.map.epoch && got.active_gid == c.map.active_gid &&
              got.active_mgr_features == 0 && got.flags == 0, "native legacy MgrMap decode differs");
    }
    decoded(root, c.name, map_fields(got));
  }
}

void digests(const std::filesystem::path& root) {
  const std::vector<std::tuple<std::string, std::string, std::string>> cases{
      {"digest-populated", "{\"epoch\":3}", "{\"status\":\"HEALTH_OK\"}"}, {"digest-empty", "", ""}};
  for (const auto& [name, status, health] : cases) {
    auto message = ceph::make_message<MMgrDigest>();
    message->mon_status_json.append(status);
    message->health_json.append(health);
    auto front = emit(root, name, message.get(), CEPH_FEATURES_ALL, MSG_MGR_DIGEST, 1, 1);
    auto d = redecode<MMgrDigest>(*message, front);
    require(d->mon_status_json.to_str() == status && d->health_json.to_str() == health,
            "native digest decode differs");
    decoded(root, name, "\"mon_status_hex\":\"" + hex(status) + "\",\"health_hex\":\"" + hex(health) + "\"");
  }
}

CryptoKey key(int type, unsigned char fill, unsigned length) {
  std::string secret;
  for (unsigned i = 0; i < length; ++i) secret.push_back(char(fill + i));
  ceph::buffer::ptr p(secret.data(), secret.size());
  return CryptoKey(type, utime_t(1600000000, 0), p);
}

// The CryptoKey wire encoding (type, created, length, secret); the secret
// accessor itself is private.
std::string secret_text(const CryptoKey& k) {
  ceph::buffer::list bl;
  k.encode(bl);
  return bl.to_str();
}

void auth_vectors(const std::filesystem::path& root) {
  auto request = ceph::make_message<MAuth>();
  request->protocol = CEPH_AUTH_CEPHX;
  CephXRequestHeader header;
  header.request_type = CEPHX_GET_ROTATING_KEY;
  header.encode(request->auth_payload);
  request->monmap_epoch = 3;
  auto front = emit(root, "auth-rotating-request", request.get(), CEPH_FEATURES_ALL, CEPH_MSG_AUTH, 1, 1);
  auto d = redecode<MAuth>(*request, front);
  require(d->protocol == CEPH_AUTH_CEPHX && d->monmap_epoch == 3 &&
          d->auth_payload.to_str() == request->auth_payload.to_str(), "native MAuth decode differs");
  decoded(root, "auth-rotating-request", "\"protocol\":2,\"monmap_epoch\":3,\"payload_hex\":\"" +
          hex(d->auth_payload.to_str()) + "\"");

  const std::vector<std::tuple<std::string, int, unsigned>> principals{
      {"auth-rotating-reply", CEPH_CRYPTO_AES256KRB5, 32}, {"auth-rotating-reply-aes", CEPH_CRYPTO_AES, 16}};
  for (const auto& [name, principal_type, principal_length] : principals) {
    RotatingSecrets secrets;
    for (unsigned i = 0; i < 3; ++i) {
      ExpiringCryptoKey ek;
      ek.key = key(CEPH_CRYPTO_AES256KRB5, 0x40 + 0x10 * i, 32);
      ek.expiration = utime_t(1700000100 + 12 * i, 250000000);
      secrets.secrets[5 + i] = ek;
    }
    secrets.max_ver = 7;
    CryptoKey principal = key(principal_type, 0x20, principal_length);
    ceph::buffer::list result;
    CephXResponseHeader response;
    response.request_type = CEPHX_GET_ROTATING_KEY;
    response.status = 0;
    response.encode(result);
    std::string error;
    require(encode_encrypt(context, secrets, principal, CEPHX_KEY_USAGE_ROTATING_SECRET, result, error) == 0 &&
            error.empty(), "native rotating encryption failed");
    auto reply = ceph::make_message<MAuthReply>(CEPH_AUTH_CEPHX, &result, 0, 4242, "");
    front = emit(root, name, reply.get(), CEPH_FEATURES_ALL, CEPH_MSG_AUTH_REPLY, 1, 1);
    auto r = redecode<MAuthReply>(*reply, front);
    auto p = r->result_bl.cbegin();
    CephXResponseHeader got_header;
    got_header.decode(p);
    RotatingSecrets got;
    require(decode_decrypt(context, got, principal, CEPHX_KEY_USAGE_ROTATING_SECRET, p, error) == 0 &&
            p.end() && got.max_ver == 7 && got.secrets.size() == 3 &&
            got_header.request_type == CEPHX_GET_ROTATING_KEY && r->global_id == 4242,
            "native rotating decode differs");
    std::string ids = "[";
    for (const auto& [id, ek] : got.secrets) {
      ids += std::string(ids.size() > 1 ? "," : "") + "{\"id\":" + std::to_string(id) + ",\"type\":" +
             std::to_string(ek.key.get_type()) + ",\"key_hex\":\"" + hex(secret_text(ek.key)) +
             "\",\"expires\":[" + std::to_string(ek.expiration.sec()) + "," +
             std::to_string(ek.expiration.nsec()) + "]}";
    }
    decoded(root, name, "\"global_id\":4242,\"result\":0,\"max_ver\":7,\"principal_type\":" +
            std::to_string(principal_type) + ",\"principal_key_hex\":\"" +
            hex(secret_text(principal)) + "\",\"keys\":" + ids + "]");
  }

  auto refused = ceph::make_message<MAuthReply>(CEPH_AUTH_CEPHX, nullptr, -EACCES, 0, "denied");
  front = emit(root, "auth-reply-refused", refused.get(), CEPH_FEATURES_ALL, CEPH_MSG_AUTH_REPLY, 1, 1);
  auto r = redecode<MAuthReply>(*refused, front);
  require(r->result == -EACCES && r->result_msg == "denied" && r->result_bl.length() == 0,
          "native refused reply decode differs");
  decoded(root, "auth-reply-refused", "\"result\":-13,\"message\":\"denied\"");
}
}

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: mgr_daemon_native_encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    CephInitParameters parameters(CEPH_ENTITY_TYPE_CLIENT);
    context = common_preinit(parameters, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    context->_conf.apply_changes(nullptr);
    common_init_finish(context);
    beacons(root);
    maps(root);
    digests(root);
    auth_vectors(root);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
