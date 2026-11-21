// Development-only wrapper around unchanged Ceph Tentacle v20.2.4 headers
// and ceph-common, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// LGPL-2.1 native implementation is invoked, not copied. No Go wire bytes
// are inputs. Every vector is produced by a fresh native object through its
// canonical encoder (Message::encode for messages), and decoded again by a
// different fresh native object whose fields are checked before the vector
// is written.
//
// Vectors for the report a MGR sends its MON: MMonMgrReport populated
// (two health checks, an encoded ServiceMap, two progress events, a GID and
// a populated PGMapDigest in the data segment) and plain (nothing but an
// empty digest); three PGMapDigests encoded with CEPH_FEATURES_ALL as
// native DaemonServer::send_report does (two populated with distinct
// values, one empty), each with its MON statfs projection
// (PGMapDigest::get_statfs without a pool) and its native dump; and the
// encoded ServiceMap. Two cut digests record native refusals.
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "messages/MMonMgrReport.h"
#include "mgr/ServiceMap.h"
#include "mon/PGMap.h"
#include "msg/async/frames_v2.h"
#include "osd/OSDMap.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {
CephContext* context = nullptr;

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
void decoded(const std::filesystem::path& root, const std::string& name, const std::string& fields) {
  write(root / (name + ".decoded.json"), "{" + fields + "}\n");
}
std::string num(int64_t v) { return std::to_string(v); }
std::string unum(uint64_t v) { return std::to_string(v); }

store_statfs_t statfs(int seed) {
  store_statfs_t s;
  s.total = (uint64_t(3 + seed) << 40) + (uint64_t(seed) << 20);
  s.available = (uint64_t(1 + seed) << 40) + (uint64_t(7 + seed) << 20);
  s.internally_reserved = uint64_t(5 + seed) << 20;
  s.allocated = (int64_t(1) << 39) + seed;
  s.data_stored = (int64_t(1) << 38) + seed;
  s.data_compressed = 7 + seed;
  s.data_compressed_allocated = 8 + seed;
  s.data_compressed_original = 9 + seed;
  s.omap_allocated = 10 + seed;
  s.internal_metadata = 11 + seed;
  return s;
}

object_stat_collection_t collection(int seed) {
  object_stat_collection_t c;
  int64_t* sum = &c.sum.num_bytes;
  for (int i = 0; i < 28; ++i) sum[i] = 100000 * (seed + 1) + 10 * seed + i;
  c.sum.num_flush_mode_high = 1;
  c.sum.num_flush_mode_low = 2;
  c.sum.num_evict_mode_some = 3;
  c.sum.num_evict_mode_full = 4;
  c.sum.num_objects_pinned = 3001 + seed;
  c.sum.num_objects_missing = 3002;
  c.sum.num_legacy_snapsets = 3003;
  c.sum.num_large_omap_objects = 3004;
  c.sum.num_objects_manifest = 3005;
  c.sum.num_omap_bytes = 3006;
  c.sum.num_omap_keys = 3007;
  c.sum.num_objects_repaired = 3008 + seed;
  return c;
}

pool_stat_t pool_stat(int seed) {
  pool_stat_t p;
  p.stats = collection(seed);
  p.store_stats = statfs(seed + 10);
  p.log_size = 40 + seed;
  p.ondisk_log_size = 41 + seed;
  p.up = 3;
  p.acting = 2;
  p.num_store_stats = 1 + seed;
  return p;
}

osd_stat_t osd_stat(int seed) {
  osd_stat_t s;
  s.statfs = statfs(seed);
  s.hb_peers = {1, 2};
  s.snap_trim_queue_len = 3;
  s.num_snap_trimming = 4;
  s.num_shards_repaired = 5 + seed;
  s.op_queue_age_hist.h = {1, 2, 3};
  s.os_perf_stat.os_commit_latency_ns = 5000000;
  s.os_perf_stat.os_apply_latency_ns = 7000000;
  s.os_alerts[0].emplace("alert", "details");
  s.up_from = 12 + seed;
  s.seq = 34 + seed;
  s.num_pgs = 2;
  s.num_osds = 2;
  s.num_per_pool_osds = 2;
  s.num_per_pool_omap_osds = 2;
  osd_stat_t::Interfaces ifs{};
  ifs.last_update = 6;
  for (int i = 0; i < 3; ++i) {
    ifs.back_pingtime[i] = 1 + i; ifs.back_min[i] = 4 + i; ifs.back_max[i] = 7 + i;
    ifs.front_pingtime[i] = 11 + i; ifs.front_min[i] = 14 + i; ifs.front_max[i] = 17 + i;
  }
  ifs.back_last = 10;
  ifs.front_last = 20;
  s.hb_pingtime[1] = ifs;
  return s;
}

// A populated digest: every collection holds an entry, the pools are IDs
// a disposable fixture never creates, and seed makes each value distinct.
PGMapDigest digest(int seed) {
  PGMapDigest d;
  d.num_pg = 64 + seed;
  d.num_pg_active = 60 + seed;
  d.num_pg_unknown = 1 + seed;
  d.num_osd = 2;
  d.pg_pool_sum[9001] = pool_stat(seed);
  d.pg_sum = pool_stat(seed + 1);
  d.osd_sum = osd_stat(seed);
  d.num_pg_by_state[PG_STATE_ACTIVE | PG_STATE_CLEAN] = 60 + seed;
  d.num_pg_by_osd[0].acting = 32;
  d.num_pg_by_osd[0].up_not_acting = 1;
  d.num_pg_by_osd[0].primary = 16 + seed;
  d.num_pg_by_pool[9001] = 64 + seed;
  d.osd_last_seq.push_back(77 + seed);
  d.osd_last_seq.push_back(78 + seed);
  d.per_pool_sum_delta[9001] = std::make_pair(pool_stat(seed + 2), utime_t(1700000000 + seed, 5));
  d.per_pool_sum_deltas_stamps[9001] = utime_t(1700000001 + seed, 6);
  d.pg_sum_delta = pool_stat(seed + 3);
  d.stamp_delta = utime_t(2 + seed, 500000000);
  d.avail_space_by_rule[0] = (int64_t(1) << 40) + seed;
  d.purged_snaps[9001].insert(snapid_t(3), 2 + seed);
  d.osd_sum_by_class["hdd"] = osd_stat(seed + 4);
  d.pool_pg_unavailable_map[9001] = {pg_t(5 + seed, 9001)};
  return d;
}

std::string stat_json(const PGMapDigest& d) {
  OSDMap osdmap;
  ceph_statfs s = d.get_statfs(osdmap, std::nullopt);
  return "\"statfs\":{\"kb\":" + unum(s.kb) + ",\"kb_used\":" + unum(s.kb_used) + ",\"kb_avail\":" +
         unum(s.kb_avail) + ",\"num_objects\":" + unum(s.num_objects) + "}";
}

std::string digest_json(const PGMapDigest& d) {
  ceph::JSONFormatter f;
  f.open_object_section("digest");
  d.dump(&f);
  f.close_section();
  std::ostringstream dump;
  f.flush(dump);
  std::string fields = "\"num_pg\":" + num(d.num_pg) + ",\"num_pg_active\":" + num(d.num_pg_active) +
                       ",\"num_pg_unknown\":" + num(d.num_pg_unknown) + ",\"num_osd\":" + num(d.num_osd) +
                       ",\"pools\":" + unum(d.pg_pool_sum.size()) + ",\"osd_last_seq\":" + unum(d.osd_last_seq.size()) +
                       ",\"pg_sum_objects\":" + num(d.pg_sum.stats.sum.num_objects) +
                       ",\"pg_sum_bytes\":" + num(d.pg_sum.stats.sum.num_bytes) +
                       ",\"osd_sum_total\":" + unum(d.osd_sum.statfs.total) +
                       ",\"osd_sum_available\":" + unum(d.osd_sum.statfs.available) +
                       ",\"stamp_delta\":[" + unum(d.stamp_delta.sec()) + "," + unum(d.stamp_delta.nsec()) + "]";
  return fields + "," + stat_json(d) + ",\"dump\":" + dump.str();
}

PGMapDigest redecode_digest(const std::string& bytes) {
  ceph::buffer::list bl;
  bl.append(bytes);
  auto p = bl.cbegin();
  PGMapDigest d;
  decode(d, p);
  require(p.end(), "native digest decode left bytes");
  return d;
}

std::string emit_digest(const std::filesystem::path& root, const std::string& name, const PGMapDigest& d) {
  ceph::buffer::list bl;
  encode(d, bl, CEPH_FEATURES_ALL);
  const std::string bytes = bl.to_str();
  auto again = redecode_digest(bytes);
  require(again.num_pg == d.num_pg && again.num_osd == d.num_osd && again.pg_pool_sum.size() == d.pg_pool_sum.size() &&
          again.osd_sum.statfs.total == d.osd_sum.statfs.total &&
          again.pg_sum.stats.sum.num_objects == d.pg_sum.stats.sum.num_objects &&
          again.purged_snaps.size() == d.purged_snaps.size() && again.osd_sum_by_class.size() == d.osd_sum_by_class.size() &&
          again.pool_pg_unavailable_map.size() == d.pool_pg_unavailable_map.size(), "native digest decode differs");
  write(root / (name + ".digest.bin"), bytes);
  decoded(root, name, digest_json(again));
  std::cout << name << " digest=" << bytes.size() << "\n";
  return bytes;
}

void refused_digest(const std::filesystem::path& root, const std::string& name, const std::string& bytes) {
  bool threw = false;
  try {
    redecode_digest(bytes);
  } catch (const ceph::buffer::error&) {
    threw = true;
  }
  require(threw, "native decoder accepted a cut digest");
  write(root / (name + ".digest.bin"), bytes);
  decoded(root, name, "\"native_refused\":true");
}

std::string service_map(const std::filesystem::path& root) {
  ServiceMap m;
  m.epoch = 7;
  m.modified = utime_t(1700000100, 7);
  auto& daemon = m.services["gotest"].daemons["reporter"];
  daemon.gid = 4242;
  require(daemon.addr.parse("v2:192.0.2.10:6800/77"), "service address parse");
  daemon.start_epoch = 5;
  daemon.start_stamp = utime_t(1700000050, 9);
  daemon.metadata["hostname"] = "fixture";
  daemon.task_status["state"] = "running";
  m.services["gotest"].summary = "one reporter";
  ceph::buffer::list bl;
  encode(m, bl, CEPH_FEATURES_ALL);
  ServiceMap again;
  auto p = bl.cbegin();
  decode(again, p);
  require(p.end() && again.epoch == 7 && again.services.at("gotest").daemons.at("reporter").gid == 4242, "native service map decode differs");
  write(root / "service-map.servicemap.bin", bl.to_str());
  decoded(root, "service-map", "\"epoch\":7,\"services\":1");
  return bl.to_str();
}

std::string emit(const std::filesystem::path& root, const std::string& name, Message* message) {
  require(message->empty_payload(), "producer must start with an empty payload");
  message->set_seq(1);
  message->set_tid(0);
  message->set_priority(CEPH_MSG_PRIO_DEFAULT);
  message->encode(CEPH_FEATURES_ALL, MSG_CRC_ALL, true);
  const auto& h = message->get_header();
  const auto& footer = message->get_footer();
  require(h.type == MSG_MON_MGR_REPORT && h.version == 3 && h.compat_version == 1 && h.tid == 0 &&
          h.middle_len == 0 && h.data_len != 0, "native envelope differs");
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
  std::cout << name << " front=" << uint32_t(h.front_len) << " data=" << uint32_t(h.data_len) << "\n";
  return message->get_payload().to_str();
}

boost::intrusive_ptr<MMonMgrReport> redecode(const Message& source, const std::string& front) {
  auto d = ceph::make_message<MMonMgrReport>();
  d->set_header(source.get_header());
  ceph::buffer::list copy;
  copy.append(front);
  d->set_payload(copy);
  d->set_data(source.get_data());
  d->decode_payload();
  return d;
}

void reports(const std::filesystem::path& root, const std::string& populated, const std::string& empty,
             const std::string& services) {
  auto full = ceph::make_message<MMonMgrReport>();
  full->gid = 4242;
  auto& warn = full->health_checks.add("GOTEST_REPORT_WARN", HEALTH_WARN, "gotest warning summary", 3);
  warn.detail.push_back("first detail");
  warn.detail.push_back("second detail \xe2\x9c\x93");
  full->health_checks.add("GOTEST_REPORT_ERR", HEALTH_ERR, "gotest error summary", 0);
  full->service_map_bl.append(services);
  full->progress_events["event-b"] = ProgressEvent{"rebalancing", 0.25f, true};
  full->progress_events["event-a"] = ProgressEvent{"", 1.0f, false};
  ceph::buffer::list data;
  data.append(populated);
  full->set_data(data);
  auto front = emit(root, "report-populated", full.get());
  auto d = redecode(*full, front);
  require(d->gid == 4242 && d->health_checks.checks.size() == 2 &&
          d->health_checks.checks.at("GOTEST_REPORT_WARN").detail.size() == 2 &&
          d->health_checks.checks.at("GOTEST_REPORT_WARN").count == 3 &&
          d->health_checks.checks.at("GOTEST_REPORT_ERR").severity == HEALTH_ERR &&
          d->service_map_bl.to_str() == services && d->progress_events.size() == 2 &&
          d->progress_events.at("event-b").progress == 0.25f && d->progress_events.at("event-b").add_to_ceph_s &&
          d->get_data().to_str() == populated, "native report decode differs");
  decoded(root, "report-populated", "\"gid\":4242,\"checks\":2,\"progress_events\":2");

  auto plain = ceph::make_message<MMonMgrReport>();
  ceph::buffer::list none;
  none.append(empty);
  plain->set_data(none);
  front = emit(root, "report-plain", plain.get());
  d = redecode(*plain, front);
  require(d->gid == 0 && d->health_checks.checks.empty() && d->service_map_bl.length() == 0 &&
          d->progress_events.empty() && d->get_data().to_str() == empty, "native plain report decode differs");
  decoded(root, "report-plain", "\"gid\":0,\"checks\":0,\"progress_events\":0");
}
}

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: mgr_report_native_encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    CephInitParameters parameters(CEPH_ENTITY_TYPE_CLIENT);
    context = common_preinit(parameters, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    context->_conf.apply_changes(nullptr);
    common_init_finish(context);
    auto a = emit_digest(root, "digest-a", digest(0));
    emit_digest(root, "digest-b", digest(1));
    auto empty = emit_digest(root, "digest-empty", PGMapDigest());
    // The envelope claims more bytes than follow.
    refused_digest(root, "digest-truncated", a.substr(0, a.size() - 1));
    // A consistent envelope one byte shorter: the last field is cut inside it.
    std::string cut = a.substr(0, a.size() - 1);
    uint32_t length = uint32_t(cut.size() - 6);
    cut[2] = char(length);
    cut[3] = char(length >> 8);
    cut[4] = char(length >> 16);
    cut[5] = char(length >> 24);
    refused_digest(root, "digest-short-struct", cut);
    auto services = service_map(root);
    reports(root, a, empty, services);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
