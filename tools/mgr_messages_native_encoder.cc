// Development-only wrapper around unchanged Ceph Tentacle v20.2.4 headers
// and ceph-common, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// LGPL-2.1 native implementation is invoked, not copied. No Go wire bytes
// are inputs. Every vector is produced by a fresh native message through the
// canonical Message::encode, and decoded again by a different fresh native
// object whose fields are checked before the vector is written.
//
// Vectors for the MGR daemon messages (what a MGR receives and sends):
// MMgrOpen (OSD, service daemon), MMgrUpdate (with and without metadata),
// MMgrClose, MMgrReport (a declaring report, the next report that withdraws
// one counter, keeps an average without redeclaring it and declares a
// third, a populated report with every optional field, health metrics, an
// OSD perf-metric report and an OSD metric payload, and an MDS metric
// payload), MPGStats (encoded with every feature, and with the MGR
// acceptor's negotiated mask, which lacks OS_PERF_STAT_NS), MMgrCommand and
// MCommand with bulk input, their replies with signed results and bulk
// output, and MMgrConfigure (plain, with an OSD perf-metric query, and with
// an MDS metric configuration with and without SERVER_PACIFIC). Two
// truncated fronts record native refusals. MPGStats is also dumped by its
// own native formatter for an independent field comparison.
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MMgrClose.h"
#include "messages/MMgrCommand.h"
#include "messages/MMgrCommandReply.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrUpdate.h"
#include "messages/MPGStats.h"
#include "msg/async/frames_v2.h"
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {
using Map = std::map<std::string, std::string>;
CephContext* context = nullptr;
const std::string binary("\0\xff", 2);
const char* fsid_text = "00112233-4455-6677-8899-aabbccddeeff";
// The MGR acceptor's negotiated mask with a stock OSD (Go
// session.MgrOSDPeerFeatures).
const uint64_t acceptor_features = 0x2a00040010a0a237ULL;

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
std::string all_bytes() {
  std::string out;
  for (int i = 0; i < 256; ++i) out += char(i);
  return out;
}
std::string encoded_hex(const ceph::buffer::list& bl) { return hex(bl.to_str()); }
ceph::buffer::list bytes(const std::string& value) {
  ceph::buffer::list bl;
  bl.append(value);
  return bl;
}

std::string emit(const std::filesystem::path& root, const std::string& name,
                 Message* message, uint64_t features, unsigned type,
                 unsigned version, unsigned compat, uint64_t tid = 0) {
  require(message->empty_payload(), "producer must start with an empty payload");
  message->set_seq(1);
  message->set_tid(tid);
  message->set_priority(CEPH_MSG_PRIO_DEFAULT);
  message->encode(features, MSG_CRC_ALL, true);
  const auto& h = message->get_header();
  const auto& footer = message->get_footer();
  require(h.type == type && h.version == version && h.compat_version == compat &&
          h.tid == tid && h.middle_len == 0, "native envelope differs");
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
            << " front=" << uint32_t(h.front_len) << " data=" << uint32_t(h.data_len) << "\n";
  return message->get_payload().to_str();
}

template<class T>
boost::intrusive_ptr<T> redecode(const Message& source, const std::string& front) {
  auto decoded = ceph::make_message<T>();
  decoded->set_header(source.get_header());
  ceph::buffer::list copy;
  copy.append(front);
  decoded->set_payload(copy);
  decoded->set_data(source.get_data());
  decoded->decode_payload();
  return decoded;
}

template<class T>
void refused(const std::filesystem::path& root, const std::string& name, const Message& source, const std::string& front) {
  bool threw = false;
  try {
    redecode<T>(source, front);
  } catch (const ceph::buffer::error&) {
    threw = true;
  }
  require(threw, "native decoder accepted a truncated front");
  write(root / (name + ".front.bin"), front);
  write(root / (name + ".decoded.json"), "{\"native_refused\":true}\n");
}

void decoded(const std::filesystem::path& root, const std::string& name, const std::string& fields) {
  write(root / (name + ".decoded.json"), "{" + fields + "}\n");
}

void opens(const std::filesystem::path& root) {
  auto osd = ceph::make_message<MMgrOpen>();
  osd->daemon_name = "0";
  osd->config_bl.append("config\0bl", 9);
  osd->config_defaults_bl.append(binary);
  auto front = emit(root, "open-osd", osd.get(), CEPH_FEATURES_ALL, MSG_MGR_OPEN, 3, 1);
  auto d = redecode<MMgrOpen>(*osd, front);
  require(d->daemon_name == "0" && d->service_name.empty() && !d->service_daemon &&
          d->daemon_metadata.empty() && d->config_bl.to_str() == std::string("config\0bl", 9) &&
          d->config_defaults_bl.to_str() == binary, "native OSD open decode differs");
  decoded(root, "open-osd", "\"daemon\":\"0\"");

  auto service = ceph::make_message<MMgrOpen>();
  service->daemon_name = "d1";
  service->service_name = "ceph-msgr";
  service->service_daemon = true;
  service->daemon_metadata = Map{{"b", binary}, {"a", "1"}};
  service->daemon_status = Map{{"s", "ok"}};
  front = emit(root, "open-service", service.get(), CEPH_FEATURES_ALL, MSG_MGR_OPEN, 3, 1);
  d = redecode<MMgrOpen>(*service, front);
  require(d->service_daemon && d->daemon_metadata.size() == 2 && d->daemon_status.at("s") == "ok" &&
          d->config_bl.length() == 0, "native service open decode differs");
  decoded(root, "open-service", "\"daemon\":\"d1\",\"service\":\"ceph-msgr\"");
}

void updates(const std::filesystem::path& root) {
  auto with = ceph::make_message<MMgrUpdate>();
  with->daemon_name = "a";
  with->service_name = "mon";
  with->need_metadata_update = true;
  with->daemon_metadata = Map{{"ceph_version_short", "20.2.4"}, {"k", binary}};
  with->daemon_status = Map{{"x", "y"}};
  auto front = emit(root, "update-metadata", with.get(), CEPH_FEATURES_ALL, MSG_MGR_UPDATE, 2, 1);
  auto d = redecode<MMgrUpdate>(*with, front);
  require(d->need_metadata_update && d->daemon_metadata.size() == 2 && d->daemon_status.size() == 1,
          "native update decode differs");
  decoded(root, "update-metadata", "\"daemon\":\"a\"");

  auto plain = ceph::make_message<MMgrUpdate>();
  plain->daemon_name = "a";
  plain->service_name = "mon";
  plain->daemon_metadata = Map{{"ignored", "x"}};
  front = emit(root, "update-plain", plain.get(), CEPH_FEATURES_ALL, MSG_MGR_UPDATE, 2, 1);
  d = redecode<MMgrUpdate>(*plain, front);
  require(!d->need_metadata_update && d->daemon_metadata.empty(), "native plain update decode differs");
  decoded(root, "update-plain", "\"daemon\":\"a\"");
}

void closes(const std::filesystem::path& root) {
  auto close = ceph::make_message<MMgrClose>();
  close->daemon_name = "d1";
  close->service_name = "ceph-msgr";
  auto front = emit(root, "close", close.get(), CEPH_FEATURES_ALL, MSG_MGR_CLOSE, 1, 1);
  auto d = redecode<MMgrClose>(*close, front);
  require(d->daemon_name == "d1" && d->service_name == "ceph-msgr", "native close decode differs");
  decoded(root, "close", "\"daemon\":\"d1\",\"service\":\"ceph-msgr\"");
}

PerfCounterType counter(const char* path, perfcounter_type_d type, uint8_t priority, unit_t unit) {
  PerfCounterType t;
  t.path = path;
  t.description = std::string("desc ") + path;
  t.nick = std::string("n") + path;
  t.type = type;
  t.priority = priority;
  t.unit = unit;
  return t;
}

OSDPerfMetricQuery osd_query() {
  OSDPerfMetricQuery q;
  q.key_descriptor.push_back(OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType::CLIENT_ID, "^(.*)$"));
  q.performance_counter_descriptors.push_back(PerformanceCounterDescriptor(PerformanceCounterType::OPS));
  q.performance_counter_descriptors.push_back(PerformanceCounterDescriptor(PerformanceCounterType::BYTES));
  return q;
}

OSDPerfMetricReport osd_report() {
  OSDPerfMetricReport r;
  r.performance_counter_descriptors.push_back(PerformanceCounterDescriptor(PerformanceCounterType::OPS));
  ceph::buffer::list packed;
  encode(uint64_t(17), packed);
  r.group_packed_performance_counters[OSDPerfMetricKey{{"client.4242"}}] = packed;
  return r;
}

// The packed counters are written as native MgrClient::_send_report writes
// them: a 1/1 envelope and, for every declared path in ascending order, a
// u64 value and for LONGRUNAVG two more u64 counts.
ceph::buffer::list packed_counters(std::initializer_list<uint64_t> values) {
  ceph::buffer::list bl;
  ENCODE_START(1, 1, bl);
  for (uint64_t v : values) encode(v, bl);
  ENCODE_FINISH(bl);
  return bl;
}

void reports(const std::filesystem::path& root) {
  const auto scalar = perfcounter_type_d(PERFCOUNTER_U64 | PERFCOUNTER_COUNTER);
  const auto average = perfcounter_type_d(PERFCOUNTER_TIME | PERFCOUNTER_LONGRUNAVG);
  auto first = ceph::make_message<MMgrReport>();
  first->daemon_name = "0";
  first->declare_types = {counter("b", scalar, 8, UNIT_NONE), counter("c", average, 5, UNIT_BYTES)};
  first->packed = packed_counters({11, 21, 22, 23});
  auto front = emit(root, "report-declare", first.get(), CEPH_FEATURES_ALL, MSG_MGR_REPORT, 9, 1);
  auto d = redecode<MMgrReport>(*first, front);
  require(d->declare_types.size() == 2 && d->declare_types[1].type == average && d->declare_types[1].unit == UNIT_BYTES &&
          d->packed.length() == 6 + 32 && !d->daemon_status && !d->task_status && !d->metric_report_message,
          "native declaring report decode differs");
  decoded(root, "report-declare", "\"values\":[[\"b\",11],[\"c\",21,22,23]]");

  auto second = ceph::make_message<MMgrReport>();
  second->daemon_name = "0";
  second->declare_types = {counter("z", perfcounter_type_d(PERFCOUNTER_U64), 2, UNIT_NONE)};
  second->undeclare_types = {"b"};
  second->packed = packed_counters({31, 32, 33, 41});
  front = emit(root, "report-second", second.get(), CEPH_FEATURES_ALL, MSG_MGR_REPORT, 9, 1);
  d = redecode<MMgrReport>(*second, front);
  require(d->undeclare_types == std::vector<std::string>{"b"} && d->packed.length() == 6 + 32,
          "native second report decode differs");
  decoded(root, "report-second", "\"values\":[[\"c\",31,32,33],[\"z\",41]]");

  auto full = ceph::make_message<MMgrReport>();
  full->daemon_name = "d1";
  full->service_name = "ceph-msgr";
  full->declare_types = {counter("a", perfcounter_type_d(PERFCOUNTER_U64), 10, UNIT_NONE)};
  full->undeclare_types = {"gone"};
  full->packed = packed_counters({7});
  full->daemon_status = Map{{"s", "ok"}, {"t", binary}};
  full->task_status = Map{};
  full->daemon_health_metrics = {DaemonHealthMetric(daemon_metric::SLOW_OPS, uint64_t(7)),
                                 DaemonHealthMetric(daemon_metric::PENDING_CREATING_PGS, 1u, 2u)};
  full->config_bl.append("cfg");
  full->osd_perf_metric_reports[osd_query()] = osd_report();
  full->metric_report_message = MetricReportMessage(OSDMetricPayload({{osd_query(), osd_report()}}));
  front = emit(root, "report-populated", full.get(), CEPH_FEATURES_ALL, MSG_MGR_REPORT, 9, 1);
  d = redecode<MMgrReport>(*full, front);
  require(d->daemon_status && d->daemon_status->size() == 2 && d->task_status && d->task_status->empty() &&
          d->daemon_health_metrics.size() == 2 && d->osd_perf_metric_reports.size() == 1 &&
          d->metric_report_message && d->config_bl.to_str() == "cfg", "native populated report decode differs");
  ceph::buffer::list query, report;
  encode(osd_query(), query);
  encode(osd_report(), report);
  decoded(root, "report-populated", "\"values\":[[\"a\",7]],\"query_hex\":\"" + encoded_hex(query) +
          "\",\"report_hex\":\"" + encoded_hex(report) + "\"");
  refused<MMgrReport>(root, "report-truncated", *full, front.substr(0, front.size() - 1));

  auto mds = ceph::make_message<MMgrReport>();
  mds->daemon_name = "m";
  mds->packed = packed_counters({});
  mds->metric_report_message = MetricReportMessage(MDSMetricPayload());
  front = emit(root, "report-mds-metric", mds.get(), CEPH_FEATURES_ALL, MSG_MGR_REPORT, 9, 1);
  d = redecode<MMgrReport>(*mds, front);
  require(d->metric_report_message && boost::get<MDSMetricPayload>(&d->metric_report_message->payload),
          "native MDS metric report decode differs");
  decoded(root, "report-mds-metric", "\"values\":[]");
}

pg_stat_t pg_stat(int seed) {
  pg_stat_t s;
  s.version = eversion_t(5 + seed, 100 + seed);
  s.reported_seq = 1000 + seed;
  s.reported_epoch = 70 + seed;
  s.state = (1ULL << 1) | (1ULL << 2) | (1ULL << 35);
  s.last_fresh = utime_t(1001, 1);
  s.last_change = utime_t(1002, 2);
  s.last_active = utime_t(1003, 3);
  s.last_peered = utime_t(1004, 4);
  s.last_clean = utime_t(1005, 5);
  s.last_unstale = utime_t(1006, 6);
  s.last_undegraded = utime_t(1007, 7);
  s.last_fullsized = utime_t(1008, 8);
  s.log_start = eversion_t(3, 4);
  s.ondisk_log_start = eversion_t(5, 6);
  s.created = 9;
  s.last_epoch_clean = 10;
  s.parent = pg_t(1, 2);
  s.parent_split_bits = 11;
  s.last_scrub = eversion_t(12, 13);
  s.last_deep_scrub = eversion_t(14, 15);
  s.last_scrub_stamp = utime_t(16, 17);
  s.last_deep_scrub_stamp = utime_t(18, 19);
  s.last_clean_scrub_stamp = utime_t(20, 21);
  s.last_scrub_duration = 22;
  int64_t* sum = &s.stats.sum.num_bytes;
  for (int i = 0; i < 28; ++i) sum[i] = 2000 + 10 * seed + i;
  s.stats.sum.num_flush_mode_high = 1;
  s.stats.sum.num_flush_mode_low = 2;
  s.stats.sum.num_evict_mode_some = 3;
  s.stats.sum.num_evict_mode_full = 4;
  s.stats.sum.num_objects_pinned = 3001;
  s.stats.sum.num_objects_missing = 3002;
  s.stats.sum.num_legacy_snapsets = 3003;
  s.stats.sum.num_large_omap_objects = 3004;
  s.stats.sum.num_objects_manifest = 3005;
  s.stats.sum.num_omap_bytes = 3006;
  s.stats.sum.num_omap_keys = 3007;
  s.stats.sum.num_objects_repaired = 3008;
  s.log_size = 23;
  s.log_dups_size = 24;
  s.ondisk_log_size = 25;
  s.objects_scrubbed = 26;
  s.scrub_duration = 0.5;
  s.up = {0, 1};
  s.acting = {1, 0};
  s.avail_no_missing = {pg_shard_t(0, shard_id_t::NO_SHARD)};
  s.object_location_counts[{pg_shard_t(1, shard_id_t(2))}] = 5;
  s.mapping_epoch = 27;
  s.blocked_by = {3};
  s.purged_snaps.insert(snapid_t(3), 2);
  s.last_became_active = utime_t(1009, 9);
  s.last_became_peered = utime_t(1010, 10);
  s.up_primary = 0;
  s.acting_primary = 1;
  s.snaptrimq_len = 28;
  s.objects_trimmed = 29;
  s.snaptrim_duration = 0.25;
  s.scrub_sched_status.m_scheduled_at = utime_t(30, 31);
  s.scrub_sched_status.m_duration_seconds = 32;
  s.scrub_sched_status.m_sched_status = pg_scrub_sched_status_t::scheduled;
  s.scrub_sched_status.m_is_active = true;
  s.scrub_sched_status.m_is_deep = scrub_level_t::deep;
  s.scrub_sched_status.m_is_periodic = false;
  s.scrub_sched_status.m_osd_to_respond = 33;
  s.scrub_sched_status.m_ordinal_of_requested_replica = 34;
  s.scrub_sched_status.m_num_to_reserve = 35;
  s.stats_invalid = true;
  s.dirty_stats_invalid = false;
  s.omap_stats_invalid = true;
  s.hitset_stats_invalid = false;
  s.hitset_bytes_stats_invalid = true;
  s.pin_stats_invalid = false;
  s.manifest_stats_invalid = true;
  return s;
}

store_statfs_t statfs(int seed) {
  store_statfs_t s;
  s.total = 1 << 30;
  s.available = (1 << 29) + seed;
  s.internally_reserved = 4096;
  s.allocated = 5000 + seed;
  s.data_stored = 6000 + seed;
  s.data_compressed = 7;
  s.data_compressed_allocated = 8;
  s.data_compressed_original = 9;
  s.omap_allocated = 10;
  s.internal_metadata = 11;
  return s;
}

void pgstats(const std::filesystem::path& root) {
  for (auto [name, features] : {std::pair<const char*, uint64_t>{"pgstats", CEPH_FEATURES_ALL},
                                std::pair<const char*, uint64_t>{"pgstats-acceptor", acceptor_features}}) {
    auto m = ceph::make_message<MPGStats>(fsid(), 77);
    m->osd_stat.statfs = statfs(0);
    m->osd_stat.hb_peers = {1, 2};
    m->osd_stat.snap_trim_queue_len = 3;
    m->osd_stat.num_snap_trimming = 4;
    m->osd_stat.num_shards_repaired = 5;
    m->osd_stat.op_queue_age_hist.h = {1, 2, 3};
    m->osd_stat.os_perf_stat.os_commit_latency_ns = 5000000;
    m->osd_stat.os_perf_stat.os_apply_latency_ns = 7000000;
    m->osd_stat.os_alerts[0].emplace("some alert", "details");
    m->osd_stat.up_from = 12;
    m->osd_stat.seq = 34;
    m->osd_stat.num_pgs = 2;
    m->osd_stat.num_osds = 1;
    m->osd_stat.num_per_pool_osds = 1;
    m->osd_stat.num_per_pool_omap_osds = 1;
    osd_stat_t::Interfaces ifs{};
    ifs.last_update = 1700000000;
    for (int i = 0; i < 3; ++i) {
      ifs.back_pingtime[i] = 100 + i; ifs.back_min[i] = 110 + i; ifs.back_max[i] = 120 + i;
      ifs.front_pingtime[i] = 200 + i; ifs.front_min[i] = 210 + i; ifs.front_max[i] = 220 + i;
    }
    ifs.back_last = 130;
    ifs.front_last = 230;
    m->osd_stat.hb_pingtime[1] = ifs;
    m->pg_stat[pg_t(0, 3)] = pg_stat(0);
    m->pg_stat[pg_t(5, 3)] = pg_stat(1);
    m->pool_stat[3] = statfs(1);
    auto front = emit(root, name, m.get(), features, MSG_PGSTATS, 2, 1);
    auto d = redecode<MPGStats>(*m, front);
    require(d->pg_stat.size() == 2 && d->epoch == 77 && d->osd_stat.up_from == 12 && d->pool_stat.size() == 1 &&
            d->pg_stat.at(pg_t(5, 3)).state == m->pg_stat.at(pg_t(5, 3)).state &&
            d->osd_stat.os_perf_stat.os_commit_latency_ns == 5000000, "native PG stats decode differs");
    ceph::JSONFormatter f;
    d->dump_stats(&f);
    std::ostringstream dump;
    f.flush(dump);
    decoded(root, name, "\"epoch\":77,\"dump\":" + dump.str());
    if (features == CEPH_FEATURES_ALL) {
      refused<MPGStats>(root, "pgstats-truncated", *m, front.substr(0, front.size() - 1));
    }
  }
}

void commands(const std::filesystem::path& root) {
  const std::vector<std::string> cmd{"{\"prefix\":\"gotest echo\"}", "x"};
  auto mgr = ceph::make_message<MMgrCommand>(uuid_d());
  mgr->cmd = cmd;
  mgr->set_data(bytes(all_bytes()));
  auto front = emit(root, "mgr-command", mgr.get(), CEPH_FEATURES_ALL, MSG_MGR_COMMAND, 1, 1, 7);
  auto m = redecode<MMgrCommand>(*mgr, front);
  require(m->cmd == cmd && m->fsid.is_zero() && m->get_data().to_str() == all_bytes(), "native mgr command decode differs");
  decoded(root, "mgr-command", "\"tid\":7");

  auto tell = ceph::make_message<MCommand>(fsid());
  tell->cmd = cmd;
  tell->set_data(bytes(binary));
  front = emit(root, "tell-command", tell.get(), CEPH_FEATURES_ALL, MSG_COMMAND, 1, 1, 9);
  auto t = redecode<MCommand>(*tell, front);
  require(t->cmd == cmd && t->fsid == fsid(), "native tell decode differs");
  decoded(root, "tell-command", "\"tid\":9");

  auto reply = ceph::make_message<MMgrCommandReply>(-22, "bad input");
  reply->set_data(bytes(all_bytes()));
  front = emit(root, "mgr-command-reply", reply.get(), CEPH_FEATURES_ALL, MSG_MGR_COMMAND_REPLY, 1, 1, 7);
  auto r = redecode<MMgrCommandReply>(*reply, front);
  require(r->r == -22 && r->rs == "bad input", "native mgr command reply decode differs");
  decoded(root, "mgr-command-reply", "\"tid\":7,\"r\":-22");

  auto tell_reply = ceph::make_message<MCommandReply>(-61, "gone");
  tell_reply->set_data(bytes(binary));
  front = emit(root, "tell-command-reply", tell_reply.get(), CEPH_FEATURES_ALL, MSG_COMMAND_REPLY, 1, 1, 9);
  auto tr = redecode<MCommandReply>(*tell_reply, front);
  require(tr->r == -61 && tr->rs == "gone", "native tell reply decode differs");
  decoded(root, "tell-command-reply", "\"tid\":9,\"r\":-61");
}

void configures(const std::filesystem::path& root) {
  auto plain = ceph::make_message<MMgrConfigure>();
  plain->stats_period = 5;
  plain->stats_threshold = 2;
  auto front = emit(root, "configure-plain", plain.get(), CEPH_FEATURES_ALL, MSG_MGR_CONFIGURE, 4, 1);
  auto d = redecode<MMgrConfigure>(*plain, front);
  require(d->stats_period == 5 && d->stats_threshold == 2 && !d->metric_config_message, "native plain configure decode differs");
  decoded(root, "configure-plain", "\"period\":5");

  auto queries = ceph::make_message<MMgrConfigure>();
  queries->stats_period = 1;
  OSDPerfMetricLimits limits{OSDPerfMetricLimit(PerformanceCounterDescriptor(PerformanceCounterType::BYTES), 10)};
  queries->osd_perf_metric_queries[osd_query()] = limits;
  front = emit(root, "configure-osd-queries", queries.get(), CEPH_FEATURES_ALL, MSG_MGR_CONFIGURE, 4, 1);
  d = redecode<MMgrConfigure>(*queries, front);
  require(d->osd_perf_metric_queries.size() == 1, "native query configure decode differs");
  ceph::buffer::list query, encoded_limits;
  encode(osd_query(), query);
  encode(limits, encoded_limits);
  decoded(root, "configure-osd-queries", "\"query_hex\":\"" + encoded_hex(query) + "\",\"limits_hex\":\"" +
          encoded_hex(encoded_limits) + "\"");

  for (auto [name, features] : {std::pair<const char*, uint64_t>{"configure-mds", CEPH_FEATURES_ALL},
                                std::pair<const char*, uint64_t>{"configure-mds-nopacific", CEPH_FEATURES_ALL & ~CEPH_FEATURE_SERVER_PACIFIC}}) {
    auto mds = ceph::make_message<MMgrConfigure>();
    mds->stats_period = 3;
    mds->metric_config_message = MetricConfigMessage(MDSConfigPayload());
    front = emit(root, name, mds.get(), features, MSG_MGR_CONFIGURE, 4, 1);
    d = redecode<MMgrConfigure>(*mds, front);
    require(bool(d->metric_config_message) == (features == CEPH_FEATURES_ALL), "native MDS configure decode differs");
    ceph::buffer::list payload;
    encode(MDSConfigPayload(), payload);
    decoded(root, name, "\"payload_hex\":\"" + encoded_hex(payload) + "\",\"features\":" + std::to_string(features));
  }
}
}

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: mgr_messages_native_encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    CephInitParameters parameters(CEPH_ENTITY_TYPE_CLIENT);
    context = common_preinit(parameters, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    context->_conf.apply_changes(nullptr);
    common_init_finish(context);
    opens(root);
    updates(root);
    closes(root);
    reports(root);
    pgstats(root);
    commands(root);
    configures(root);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
