// Development-only wrapper around unchanged Ceph Tentacle v20.2.4 headers
// and ceph-common, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// LGPL-2.1 native implementation is invoked, not copied. No Go wire bytes
// are inputs. Each producer is fresh; each decoder is a different object.
#include "messages/MMgrOpen.h"
#include "messages/MMgrUpdate.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrClose.h"
#include "msg/async/frames_v2.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

namespace {
using Map = std::map<std::string, std::string>;
const std::string binary("\0\xff", 2);
const Map metadata{{"b", "2"}, {"a", "1"}, {"binary", binary}};
const Map status{{"state", "up"}};
const Map updated{{"zone", "b"}, {"binary", binary}};
const Map busy{{"state", "busy"}, {"binary", binary}};
const Map task{{"task", "x"}};

void require(bool condition, const char* reason) {
  if (!condition) throw std::runtime_error(reason);
}
std::string hex(const std::string& input) {
  const char* digits = "0123456789abcdef";
  std::string out;
  for (unsigned char b : input) { out += digits[b >> 4]; out += digits[b & 15]; }
  return out;
}
std::string json_map(const Map& input) {
  std::string out = "{";
  for (const auto& [key, value] : input) {
    if (out.size() > 1) out += ",";
    out += "\"" + hex(key) + "\":\"" + hex(value) + "\"";
  }
  return out + "}";
}
std::string json_optional(const boost::optional<Map>& input) {
  return input ? json_map(*input) : "null";
}
std::string identity(const std::string& service, const std::string& daemon) {
  return "\"service_hex\":\"" + hex(service) + "\",\"daemon_hex\":\"" + hex(daemon) + "\"";
}
void write(const std::filesystem::path& path, const std::string& value) {
  std::ofstream out(path, std::ios::binary);
  out.exceptions(std::ios::failbit | std::ios::badbit);
  out.write(value.data(), value.size());
}

template<class T, class Verify>
void emit(const std::filesystem::path& root, const std::string& name,
          const boost::intrusive_ptr<T>& message, Verify verify) {
  require(message->empty_payload(), "producer must start with an empty payload");
  message->set_seq(1);
  message->set_priority(CEPH_MSG_PRIO_DEFAULT);
  message->encode(0, MSG_CRC_ALL, true);
  const auto& h = message->get_header();
  const auto& footer = message->get_footer();
  require(h.compat_version == 1 && h.tid == 0 && h.middle_len == 0 && h.data_len == 0,
          "native service envelope differs");
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
  auto decoded = ceph::make_message<T>();
  decoded->set_header(h);
  ceph::buffer::list copy(message->get_payload());
  decoded->set_payload(copy);
  decoded->decode_payload();
  const auto fields = verify(*decoded);
  write(root / (name + ".decoded.json"), "{" + fields + "}\n");
  std::cout << name << " type=" << uint16_t(h.type) << " v=" << unsigned(h.version)
            << " compat=" << unsigned(h.compat_version) << " front=" << uint32_t(h.front_len) << "\n";
}

void open(const std::filesystem::path& root, bool populated) {
  auto m = ceph::make_message<MMgrOpen>();
  m->daemon_name = "d1"; m->service_name = "svc"; m->service_daemon = true;
  m->daemon_metadata = populated ? metadata : Map{};
  m->daemon_status = populated ? status : Map{};
  emit(root, populated ? "open-populated" : "open-empty", m, [&](const MMgrOpen& d) {
    require(d.service_name == "svc" && d.daemon_name == "d1" && d.service_daemon &&
            d.daemon_metadata == m->daemon_metadata && d.daemon_status == m->daemon_status &&
            d.config_bl.length() == 0 && d.config_defaults_bl.length() == 0,
            "native open decode differs");
    return identity(d.service_name, d.daemon_name) + ",\"service_daemon\":true,\"metadata_hex\":" +
           json_map(d.daemon_metadata) + ",\"status_hex\":" + json_map(d.daemon_status) +
           ",\"config_bytes\":0,\"defaults_bytes\":0";
  });
}
void report(const std::filesystem::path& root, const std::string& kind) {
  auto m = ceph::make_message<MMgrReport>();
  m->daemon_name = "d1"; m->service_name = "svc";
  // MgrClient::_send_report, with no counters selected. Use the native
  // versioned envelope macros rather than embedding Go's expected bytes.
  { ENCODE_START(1, 1, m->packed); ENCODE_FINISH(m->packed); }
  if (kind != "omitted") {
    m->daemon_status = kind == "empty" ? Map{} : busy;
    m->task_status = kind == "empty" ? Map{} : task;
  }
  emit(root, "report-" + kind, m, [&](const MMgrReport& d) {
    require(d.service_name == "svc" && d.daemon_name == "d1" &&
            d.daemon_status == m->daemon_status && d.task_status == m->task_status &&
            d.declare_types.empty() && d.undeclare_types.empty() &&
            d.daemon_health_metrics.empty() && d.config_bl.length() == 0 &&
            d.osd_perf_metric_reports.empty() && !d.metric_report_message,
            "native report decode differs");
    auto p = d.packed.cbegin();
    { DECODE_START(1, p); DECODE_FINISH(p); }
    require(p.end() && d.packed.to_str() == m->packed.to_str(), "native packed counter envelope differs");
    return identity(d.service_name, d.daemon_name) + ",\"status_hex\":" + json_optional(d.daemon_status) +
           ",\"task_hex\":" + json_optional(d.task_status) + ",\"packed_hex\":\"" + hex(d.packed.to_str()) +
           "\",\"empty_auxiliary_fields\":true";
  });
}
}

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: mgr_service_native_encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    open(root, false); open(root, true);
    auto update = ceph::make_message<MMgrUpdate>();
    update->service_name = "svc"; update->daemon_name = "d1";
    update->need_metadata_update = true; update->daemon_metadata = updated;
    emit(root, "update", update, [](const MMgrUpdate& d) {
      require(d.service_name == "svc" && d.daemon_name == "d1" && d.need_metadata_update &&
              d.daemon_metadata == updated && d.daemon_status.empty(), "native update decode differs");
      return identity(d.service_name, d.daemon_name) + ",\"need_metadata_update\":true,\"metadata_hex\":" +
             json_map(d.daemon_metadata) + ",\"status_hex\":{}";
    });
    for (const auto& kind : {"omitted", "empty", "populated"}) report(root, kind);
    auto configure = ceph::make_message<MMgrConfigure>();
    configure->stats_period = 5; configure->stats_threshold = 2;
    emit(root, "configure", configure, [](const MMgrConfigure& d) {
      require(d.stats_period == 5 && d.stats_threshold == 2 &&
              d.osd_perf_metric_queries.empty() && !d.metric_config_message, "native configure decode differs");
      return std::string("\"period\":5,\"threshold\":2,\"empty_extensions\":true");
    });
    auto close = ceph::make_message<MMgrClose>();
    close->service_name = "svc"; close->daemon_name = "d1";
    emit(root, "close", close, [](const MMgrClose& d) {
      require(d.service_name == "svc" && d.daemon_name == "d1", "native close decode differs");
      return identity(d.service_name, d.daemon_name);
    });
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
