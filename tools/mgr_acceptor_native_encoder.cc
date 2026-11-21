// Development-only wrapper around unchanged Ceph Tentacle v20.2.4 headers
// and ceph-common, commit 7f793731f1b39eb4f465e960113d2363c311b964.
// LGPL-2.1 native implementation is invoked, not copied. No Go wire bytes
// are inputs.
//
// Vectors for the MGR acceptor (service side of CephX and the accepting
// msgr2 control frames):
// - For an aes256k and an aes MGR service secret: a CephXServiceTicketInfo
//   for osd.3 sealed natively, a native CephXTicketHandler authorizer, the
//   challenge the native cephx_verify_authorizer issues, the native client's
//   add_challenge response, and the native verifier's final reply in secure
//   mode (64-byte connection secret) and CRC mode (none). The decrypted
//   server challenge and the authorizer nonce are recorded so Go can repeat
//   the exchange; every later step is checked natively first.
// - Native refusals: the native verifier is run on authorizers with another
//   service, an unknown, the all-ones and a mismatched global ID, a flipped
//   ticket byte, a flipped authorize byte, and the response of one
//   handshake replayed after another handshake's challenge.
// - Plaintext encodings of CephXAuthorizeChallenge, CephXAuthorizeReply v1
//   and v2, CephXAuthorize and the ticket info (also with allow_all and no
//   caps), and their aes ciphertexts, which are deterministic.
// - CRC-mode msgr2.1 frames, assembled natively: HELLO, AUTH_REPLY_MORE,
//   AUTH_DONE, AUTH_BAD_METHOD, IDENT_MISSING_FEATURES, SERVER_IDENT,
//   SESSION_RESET and COMPRESSION_DONE with the acceptor's values.
// All keys are synthetic constants; none belongs to a cluster.
#include "auth/Auth.h"
#include "auth/Crypto.h"
#include "auth/cephx/CephxProtocol.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "compressor/Compressor.h"
#include "include/stringify.h"
#include "msg/async/compression_onwire.h"
#include "msg/async/crypto_onwire.h"
#include "msg/async/frames_v2.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace {
CephContext* context = nullptr;
using namespace ceph::msgr::v2;

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
CryptoKey key(int type, unsigned char fill, unsigned length) {
  std::string secret(length, char(fill));
  ceph::buffer::ptr p(secret.data(), secret.size());
  return CryptoKey(type, utime_t(0, 0), p);
}
std::string encoded(const CryptoKey& k) {
  ceph::buffer::list bl;
  k.encode(bl);
  return bl.to_str();
}

// One rotating MGR secret, ID 7, and nothing else.
struct OneServiceKey : public KeyStore {
  CryptoKey service;
  bool get_secret(const EntityName&, CryptoKey&) const override { return false; }
  bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& out) const override {
    if (service_id != CEPH_ENTITY_TYPE_MGR || secret_id != 7) return false;
    out = service;
    return true;
  }
};

CephXServiceTicketInfo ticket_info(const CryptoKey& session, bool allow_all, const std::string& caps) {
  using ceph::encode;
  CephXServiceTicketInfo info;
  info.ticket.name.set(CEPH_ENTITY_TYPE_OSD, "3");
  info.ticket.global_id = 4242;
  info.ticket.created = utime_t(1700000000, 5);
  info.ticket.expires = utime_t(1700003600, 6);
  info.ticket.caps.allow_all = allow_all;
  if (!caps.empty()) encode(caps, info.ticket.caps.caps);
  info.ticket.flags = 0;
  info.session_key = session;
  return info;
}

// verify runs the native service verifier on one authorizer round.
bool verify(const KeyStore& keys, const ceph::buffer::list& authorizer, size_t secret_length,
            std::unique_ptr<AuthAuthorizerChallenge>* challenge, std::string* secret,
            ceph::buffer::list* reply, CephXServiceTicketInfo* info) {
  auto p = authorizer.cbegin();
  return cephx_verify_authorizer(context, keys, p, secret_length, *info, challenge, secret, reply);
}

std::string json_string(const std::string& name, const std::string& value) {
  return "\"" + name + "\":\"" + hex(value) + "\"";
}

void exchange(const std::filesystem::path& root, const std::string& name, int type, unsigned length) {
  OneServiceKey keys;
  keys.service = key(type, 0x31, length);
  CryptoKey session = key(type, 0x52, length);
  std::string error;
  ceph::buffer::list blob;
  encode_encrypt_enc_bl(context, ticket_info(session, false, "allow profile osd"), keys.service,
                        CEPHX_KEY_USAGE_TICKET_INFO, blob, error);
  using ceph::encode;
  require(error.empty(), "native ticket info encryption failed");
  CephXTicketHandler handler(context, CEPH_ENTITY_TYPE_MGR);
  handler.session_key = session;
  handler.ticket.secret_id = 7;
  handler.ticket.blob = blob;
  handler.have_key_flag = true;
  std::unique_ptr<CephXAuthorizer> authorizer(handler.build_authorizer(4242));
  require(authorizer != nullptr, "native authorizer build failed");
  const ceph::buffer::list first = authorizer->bl;
  std::string fields = "\"key_type\":" + std::to_string(type) + "," +
                       json_string("service_key", encoded(keys.service)) + "," +
                       json_string("session_key", encoded(session)) + "," +
                       json_string("ticket_blob", blob.to_str()) + "," +
                       "\"nonce\":" + std::to_string(authorizer->nonce) + "," +
                       json_string("authorizer", first.to_str());
  for (const auto& [mode, secret_length] : std::vector<std::pair<std::string, size_t>>{{"secure", 64}, {"crc", 0}}) {
    std::unique_ptr<AuthAuthorizerChallenge> challenge;
    std::string secret;
    ceph::buffer::list challenge_bl;
    CephXServiceTicketInfo info;
    require(!verify(keys, first, secret_length, &challenge, &secret, &challenge_bl, &info) && challenge,
            "native first round did not challenge");
    CephXAuthorizeChallenge plain;
    decode_decrypt_enc_bl(context, plain, session, CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, challenge_bl, error);
    require(error.empty(), "native challenge decrypt failed");
    std::unique_ptr<CephXAuthorizer> client(handler.build_authorizer(4242));
    client->nonce = authorizer->nonce;
    client->base_bl = authorizer->base_bl;
    require(client->add_challenge(context, challenge_bl), "native add_challenge failed");
    const ceph::buffer::list second = client->bl;
    ceph::buffer::list reply;
    require(verify(keys, second, secret_length, &challenge, &secret, &reply, &info),
            "native response refused");
    require(info.ticket.global_id == 4242 && info.ticket.name.to_str() == "osd.3" &&
            secret.size() == secret_length, "native verified ticket differs");
    auto r = reply.cbegin();
    std::string got_secret;
    require(client->verify_reply(r, &got_secret) && got_secret == secret, "native reply verify failed");
    // A replay: a new handshake issues another challenge; the old response
    // must not answer it.
    std::unique_ptr<AuthAuthorizerChallenge> fresh;
    ceph::buffer::list fresh_bl, replay_reply;
    std::string replay_secret;
    require(!verify(keys, first, secret_length, &fresh, &replay_secret, &fresh_bl, &info) && fresh,
            "native second handshake did not challenge");
    bool replay_accepted = verify(keys, second, secret_length, &fresh, &replay_secret, &replay_reply, &info);
    fields += ",\"" + mode + "\":{\"server_challenge\":" +
              std::to_string(static_cast<CephXAuthorizeChallenge*>(challenge.get())->server_challenge) + "," +
              json_string("challenge", challenge_bl.to_str()) + "," + json_string("response", second.to_str()) + "," +
              json_string("reply", reply.to_str()) + "," + json_string("connection_secret", secret) + "," +
              "\"replay_accepted\":" + (replay_accepted ? "true" : "false") + "}";
  }
  // Native refusals of altered first-round authorizers. Plaintext offsets:
  // version 0, global ID 1, service 9, ticket version 13, secret ID 14.
  struct Case { std::string name; size_t at; std::string bytes; bool flip; };
  auto le = [](uint64_t v, size_t n) { return std::string(reinterpret_cast<const char*>(&v), n); };
  std::string text = first.to_str();
  const std::vector<Case> cases{
      {"service_osd", 9, le(CEPH_ENTITY_TYPE_OSD, 4), false},
      {"unknown_secret", 14, le(8, 8), false},
      {"all_ones_secret", 14, le(~0ull, 8), false},
      {"global_id", 1, le(4243, 8), false},
      {"ticket_byte", 30, "", true},
      {"authorize_byte", text.size() - 1, "", true},
  };
  fields += ",\"refusals\":{";
  bool comma = false;
  for (const auto& c : cases) {
    std::string altered = text;
    if (c.flip) altered[c.at] ^= 1; else altered.replace(c.at, c.bytes.size(), c.bytes);
    ceph::buffer::list bl, reply;
    bl.append(altered);
    std::unique_ptr<AuthAuthorizerChallenge> challenge;
    std::string secret;
    CephXServiceTicketInfo info;
    bool accepted = verify(keys, bl, 64, &challenge, &secret, &reply, &info);
    // A refused first round issues no challenge.
    fields += std::string(comma ? "," : "") + "\"" + c.name + "\":{" + json_string("authorizer", altered) +
              ",\"accepted\":" + (accepted ? "true" : "false") + ",\"challenged\":" + (challenge ? "true" : "false") + "}";
    comma = true;
  }
  fields += "}";
  write(root / (name + ".json"), "{" + fields + "}\n");
  std::cout << name << " exchange verified\n";
}

void plaintexts(const std::filesystem::path& root) {
  using ceph::encode;
  std::string fields;
  auto add = [&fields](const std::string& name, const ceph::buffer::list& bl) {
    fields += std::string(fields.empty() ? "" : ",") + json_string(name, bl.to_str());
  };
  CryptoKey aes = key(CEPH_CRYPTO_AES, 0x52, 16);
  std::string error;
  {
    CephXAuthorizeChallenge c;
    c.server_challenge = 0x1122334455667788ull;
    ceph::buffer::list plain, sealed;
    encode(c, plain);
    add("challenge_plain", plain);
    encode_encrypt_enc_bl(context, c, aes, CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, sealed, error);
    add("challenge_aes", sealed);
  }
  for (const auto& [name, secret] : std::vector<std::pair<std::string, std::string>>{
           {"reply_v1", ""}, {"reply_v2", std::string(64, '\x5a')}}) {
    CephXAuthorizeReply r;
    r.nonce_plus_one = 0x0102030405060709ull;
    r.connection_secret = secret;
    ceph::buffer::list plain, sealed;
    encode(r, plain);
    add(name + "_plain", plain);
    require(encode_encrypt(context, r, aes, CEPHX_KEY_USAGE_AUTHORIZE_REPLY, sealed, error) == 0, "reply seal");
    add(name + "_aes", sealed);
  }
  {
    CephXAuthorize a;
    a.nonce = 0x0102030405060708ull;
    a.have_challenge = true;
    a.server_challenge_plus_one = 0x1122334455667789ull;
    ceph::buffer::list plain;
    encode(a, plain);
    add("authorize_plain", plain);
  }
  for (const auto& [name, allow_all, caps] : std::vector<std::tuple<std::string, bool, std::string>>{
           {"ticket_info_caps", false, "allow profile osd"}, {"ticket_info_allow_all", true, ""}}) {
    ceph::buffer::list plain;
    encode(ticket_info(aes, allow_all, caps), plain);
    add(name + "_plain", plain);
  }
  require(error.empty(), "native plaintext sealing failed");
  write(root / "plaintexts.json", "{" + fields + "}\n");
}

void frames(const std::filesystem::path& root) {
  ceph::crypto::onwire::rxtx_t crypto{};
  ceph::compression::onwire::rxtx_t compression{};
  FrameAssembler assembler(&crypto, true, true, &compression);
  entity_addr_t peer;
  require(peer.parse("v2:192.0.2.20:51234/0"), "peer address");
  entity_addrvec_t mine;
  require(mine.parse("[v2:192.0.2.10:6800/77]"), "own address");
  ceph::buffer::list payload;
  payload.append(std::string("\x01\x02\x03", 3));
  std::string fields;
  auto add = [&fields](const std::string& name, ceph::buffer::list bl) {
    fields += std::string(fields.empty() ? "" : ",") + json_string(name, bl.to_str());
  };
  add("hello", HelloFrame::Encode(CEPH_ENTITY_TYPE_MGR, peer).get_buffer(assembler));
  add("auth_reply_more", AuthReplyMoreFrame::Encode(payload).get_buffer(assembler));
  add("auth_done", AuthDoneFrame::Encode(4242, CEPH_CON_MODE_SECURE, payload).get_buffer(assembler));
  add("auth_bad_method", AuthBadMethodFrame::Encode(CEPH_AUTH_CEPHX, -EACCES, std::vector<uint32_t>{CEPH_AUTH_CEPHX},
                                                    std::vector<uint32_t>{CEPH_CON_MODE_SECURE}).get_buffer(assembler));
  add("ident_missing_features", IdentMissingFeaturesFrame::Encode(CEPH_FEATURE_SERVER_LUMINOUS).get_buffer(assembler));
  add("server_ident", ServerIdentFrame::Encode(mine, 9001, 1, 0x2a00040010a0a237ull, 0x0800000000200000ull,
                                               CEPH_MSG_CONNECT_LOSSY, 0).get_buffer(assembler));
  add("session_reset", ResetFrame::Encode(true).get_buffer(assembler));
  add("compression_done", CompressionDoneFrame::Encode(false, Compressor::COMP_ALG_NONE).get_buffer(assembler));
  write(root / "frames.json", "{" + fields + "}\n");
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2) throw std::runtime_error("usage: mgr_acceptor_native_encoder OUTPUT_DIR");
    const std::filesystem::path root(argv[1]);
    std::filesystem::create_directories(root);
    CephInitParameters parameters(CEPH_ENTITY_TYPE_MGR);
    context = common_preinit(parameters, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    context->_conf.apply_changes(nullptr);
    common_init_finish(context);
    exchange(root, "exchange-aes256k", CEPH_CRYPTO_AES256KRB5, 32);
    exchange(root, "exchange-aes", CEPH_CRYPTO_AES, 16);
    plaintexts(root);
    frames(root);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << "\n";
    return 1;
  }
}
