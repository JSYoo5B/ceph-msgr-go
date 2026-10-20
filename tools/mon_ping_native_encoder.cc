// Development-only native MPing producer/decoder, Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964 (LGPL-2.1 headers/library).
// No native implementation copied. Native MPing, Ceph string encode/decode,
// Message::encode and MessageFrame APIs are invoked unchanged.
//
// Request: MonClient::ping_monitor sends a fresh empty MPing; canonical
// Message::encode normalizes its compat 0 to 1. Reply: Monitor::handle_ping
// encodes a string into a buffer and calls set_payload BEFORE sending a
// fresh MPing. Message::encode therefore retains the constructor's compat0.
// Do not pre-encode that reply or override its header to force compat1.
// Reply bodies are explicit raw codec test inputs, not live MON health data.
#include "messages/MPing.h"
#include "msg/async/frames_v2.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
namespace {
std::string hex(const std::string& input) {
 const char* digits="0123456789abcdef";std::string out;
 for(unsigned char b:input){out+=digits[b>>4];out+=digits[b&15];}return out;
}
void write(const std::filesystem::path& path,const std::string& data) {
 std::ofstream out(path,std::ios::binary);out.exceptions(std::ios::failbit|std::ios::badbit);out.write(data.data(),data.size());
}
void emit(const std::filesystem::path& root,const std::string& name,bool reply,const std::string& raw) {
 auto message=ceph::make_message<MPing>();
 if(!message->empty_payload() || message->get_middle().length() || message->get_data().length() || message->get_header().compat_version!=0)
  throw std::runtime_error("MPing construction must be fresh, default compat0 and empty");
 if(reply){
  // This is exactly Monitor::handle_ping's payload producer sequence.
  ceph::buffer::list payload;ceph::encode(raw,payload);message->set_payload(payload);
  if(message->empty_payload())throw std::runtime_error("native string must include its four-byte length even when empty");
 }
 message->set_seq(1);message->set_priority(CEPH_MSG_PRIO_DEFAULT);
 message->encode(0,MSG_CRC_ALL,true); // one canonical native send encode
 const auto& h=message->get_header();const auto& footer=message->get_footer();
 const unsigned compat=reply?0:1;
 if(h.type!=CEPH_MSG_PING || h.version!=1 || h.compat_version!=compat || h.tid!=0 || h.middle_len!=0 || h.data_len!=0 ||
    h.front_len!=message->get_payload().length())throw std::runtime_error("native MPing envelope differs from source producer");
 ceph_msg_header2 h2{};h2.seq=h.seq;h2.tid=h.tid;h2.type=h.type;h2.priority=h.priority;h2.version=h.version;
 h2.data_off=h.data_off;h2.ack_seq=3;h2.flags=footer.flags;h2.compat_version=h.compat_version;h2.reserved=h.reserved;
 auto frame=ceph::msgr::v2::MessageFrame::Encode(h2,message->get_payload(),message->get_middle(),message->get_data());
 std::string header(reinterpret_cast<const char*>(&frame.header()),sizeof(frame.header()));
 if(header.size()!=41)throw std::runtime_error("native header2 size differs");
 write(root/(name+".header.bin"),header);write(root/(name+".front.bin"),message->get_payload().to_str());
 write(root/(name+".middle.bin"),message->get_middle().to_str());write(root/(name+".data.bin"),message->get_data().to_str());
 std::ofstream meta(root/(name+".native-header.json"));meta.exceptions(std::ios::failbit|std::ios::badbit);
 meta<<"{\"type\":"<<uint16_t(h.type)<<",\"version\":"<<unsigned(h.version)<<",\"compat\":"<<unsigned(h.compat_version)
     <<",\"tid\":"<<uint64_t(h.tid)<<",\"seq\":"<<uint64_t(h.seq)<<",\"priority\":"<<uint16_t(h.priority)
     <<",\"front_len\":"<<uint32_t(h.front_len)<<",\"middle_len\":"<<uint32_t(h.middle_len)<<",\"data_len\":"<<uint32_t(h.data_len)
     <<",\"data_off\":"<<uint16_t(h.data_off)<<",\"flags\":"<<unsigned(footer.flags)<<",\"ack_seq\":3,\"reserved\":"<<uint16_t(h.reserved)<<"}\n";
 // A separate fresh MPing decoder never becomes an encoder input. Native
 // MPing::decode_payload itself is empty; MonClientPinger subsequently calls
 // the native Ceph string decoder on its payload. Reproduce both steps.
 auto decoded=ceph::make_message<MPing>();decoded->set_header(h);ceph::buffer::list copied(message->get_payload());decoded->set_payload(copied);decoded->decode_payload();
 std::string got;
 if(reply){auto p=decoded->get_payload().cbegin();ceph::decode(got,p);if(!p.end() || got!=raw)throw std::runtime_error("fresh native string decoder mismatch");}
 else if(!decoded->empty_payload())throw std::runtime_error("native request decoder payload was nonempty");
 std::ofstream receipt(root/(name+".decoded-ping.json"));receipt.exceptions(std::ios::failbit|std::ios::badbit);
 receipt<<"{\"is_reply\":"<<(reply?"true":"false")<<",\"raw_hex\":\""<<hex(got)<<"\",\"payload_prepopulated\":"<<(reply?"true":"false")<<",\"payload_bytes\":"<<got.size()<<"}\n";
 std::cout<<name<<"\tversion="<<unsigned(h.version)<<" compat="<<unsigned(h.compat_version)<<" front="<<uint32_t(h.front_len)<<"\n";
}
}
int main(int argc,char**argv){try{
 if(argc!=2)throw std::runtime_error("usage: mon_ping_native_encoder OUTPUT_DIR");
 const std::filesystem::path root(argv[1]);std::filesystem::create_directories(root);
 emit(root,"mon-ping-native-request",false,"");
 emit(root,"mon-ping-native-reply-json",true,"{\n  \"pong\": {\"fixture\": \"native\"}\n}\n");
 emit(root,"mon-ping-native-reply-empty",true,"");
 emit(root,"mon-ping-native-reply-binary",true,std::string("raw\0\xff\x80\n",7));
 return 0;
 }catch(const std::exception&e){std::cerr<<e.what()<<"\n";return 1;}}
