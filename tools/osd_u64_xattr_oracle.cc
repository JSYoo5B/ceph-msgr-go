// Test-only pinned Tentacle public C++ API U64 comparison oracle. LGPL-2.1
// public librados header; no Ceph implementation copied. Operates only names
// admitted by the owning Python fixture wrapper, never a product dependency.
#include "include/rados/librados.hpp"
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

uint64_t number(const char* text) {
 std::string value(text);size_t used=0;
 if(value.empty()||value.find_first_not_of("0123456789")!=std::string::npos)throw std::runtime_error("invalid decimal input");
 auto result=std::stoull(value,&used);
 if(used!=value.size())throw std::runtime_error("invalid decimal suffix");
 return result;
}
int main(int argc,char** argv) {
 try {
  if(argc!=10)throw std::runtime_error("usage: oracle CONF KEYRING POOL OBJECT ACTION COMPARE_OP OPERAND FLAGS EXISTS");
  std::string action(argv[5]);
  auto comparison=number(argv[6]),operand=number(argv[7]),flags=number(argv[8]),exists=number(argv[9]);
  if((action!="read"&&action!="write"&&action!="staged-write")||comparison<1||comparison>6||(flags!=0&&flags!=2)||exists>1)throw std::runtime_error("invalid closed comparison request");
  // EXISTS labels admission/fixture identity only; the native object lookup
  // decides absence. The C++ helper never creates/seeds objects itself.
  librados::Rados client;int code=client.init2("client.test","ceph",0);
  if(code>=0)code=client.conf_read_file(argv[1]);
  if(code>=0)code=client.conf_set("keyring",argv[2]);
  if(code>=0)code=client.connect();
  if(code<0)throw std::runtime_error("native connect "+std::to_string(code));
  librados::IoCtx io;code=client.ioctx_create(argv[3],io);
  if(code<0)throw std::runtime_error("native pool "+std::to_string(code));
  if(action=="read") {
   librados::ObjectReadOperation op;op.cmpxattr("number",static_cast<uint8_t>(comparison),operand);
   if(flags)op.set_op_flags2(librados::OP_FAILOK);
   code=io.operate(argv[4],&op,nullptr);
  } else {
   librados::ObjectWriteOperation op;
   if(action=="staged-write") {librados::bufferlist bytes;bytes.append(std::string("N\0\xff",3));op.write(1,bytes);}
   op.cmpxattr("number",static_cast<uint8_t>(comparison),operand);
   if(flags)op.set_op_flags2(librados::OP_FAILOK);
   librados::bufferlist value;value.append(std::string("u64-native-marker"));op.setxattr("marker",value);op.rmxattr("remove");
   time_t stamp=1700000001;op.mtime(&stamp);
   code=io.operate(argv[4],&op);
  }
  std::cout<<"{\"code\":"<<code<<",\"version\":"<<io.get_last_version()<<",\"raw_cmp_per_op_result_available\":false}\n";
  io.close();client.shutdown();return 0;
 } catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
