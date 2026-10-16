// Test-only pinned Tentacle public C++ WRITESAME oracle. LGPL-2.1 public
// headers, no Ceph implementation copied. Operates only fixture-owned names
// admitted by the bounded Python wrapper. Never a product dependency.
#include "include/rados/librados.hpp"
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

uint64_t number(const char* text) {
 std::string v(text);size_t used=0;
 if(v.empty()||v.find_first_not_of("0123456789")!=std::string::npos)throw std::runtime_error("invalid decimal");
 auto n=std::stoull(v,&used);if(used!=v.size())throw std::runtime_error("invalid decimal suffix");return n;
}
std::string pattern(const char* text) {
 std::string v(text),out;
 if(v.empty()||v.size()>512||v.size()%2)throw std::runtime_error("invalid pattern size");
 for(size_t i=0;i<v.size();i+=2){auto a=v.find_first_not_of("0123456789abcdef",i);if(a!=std::string::npos)throw std::runtime_error("invalid hex");out+=static_cast<char>(std::stoul(v.substr(i,2),nullptr,16));}
 return out;
}
int main(int argc,char** argv) {
 try {
  if(argc!=11)throw std::runtime_error("usage: oracle CONF KEYRING POOL OBJECT ACTION OFFSET LENGTH PATTERN_HEX FLAGS SNAP");
  std::string action(argv[5]);auto off=number(argv[6]),len=number(argv[7]),flags=number(argv[9]),snap=number(argv[10]);auto bytes=pattern(argv[8]);
  if((action!="plain"&&action!="append"&&action!="guard"&&action!="failok"&&action!="snapshot")||off>(1ULL<<32)||len>4096||len%bytes.size()||(flags!=0&&flags!=2)||(action=="failok")!=(flags==2)||(action=="snapshot")!=(snap>0))throw std::runtime_error("outside closed writesame bounds");
  librados::Rados client;int code=client.init2("client.test","ceph",0);
  if(code>=0)code=client.conf_read_file(argv[1]);if(code>=0)code=client.conf_set("keyring",argv[2]);if(code>=0)code=client.connect();
  if(code<0)throw std::runtime_error("native connect "+std::to_string(code));
  librados::IoCtx io;code=client.ioctx_create(argv[3],io);if(code<0)throw std::runtime_error("native pool "+std::to_string(code));
  if(snap){std::vector<uint64_t> snaps{snap};code=io.selfmanaged_snap_set_write_ctx(snap,snaps);if(code<0)throw std::runtime_error("native write context "+std::to_string(code));}
  librados::ObjectWriteOperation op;librados::bufferlist data;data.append(bytes);op.writesame(off,len,data);
  if(flags)op.set_op_flags2(librados::OP_FAILOK);
  if(action=="guard"){librados::bufferlist expected;expected.append(std::string("never"));op.cmpxattr("marker",LIBRADOS_CMPXATTR_OP_EQ,expected);}
  if(action=="append"||action=="failok"){librados::bufferlist tail;tail.append(std::string("tail\0\xff",6));op.append(tail);}
  if(action=="guard"||action=="append"||action=="failok"){librados::bufferlist value;value.append(std::string("writesame-native-marker"));op.setxattr("marker",value);op.rmxattr("remove");}
  time_t stamp=1700000001;op.mtime(&stamp);code=io.operate(argv[4],&op);
  std::cout<<"{\"code\":"<<code<<",\"version\":"<<io.get_last_version()<<",\"raw_writesame_per_op_result_available\":false}\n";
  io.close();client.shutdown();return 0;
 }catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
