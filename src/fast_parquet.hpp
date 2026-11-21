#pragma once
// Tiny self-contained writer for the seven primitive types required by this
// competition. Standard Parquet V1, PLAIN, UNCOMPRESSED, Thrift Compact footer.
// It deliberately does not depend on Arrow, DuckDB or Python at runtime.
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <algorithm>

namespace fastparquet {
using u8=uint8_t;using u32=uint32_t;using i64=int64_t;
static inline void leb(std::string& s,uint64_t x){while(x>=128){s.push_back(char((x&127)|128));x>>=7;}s.push_back(char(x));}
static inline void le32(std::string& s,u32 x){for(int i=0;i<4;i++){s.push_back(char(x));x>>=8;}}
static inline void le64(std::string& s,uint64_t x){for(int i=0;i<8;i++){s.push_back(char(x));x>>=8;}}

// Thrift TCompactProtocol. Each struct has its own field-id delta context.
class Thrift {
 std::vector<int> parent;
 int last=0;
public:
 std::string b;
 void begin(){parent.push_back(last);last=0;}
 void end(){b.push_back(char(0));last=parent.back();parent.pop_back();}
 void field(int id,u8 type){int delta=id-last;
  if(delta>0&&delta<=15)b.push_back(char((delta<<4)|type));
  else {b.push_back(char(type));varint(uint64_t(id<<1));}
  last=id;
 }
 void varint(uint64_t v){leb(b,v);}
 void number(int id,i64 value,u8 type=6) {
  field(id,type);varint((uint64_t(value)<<1)^uint64_t(value>>63));
 }
 void text(int id,std::string_view v){field(id,8);str(v);}
 void str(std::string_view v){varint(v.size());b.append(v.data(),v.size());}
 void list(int id,size_t n,u8 elemType){field(id,9);list_head(n,elemType);}
 void list_head(size_t n,u8 elemType){
  if(n<15)b.push_back(char((n<<4)|elemType));
  else{b.push_back(char(0xf0|elemType));varint(n);}
 }
 void struct_field(int id){field(id,12);begin();}
};

enum Type:int {INT32=1,INT64=2,BYTE_ARRAY=6};
struct Column {
 std::string name;
 Type type;
 bool optional=false;
 int converted=-1; // UTF8 = 0, DATE = 6
};
struct Value {
 bool valid=true;
 std::string_view text;
 i64 number=0;
};
struct ChunkMeta {
 i64 offset=0, size=0, num=0;
};
struct RowMeta {
 std::vector<ChunkMeta> chunks;
 i64 num=0,offset=0,total=0;
};
static inline std::string page_header(int n,int bytes){
 Thrift t;t.begin();
 t.number(1,0,5); // DATA_PAGE
 t.number(2,bytes,5);t.number(3,bytes,5);
 t.struct_field(5);
 t.number(1,n,5);t.number(2,0,5);t.number(3,3,5);t.number(4,3,5);
 t.end();t.end();return std::move(t.b);
}
static inline void add_schema(Thrift& t,const Column& c) {
 t.begin();t.number(1,c.type,5);t.number(3,c.optional?1:0,5);
 t.text(4,c.name);
 if(c.converted>=0)t.number(6,c.converted,5);
 t.end();
}
static inline void add_chunk(Thrift& t,const Column& c,const ChunkMeta& ch) {
 t.begin();t.number(2,0);t.struct_field(3);
 t.number(1,c.type,5);
 t.list(2,c.optional?2:1,5);
 t.varint(0);if(c.optional)t.varint(6); // PLAIN=0; RLE=3, zigzag(3)=6
 t.list(3,1,8);t.str(c.name);
 t.number(4,0,5); // UNCOMPRESSED
 t.number(5,ch.num);t.number(6,ch.size);t.number(7,ch.size);
 t.number(9,ch.offset);
 t.end();t.end();
}
static inline void add_row(Thrift& t,const std::vector<Column>& cols,const RowMeta& rg){
 t.begin();t.list(1,cols.size(),12);
 for(size_t i=0;i<cols.size();i++)add_chunk(t,cols[i],rg.chunks[i]);
 t.number(2,rg.total);t.number(3,rg.num);
 t.number(5,rg.offset);t.number(6,rg.total);
 t.end();
}
static inline std::string footer(const std::vector<Column>& cols,const std::vector<RowMeta>& rows,i64 count){
 Thrift t;t.begin();t.number(1,1,5);
 t.list(2,cols.size()+1,12);
 t.begin();t.text(4,"schema");t.number(5,cols.size(),5);t.end();
 for(auto& c:cols)add_schema(t,c);
 t.number(3,count);
 t.list(4,rows.size(),12);
 for(auto& rg:rows)add_row(t,cols,rg);
 t.text(6,"velocity-native 5.0");t.end();return std::move(t.b);
}

template<class Getter>
void write(const std::string& path,const std::vector<Column>& cols,size_t nrows,Getter get){
 std::ofstream out(path,std::ios::binary|std::ios::trunc);
 if(!out)throw std::runtime_error("cannot open parquet output: "+path);
 out.write("PAR1",4);
 const size_t GROUP=262144,PAGE=32768;
 std::vector<RowMeta> groups;
 for(size_t g=0;g<nrows;g+=GROUP){
  size_t count=std::min(GROUP,nrows-g);
  RowMeta rg;rg.num=count;rg.offset=out.tellp();
  rg.chunks.reserve(cols.size());
  for(size_t col=0;col<cols.size();++col){
   ChunkMeta cm;cm.num=count;cm.offset=out.tellp();
   for(size_t off=0;off<count;off+=PAGE){
    int len=int(std::min(PAGE,count-off));
    std::string payload;
    payload.reserve(size_t(len)*(cols[col].type==BYTE_ARRAY?18:
      cols[col].type==INT32?4:8)+512);
    if(cols[col].optional){
     std::string defs;
     int blocks=(len+7)/8;
     defs.reserve(size_t(blocks)+8);
     leb(defs,uint64_t(blocks*2+1));
     for(int b=0;b<blocks;b++){
      u8 bits=0;
      for(int j=0;j<8;j++)if(b*8+j<len && get(g+off+b*8+j,col).valid)bits|=u8(1<<j);
      defs.push_back(char(bits));
     }
     le32(payload,u32(defs.size()));payload+=defs;
    }
    for(int i=0;i<len;++i){
     const Value v=get(g+off+i,col);
     if(!v.valid)continue;
     if(cols[col].type==BYTE_ARRAY){le32(payload,u32(v.text.size()));payload.append(v.text.data(),v.text.size());}
     else if(cols[col].type==INT32)le32(payload,u32(v.number));
     else le64(payload,uint64_t(v.number));
    }
    auto hdr=page_header(len,int(payload.size()));
    out.write(hdr.data(),hdr.size());out.write(payload.data(),payload.size());
   }
   cm.size=i64(out.tellp())-cm.offset;rg.total+=cm.size;
   rg.chunks.push_back(cm);
  }
  groups.push_back(std::move(rg));
 }
 std::string meta=footer(cols,groups,nrows);
 out.write(meta.data(),meta.size());
 std::string end;le32(end,u32(meta.size()));end.append("PAR1",4);
 out.write(end.data(),end.size());
 if(!out)throw std::runtime_error("failed writing parquet: "+path);
}
}
