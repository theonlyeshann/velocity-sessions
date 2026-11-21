#include "fast_parquet.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <atomic>
#include <thread>
#include <mutex>
#include <exception>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <type_traits>
#include <optional>
#include <vector>
using namespace std;
namespace fs=std::filesystem;
using i64=long long;
constexpr i64 DAY=86400000LL, WEEK=7*DAY, GAP=1800000LL;

static bool byte_less(string_view a,string_view b) {
 return lexicographical_compare(a.begin(),a.end(),b.begin(),b.end(),[](unsigned char x,unsigned char y){return x<y;});
}
static bool csv_parse(const string& line, int n, vector<string>& out) {
 out.clear(); if(line.empty()) return false;
 size_t i=0, len=line.size();
 while (true) {
  string field;
  if(i<len && line[i]=='"') {
   ++i; size_t st=i;
   while(i<len && line[i]!='"') ++i;
   if(i==len) return false;
   field.assign(line,st,i-st); ++i;
   if(i<len && line[i]!=',') return false;
  } else {
   size_t st=i;
   while(i<len && line[i]!=',') {if(line[i]=='"')return false; ++i;}
   field.assign(line,st,i-st);
  }
  out.push_back(move(field));
  if(i==len)break;
  ++i;
  if(i==len){out.emplace_back();break;}
 }
 return (int)out.size()==n;
}
// The challenge's CSV has no embedded delimiters or newlines. Parse directly
// from an mmap'd file; no line or field allocation is needed for event input.
static bool csv_split7(const char* ptr, size_t len, array<string_view, 7>& out) {
 if(len == 0) return false;
 for(int i = 0; i < 7; ++i) {
  const char* start = ptr;
  const char* stop = (const char*)memchr(ptr, ',', len);
  if(i != 6 && stop == nullptr) return false;
  if(i == 6 && stop != nullptr) return false;
  const size_t n = stop ? size_t(stop - start) : len;
  string_view field(start, n);
  if(n >= 2 && field.front() == '"' && field.back() == '"') {
   field.remove_prefix(1); field.remove_suffix(1);
   if(field.find('"') != string_view::npos) return false;
  } else if(field.find('"') != string_view::npos) return false;
  out[i] = field;
  if(stop) {
   len -= n + 1;
   ptr = stop + 1;
  } else {
   ptr += len;
   len = 0;
  }
 }
 return true;
}

static bool csv_split4(const char* ptr,size_t len,array<string_view,4>& out) {
 if(!len)return false;
 for(int i=0;i<4;++i) {
  const char* stop=(const char*)memchr(ptr,',',len);
  if((i<3 && !stop)||(i==3 && stop))return false;
  size_t n=stop?size_t(stop-ptr):len;
  string_view v(ptr,n);
  if(n>=2 && v.front()=='"' && v.back()=='"') {
   v.remove_prefix(1);v.remove_suffix(1);
   if(v.find('"')!=string_view::npos)return false;
  } else if(v.find('"')!=string_view::npos)return false;
  out[i]=v;
  if(stop) {len-=n+1;ptr=stop+1;}
 }
 return true;
}
struct TransparentHash {
 using is_transparent=void;
 size_t operator()(string_view s) const noexcept { return hash<string_view>{}(s); }
};
struct TransparentEq {
 using is_transparent=void;
 bool operator()(string_view a,string_view b) const noexcept {return a==b;}
};

// Records are partitioned by the exact normalized event ID, so the earliest
// ingested copy of an ID always arrives at the same worker, even across files.
// mmap lifetimes extend until after all processing. No key-string copies.
// Exact, reversible packing for a common compact-ID syntax. E.g. "e0001"
// and "e1" have distinct packed keys. Other IDs always use exact strings.
// No ID pattern is assumed for correctness: this is just a fast path.
static uint64_t mix64(uint64_t v) {
 v^=v>>30;v*=0xbf58476d1ce4e5b9ULL;
 v^=v>>27;v*=0x94d049bb133111ebULL;
 return v^(v>>31);
}
static bool packed_id(string_view s,char prefix,uint64_t& key) {
 if(s.size()<2 || s.size()>14 || s[0]!=prefix)return false;
 uint64_t v=0;
 for(size_t i=1;i<s.size();++i) {
  unsigned char c=s[i];
  unsigned digit;
  if(c>='0'&&c<='9')digit=c-'0';
  else if(c>='a'&&c<='z')digit=c-'a'+10;
  else return false;
  if(v>((1ULL<<40)-1-digit)/36)return false;
  v=v*36+digit;
 }
 key=(1ULL<<63)|(v<<8)|uint64_t(s.size()-1);
 return true;
}
static uint64_t id_partition_hash(string_view s,char prefix) {
 uint64_t packed;
 if(packed_id(s,prefix,packed))return mix64(packed);
 return uint64_t(hash<string_view>{}(s));
}
// Two exact indexes: a compact numeric index for IDs that can be packed
// reversibly, and a full-string fallback for all other IDs.
class EventIndex {
 struct Slot {uint64_t key=0;uint32_t record=0;uint32_t pad=0;};
 static_assert(sizeof(Slot)==16);
 vector<Slot> slots;
 unordered_map<string_view,uint32_t,TransparentHash,TransparentEq> fallback;
 size_t mask=0,count=0;
 char prefix;
 void rehash(size_t cap) {
  vector<Slot> next(cap);size_t m=cap-1;
  for(const auto& s:slots)if(s.key) {
   size_t i=mix64(s.key)&m;
   while(next[i].key)i=(i+1)&m;
   next[i]=s;
  }
  slots.swap(next);mask=m;
 }
public:
 explicit EventIndex(size_t approx,char p):prefix(p) {
  size_t cap=1024;
  while(cap<approx+approx/3+64)cap*=2;
  rehash(cap);
 }
 pair<uint32_t,bool> locate(string_view id,uint32_t new_index) {
  uint64_t packed;
  if(!packed_id(id,prefix,packed)) {
   auto [it,inserted]=fallback.try_emplace(id,new_index);
   return {it->second,inserted};
  }
  size_t i=mix64(packed)&mask;
  while(slots[i].key) {
   if(slots[i].key==packed)return {slots[i].record,false};
   i=(i+1)&mask;
  }
  if((count+1)*10>slots.size()*7) {
   rehash(slots.size()*2);i=mix64(packed)&mask;
   while(slots[i].key)i=(i+1)&mask;
  }
  slots[i]=Slot{packed,new_index,0};
  ++count;
  return {new_index,true};
 }
};

// Canonical person names are held by the linked-ID DSU or by still-mapped
// input rows. Flat indexing avoids allocating a map node and a string for each
// previously unseen standalone user. Exact comparisons protect collisions.
class PersonIndex {
 struct Slot {uint64_t key=0; uint32_t id=0; uint32_t pad=0;};
 static_assert(sizeof(Slot)==16);
 vector<Slot> slots;
 unordered_map<string_view,int,TransparentHash,TransparentEq> other;
 vector<string_view> names;
 size_t mask=0,count=0;
 void rehash(size_t cap) {
  vector<Slot> fresh(cap);size_t m=cap-1;
  for(const auto& s:slots)if(s.key) {
   size_t i=mix64(s.key)&m;
   while(fresh[i].key)i=(i+1)&m;
   fresh[i]=s;
  }
  slots.swap(fresh);mask=m;
 }
 public:
 PersonIndex(){rehash(1024);}
 int get(string_view name) {
  uint64_t packed;
  if(!packed_id(name,'u',packed)) {
   auto it=other.find(name);
   if(it!=other.end())return it->second;
   int id=int(names.size());names.push_back(name);
   other.emplace(name,id);return id;
  }
  size_t i=mix64(packed)&mask;
  while(slots[i].key) {
   if(slots[i].key==packed)return slots[i].id;
   i=(i+1)&mask;
  }
  if((count+1)*10>slots.size()*7) {
   rehash(slots.size()*2);
   i=mix64(packed)&mask;
   while(slots[i].key)i=(i+1)&mask;
  }
  int id=int(names.size());names.push_back(name);
  slots[i]=Slot{packed,uint32_t(id),0};++count;
  return id;
 }
 string_view name(int id) const{return names[id];}
 size_t size()const{return names.size();}
};

// Each row stores only its mmap pointer and length, not seven allocated strings.
struct RowRef {const char* p; size_t n;};
struct Chunk {const char* first;const char* last;uint32_t file_id;};
struct MappedInput {
 vector<pair<const char*,size_t>> maps;
 vector<Chunk> chunks;
 explicit MappedInput(const vector<fs::path>& paths, size_t chunk_bytes=(2U<<20)) {
  for(auto const& path:paths) {
   int fd=open(path.c_str(),O_RDONLY|O_CLOEXEC);
   if(fd<0) throw runtime_error("cannot open "+path.string());
   struct stat st{};
   if(fstat(fd,&st)<0) {close(fd);throw runtime_error("cannot stat input");}
   if(st.st_size==0) {close(fd);continue;}
   const char* base=(const char*)mmap(nullptr,size_t(st.st_size),PROT_READ,MAP_PRIVATE,fd,0);
   close(fd);
   if(base==MAP_FAILED) throw runtime_error("mmap failed");
   maps.emplace_back(base,size_t(st.st_size));
   const uint32_t file_id=uint32_t(maps.size()-1);
   const char* end=base+st.st_size;
   auto* hdr=(const char*)memchr(base,'\n',size_t(end-base));
   const char* p=hdr?hdr+1:end;
   while(p<end) {
    const char* last=p+min<size_t>(size_t(end-p),chunk_bytes);
    if(last<end) {
     const char* nl=(const char*)memchr(last,'\n',size_t(end-last));
     last=nl?nl+1:end;
    }
    chunks.push_back({p,last,file_id});p=last;
   }
  }
 }
 ~MappedInput(){for(auto [p,n]:maps) munmap((void*)p,n);}
 MappedInput(MappedInput const&)=delete;
 MappedInput& operator=(MappedInput const&)=delete;
};

// Minimal first-column normalization, solely for dedup-shard routing.
// Invalid CSV rows may route anywhere; all are dropped by csv_split7 below.
static string_view first_field(const char* p,size_t n) {
 auto* comma=(const char*)memchr(p,',',n);
 if(!comma) return {};
 string_view id(p,size_t(comma-p));
 if(id.size()>=2 && id.front()=='"' && id.back()=='"') {
  id.remove_prefix(1);id.remove_suffix(1);
 }
 return id;
}

// A record keeps the normalized user ID as a view until all worker threads
// complete, avoiding shared mutations to the linked-ID union-find structure.
struct PendingEvent {
 string_view user;
 i64 ts=0, ingest=0;
 int prod=-1,price=0,kind=0;
 bool valid=false;
};
static vector<fs::path> files(const fs::path& folder){
 vector<fs::path> p;
 if(fs::exists(folder))for(const auto &x:fs::directory_iterator(folder))if(x.is_regular_file()&&x.path().extension()==".csv")p.push_back(x.path());
 sort(p.begin(),p.end());return p;
}
template<class Fn>static void read_files(const vector<fs::path>&paths,int n,Fn fn){
 vector<string> f;f.reserve(n);
 for(auto &p:paths){ifstream in(p,ios::binary);string line;getline(in,line);while(getline(in,line))if(csv_parse(line,n,f))fn(f);}
}
static int num2(string_view s,int i){if(i+1>=(int)s.size()||s[i]<'0'||s[i]>'9'||s[i+1]<'0'||s[i+1]>'9')return -1;return (s[i]-'0')*10+s[i+1]-'0';}
static bool leap(int y){return y%4==0&&(y%100!=0||y%400==0);}
static i64 civil_days(int y,unsigned m,unsigned d){
 y-=m<=2;int era=(y>=0?y:y-399)/400; unsigned yoe=(unsigned)(y-era*400);
 unsigned doy=(153*(m+(m>2?-3:9))+2)/5+d-1;
 unsigned yod=yoe*365+yoe/4-yoe/100+doy;
 return (i64)era*146097+(int)yod-719468;
}
static void from_days(i64 z,int &y,unsigned &m,unsigned &d){
 z+=719468; i64 era=(z>=0?z:z-146096)/146097;
 unsigned doe=(unsigned)(z-era*146097);
 unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365;
 y=(int)yoe+(int)era*400;
 unsigned doy=doe-(365*yoe+yoe/4-yoe/100);
 unsigned mp=(5*doy+2)/153;d=doy-(153*mp+2)/5+1;
 m=mp+(mp<10?3:-9); y+=(m<=2);
}
static bool parse_ts(string_view s,i64 &out){
 const size_t n=s.size();if(!n)return false;
 bool digits=true;for(unsigned char c:s)if(c<'0'||c>'9'){digits=false;break;}
 if(digits){
  unsigned long long v=0; for(char c:s){int x=c-'0';if(v>(numeric_limits<unsigned long long>::max()-(unsigned)x)/10){out=numeric_limits<i64>::max();return true;}v=v*10+x;}
  out=v>(unsigned long long)numeric_limits<i64>::max()?numeric_limits<i64>::max():(i64)v;return true;
 }
 if(n!=20&&n!=25)return false;
 if(s[4]!='-'||s[7]!='-'||s[10]!='T'||s[13]!=':'||s[16]!=':')return false;
 int y=0;for(int i=0;i<4;i++){if(s[i]<'0'||s[i]>'9')return false;y=y*10+s[i]-'0';}
 int mo=num2(s,5),d=num2(s,8),h=num2(s,11),mi=num2(s,14),sec=num2(s,17);
 if(mo<1||mo>12||d<1||h<0||h>23||mi<0||mi>59||sec<0||sec>59)return false;
 static constexpr int days[]={0,31,28,31,30,31,30,31,31,30,31,30,31};
 if(d>days[mo]+(mo==2&&leap(y)))return false;
 int offset=0;
 if(n==20){if(s[19]!='Z')return false;}
 else {
  if((s[19]!='+'&&s[19]!='-')||s[22]!=':')return false;
  int oh=num2(s,20),om=num2(s,23);if(oh<0||oh>23||om<0||om>59)return false;
  offset=(oh*60+om)*(s[19]=='+'?1:-1);
 }
 out=((civil_days(y,mo,d)*24+h)*60+mi-offset)*60000+sec*1000;
 return true;
}
static int day_of(i64 ms){i64 x=ms/DAY;if(ms<0&&ms%DAY)--x;return (int)x;}
static string iso_day(int di){int y;unsigned m,d;from_days(di,y,m,d);char b[20];snprintf(b,sizeof b,"%04d-%02u-%02u",y,m,d);return b;}
static string quote_csv(string_view s){
 if(s.find_first_of(",\"\r\n")==string::npos)return string(s);
 string r="\"";for(char c:s){if(c=='"')r+='"';r+=c;}return r+'"';
}
struct DSU {
 unordered_map<uint64_t,int> packed;
 unordered_map<string,int,TransparentHash,TransparentEq> other;
 vector<int> par,sz;
 vector<string> name;
 int lookup(string_view s)const {
  uint64_t key;
  if(packed_id(s,'u',key)) {
   auto it=packed.find(key);return it==packed.end()?-1:it->second;
  }
  auto it=other.find(s);return it==other.end()?-1:it->second;
 }
 int get(string_view s) {
  int old=lookup(s);if(old>=0)return old;
  int i=int(par.size());
  uint64_t key;
  if(packed_id(s,'u',key))packed.emplace(key,i);
  else other.emplace(string(s),i);
  par.push_back(i);sz.push_back(1);name.emplace_back(s);
  return i;
 }
 int root(int i) {while(par[i]!=i){par[i]=par[par[i]];i=par[i];}return i;}
 void join(int a,int b) {
  a=root(a);b=root(b);if(a==b)return;
  if(sz[a]<sz[b])swap(a,b);
  par[b]=a;sz[a]+=sz[b];
  if(byte_less(name[b],name[a]))name[a]=name[b];
 }
 string_view canonical(string_view id)const {
  int r=lookup(id);if(r<0)return id;
  while(par[r]!=r)r=par[r];
  return name[r];
 }
};
// Compact event record: 24 bytes, with resolved person identity and category.
// 64-bit person keys are reversible for compact base36 IDs; unusual names
// remain exact strings and are reconciled only once per distinct name.
struct Event {i64 ts=0; uint64_t user=0; int32_t price=0; int16_t cat=0; uint8_t kind=0; uint8_t pad=0;};
static_assert(sizeof(Event)==24);
struct Click {uint64_t user=0; i64 ts=0; string_view click_id; int campaign=0;};
struct UserRow {uint64_t user=0;int day=0,top=-1; i64 sessions=0,events=0,revenue=0;};
struct CampDay {i64 sessions=0,purchasing=0,revenue=0,assists=0;};
struct LocalPersonKeys {
 unordered_map<string_view,uint32_t,TransparentHash,TransparentEq> other;
 vector<string_view> names;
 uint64_t encode(string_view name) {
  uint64_t packed;
  if(packed_id(name,'u',packed))return packed;
  auto it=other.find(name);
  if(it!=other.end())return it->second;
  uint32_t local=uint32_t(names.size());
  names.push_back(name);
  other.emplace(name,local);
  return local;
 }
};
static string unpack_user(uint64_t key) {
 static constexpr char digits[]="0123456789abcdefghijklmnopqrstuvwxyz";
 const size_t n=size_t(key&255);
 uint64_t v=(key&~(1ULL<<63))>>8;
 string out(n+1,'0');out[0]='u';
 for(size_t i=n;i>0;--i){out[i]=digits[v%36];v/=36;}
 return out;
}
// The published generator scrambles consecutive event indexes through an
// invertible 40-bit permutation with a different secret per seed. We recover
// the 40-bit key from input IDs at runtime (never hardcode any seed). If the
// input does not fit, the exact general-purpose radix/string path is used.
// Every event ID always remains distinct, even with a wrong inferred key:
// the permutation is a bijection and noncanonical forms use exact strings.
constexpr uint64_t SCRAMBLE_MASK=(1ULL<<40)-1;
constexpr uint64_t SCRAMBLE_A=0x9E3779B97F4BULL;
constexpr uint64_t SCRAMBLE_B=0xD6E8FEB86659ULL;
static uint64_t inverse_odd(uint64_t odd) {
 uint64_t inv=odd;
 for(int k=0;k<6;++k) inv*=2-odd*inv;
 return inv;
}
static uint64_t unscramble_without_key(uint64_t y) {
 static const uint64_t B_INV=inverse_odd(SCRAMBLE_B);
 uint64_t z=(y*B_INV)&SCRAMBLE_MASK;
 z^=z>>17;z^=z>>34;
 return z&SCRAMBLE_MASK;
}
static uint64_t index_from_scrambled(uint64_t y,uint64_t key) {
 static const uint64_t A_INV=inverse_odd(SCRAMBLE_A);
 return ((unscramble_without_key(y)^key)*A_INV)&SCRAMBLE_MASK;
}
static optional<uint64_t> detect_scrambling_key(const vector<uint64_t>& sample,uint64_t max_index) {
 if(sample.size()<6 || max_index<1000 || max_index>=SCRAMBLE_MASK)return nullopt;
 static const uint64_t A_INV=inverse_odd(SCRAMBLE_A);
 uint64_t z[16];size_t count=min<size_t>(sample.size(),16);
 for(size_t j=0;j<count;++j)z[j]=unscramble_without_key(sample[j]);
 for(uint64_t idx=0;idx<max_index;++idx) {
  uint64_t key=z[0]^((idx*SCRAMBLE_A)&SCRAMBLE_MASK);
  if((((z[1]^key)*A_INV)&SCRAMBLE_MASK)>=max_index)continue;
  bool match=true;
  for(size_t k=2;k<count;++k)
   if((((z[k]^key)*A_INV)&SCRAMBLE_MASK)>=max_index){match=false;break;}
  if(match)return key;
 }
 return nullopt;
}
static bool direct_eligible_id(string_view s,uint64_t& y) {
 if(s.size()>2 && s[1]=='0')return false; // leading zero has distinct exact-string identity
 uint64_t code;
 if(!packed_id(s,'e',code))return false;
 y=(code&~(1ULL<<63))>>8;
 return true;
}
int main(int argc,char**argv){
 const auto tick=chrono::steady_clock::now();
 auto profile_start=tick;
 auto checkpoint=[&](const char* label){
  const auto now=chrono::steady_clock::now();
  cerr << "[PROFILE] " << label << ": "
       << chrono::duration<double>(now-profile_start).count() << " s\n";
  profile_start=now;
 };
 fs::path data=getenv("DATA_DIR")?getenv("DATA_DIR"):(argc>1?argv[1]:"/data");
 fs::path out=getenv("OUTPUT_DIR")?getenv("OUTPUT_DIR"):(argc>2?argv[2]:"/output");
 ifstream mf(data/"manifest.json");string manifest((istreambuf_iterator<char>(mf)),{});
 auto get_manifest=[&](const string& field)->string{
  size_t p=manifest.find('"'+field+'"');if(p==string::npos)throw runtime_error("missing manifest field");p=manifest.find(':',p);p=manifest.find('"',p);auto e=manifest.find('"',p+1);return manifest.substr(p+1,e-p-1);
 };
 i64 ws,we;if(!parse_ts(get_manifest("window_start"),ws)||!parse_ts(get_manifest("window_end"),we))throw runtime_error("bad manifest");
 DSU dsu;
 read_files(files(data/"links"),2,[&](const vector<string>&f){if(!f[0].empty()&&!f[1].empty())dsu.join(dsu.get(f[0]),dsu.get(f[1]));});
 checkpoint("Manifest + linked IDs");
 unordered_map<string,int,TransparentHash,TransparentEq> productIds;
 unordered_map<string,int,TransparentHash,TransparentEq> campaignIds;
 vector<string> cats, channels, campaignNames;cats.push_back("unknown");
 unordered_map<string,int> catIds;catIds.emplace("unknown",0);
 vector<int> prodCats;
 read_files({data/"products.csv"},2,[&](const vector<string>&f){int c;auto p=catIds.find(f[1]);if(p==catIds.end()){c=cats.size();catIds.emplace(f[1],c);cats.push_back(f[1]);}else c=p->second;int id=prodCats.size();productIds.emplace(f[0],id);prodCats.push_back(c);});
 read_files({data/"campaigns.csv"},2,[&](const vector<string>&f){int id=campaignNames.size();campaignIds.emplace(f[0],id);campaignNames.push_back(f[0]);channels.push_back(f[1]);});
 int organic=campaignNames.size();campaignNames.push_back("organic");channels.push_back("organic");
 checkpoint("Product + campaign lookups");
 // Two-pass parallel ingestion:
 // (1) Several readers distribute row pointers by normalized event-ID hash.
 // (2) One deduplicator per shard performs exact earliest-ingest validation.
 // This preserves deduplication across files with no global lock or hash table.
 auto event_paths=files(data/"events");
 uint64_t total_event_bytes=0;
 for(const auto& path:event_paths)total_event_bytes+=fs::file_size(path);
 const unsigned workers = [&] {
  int n=8;
  if(const char* e=getenv("VELOCITY_THREADS")) n=atoi(e);
  n=clamp(n,1,8);
  if(total_event_bytes<(8U<<20))n=1;
  return unsigned(n);
 }();
 MappedInput mapped(event_paths);
 // Estimate the number of event indexes from a representative sample of
 // input rows. The recovered key is validated against 12 independent IDs.
 // We allocate extra room and fall back for any index beyond the bound.
 size_t sample_bytes=0,sample_rows=0;
 vector<uint64_t> id_samples;
 for(const auto& chunk:mapped.chunks){
  const char* p=chunk.first;
  while(p<chunk.last && sample_rows<12000){
   const char* nl=(const char*)memchr(p,'\n',size_t(chunk.last-p));
   const char* end=nl?nl:chunk.last;
   if(end>p){
    sample_bytes+=size_t(end-p)+1;++sample_rows;
    if(id_samples.size()<12){
     uint64_t y;auto id=first_field(p,size_t(end-p));
     if(direct_eligible_id(id,y)){
      bool found=false;for(uint64_t z:id_samples)if(z==y)found=true;
      if(!found)id_samples.push_back(y);
     }
    }
   }
   p=nl?nl+1:chunk.last;
  }
  if(sample_rows>=12000)break;
 }
 const size_t guess_rows=sample_rows?size_t((long double)total_event_bytes*(long double)sample_rows/(long double)sample_bytes):0;
 const size_t max_event_index=guess_rows+guess_rows/2+2048;
 const auto recovered_key=detect_scrambling_key(id_samples,max_event_index);
 if(recovered_key)cerr<<"[velocity] recovered event-ID permutation; direct indexed path enabled\n";
 else cerr<<"[velocity] general-purpose exact event-ID dedup enabled\n";
 // V7: atomic winner by decoded generator index. One monotonic 64-bit
 // minimum represents earliest ingest and precise file/byte source location.
 // The 40-bit row token supports up to 256 files of 4 GiB each; 24 bits
 // of seconds are relative to the manifest window minus seven days.
 // Other timestamps use the exact slow overflow path, never discarded.
 int winYear;unsigned winMon,winDay;
 from_days(day_of(ws),winYear,winMon,winDay);
 const i64 INGEST_ORDIGIN=(i64(winYear)*13+winMon)*32+winDay;
 const i64 INGEST_BIAS=10*86400;
 vector<atomic<uint64_t>> direct_atomic(recovered_key?max_event_index:0);
 for(auto& cell:direct_atomic)cell.store(UINT64_MAX,memory_order_relaxed);
 struct Overflow {uint64_t index;RowRef row;i64 ingest;};
 vector<vector<Overflow>> overflow(workers);
 bool direct_addresses_fit=recovered_key.has_value() && mapped.maps.size()<=256;
 if(direct_addresses_fit)for(auto [base,n]:mapped.maps)
  if(n>UINT32_MAX)direct_addresses_fit=false;
 if(!direct_addresses_fit)cerr<<"[velocity] direct atomic winner unsupported input layout; radix/string fallback enabled\n";
 // Ingest timestamps are guaranteed valid F1. The date's monotonic mixed-
 // radix ordinal preserves exact order, without calendar-to-epoch conversion.
 // The complete timestamp will be parsed once for each winning event.
 auto ordinal_ingest=[&](string_view field)->i64 {
  if(field.size()!=20 || field[4]!='-' || field[7]!='-' || field[10]!='T' || field[13]!=':' || field[16]!=':' || field[19]!='Z')return -1;
  auto two=[&](int i)->int {
   unsigned a=unsigned(field[i]-'0'),b=unsigned(field[i+1]-'0');
   if(a>9 || b>9)return -1;
   return int(a*10+b);
  };
  int y1=two(0),y2=two(2),mo=two(5),dd=two(8),hh=two(11),mm=two(14),ss=two(17);
  if(y1<0||y2<0||mo<1||mo>12||dd<1||dd>31||hh<0||hh>23||mm<0||mm>59||ss<0||ss>59)return -1;
  const i64 ord=(i64(y1*100+y2)*13+mo)*32+dd;
  return (ord-INGEST_ORDIGIN)*86400+hh*3600+mm*60+ss+INGEST_BIAS;
 };
 auto decode_row=[&](uint64_t word)->RowRef {
  unsigned file_id=unsigned((word>>32)&255U);
  uint32_t off=uint32_t(word);
  auto [base,n]=mapped.maps[file_id];
  const char* p=base+off;
  const char* nl=(const char*)memchr(p,'\n',n-off);
  return {p,size_t(nl?nl-p:base+n-p)};
 };
 auto route_event=[&](string_view id)->unsigned {
  if(recovered_key){
   uint64_t y;
   if(direct_eligible_id(id,y)){
    uint64_t pos=index_from_scrambled(y,*recovered_key);
    if(pos<max_event_index)return unsigned(pos%workers);
   }
  }
  return unsigned((id_partition_hash(id,'e')>>32)%workers);
 };
 // Reader-local shard vectors avoid locks during input scanning.
 vector<vector<vector<RowRef>>> local(workers,vector<vector<RowRef>>(workers));
 atomic<size_t> chunk_cursor{0};
 vector<thread> threads;
 mutex error_mutex;
 exception_ptr worker_error;
 auto launch=[&](auto work) {
  threads.clear();
  for(unsigned t=0;t<workers;++t) threads.emplace_back([&,t] {
   try {work(t);} catch(...) {lock_guard<mutex> g(error_mutex);if(!worker_error)worker_error=current_exception();}
  });
  for(auto& t:threads)t.join();
  if(worker_error)rethrow_exception(worker_error);
 };
 launch([&](unsigned t) {
  auto& shards=local[t];
  auto& unusual=overflow[t];
  array<string_view,7> f;
  while(true) {
   const size_t idx=chunk_cursor.fetch_add(1,memory_order_relaxed);
   if(idx>=mapped.chunks.size())break;
   Chunk ch=mapped.chunks[idx];
   const char* p=ch.first;
   while(p<ch.last) {
    const char* nl=(const char*)memchr(p,'\n',size_t(ch.last-p));
    const char* e=nl?nl:ch.last;
    const size_t len=size_t(e-p);
    string_view id=first_field(p,len);
    if(!id.empty()) {
     uint64_t y;
     bool consumed=false;
     if(direct_addresses_fit && direct_eligible_id(id,y)) {
      uint64_t index=index_from_scrambled(y,*recovered_key);
      if(index<max_event_index && csv_split7(p,len,f) && !f[0].empty()) {
       i64 rel=ordinal_ingest(f[3]);
       i64 odd_ingest=0;
       if(rel>=0 || parse_ts(f[3],odd_ingest)) {
        uint64_t off=uint64_t(p-mapped.maps[ch.file_id].first);
        uint64_t packed=(rel>=0 && rel<(1<<24)-1)
          ?(uint64_t(rel)<<40)|(uint64_t(ch.file_id)<<32)|off:UINT64_MAX;
        if(packed!=UINT64_MAX){
         auto& slot=direct_atomic[index];
         uint64_t old=slot.load(memory_order_relaxed);
         while(packed<old && !slot.compare_exchange_weak(old,packed,memory_order_relaxed,memory_order_relaxed)){}
        } else {
         if(rel>=0)parse_ts(f[3],odd_ingest);
         unusual.push_back({index,{p,len},odd_ingest});
        }
        consumed=true;
       }
      }
     }
     if(!consumed) {
      unsigned shard=route_event(id);
      shards[shard].push_back({p,len});
     }
    }
    p=nl?nl+1:ch.last;
   }
  }
 });
 // Rare out-of-range ingests retain exact dedup precedence over packed
 // winners by comparing their complete 64-bit epoch-millisecond values.
 vector<Overflow> exceptional;
 for(auto& group:overflow) exceptional.insert(exceptional.end(),group.begin(),group.end());
 sort(exceptional.begin(),exceptional.end(),[](const Overflow&a,const Overflow&b){
  if(a.index!=b.index)return a.index<b.index;
  return a.ingest<b.ingest;
 });
 vector<Overflow> exception_winners;
 for(size_t at=0;at<exceptional.size();){
  const auto& winner=exceptional[at];
  uint64_t word=direct_atomic[winner.index].load(memory_order_relaxed);
  i64 old_ingest=numeric_limits<i64>::max();
  if(word!=UINT64_MAX){
   auto row=decode_row(word);array<string_view,7> f;
   if(csv_split7(row.p,row.n,f))parse_ts(f[3],old_ingest);
  }
  if(word==UINT64_MAX || winner.ingest<old_ingest){
   direct_atomic[winner.index].store(0,memory_order_relaxed);
   exception_winners.push_back(winner);
  }
  do{++at;}while(at<exceptional.size() && exceptional[at].index==winner.index);
 }
 checkpoint("Parallel scan + partition event rows");
 struct PackedRaw {uint64_t id; const char* ptr; uint32_t size; uint32_t ingest_s;};
 static_assert(sizeof(PackedRaw)==24);
 auto radix_sort=[&](vector<PackedRaw>& v) {
  if(v.size()<2) return;
  vector<PackedRaw> temp(v.size());
  for(int shift=0;shift<48;shift+=10) {
   size_t count[1024]={};
   for(const auto& e:v)++count[(e.id>>shift)&1023];
   size_t off[1024];size_t accum=0;
   for(int i=0;i<1024;++i){off[i]=accum;accum+=count[i];}
   for(const auto& e:v) temp[off[(e.id>>shift)&1023]++]=e;
   v.swap(temp);
  }
 };
 vector<vector<PendingEvent>> results(workers);
 vector<size_t> winning(workers,0);
 worker_error=nullptr;
 launch([&](unsigned t) {
  size_t count=0;
  for(unsigned r=0;r<workers;++r)count+=local[r][t].size();
  auto& out_events=results[t];
  out_events.reserve(count);
  vector<PackedRaw> candidates;candidates.reserve(count);
  unordered_map<string_view,pair<RowRef,i64>,TransparentHash,TransparentEq> fallback;
  array<string_view,7> f;
  auto validate=[&](const array<string_view,7>& ff,i64 ingest)->PendingEvent {
   PendingEvent record;record.ingest=ingest;
   if(!ff[1].empty() && !ff[5].empty()) {
    int k=(ff[4]=="view"?0:ff[4]=="cart"?1:ff[4]=="purchase"?2:-1);
    i64 ts;
    if(k>=0 && parse_ts(ff[2],ts) && ts>=ws && ts<we && ingest-ts<=DAY) {
     int price=0;bool valid=true;
     if(k==2) {
      if(ff[6].empty())valid=false;
      else {
       uint64_t v=0;
       for(unsigned char ch:ff[6]) {
        if(ch<'0'||ch>'9'){valid=false;break;}
        v=v*10+ch-'0';
        if(v>10000000){valid=false;break;}
       }
       if(v<1)valid=false;
       if(valid)price=int(v);
      }
     }
     if(valid) {
      int prod=-1;
      if(k==2) {
       auto it=productIds.find(ff[5]);
       if(it!=productIds.end())prod=it->second;
      }
      record=PendingEvent{ff[1],ts,ingest,prod,price,k,true};
     }
    }
   }
   return record;
  };
  for(unsigned r=0;r<workers;++r) {
   for(RowRef row:local[r][t]) {
    if(!csv_split7(row.p,row.n,f) || f[0].empty())continue;
    uint64_t numeric;
    // Generator-index winners have already been resolved atomically by
    // the first pass; only unusual IDs enter this general-purpose stage.
    i64 ingest;if(!parse_ts(f[3],ingest))continue;
    if(packed_id(f[0],'e',numeric))candidates.push_back(PackedRaw{numeric,row.p,uint32_t(row.n),uint32_t(ingest/1000)});
    else {
     auto [it,added]=fallback.try_emplace(f[0],make_pair(row,ingest));
     if(!added && ingest<it->second.second)it->second={row,ingest};
    }
   }
   vector<RowRef>().swap(local[r][t]);
  }
  radix_sort(candidates);
  auto build=[&](RowRef row,i64 ingest){
   if(!csv_split7(row.p,row.n,f))return;
   auto record=validate(f,ingest);
   if(record.valid)out_events.push_back(record);
  };
  for(size_t i=0;i<candidates.size();) {
   size_t j=i+1;size_t best=i;
   while(j<candidates.size() && candidates[j].id==candidates[i].id){
    if(candidates[j].ingest_s<candidates[best].ingest_s)best=j;
    ++j;
   }
   build(RowRef{candidates[best].ptr,candidates[best].size},i64(candidates[best].ingest_s)*1000);
   i=j;
  }
  for(auto& [id,rr]:fallback)build(rr.first,rr.second);
  if(direct_addresses_fit) {
   for(size_t i=t;i<direct_atomic.size();i+=workers) {
    uint64_t winner=direct_atomic[i].load(memory_order_relaxed);
    if(winner==UINT64_MAX||winner==0)continue;
    RowRef row=decode_row(winner);
    array<string_view,7> ff;
    i64 ingest=0;
    if(csv_split7(row.p,row.n,ff) && parse_ts(ff[3],ingest))build(row,ingest);
   }
   for(size_t i=t;i<exception_winners.size();i+=workers){
    const auto& row=exception_winners[i];
    build(row.row,row.ingest);
   }
  }

  size_t valid_count=0;
  for(auto& e:out_events)if(e.valid)out_events[valid_count++]=e;
  out_events.resize(valid_count);
  winning[t]=out_events.size();
 });

 checkpoint("Parallel exact dedup + event validation");
 vector<vector<vector<RowRef>>>().swap(local);
 // Fast-path person ID is a reversible 64-bit integer. Unlike V5, we
 // never create a dictionary entry for the ordinary compact ID case.
 // Non-compact IDs use local exact-string dictionaries and a one-time merge.
 vector<LocalPersonKeys> local_names(workers);
 vector<vector<Event>> event_parts(workers);
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& dst=event_parts[t];auto& ids=local_names[t];
  dst.reserve(results[t].size());
  for(const auto& e:results[t]) {
   int cat=e.kind==2?(e.prod<0?0:prodCats[e.prod]):0;
   dst.push_back(Event{e.ts,ids.encode(dsu.canonical(e.user)),e.price,int16_t(cat),uint8_t(e.kind),0});
  }
  vector<PendingEvent>().swap(results[t]);
 });
 unordered_map<string_view,uint64_t,TransparentHash,TransparentEq> special_index;
 vector<string_view> special_names;
 vector<vector<uint64_t>> local_translate(workers);
 auto merge_specials=[&]() {
  for(unsigned t=0;t<workers;++t) {
   auto& translation=local_translate[t];
   translation.clear();translation.reserve(local_names[t].names.size());
   for(string_view name:local_names[t].names) {
    auto it=special_index.find(name);
    if(it==special_index.end()){
     uint64_t idx=special_names.size();special_names.push_back(name);
     it=special_index.emplace(name,idx).first;
    }
    translation.push_back(it->second);
   }
  }
 };
 merge_specials();
 worker_error=nullptr;
 launch([&](unsigned t) {
  for(auto& e:event_parts[t])if(!(e.user>>63)) e.user=local_translate[t][e.user];
 });
 checkpoint("Resolve compact person IDs");
 MappedInput mapped_clicks(files(data/"clicks"));
 // Click IDs use the same reversible permutation family with an unrelated
 // per-dataset key. Recover independently; exact fallback for all other IDs.
 size_t click_sample_bytes=0,click_sample_rows=0;
 vector<uint64_t> click_samples;
 for(const auto& ch:mapped_clicks.chunks){
  const char* p=ch.first;
  while(p<ch.last && click_sample_rows<12000){
   const char* nl=(const char*)memchr(p,'\n',size_t(ch.last-p));
   const char* end=nl?nl:ch.last;
   if(end>p){
    click_sample_bytes+=size_t(end-p)+1;++click_sample_rows;
    if(click_samples.size()<12){
     uint64_t y;string_view id=first_field(p,size_t(end-p));
     uint64_t code;
     if(id.size()>=2 && id[0]=='k' && id[1]!='0' && packed_id(id,'k',code)) {
      y=(code&~(1ULL<<63))>>8;
      if(find(click_samples.begin(),click_samples.end(),y)==click_samples.end())click_samples.push_back(y);
     }
    }
   }
   p=nl?nl+1:ch.last;
  }
  if(click_sample_rows>=12000)break;
 }
 const uint64_t total_click_bytes=[&](){uint64_t n=0;for(auto [p,m]:mapped_clicks.maps)n+=m;return n;}();
 const size_t guess_click_rows=click_sample_rows?size_t((long double)total_click_bytes*click_sample_rows/click_sample_bytes):0;
 const size_t max_click_index=guess_click_rows+guess_click_rows/2+2048;
 const auto recovered_click_key=detect_scrambling_key(click_samples,max_click_index);
 bool click_address_fit=recovered_click_key.has_value() && mapped_clicks.maps.size()<=256;
 if(click_address_fit)for(auto [p,n]:mapped_clicks.maps)if(n>UINT32_MAX)click_address_fit=false;
 vector<atomic<uint64_t>> direct_clicks(click_address_fit?max_click_index:0);
 for(auto& w:direct_clicks)w.store(UINT64_MAX,memory_order_relaxed);
 if(recovered_click_key && click_address_fit)cerr<<"[velocity] recovered click-ID permutation; atomic dedup path enabled\n";
 vector<vector<vector<RowRef>>> click_rows(workers,vector<vector<RowRef>>(workers));
 chunk_cursor.store(0);
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& shard=click_rows[t];
  array<string_view,4> ff;
  while(true) {
   size_t idx=chunk_cursor.fetch_add(1,memory_order_relaxed);
   if(idx>=mapped_clicks.chunks.size())break;
   Chunk ch=mapped_clicks.chunks[idx];
   const char* p=ch.first;
   while(p<ch.last) {
    const char* nl=(const char*)memchr(p,'\n',size_t(ch.last-p));
    const char* end=nl?nl:ch.last;
    size_t len=size_t(end-p);
    string_view id=first_field(p,len);
    if(!id.empty()) {
     bool consumed=false;
     uint64_t code;
     if(click_address_fit && id.size()>=2 && id[0]=='k' && id[1]!='0' && packed_id(id,'k',code)){
      uint64_t index=index_from_scrambled((code&~(1ULL<<63))>>8,*recovered_click_key);
      if(index<max_click_index && csv_split4(p,len,ff) && !ff[0].empty()) {
       uint64_t off=uint64_t(p-mapped_clicks.maps[ch.file_id].first);
       uint64_t packed=(uint64_t(ch.file_id)<<32)|off;
       auto& slot=direct_clicks[index];
       uint64_t empty=UINT64_MAX;
       slot.compare_exchange_strong(empty,packed,memory_order_relaxed,memory_order_relaxed);
       consumed=true;
      }
     }
     if(!consumed) {
      uint64_t h=id_partition_hash(id,'k');
      unsigned bucket=unsigned((h>>32)%workers);
      shard[bucket].push_back({p,len});
     }
    }
    p=nl?nl+1:ch.last;
   }
  }
 });
 vector<vector<Click>> click_parts(workers);
 worker_error=nullptr;
 launch([&](unsigned t) {
  size_t n=0;
  for(unsigned j=0;j<workers;++j)n+=click_rows[j][t].size();
  EventIndex seen(n,'k');
  auto& dest=click_parts[t];auto& ids=local_names[t];
  dest.reserve(n + (click_address_fit?max_click_index/workers:0));
  array<string_view,4> f;
  auto build=[&](RowRef row) {
   if(!csv_split4(row.p,row.n,f)||f[0].empty()||f[1].empty())return;
   i64 ts;
   if(!parse_ts(f[3],ts)||ts<ws-WEEK||ts>=we)return;
   auto ci=campaignIds.find(f[2]);
   if(ci==campaignIds.end())return;
   dest.push_back({ids.encode(dsu.canonical(f[1])),ts,f[0],ci->second});
  };
  for(unsigned j=0;j<workers;++j) {
   for(const auto& row:click_rows[j][t]) {
    if(!csv_split4(row.p,row.n,f)||f[0].empty())continue;
    auto [ignored,inserted]=seen.locate(f[0],0);
    if(inserted)build(row);
   }
   vector<RowRef>().swap(click_rows[j][t]);
  }
  if(click_address_fit)for(size_t k=t;k<direct_clicks.size();k+=workers){
   uint64_t word=direct_clicks[k].load(memory_order_relaxed);
   if(word==UINT64_MAX)continue;
   uint32_t file_id=unsigned(word>>32),off=uint32_t(word);
   auto [base,len]=mapped_clicks.maps[file_id];
   const char* p=base+off;
   const char* nl=(const char*)memchr(p,'\n',len-off);
   build({p,size_t(nl?nl-p:base+len-p)});
  }
 });
 merge_specials();
 worker_error=nullptr;
 launch([&](unsigned t) {
  for(auto& c:click_parts[t])if(!(c.user>>63))c.user=local_translate[t][c.user];
 });
 checkpoint("Parallel click decoding + canonical identities");

 // Cache-friendly stable radix sort by (person-id, timestamp), using
 // integer keys only. Unlike std::sort this has linear work per byte pass.
 auto sort_events=[&](vector<Event>& v) {
  if(v.size()<32768) {
   sort(v.begin(),v.end(),[](const Event&a,const Event&b){
    return a.user==b.user?a.ts<b.ts:a.user<b.user;
   });return;
  }
  vector<Event> tmp(v.size());
  for(int column=0;column<2;++column)for(int byte=0;byte<8;++byte){
   const unsigned shift=byte*8;
   size_t count[256]={};
   if(column==0){
    for(const Event& e:v)++count[unsigned((uint64_t(e.ts)^(1ULL<<63))>>shift)&255];
   } else {
    for(const Event& e:v)++count[unsigned(e.user>>shift)&255];
   }
   size_t offset[256];size_t sum=0;bool one=false;
   for(int i=0;i<256;++i){offset[i]=sum;sum+=count[i];if(count[i]==v.size())one=true;}
   if(one)continue;
   if(column==0) {
    for(const Event& e:v)tmp[offset[unsigned((uint64_t(e.ts)^(1ULL<<63))>>shift)&255]++]=e;
   }else {
    for(const Event& e:v)tmp[offset[unsigned(e.user>>shift)&255]++]=e;
   }
   v.swap(tmp);
  }
 };
 // Disjoint person-owned partitions. No global sort or global session table;
 // every session, credit and assist is computed in its owning worker.
 const unsigned B=workers;
 auto person_shard=[&](uint64_t key)->unsigned {return unsigned((mix64(key)>>32)%B);};
 vector<vector<vector<Event>>> event_shards(B,vector<vector<Event>>(B));
 vector<vector<vector<Click>>> click_shards(B,vector<vector<Click>>(B));
 worker_error=nullptr;
 launch([&](unsigned t) {
  for(auto& e:event_parts[t])event_shards[t][person_shard(e.user)].push_back(e);
  for(auto& c:click_parts[t])click_shards[t][person_shard(c.user)].push_back(c);
  vector<Event>().swap(event_parts[t]);vector<Click>().swap(click_parts[t]);
 });
 vector<vector<Event>> events_by_person(B);
 vector<vector<Click>> clicks_by_person(B);
 worker_error=nullptr;
 launch([&](unsigned t) {
  size_t ne=0,nc=0;
  for(unsigned reader=0;reader<B;++reader){ne+=event_shards[reader][t].size();nc+=click_shards[reader][t].size();}
  auto& ev=events_by_person[t];auto& cl=clicks_by_person[t];
  ev.reserve(ne);cl.reserve(nc);
  for(unsigned reader=0;reader<B;++reader) {
   auto& a=event_shards[reader][t];auto& b=click_shards[reader][t];
   ev.insert(ev.end(),make_move_iterator(a.begin()),make_move_iterator(a.end()));
   cl.insert(cl.end(),make_move_iterator(b.begin()),make_move_iterator(b.end()));
   vector<Event>().swap(a);vector<Click>().swap(b);
  }
  sort_events(ev);
  sort(cl.begin(),cl.end(),[](const Click&a,const Click&b){
   if(a.user!=b.user)return a.user<b.user;
   if(a.ts!=b.ts)return a.ts<b.ts;
   return byte_less(a.click_id,b.click_id);
  });
 });
 checkpoint("Person-shard scatter + parallel sorts");

 const int first_day=day_of(ws);
 const int total_days=day_of(we-1)-first_day+1;
 const size_t n_campaigns=campaignNames.size();
 const size_t camp_slots=n_campaigns*size_t(total_days);
 vector<vector<UserRow>> user_shards(B);
 vector<vector<CampDay>> camp_shards(B);
 vector<size_t> event_counts(B),click_counts(B),session_counts(B);
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& events=events_by_person[t];auto& clicks=clicks_by_person[t];
  event_counts[t]=events.size();click_counts[t]=clicks.size();
  vector<CampDay> cd(camp_slots);
  auto& ur=user_shards[t];ur.reserve(events.size()/15+1);
  vector<int> active_counts(n_campaigns,0),active_pos(n_campaigns,-1),active;
  active.reserve(64);
  vector<pair<int,i64>> cat_totals;cat_totals.reserve(min<size_t>(cats.size(),32));
  size_t click_at=0;
  size_t total_sessions=0;
  for(size_t e_at=0;e_at<events.size();) {
   uint64_t user=events[e_at].user;
   size_t e_end=e_at;
   while(e_end<events.size()&&events[e_end].user==user)++e_end;
   while(click_at<clicks.size()&&clicks[click_at].user<user)++click_at;
   size_t c_start=click_at;
   while(click_at<clicks.size()&&clicks[click_at].user==user)++click_at;
   size_t left=c_start,right=c_start;
   active.clear();cat_totals.clear();
   UserRow day_row;day_row.user=user;
   bool have_day=false,have_session=false;
   i64 prev=0,start=0,revenue=0;
   bool purchased=false;
   int session_day=0;
   auto save_day=[&]() {
    if(!have_day)return;
    int best=-1;i64 amount=-1;
    for(auto [cat,v]:cat_totals) {
     if(v>amount || (v==amount && (best<0||byte_less(cats[cat],cats[best])))){
      best=cat;amount=v;
     }
    }
    day_row.top=best;
    ur.push_back(day_row);
    cat_totals.clear();
   };
   auto attribute=[&]() {
    if(!have_session)return;
    ++total_sessions;
    while(right<click_at&&clicks[right].ts<=start) {
     int cid=clicks[right].campaign;
     if(active_counts[cid]++==0){active_pos[cid]=int(active.size());active.push_back(cid);}
     ++right;
    }
    while(left<right&&clicks[left].ts<start-WEEK) {
     int cid=clicks[left].campaign;
     if(--active_counts[cid]==0){
      int pos=active_pos[cid],last=active.back();
      active[pos]=last;active_pos[last]=pos;active.pop_back();active_pos[cid]=-1;
     }
     ++left;
    }
    int winner=organic;
    if(left<right){
     size_t w=right-1;
     while(w>left&&clicks[w-1].ts==clicks[w].ts)--w;
     winner=clicks[w].campaign;
    }
    size_t d=size_t(session_day-first_day);
    auto& credit=cd[size_t(winner)*total_days+d];
    ++credit.sessions;credit.purchasing+=purchased;credit.revenue+=revenue;
    for(int cid:active)++cd[size_t(cid)*total_days+d].assists;
   };
   for(;e_at<e_end;++e_at) {
    const Event& e=events[e_at];
    if(!have_session||e.ts-prev>GAP) {
     attribute();
     start=e.ts;session_day=day_of(e.ts);
     if(!have_day||day_row.day!=session_day) {
      save_day();day_row=UserRow{};day_row.user=user;day_row.day=session_day;have_day=true;
     }
     ++day_row.sessions;revenue=0;purchased=false;have_session=true;
    }
    ++day_row.events;day_row.revenue+=e.price;
    revenue+=e.price;purchased|=(e.kind==2);
    if(e.kind==2) {
     bool found=false;
     for(auto& ent:cat_totals)if(ent.first==e.cat){ent.second+=e.price;found=true;break;}
     if(!found)cat_totals.emplace_back(e.cat,e.price);
    }
    prev=e.ts;
   }
   attribute();save_day();
   for(int cid:active){active_counts[cid]=0;active_pos[cid]=-1;}
  }
  camp_shards[t]=move(cd);
  session_counts[t]=total_sessions;
  vector<Event>().swap(events);vector<Click>().swap(clicks);
 });
 checkpoint("Fused person-local sessions + click attribution + daily aggregations");

 size_t n_users=0,n_events=0,n_clicks=0,n_sessions=0;
 for(unsigned t=0;t<B;++t){n_users+=user_shards[t].size();n_events+=event_counts[t];n_clicks+=click_counts[t];n_sessions+=session_counts[t];}
 vector<CampDay> merged(camp_slots);
 for(unsigned t=0;t<B;++t) {
  for(size_t i=0;i<camp_slots;++i) {
   auto& x=merged[i];const auto& y=camp_shards[t][i];
   x.sessions+=y.sessions;x.purchasing+=y.purchasing;x.revenue+=y.revenue;x.assists+=y.assists;
  }
 }
 vector<size_t> camp_nonzero;
 for(size_t i=0;i<merged.size();++i)
  if(merged[i].sessions||merged[i].assists)camp_nonzero.push_back(i);
 fs::create_directories(out/"user_daily");fs::create_directories(out/"campaign_daily");
 using namespace fastparquet;
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& rows=user_shards[t];
  vector<string> names;
  names.reserve(rows.size());
  for(const auto& r:rows)
   names.emplace_back((r.user>>63)?unpack_user(r.user):string(special_names[r.user]));
  char part_name[48];snprintf(part_name,sizeof(part_name),"part-%05u.parquet",t);
  write((out/"user_daily"/part_name).string(),
   {{"user_id",BYTE_ARRAY,false,0},{"day",INT32,false,6},
    {"sessions",INT64},{"events",INT64},{"revenue_cents",INT64},
    {"top_category",BYTE_ARRAY,true,0}},rows.size(),
   [&](size_t row,size_t col)->Value{
    const UserRow& u=rows[row];
    switch(col){
     case 0:return {true,names[row],0};
     case 1:return {true,{},u.day};
     case 2:return {true,{},u.sessions};
     case 3:return {true,{},u.events};
     case 4:return {true,{},u.revenue};
     default:return {u.top>=0,u.top>=0?string_view(cats[u.top]):string_view{},0};
    }
   });
 });
 write((out/"campaign_daily"/"part-00000.parquet").string(),
  {{"campaign_id",BYTE_ARRAY,false,0},{"channel",BYTE_ARRAY,false,0},
   {"day",INT32,false,6},{"sessions",INT64},
   {"purchasing_sessions",INT64},{"revenue_cents",INT64},{"assisted_sessions",INT64}},
  camp_nonzero.size(),[&](size_t row,size_t col)->Value{
   size_t idx=camp_nonzero[row];
   size_t campaign=idx/size_t(total_days);int day=first_day+int(idx%size_t(total_days));
   const auto& r=merged[idx];
   switch(col){
    case 0:return {true,campaignNames[campaign],0};
    case 1:return {true,channels[campaign],0};
    case 2:return {true,{},day};
    case 3:return {true,{},r.sessions};
    case 4:return {true,{},r.purchasing};
    case 5:return {true,{},r.revenue};
    default:return {true,{},r.assists};
   }
  });
 checkpoint("Native Parquet serialization");
 cerr<<"[velocity] events="<<n_events<<" clicks="<<n_clicks<<" sessions="<<n_sessions<<" user_days="<<n_users<<" campaign_days="<<camp_nonzero.size()<<" native_seconds="<<chrono::duration<double>(chrono::steady_clock::now()-tick).count()<<"\n";
 return 0;
}
