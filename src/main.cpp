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
struct Chunk {const char* first;const char* last;};
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
   const char* end=base+st.st_size;
   auto* hdr=(const char*)memchr(base,'\n',size_t(end-base));
   const char* p=hdr?hdr+1:end;
   while(p<end) {
    const char* last=p+min<size_t>(size_t(end-p),chunk_bytes);
    if(last<end) {
     const char* nl=(const char*)memchr(last,'\n',size_t(end-last));
     last=nl?nl+1:end;
    }
    chunks.push_back({p,last});p=last;
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
// 32 bytes per winner instead of seven std::strings per raw event.
// user == -1 marks the earliest copy as invalid; later copies must not rescue it.
struct Event{i64 ts=0,ingest=0;int user=-1,prod=-1,price=0,kind=0;};
struct Click{int user,campaign; i64 ts;string_view click_id;};
struct Session{int person,day; i64 start; i64 revenue=0;int count=0;bool purchase=false;};
struct UserDay{int person,day; i64 sessions=0,events=0,revenue=0;unordered_map<int,i64> categories;};
struct CampDay{int campaign,day; i64 sessions=0,purchasing=0,revenue=0,assists=0;};
static uint64_t key(int a,int b){return (uint64_t)(uint32_t)a<<32|(uint32_t)b;}

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
  while(true) {
   const size_t idx=chunk_cursor.fetch_add(1,memory_order_relaxed);
   if(idx>=mapped.chunks.size())break;
   Chunk ch=mapped.chunks[idx];
   const char* p=ch.first;
   while(p<ch.last) {
    const char* nl=(const char*)memchr(p,'\n',size_t(ch.last-p));
    const char* e=nl?nl:ch.last;
    string_view id=first_field(p,size_t(e-p));
    if(!id.empty()) {
     uint64_t h=id_partition_hash(id,'e');
     unsigned shard=unsigned((h>>32)%workers);
     shards[shard].push_back({p,size_t(e-p)});
    }
    p=nl?nl+1:ch.last;
   }
  }
 });
 checkpoint("Parallel scan + partition event rows");
 vector<vector<PendingEvent>> results(workers);
 vector<size_t> winning(workers,0);
 worker_error=nullptr;
 launch([&](unsigned t) {
  size_t count=0;
  for(unsigned r=0;r<workers;++r)count+=local[r][t].size();
  auto& out_events=results[t];
  out_events.reserve(count);
  EventIndex index(count,'e');
  array<string_view,7> f;
  for(unsigned r=0;r<workers;++r) {
   for(RowRef row:local[r][t]) {
    if(!csv_split7(row.p,row.n,f) || f[0].empty())continue;
    i64 ingest;
    if(!parse_ts(f[3],ingest))continue;
    auto [i,added]=index.locate(f[0],uint32_t(out_events.size()));
    if(!added && out_events[i].ingest<=ingest)continue;
    PendingEvent record;
    record.ingest=ingest;
    if(!f[1].empty() && !f[5].empty()) {
     int k=(f[4]=="view"?0:f[4]=="cart"?1:f[4]=="purchase"?2:-1);
     i64 ts;
     if(k>=0 && parse_ts(f[2],ts) && ts>=ws && ts<we && ingest-ts<=DAY) {
      int price=0;
      bool valid=true;
      if(k==2) {
       if(f[6].empty()) valid=false;
       else {
        uint64_t v=0;
        for(unsigned char ch:f[6]) {
         if(ch<'0'||ch>'9') {valid=false;break;}
         v=v*10+ch-'0';
         if(v>10000000) {valid=false;break;}
        }
        if(v<1)valid=false;
        if(valid)price=(int)v;
       }
      }
      if(valid) {
       int prod=-1;
       if(k==2) {
        auto it=productIds.find(f[5]);
        if(it!=productIds.end())prod=it->second;
       }
       record=PendingEvent{f[1],ts,ingest,prod,price,k,true};
      }
     }
    }
    if(added)out_events.push_back(record);
    else out_events[i]=record;
   }
   vector<RowRef>().swap(local[r][t]);
  }
  size_t nvalid=0;
  for(size_t i=0;i<out_events.size();++i)
   if(out_events[i].valid)out_events[nvalid++]=out_events[i];
  out_events.resize(nvalid);
  winning[t]=nvalid;
 });
 checkpoint("Parallel exact dedup + event validation");
 vector<vector<vector<RowRef>>>().swap(local);
 // Each worker resolves its own canonical names to *local* 32-bit IDs.
 // A global merge touches distinct names only, not millions of event rows.
 vector<PersonIndex> local_persons(workers);
 vector<vector<Event>> event_parts(workers);
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& dst=event_parts[t];
  auto& local_ids=local_persons[t];
  dst.reserve(results[t].size());
  for(const auto& r:results[t])
   dst.push_back(Event{r.ts,r.ingest,local_ids.get(dsu.canonical(r.user)),r.prod,r.price,r.kind});
  vector<PendingEvent>().swap(results[t]);
 });
 PersonIndex persons;
 vector<vector<int>> event_translate(workers);
 for(unsigned t=0;t<workers;++t) {
  auto& map=event_translate[t];
  map.reserve(local_persons[t].size());
  for(size_t i=0;i<local_persons[t].size();++i)
   map.push_back(persons.get(local_persons[t].name(int(i))));
 }
 worker_error=nullptr;
 launch([&](unsigned t) {
  for(Event& e:event_parts[t])e.user=event_translate[t][e.user];
 });
 vector<Event> events;
 size_t total_valid=0;for(auto n:winning)total_valid+=n;
 events.reserve(total_valid);
 for(auto& part:event_parts) {
  events.insert(events.end(),make_move_iterator(part.begin()),make_move_iterator(part.end()));
  vector<Event>().swap(part);
 }
 checkpoint("Resolve deduplicated user IDs");
 MappedInput mapped_clicks(files(data/"clicks"));
 vector<vector<vector<RowRef>>> click_rows(workers,vector<vector<RowRef>>(workers));
 chunk_cursor.store(0);
 worker_error=nullptr;
 launch([&](unsigned t) {
  auto& shard=click_rows[t];
  while(true) {
   size_t idx=chunk_cursor.fetch_add(1,memory_order_relaxed);
   if(idx>=mapped_clicks.chunks.size())break;
   Chunk ch=mapped_clicks.chunks[idx];
   const char* p=ch.first;
   while(p<ch.last) {
    const char* nl=(const char*)memchr(p,'\n',size_t(ch.last-p));
    const char* end=nl?nl:ch.last;
    string_view id=first_field(p,size_t(end-p));
    if(!id.empty()) {
     uint64_t h=id_partition_hash(id,'k');
     unsigned bucket=unsigned((h>>32)%workers);
     shard[bucket].push_back({p,size_t(end-p)});
    }
    p=nl?nl+1:ch.last;
   }
  }
 });
 vector<vector<Click>> click_parts(workers);
 vector<PersonIndex> click_persons(workers);
 worker_error=nullptr;
 launch([&](unsigned t) {
  size_t n=0;
  for(unsigned j=0;j<workers;++j)n+=click_rows[j][t].size();
  EventIndex seen(n,'k');
  auto& dest=click_parts[t];
  auto& ids=click_persons[t];
  dest.reserve(n);
  array<string_view,4> f;
  for(unsigned j=0;j<workers;++j) {
   for(const auto& row:click_rows[j][t]) {
    if(!csv_split4(row.p,row.n,f)||f[0].empty())continue;
    auto [ignored,inserted]=seen.locate(f[0],0);
    if(!inserted||f[1].empty())continue;
    i64 ts;
    if(!parse_ts(f[3],ts)||ts<ws-WEEK||ts>=we)continue;
    auto ci=campaignIds.find(f[2]);
    if(ci==campaignIds.end())continue;
    dest.push_back({ids.get(dsu.canonical(f[1])),ci->second,ts,f[0]});
   }
   vector<RowRef>().swap(click_rows[j][t]);
  }
 });
 vector<vector<int>> click_translate(workers);
 for(unsigned t=0;t<workers;++t) {
  auto& map=click_translate[t];
  map.reserve(click_persons[t].size());
  for(size_t i=0;i<click_persons[t].size();++i)
   map.push_back(persons.get(click_persons[t].name(int(i))));
 }
 worker_error=nullptr;
 launch([&](unsigned t) {
  for(auto& c:click_parts[t])c.user=click_translate[t][c.user];
 });
 vector<Click> clicks;
 size_t nc=0; for(auto& part:click_parts)nc+=part.size();
 clicks.reserve(nc);
 for(auto& part:click_parts) {
  clicks.insert(clicks.end(),make_move_iterator(part.begin()),make_move_iterator(part.end()));
  vector<Click>().swap(part);
 }
 checkpoint("Read + validate clicks");
 auto sort_by_person=[&](auto& records,auto less) {
  if(workers<2 || records.size()<150000) {
   sort(records.begin(),records.end(),less);return;
  }
  using Record=typename decay_t<decltype(records)>::value_type;
  size_t num_persons=persons.size();
  vector<size_t> count(workers,0),offset(workers+1,0),next(workers);
  auto bucket=[&](int person)->size_t {
   return (uint64_t(uint32_t(person))*workers)/num_persons;
  };
  for(const auto& r:records)++count[bucket(r.user)];
  for(unsigned t=0;t<workers;++t)offset[t+1]=offset[t]+count[t];
  for(unsigned t=0;t<workers;++t)next[t]=offset[t];
  vector<Record> shuffled(records.size());
  for(auto& r:records)shuffled[next[bucket(r.user)]++]=move(r);
  records.swap(shuffled);
  worker_error=nullptr;
  launch([&](unsigned t) {
   sort(records.begin()+offset[t],records.begin()+offset[t+1],less);
  });
 };
 sort_by_person(events,[](const Event&a,const Event&b){
  return a.user==b.user?a.ts<b.ts:a.user<b.user;
 });
 sort_by_person(clicks,[](const Click&a,const Click&b){
  if(a.user!=b.user)return a.user<b.user;
  if(a.ts!=b.ts)return a.ts<b.ts;
  return byte_less(a.click_id,b.click_id);
 });
 vector<Session> sessions;sessions.reserve(events.size()/3+1);
 checkpoint("Resolve roots + sort events/clicks");
 unordered_map<uint64_t,UserDay> ud;
 for(size_t i=0;i<events.size();){int person=events[i].user;size_t j=i;
  i64 prev=numeric_limits<i64>::min();int sid=-1;
  while(j<events.size()&&events[j].user==person){auto&e=events[j];if(sid<0||e.ts-prev>GAP){sid=sessions.size();sessions.push_back({person,day_of(e.ts),e.ts});auto&u=ud[key(person,sessions.back().day)];u.person=person;u.day=sessions.back().day;u.sessions++;}
   Session&s=sessions[sid];s.count++;s.revenue+=e.price;if(e.kind==2)s.purchase=true;
   auto&u=ud[key(person,s.day)];u.events++;u.revenue+=e.price;
   if(e.kind==2)u.categories[e.prod<0?0:prodCats[e.prod]]+=e.price;
   prev=e.ts;++j;
  }
  i=j;
 }
 unordered_map<uint64_t,CampDay> cd;
 checkpoint("Sessionize + user daily aggregation");
 auto ensure_cd=[&](int c,int day)->CampDay&{auto&x=cd[key(c,day)];x.campaign=c;x.day=day;return x;};
 size_t ci=0,si=0;
 while(si<sessions.size()){
  int person=sessions[si].person;
  while(ci<clicks.size()&&clicks[ci].user<person)++ci;
  size_t cbegin=ci;while(ci<clicks.size()&&clicks[ci].user==person)++ci;
  size_t end=si;while(end<sessions.size()&&sessions[end].person==person)++end;
  size_t left=cbegin,right=cbegin;
  unordered_map<int,int> active;active.reserve(32);
  for(;si<end;++si){const auto&s=sessions[si];
   while(right<ci&&clicks[right].ts<=s.start){++active[clicks[right].campaign];++right;}
   while(left<right&&clicks[left].ts<s.start-WEEK){auto it=active.find(clicks[left].campaign);if(--it->second==0)active.erase(it);++left;}
   // Clicks are ordered by timestamp then byte-wise click_id.
   // At the most recent timestamp, choose the first (smallest ID), not the last.
   size_t winning=right;
   if(left<right){winning=right-1;while(winning>left&&clicks[winning-1].ts==clicks[winning].ts)--winning;}
   int credit=(left<right?clicks[winning].campaign:organic);
   auto&row=ensure_cd(credit,s.day);row.sessions++;row.purchasing+=s.purchase;row.revenue+=s.revenue;
   for(auto&[campaign,n]:active)ensure_cd(campaign,s.day).assists++;
  }
 }
 checkpoint("Click attribution + campaign aggregation");
 fs::create_directories(out/"user_daily");fs::create_directories(out/"campaign_daily");
 if(const char* f=getenv("VELOCITY_NATIVE_PARQUET"); f && string_view(f)=="1"){
  vector<const UserDay*> user_rows;
  user_rows.reserve(ud.size());
  for(const auto& entry:ud)user_rows.push_back(&entry.second);
  vector<const CampDay*> camp_rows;
  camp_rows.reserve(cd.size());
  for(const auto& entry:cd)camp_rows.push_back(&entry.second);
  using namespace fastparquet;
  write((out/"user_daily"/"part-00000.parquet").string(),
   {{"user_id",BYTE_ARRAY,false,0},{"day",INT32,false,6},
    {"sessions",INT64},{"events",INT64},{"revenue_cents",INT64},
    {"top_category",BYTE_ARRAY,true,0}},user_rows.size(),
   [&](size_t row,size_t col)->Value{
    const UserDay& u=*user_rows[row];
    switch(col){
     case 0:return {true,persons.name(u.person),0};
     case 1:return {true,{},u.day};
     case 2:return {true,{},u.sessions};
     case 3:return {true,{},u.events};
     case 4:return {true,{},u.revenue};
     default: {
      bool found=false;int bestid=0;long long best=-1;
      for(const auto& [id,v]:u.categories)
       if(v>best || (v==best && byte_less(cats[id],cats[bestid])))
         {best=v;bestid=id;found=true;}
      return {found,found?string_view(cats[bestid]):string_view{},0};
     }
    }
   });
  write((out/"campaign_daily"/"part-00000.parquet").string(),
   {{"campaign_id",BYTE_ARRAY,false,0},{"channel",BYTE_ARRAY,false,0},
    {"day",INT32,false,6},{"sessions",INT64},
    {"purchasing_sessions",INT64},{"revenue_cents",INT64},{"assisted_sessions",INT64}},
   camp_rows.size(),[&](size_t row,size_t col)->Value{
    const CampDay& c=*camp_rows[row];
    switch(col){
     case 0:return {true,campaignNames[c.campaign],0};
     case 1:return {true,channels[c.campaign],0};
     case 2:return {true,{},c.day};
     case 3:return {true,{},c.sessions};
     case 4:return {true,{},c.purchasing};
     case 5:return {true,{},c.revenue};
     default:return {true,{},c.assists};
    }
   });
  checkpoint("Native Parquet serialization");
  cerr<<"[velocity] events="<<events.size()<<" clicks="<<clicks.size()<<" sessions="<<sessions.size()<<" user_days="<<ud.size()<<" campaign_days="<<cd.size()<<" native_seconds="<<chrono::duration<double>(chrono::steady_clock::now()-tick).count()<<"\n";
  return 0;
 }

 ofstream uf(out/"user_daily"/"result.csv",ios::binary);
 uf<<"user_id,day,sessions,events,revenue_cents,top_category\n";
 for(auto&[k,u]:ud){string cat;bool found=false; i64 best=-1;
  for(const auto&[id,value]:u.categories)if(value>best||(value==best&&byte_less(cats[id],cat))){best=value;cat=cats[id];found=true;}
  uf<<quote_csv(persons.name(u.person))<<','<<iso_day(u.day)<<','<<u.sessions<<','<<u.events<<','<<u.revenue<<','<<(found?quote_csv(cat):"")<<'\n';
 }
 ofstream cf(out/"campaign_daily"/"result.csv",ios::binary);
 cf<<"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
 for(auto&[k,c]:cd)cf<<quote_csv(campaignNames[c.campaign])<<','<<quote_csv(channels[c.campaign])<<','<<iso_day(c.day)<<','<<c.sessions<<','<<c.purchasing<<','<<c.revenue<<','<<c.assists<<'\n';
 uf.close();cf.close();
 checkpoint("Write intermediate CSV output");
 cerr<<"[velocity] events="<<events.size()<<" clicks="<<clicks.size()<<" sessions="<<sessions.size()<<" user_days="<<ud.size()<<" campaign_days="<<cd.size()<<" native_seconds="<<chrono::duration<double>(chrono::steady_clock::now()-tick).count()<<"\n";
 return 0;
}
