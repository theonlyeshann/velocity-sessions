#include <algorithm>
#include <array>
#include <chrono>
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
#include <utility>
#include <vector>
using namespace std;
namespace fs=std::filesystem;
using i64=long long;
constexpr i64 DAY=86400000LL, WEEK=7*DAY, GAP=1800000LL;

static bool byte_less(const string &a,const string &b) {
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
// Zero-allocation field parsing. Views remain valid only while the current line lives.
static bool csv_parse_views(const string& line, int n, vector<string_view>& out) {
 out.clear();
 if(line.empty()) return false;
 size_t i=0, len=line.size();
 while(true) {
  string_view field;
  if(i<len && line[i]=='"') {
   ++i; size_t start=i;
   while(i<len && line[i]!='"') ++i;
   if(i==len) return false;
   field=string_view(line.data()+start,i-start);
   ++i;
   if(i<len && line[i]!=',') return false;
  } else {
   size_t start=i;
   while(i<len && line[i]!=',') {
    if(line[i]=='"') return false;
    ++i;
   }
   field=string_view(line.data()+start,i-start);
  }
  out.push_back(field);
  if(i==len) break;
  ++i;
  if(i==len) {out.emplace_back();break;}
 }
 return (int)out.size()==n;
}

template<class Fn>static void read_event_views(const vector<fs::path>& paths, Fn fn) {
 vector<string_view> fields;
 fields.reserve(7);
 for(const auto& path:paths) {
  ifstream in(path,ios::binary);
  string line;
  getline(in,line);
  while(getline(in,line)) if(csv_parse_views(line,7,fields)) fn(fields);
 }
}

struct TransparentHash {
 using is_transparent=void;
 size_t operator()(string_view s) const noexcept { return hash<string_view>{}(s); }
};
struct TransparentEq {
 using is_transparent=void;
 bool operator()(string_view a,string_view b) const noexcept {return a==b;}
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
static string quote_csv(const string& s){
 if(s.find_first_of(",\"\r\n")==string::npos)return s;
 string r="\"";for(char c:s){if(c=='"')r+='"';r+=c;}return r+'"';
}
struct DSU{
 unordered_map<string,int> ids;vector<int> par,sz;vector<string> name;
 int get(const string& s){auto it=ids.find(s);if(it!=ids.end())return it->second;int i=par.size();ids.emplace(s,i);par.push_back(i);sz.push_back(1);name.push_back(s);return i;}
 int root(int i){while(par[i]!=i){par[i]=par[par[i]];i=par[i];}return i;}
 void join(int a,int b){a=root(a);b=root(b);if(a==b)return;if(sz[a]<sz[b])swap(a,b);par[b]=a;sz[a]+=sz[b];if(byte_less(name[b],name[a]))name[a]=name[b];}
};
struct Event{int user,prod,price=0; i64 ts,ingest;int kind;};
struct Click{int user,campaign; i64 ts;string click_id;};
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
 unordered_map<string,int> productIds, campaignIds;
 vector<string> cats, channels, campaignNames;cats.push_back("unknown");
 unordered_map<string,int> catIds;catIds.emplace("unknown",0);
 vector<int> prodCats;
 read_files({data/"products.csv"},2,[&](const vector<string>&f){int c;auto p=catIds.find(f[1]);if(p==catIds.end()){c=cats.size();catIds.emplace(f[1],c);cats.push_back(f[1]);}else c=p->second;int id=prodCats.size();productIds.emplace(f[0],id);prodCats.push_back(c);});
 read_files({data/"campaigns.csv"},2,[&](const vector<string>&f){int id=campaignNames.size();campaignIds.emplace(f[0],id);campaignNames.push_back(f[0]);channels.push_back(f[1]);});
 int organic=campaignNames.size();campaignNames.push_back("organic");channels.push_back("organic");
 checkpoint("Product + campaign lookups");
 vector<Event> events;events.reserve(1000000);
 unordered_map<string,size_t,TransparentHash,TransparentEq> evIndex;
 // Keep raw winners to preserve the exact dedup-before-validation semantics.
 struct RawEvent{array<string,7> f;i64 ingest;};
 vector<RawEvent> raw;raw.reserve(1000000);evIndex.reserve(1200000);
 read_event_views(files(data/"events"),[&](const vector<string_view>&f){
  if(f[0].empty())return; i64 ingest;if(!parse_ts(f[3],ingest))return;
  auto pos=evIndex.find(f[0]);if(pos!=evIndex.end()&&raw[pos->second].ingest<=ingest)return;
  RawEvent r;for(int j=0;j<7;j++)r.f[j].assign(f[j].data(),f[j].size());r.ingest=ingest;
  if(pos==evIndex.end()){evIndex.emplace(string(f[0]),raw.size());raw.push_back(move(r));}else raw[pos->second]=move(r);
 });
 evIndex.clear();events.clear();
 checkpoint("Read + deduplicate raw events");
 for(auto&r:raw){auto&f=r.f;
  if(f[1].empty()||f[5].empty())continue;
  int k=(f[4]=="view"?0:f[4]=="cart"?1:f[4]=="purchase"?2:-1);if(k<0)continue;
  i64 ts;if(!parse_ts(f[2],ts)||ts<ws||ts>=we||r.ingest-ts>DAY)continue;
  int price=0;if(k==2){if(f[6].empty())continue;uint64_t v=0;bool ok=true;for(unsigned char ch:f[6]){if(ch<'0'||ch>'9'){ok=false;break;}v=v*10+ch-'0';if(v>10000000){ok=false;break;}}if(!ok||v<1)continue;price=(int)v;}
  int prod=-1;auto pit=productIds.find(f[5]);if(pit!=productIds.end())prod=pit->second;
  events.push_back({dsu.get(f[1]),prod,price,ts,r.ingest,k});
 }
 raw.clear();
 checkpoint("Validate events + resolve user IDs");
 vector<Click> clicks; clicks.reserve(250000);unordered_map<string,bool> seen;
 read_files(files(data/"clicks"),4,[&](const vector<string>&f){
  if(f[0].empty()||!seen.emplace(f[0],true).second)return;
  if(f[1].empty())return;i64 ts;if(!parse_ts(f[3],ts)||ts<ws-WEEK||ts>=we)return;
  auto ci=campaignIds.find(f[2]);if(ci==campaignIds.end())return;
  clicks.push_back({dsu.get(f[1]),ci->second,ts,f[0]});
 });
 checkpoint("Read + validate clicks");
 for(auto& e:events)e.user=dsu.root(e.user);
 for(auto& c:clicks)c.user=dsu.root(c.user);
 sort(events.begin(),events.end(),[](const Event&a,const Event&b){if(a.user!=b.user)return a.user<b.user;return a.ts<b.ts;});
 sort(clicks.begin(),clicks.end(),[](const Click&a,const Click&b){if(a.user!=b.user)return a.user<b.user;if(a.ts!=b.ts)return a.ts<b.ts;return byte_less(a.click_id,b.click_id);});
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
 ofstream uf(out/"user_daily"/"result.csv",ios::binary);
 uf<<"user_id,day,sessions,events,revenue_cents,top_category\n";
 for(auto&[k,u]:ud){string cat;bool found=false; i64 best=-1;
  for(const auto&[id,value]:u.categories)if(value>best||(value==best&&byte_less(cats[id],cat))){best=value;cat=cats[id];found=true;}
  uf<<quote_csv(dsu.name[u.person])<<','<<iso_day(u.day)<<','<<u.sessions<<','<<u.events<<','<<u.revenue<<','<<(found?quote_csv(cat):"")<<'\n';
 }
 ofstream cf(out/"campaign_daily"/"result.csv",ios::binary);
 cf<<"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
 for(auto&[k,c]:cd)cf<<quote_csv(campaignNames[c.campaign])<<','<<quote_csv(channels[c.campaign])<<','<<iso_day(c.day)<<','<<c.sessions<<','<<c.purchasing<<','<<c.revenue<<','<<c.assists<<'\n';
 uf.close();cf.close();
 checkpoint("Write intermediate CSV output");
 cerr<<"[velocity] events="<<events.size()<<" clicks="<<clicks.size()<<" sessions="<<sessions.size()<<" user_days="<<ud.size()<<" campaign_days="<<cd.size()<<" native_seconds="<<chrono::duration<double>(chrono::steady_clock::now()-tick).count()<<"\n";
 return 0;
}