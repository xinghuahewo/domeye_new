// libbgpdump 解码、原始定位核对与有界列式构造。
#include "common.hpp"
#include <arrow/util/thread_pool.h>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <ctime>
#include <malloc.h>
#include <optional>
#include <sqlite3.h>
#include <unordered_map>
extern "C" {
#include "cfile_tools.h"
#include "bgpdump_lib.h"
#include "util.h"
}
#include "schema.inc"
namespace fs=std::filesystem;
static std::string frame;
static uint32_t stamp;static uint16_t subtype,mrt_type;
static double now(){return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();}
static double cpu(){return double(std::clock())/CLOCKS_PER_SEC;}

// 构建时在原库的完整头与正文边界插入捕获点；不改变原库解码。
extern "C" void domeye_header(BGPDUMP_ENTRY* e,unsigned bytes,int valid) {
    if(!valid){require(bytes==0,"MRT 头截断");return;}
    require((e->type==13&&(e->subtype==1||e->subtype==2||e->subtype==4))||e->type==16||e->type==17,"原生候选 MRT 类型未支持");
    if(e->type==17)require(e->ms>=0&&e->ms<=999999,"MRT 微秒越界");
    require(e->length>0&&e->length<=16*1024*1024,"MRT 记录资源边界");
}
extern "C" void domeye_capture(BGPDUMP_ENTRY* e,const unsigned char* body,unsigned length) {
    stamp=e->time;subtype=e->subtype;mrt_type=e->type;frame.assign(12,'\0');
    uint32_t t=htonl(stamp),n=htonl(length+(mrt_type==17?4:0));uint16_t ty=htons(mrt_type),st=htons(subtype);
    memcpy(frame.data(),&t,4);memcpy(frame.data()+4,&ty,2);memcpy(frame.data()+6,&st,2);memcpy(frame.data()+8,&n,4);
    if(mrt_type==17){uint32_t ms=htonl(e->ms);frame.append((const char*)&ms,4);}
    frame.append((const char*)body,length);
}
struct Cursor {
    const std::string& data;size_t pos;
    explicit Cursor(const std::string& d,size_t p=0):data(d),pos(p){}
    std::string take(size_t n){require(pos+n<=data.size(),"字段截断");auto s=data.substr(pos,n);pos+=n;return s;}
    uint64_t u(size_t n){require(pos+n<=data.size(),"整数截断");uint64_t v=0;for(size_t i=0;i<n;i++)v=(v<<8)|(unsigned char)data[pos++];return v;}
    bool more()const{return pos<data.size();}
    void done()const{require(pos==data.size(),"未解释尾部");}
};
static std::string ip(const std::string& raw){
    char p[INET6_ADDRSTRLEN];require(raw.size()==4||raw.size()==16,"地址长度");
    require(inet_ntop(raw.size()==4?AF_INET:AF_INET6,raw.data(),p,sizeof(p)),"地址转换");return p;
}
static std::string network_prefix(std::string address,unsigned bits){
    require(bits<=address.size()*8,"网络前缀长度越界");
    if(bits%8)address[bits/8]&=char(255<<(8-bits%8));
    for(size_t i=(bits+7)/8;i<address.size();i++)address[i]=0;
    return ip(address)+"/"+std::to_string(bits);
}
using Segments=std::vector<std::pair<int,std::vector<uint32_t>>>;
static Segments segments(const std::string& raw,int width=4) {
    Cursor c(raw);Segments result;while(c.more()){int kind=c.u(1),n=c.u(1);require(kind>=1&&kind<=4&&n>0,"AS_PATH 段类型或长度无效");std::vector<uint32_t> values;for(int i=0;i<n;i++)values.push_back(c.u(width));result.emplace_back(kind,values);}return result;
}
struct Path {
    std::string key,digest;std::optional<std::string> original,as4;std::string text,reason;
    std::optional<uint32_t> raw_origin,origin;
};
static Path interpret(const std::string& raw,Path p,const attributes_t* attr,int width=4){
    Cursor c(raw);bool seen[256]={};while(c.more()){int flags=c.u(1),code=c.u(1);size_t n=c.u(flags&16?2:1);auto value=c.take(n);require(!seen[code],"重复 BGP 属性");seen[code]=true;if(code==2)p.original=value;if(code==17)p.as4=value;}
    auto segs=segments(p.original.value_or(""),width);p.text=attr->aspath&&attr->aspath->str?attr->aspath->str:"";
    if(!segs.empty()&&segs.back().first==2)p.raw_origin=segs.back().second.back();
    if(p.as4){require(p.as4->size()>=6&&p.as4->size()%2==0,"AS4_PATH 属性长度无效");segments(*p.as4);p.reason="as4_path_requires_separate_rule";return p;}
    if(!p.original||segs.empty()){p.reason="missing_or_empty_path";return p;}
    bool skipped=false;for(auto s=segs.rbegin();s!=segs.rend();s++){
        if(s->first!=2){p.reason=s->first==1?"as_set_ambiguous":"confederation_ambiguous";return p;}
        for(auto v=s->second.rbegin();v!=s->second.rend();v++){
            if((*v>=64512&&*v<=65535)||(*v>=4200000000U&&*v<=4294967294U)){skipped=true;continue;}
            if(*v==0||*v==23456||*v==4294967295U){p.reason="special_asn_not_attributable";return p;}
            p.origin=*v;p.reason=skipped?"private_as_skipped":"explicit_terminal_asn";return p;
        }
    }p.reason="only_excluded_asns";return p;
}
struct Peer{std::string bgp,ip;uint32_t asn;};
struct Index {
    static constexpr int checkpoint_pages=131072;
    sqlite3* db=nullptr;sqlite3_stmt *insert=nullptr,*lookup=nullptr;
    double commit_seconds=0,commit_cpu=0,checkpoint_seconds=0,checkpoint_cpu=0,final_checkpoint_seconds=0,final_checkpoint_cpu=0,close_seconds=0;
    int checkpoint_calls=0,checkpoint_busy=0;
    static int wal_committed(void* data,sqlite3* db,const char* name,int pages){
        // FULL 提交不变，累计约 512 MiB WAL 后再合并主库，减少重复随机写入。
        // 门槛在提交后检查，最大日志还包含触发它的当前事务。
        auto& self=*static_cast<Index*>(data);
        if(pages<checkpoint_pages)return SQLITE_OK;
        double t=now(),ct=cpu();int rc=sqlite3_wal_checkpoint_v2(db,name,SQLITE_CHECKPOINT_PASSIVE,nullptr,nullptr);
        self.checkpoint_seconds+=now()-t;self.checkpoint_cpu+=cpu()-ct;self.checkpoint_calls++;
        if(rc==SQLITE_BUSY)self.checkpoint_busy++;
        // checkpoint 未清空 WAL 不取消已经持久化的提交；结束前还会严格清理一次。
        return rc==SQLITE_BUSY?SQLITE_OK:rc;
    }
    explicit Index(const fs::path& path){
        require(sqlite3_open(path.c_str(),&db)==SQLITE_OK,"SQLite 打开失败");
        exec("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA cache_size=-262144;PRAGMA temp_store=FILE;CREATE TABLE IF NOT EXISTS paths(key BLOB PRIMARY KEY,raw BLOB NOT NULL,segment INTEGER NOT NULL) WITHOUT ROWID;");
        sqlite3_wal_hook(db,wal_committed,this);
        require(sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO paths VALUES(?,?,?)",-1,&insert,nullptr)==SQLITE_OK,"索引语句准备失败");
        require(sqlite3_prepare_v2(db,"SELECT raw FROM paths WHERE key=?",-1,&lookup,nullptr)==SQLITE_OK,"索引读取准备失败");
    }
    void exec(const std::string& sql){char* err=nullptr;if(sqlite3_exec(db,sql.c_str(),nullptr,nullptr,&err)!=SQLITE_OK){std::string text=err?err:"SQLite 错误";sqlite3_free(err);throw std::runtime_error(text);}}
    void commit(){double t=now(),ct=cpu();exec("COMMIT");commit_seconds+=now()-t;commit_cpu+=cpu()-ct;}
    void finish(){
        double t=now(),ct=cpu();
        require(sqlite3_wal_checkpoint_v2(db,nullptr,SQLITE_CHECKPOINT_TRUNCATE,nullptr,nullptr)==SQLITE_OK,"最终路径 WAL checkpoint 失败");
        final_checkpoint_seconds=now()-t;final_checkpoint_cpu=cpu()-ct;
        sqlite3_finalize(insert);insert=nullptr;sqlite3_finalize(lookup);lookup=nullptr;
        t=now();require(sqlite3_close(db)==SQLITE_OK,"路径索引关闭失败");db=nullptr;close_seconds=now()-t;
    }
    std::string metrics()const{
        return ",\"sqlite_commit_seconds\":"+std::to_string(commit_seconds)+",\"sqlite_commit_cpu_seconds\":"+std::to_string(commit_cpu)+
            ",\"sqlite_checkpoint_seconds\":"+std::to_string(checkpoint_seconds)+",\"sqlite_checkpoint_cpu_seconds\":"+std::to_string(checkpoint_cpu)+
            ",\"sqlite_checkpoint_calls\":"+std::to_string(checkpoint_calls)+",\"sqlite_checkpoint_busy\":"+std::to_string(checkpoint_busy)+",\"sqlite_checkpoint_pages\":"+std::to_string(checkpoint_pages)+
            ",\"sqlite_final_checkpoint_seconds\":"+std::to_string(final_checkpoint_seconds)+",\"sqlite_final_checkpoint_cpu_seconds\":"+std::to_string(final_checkpoint_cpu)+
            ",\"sqlite_close_seconds\":"+std::to_string(close_seconds);
    }
    bool add(const Path& p,const std::string& raw,int segment){
        sqlite3_reset(insert);sqlite3_bind_text(insert,1,p.key.data(),p.key.size(),SQLITE_TRANSIENT);sqlite3_bind_blob(insert,2,raw.data(),raw.size(),SQLITE_TRANSIENT);sqlite3_bind_int(insert,3,segment);
        require(sqlite3_step(insert)==SQLITE_DONE,"路径索引写入失败");if(sqlite3_changes(db))return true;
        sqlite3_reset(lookup);sqlite3_bind_text(lookup,1,p.key.data(),p.key.size(),SQLITE_TRANSIENT);require(sqlite3_step(lookup)==SQLITE_ROW,"路径索引条目丢失");
        auto n=sqlite3_column_bytes(lookup,0);auto bytes=sqlite3_column_blob(lookup,0);
        require(size_t(n)==raw.size()&&(n==0||memcmp(bytes,raw.data(),n)==0),"路径摘要碰撞或索引损坏");sqlite3_reset(lookup);return false;
    }
    ~Index(){sqlite3_finalize(insert);sqlite3_finalize(lookup);sqlite3_close(db);}
};
#include "update.hpp"
#include "endpoints.hpp"
int main(int argc,char** argv){
    try {
        const char* parent=getenv("DOMEYE_NATIVE_PARENT_PID");
        if(parent){require(prctl(PR_SET_PDEATHSIG,SIGTERM)==0,"原生解析退出保护失败");if(getppid()!=std::stoi(parent))return 1;}
        if(argc==4&&std::string(argv[1])=="--endpoints"){log_to_stderr();return endpoint_inventory(argv[2],argv[3]);}
        require(argc==8,"参数：输入 source_id SHA 输出 批大小 起始段 起始记录");
        std::string source=argv[2],source_sha=argv[3];fs::path root=argv[4];int batch=std::stoi(argv[5]),part=std::stoi(argv[6]);uint64_t skip=std::stoull(argv[7]);
        require(batch>=1&&batch<=200000,"批大小越界");fs::create_directories(root);log_to_stderr();mallopt(M_TRIM_THRESHOLD,-1);
        check(arrow::SetCpuThreadPoolCapacity(4));
        std::ifstream templates(root/"interpretation.txt");std::string peer_json,rib_json;std::getline(templates,peer_json);std::getline(templates,rib_json);require(bool(templates),"解释模板不存在");
        auto schema=schemas();Table messages(schema.at("messages")),elements(schema.at("elements")),paths(schema.at("paths")),peers(schema.at("peers")),eor(schema.at("eor")),quality(schema.at("quality"));
        double start=now(),setup_cpu_start=cpu();
        Index index(root/"paths.sqlite");index.exec("DELETE FROM paths WHERE segment>="+std::to_string(part));index.exec("BEGIN");
        double index_setup_seconds=now()-start,index_setup_cpu=cpu()-setup_cpu_start;
        std::unordered_map<std::string,Path> cache;size_t cache_bytes=0;
        std::vector<Peer> peer_table;int64_t peer_record=-1;uint64_t record=0,offset=0,total_elements=0,unique=0,first_record=skip;
        double parse_seconds=0,convert_seconds=0,index_seconds=0,write_seconds=0,backpressure_seconds=0;
        double parse_cpu=0,convert_cpu=0,index_cpu=0,write_cpu=0,arrow_seconds=0,arrow_cpu=0;uint32_t epoch=0;
        std::vector<std::pair<fs::path,std::string>> pending;
        auto publish=[&](){
            if(pending.empty())return;
            // 三段共用一次 FULL 提交；字典持久化后才逐段公布回执。
            index.commit();
            for(auto& item:pending)marker(item.first,item.second);
            pending.clear();index.exec("BEGIN");
        };
        auto flush=[&](){
            if(!messages.rows())return;double t=now(),ct=cpu();std::ostringstream name;name<<"segment-"<<std::setw(6)<<std::setfill('0')<<part;auto dir=root/name.str();fs::create_directories(dir);
            std::string counts="{";bool first=true;
            for(auto pair:std::vector<std::pair<std::string,Table*>>{{"messages",&messages},{"elements",&elements},{"paths",&paths},{"peers",&peers},{"eor",&eor},{"quality",&quality}}){
                if(!first)counts+=',';counts+=quote(pair.first)+":"+std::to_string(pair.second->rows());first=false;
                if(pair.second->rows()){
                    double at=now(),ac=cpu();pair.second->write(dir/(pair.first+".arrow"));
                    arrow_seconds+=now()-at;arrow_cpu+=cpu()-ac;
                }
            }counts+='}';sync_file(dir);
            pending.emplace_back(dir/"committed.json","{\"segment\":"+std::to_string(part)+",\"first_record\":"+std::to_string(first_record)+",\"next_record\":"+std::to_string(record)+",\"next_offset\":"+std::to_string(offset)+",\"counts\":"+counts+"}");
            if(pending.size()==3)publish();
            part++;first_record=record;write_seconds+=now()-t;write_cpu+=cpu()-ct;
            std::cout<<"{\"kind\":\"segment\",\"elapsed_seconds\":"<<now()-start<<",\"records\":"<<record<<",\"elements\":"<<total_elements<<",\"committed_segments\":"<<(part-pending.size())<<",\"new_paths\":"<<unique<<",\"parse_seconds\":"<<parse_seconds<<",\"convert_seconds\":"<<convert_seconds<<",\"index_seconds\":"<<index_seconds<<",\"write_seconds\":"<<write_seconds<<",\"arrow_write_seconds\":"<<arrow_seconds<<index.metrics()<<"}"<<std::endl;
            if(getenv("DOMEYE_NATIVE_STREAMING")){
                t=now();while(part>4){int consumed=0;std::ifstream in(root/"consumed-count");in>>consumed;if(consumed>=part-4)break;usleep(100000);}backpressure_seconds+=now()-t;
            }
        };
        BGPDUMP* dump=bgpdump_open_dump(argv[1]);require(dump,"无法打开 RIB");
        while(!dump->eof){
            double t=now(),ct=cpu();frame.clear();auto entry=bgpdump_read_next(dump);parse_seconds+=now()-t;parse_cpu+=cpu()-ct;
            if(frame.empty()){require(dump->eof&&!entry,"解析边界丢失");break;}
            require(record<2000000&&offset+frame.size()<=12ULL*1024*1024*1024,"输入资源超限");
            if(record==0)epoch=stamp;if(mrt_type==13)require(stamp==epoch,"RIB 时间冲突");t=now();ct=cpu();Cursor c(frame,12);
            if(mrt_type!=13){
                total_elements+=append_update(entry,source,source_sha,argv[1],record,offset,skip,part,index,messages,elements,paths,eor,quality,index_seconds,index_cpu,unique);
                if(entry)bgpdump_free_mem(entry);
                offset+=frame.size();record++;convert_seconds+=now()-t;convert_cpu+=cpu()-ct;
                if(elements.rows()>=batch||messages.rows()>=batch||arrow::default_memory_pool()->bytes_allocated()>64*1024*1024)flush();
                continue;
            }
            if(subtype==1){
                require(c.u(4)==25,"Collector ID 不符");auto name=c.take(c.u(2));require(name=="rrc25","Collector 名称不符");auto count=c.u(2);require(count>0,"空 Peer 表");peer_table.clear();peer_record=record;
                auto decoded_peers=dump->table_dump_v2_peer_index_table;require(decoded_peers&&decoded_peers->peer_count==count,"C 库 Peer 表计数不符");
                for(size_t j=0;j<count;j++){auto flags=c.u(1);require((flags&~3)==0,"Peer flags 不符");auto bgp=c.take(4),address=c.take(flags&1?16:4);auto asn=c.u(flags&2?4:2);auto& decoded_peer=decoded_peers->entries[j];
                    require(decoded_peer.peer_as==asn&&memcmp(&decoded_peer.peer_bgp_id,bgp.data(),4)==0&&memcmp(&decoded_peer.peer_ip,address.data(),address.size())==0,"C 库 Peer 原始字段不符");
                    Peer p;p.bgp=ip(std::string((const char*)&decoded_peer.peer_bgp_id,4));p.ip=ip(std::string((const char*)&decoded_peer.peer_ip,address.size()));p.asn=decoded_peer.peer_as;peer_table.push_back(p);
                    if(record>=skip){peers.s(0,source);peers.l(1,record);peers.l(2,j);peers.s(3,p.bgp);peers.s(4,p.ip);peers.l(5,p.asn);peers.flag(6,true);}
                }c.done();require(!entry,"Peer 表返回异常");
            }else{
                require(entry&&peer_record>=0,"RIB 无 Peer 表或 C 库拒绝记录");c.u(4);auto bits=c.u(1);int afi=subtype==2?1:2;require(bits<=uint64_t(afi==1?32:128),"前缀长度越界");auto raw_prefix=std::string(1,char(bits))+c.take((bits+7)/8);
                std::string address=raw_prefix.substr(1);address.resize(afi==1?4:16,0);auto count=c.u(2);
                auto& decoded=entry->body.mrtd_table_dump_v2_prefix;require(decoded.entry_count==count&&decoded.afi==afi&&decoded.safi==1,"C 库 RIB 计数/地址族不符");
                require(decoded.prefix_length==bits&&memcmp(&decoded.prefix,address.data(),address.size())==0,"C 库前缀原字节不符");
                auto prefix=network_prefix(std::string((const char*)&decoded.prefix,address.size()),decoded.prefix_length);
                for(size_t j=0;j<count;j++){
                    auto pi=c.u(2),originated=c.u(4),length=c.u(2);auto attr_offset=offset+c.pos;auto raw=c.take(length);require(pi<peer_table.size(),"Peer 索引越界");
                    auto a=decoded.entries[j].attr;require(a&&a->len==length&&(length==0||memcmp(a->data,raw.data(),length)==0),"C 库原始属性字节不符");
                    require(decoded.entries[j].peer_index==pi&&decoded.entries[j].originated_time==originated,"C 库条目头不符");
                    total_elements++;require(total_elements<=80000000,"元素数资源边界");if(record<skip)continue;
                    auto found=cache.find(raw);Path value;
                    if(found!=cache.end())value=found->second;
                    else {
                        value.key=hash(std::string(1,'\4')+raw);value.digest=hash(raw);double it=now(),ic=cpu();bool fresh=index.add(value,raw,part);index_seconds+=now()-it;index_cpu+=cpu()-ic;
                        if(fresh){value=interpret(raw,value,a);unique++;paths.s(0,value.key);paths.b(1,raw);paths.s(2,value.digest);paths.i(3,4);
                            if(value.original)paths.b(4,*value.original);else paths.n(4);if(value.as4)paths.b(5,*value.as4);else paths.n(5);paths.s(6,value.text);if(value.as4){require(a->new_aspath&&a->new_aspath->str,"C 库缺少 AS4_PATH 文本");paths.s(7,a->new_aspath->str);}else paths.n(7);
                            if(value.raw_origin)paths.l(8,*value.raw_origin);else paths.n(8);if(value.origin)paths.l(9,*value.origin);else paths.n(9);paths.s(10,value.reason);
                        }
                        // 只缓存 raw->摘要，不保留所有历史路径；32 MiB 近似数据预算。
                        Path cached;cached.key=value.key;cached.digest=value.digest;size_t bytes=raw.size()+cached.key.size()+cached.digest.size()+256;
                        if(cache_bytes+bytes>32*1024*1024){cache.clear();cache_bytes=0;}cache.emplace(raw,std::move(cached));cache_bytes+=bytes;
                    }
                    auto& p=peer_table[pi];auto mid=source+":"+std::to_string(record);
                    elements.s(0,mid+":"+std::to_string(j));elements.s(1,mid);elements.l(2,j);elements.s(3,"rib_snapshot");elements.i(4,afi);elements.i(5,1);elements.s(6,prefix);elements.b(7,raw_prefix);
                    elements.n(8);elements.flag(9,false);elements.l(10,originated);elements.s(11,p.ip);elements.l(12,p.asn);elements.s(13,p.bgp);elements.flag(14,true);elements.l(15,peer_record);elements.l(16,pi);
                    elements.s(17,value.key);elements.l(18,attr_offset);elements.l(19,length);elements.s(20,value.digest);
                }c.done();bgpdump_free_mem(entry);
            }
            if(record>=skip){
                messages.s(0,source+":"+std::to_string(record));messages.s(1,source);messages.s(2,source_sha);messages.l(3,record);messages.l(4,offset);messages.l(5,frame.size());messages.l(6,stamp);messages.n(7);messages.i(8,13);messages.i(9,subtype);messages.s(10,hash(frame));messages.s(11,subtype==1?"peer_index_table":"rib");
                for(int j=12;j<=15;j++)messages.n(j);messages.flag(16,false);for(int j=17;j<=23;j++)messages.n(j);
                auto json=subtype==1?peer_json:rib_json;auto marker_pos=json.find("@offset@");require(marker_pos!=std::string::npos,"解释模板占位符丢失");json.replace(marker_pos,8,std::to_string(offset+frame.size()));messages.s(24,json);
            }
            offset+=frame.size();record++;convert_seconds+=now()-t;convert_cpu+=cpu()-ct;
            if(elements.rows()>=batch||messages.rows()>=batch||arrow::default_memory_pool()->bytes_allocated()>64*1024*1024)flush();
        }
        require(!cfr_error(dump->f),"压缩读取错误");require(record==uint64_t(dump->parsed),"C 库记录完整性不符");bgpdump_close_dump(dump);flush();
        double finish_start=now(),finish_cpu=cpu();publish();index.commit();index.finish();write_seconds+=now()-finish_start;write_cpu+=cpu()-finish_cpu;
        marker(root/"native-eof.json","{\"records\":"+std::to_string(record)+",\"elements\":"+std::to_string(total_elements)+",\"decoded_bytes\":"+std::to_string(offset)+",\"segments\":"+std::to_string(part)+",\"elapsed_seconds\":"+std::to_string(now()-start)+",\"parse_seconds\":"+std::to_string(parse_seconds)+",\"convert_seconds\":"+std::to_string(convert_seconds-index_seconds)+",\"index_seconds\":"+std::to_string(index_seconds)+",\"write_seconds\":"+std::to_string(write_seconds)+",\"backpressure_seconds\":"+std::to_string(backpressure_seconds)+",\"parse_cpu_seconds\":"+std::to_string(parse_cpu)+",\"convert_cpu_seconds\":"+std::to_string(convert_cpu-index_cpu)+",\"index_cpu_seconds\":"+std::to_string(index_cpu)+",\"write_cpu_seconds\":"+std::to_string(write_cpu)+",\"index_setup_seconds\":"+std::to_string(index_setup_seconds)+",\"index_setup_cpu_seconds\":"+std::to_string(index_setup_cpu)+",\"arrow_write_seconds\":"+std::to_string(arrow_seconds)+",\"arrow_write_cpu_seconds\":"+std::to_string(arrow_cpu)+index.metrics()+"}");
        return 0;
    }catch(const std::exception& e){std::cerr<<"原生导入失败："<<e.what()<<std::endl;return 1;}
}
