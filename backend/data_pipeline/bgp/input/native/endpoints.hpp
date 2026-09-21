// 使用相同解码与拒绝规则盘点 UPDATE 端点；不生成观察行或路径索引。
#include <tuple>
#include <csignal>
#include <sys/prctl.h>
static int endpoint_inventory(const char* path,const std::string& source){
    const char* parent=getenv("DOMEYE_ENDPOINT_PARENT_PID");
    if(parent){require(prctl(PR_SET_PDEATHSIG,SIGTERM)==0,"端点预扫退出保护失败");if(getppid()!=std::stoi(parent))return 1;}
    using Key=std::tuple<std::string,uint64_t,std::string,uint64_t,int>;
    std::map<Key,std::pair<std::string,uint64_t>> endpoints;
    uint64_t record=0,offset=0;double start=now(),ct=cpu();
    BGPDUMP* dump=bgpdump_open_dump(path);require(dump,"无法打开端点盘点输入");
    while(!dump->eof){
        frame.clear();auto entry=bgpdump_read_next(dump);
        if(frame.empty()){require(dump->eof&&!entry,"端点盘点边界丢失");break;}
        require((mrt_type==16||mrt_type==17)&&record<2000000&&offset+frame.size()<=12ULL*1024*1024*1024,"端点盘点输入范围不符");
        UpdateFrame u;bool decoded=true;
        try{decode_update(entry,u,offset);}
        catch(const UnsupportedUpdate&){decoded=false;}
        catch(const std::runtime_error&){
            const char* policy=getenv("DOMEYE_NATIVE_POLICY");
            if(!policy||std::string(policy)!="isolate-payload/v1"){if(entry)bgpdump_free_mem(entry);bgpdump_close_dump(dump);throw;}
            decoded=false;
        }
        if(decoded&&u.endpoint&&!u.local){
            Key key{u.peer_ip,u.peer_asn,u.local_ip,u.local_asn,u.interface};
            auto mid=source+":"+std::to_string(record);auto& value=endpoints[key];
            if(value.second==0||mid<value.first)value.first=mid;
            value.second++;require(endpoints.size()<=65536,"端点盘点容量上限");
        }
        if(entry)bgpdump_free_mem(entry);record++;offset+=frame.size();
    }
    require(!cfr_error(dump->f)&&record==uint64_t(dump->parsed),"端点盘点 EOF 不符");bgpdump_close_dump(dump);
    std::cout<<"{\"records\":"<<record<<",\"seconds\":"<<now()-start<<",\"cpu_seconds\":"<<cpu()-ct<<",\"endpoints\":[";
    bool first=true;for(auto& item:endpoints){
        if(!first)std::cout<<',';first=false;auto& k=item.first;auto& v=item.second;
        std::cout<<'['<<quote(std::get<0>(k))<<','<<std::get<1>(k)<<','<<quote(std::get<2>(k))<<','<<std::get<3>(k)<<','<<std::get<4>(k)<<','<<quote(v.first)<<','<<v.second<<']';
    }
    std::cout<<"]}"<<std::endl;return 0;
}
