// UPDATE 先完整核对当前帧，再向列追加；拒绝帧不留下半条路径或元素。
struct UnsupportedUpdate:std::runtime_error{using std::runtime_error::runtime_error;};
struct UpdateRoute {
    int afi,safi;std::string action,raw,prefix;std::optional<uint32_t> path_id;
};
struct UpdateFrame {
    std::string peer_ip,local_ip,kind="unknown",raw;uint64_t peer_asn=0,local_asn=0,attr_offset=0;
    int interface=0,afi=0,bgp_type=0,old_state=0,new_state=0,width=4;
    bool endpoint=false,bgp_header=false,state=false,local=false,addpath=false;
    std::optional<int> microsecond;std::vector<UpdateRoute> routes;std::vector<std::pair<int,int>> eors;
    Path path;
};
static void decode_update(BGPDUMP_ENTRY* entry,UpdateFrame& u,uint64_t offset){
    Cursor c(frame,12);if(mrt_type==17)u.microsecond=c.u(4);
    if(subtype>11||subtype==2||subtype==3)throw UnsupportedUpdate("unsupported_bgp4mp_subtype");
    u.width=(subtype==4||subtype==5||subtype==7||subtype==9||subtype==11)?4:2;
    u.local=subtype==6||subtype==7||subtype==10||subtype==11;u.addpath=subtype>=8;
    u.peer_asn=c.u(u.width);u.local_asn=c.u(u.width);u.interface=c.u(2);u.afi=c.u(2);
    if(u.afi!=1&&u.afi!=2)throw UnsupportedUpdate("unsupported_endpoint_afi");
    auto peer_raw=c.take(u.afi==1?4:16),local_raw=c.take(u.afi==1?4:16);
    u.peer_ip=ip(peer_raw);u.local_ip=ip(local_raw);u.endpoint=true;
    if(subtype==0||subtype==5){
        u.state=true;u.kind="state_change";u.old_state=c.u(2);u.new_state=c.u(2);c.done();
        require(entry,"libbgpdump 拒绝状态记录");auto& d=entry->body.zebra_state_change;
        require(d.source_as==u.peer_asn&&d.destination_as==u.local_asn&&d.interface_index==u.interface&&d.address_family==u.afi&&
            memcmp(&d.source_ip,peer_raw.data(),peer_raw.size())==0&&memcmp(&d.destination_ip,local_raw.data(),local_raw.size())==0&&d.old_state==u.old_state&&d.new_state==u.new_state,"C 库状态字段不符");return;
    }
    auto bgp_start=c.pos;require(c.take(16)==std::string(16,char(255)),"BGP marker 无效");auto length=c.u(2);u.bgp_type=c.u(1);
    require(length>=19&&bgp_start+length==frame.size(),"BGP 消息长度不符");u.bgp_header=true;
    switch(u.bgp_type){case 1:u.kind="open";break;case 2:u.kind="update";break;case 3:u.kind="notification";break;case 4:u.kind="keepalive";break;case 5:case 128:throw UnsupportedUpdate("bgpdump_route_refresh_not_implemented");default:throw UnsupportedUpdate("unsupported_bgp_message_type");}
    require(entry,"libbgpdump 拒绝 BGP 消息");auto& d=entry->body.zebra_message;
    auto& remote_ip=u.local?d.destination_ip:d.source_ip;auto& local_ip=u.local?d.source_ip:d.destination_ip;
    require((u.local?d.destination_as:d.source_as)==u.peer_asn&&(u.local?d.source_as:d.destination_as)==u.local_asn&&d.interface_index==u.interface&&d.address_family==u.afi&&d.type==u.bgp_type&&d.size==length&&
        memcmp(&remote_ip,peer_raw.data(),peer_raw.size())==0&&memcmp(&local_ip,local_raw.data(),local_raw.size())==0,"C 库消息头不符");
    if(u.bgp_type!=2)return;
    require(!d.cut_bytes&&!d.incomplete.orig_len,"libbgpdump 报告不完整 UPDATE");
    auto withdrawn=c.take(c.u(2));auto attr_size=c.u(2);u.attr_offset=offset+c.pos;u.raw=c.take(attr_size);auto a=entry->attr;
    require(a&&a->len==attr_size&&(attr_size==0||memcmp(a->data,u.raw.data(),attr_size)==0),"C 库 UPDATE 原始属性不符");
    u.path.key=hash(std::string(1,char(u.width))+u.raw);u.path.digest=hash(u.raw);u.path=interpret(u.raw,u.path,a,u.width);
    auto routes=[&](const std::string& raw,int afi,int safi,const std::string& action,const struct prefix* decoded,size_t count){
        if((afi!=1&&afi!=2)||safi<1||safi>3)throw UnsupportedUpdate("unsupported_nlri_family");
        Cursor n(raw);size_t i=0;
        while(n.more()){
            UpdateRoute r;r.afi=afi;r.safi=safi;r.action=action;if(u.addpath)r.path_id=n.u(4);
            auto bits=n.u(1);require(bits<=uint64_t(afi==1?32:128),"NLRI 前缀长度越界");r.raw=std::string(1,char(bits))+n.take((bits+7)/8);
            require(i<count&&decoded,"C 库 NLRI 数量不符");auto& p=decoded[i++];auto address=r.raw.substr(1);address.resize(afi==1?4:16,0);
            // 先核对 bgpdump 保留的原字节，再独立生成规范网络键。
            require(p.len==bits&&memcmp(&p.address,address.data(),address.size())==0&&(!r.path_id||p.path_id==*r.path_id),"C 库 NLRI 或 Add-Path 身份不符");
            r.prefix=network_prefix(std::string((const char*)&p.address,address.size()),p.len);u.routes.push_back(std::move(r));
        }require(i==count,"C 库 NLRI 数量不符");
    };
    routes(withdrawn,1,1,"withdraw",d.withdraw,d.withdraw_count);
    Cursor attrs(u.raw);while(attrs.more()){
        int flags=attrs.u(1),code=attrs.u(1);auto value=attrs.take(attrs.u(flags&16?2:1));if(code!=14&&code!=15)continue;
        Cursor mp(value);int afi=mp.u(2),safi=mp.u(1);
        if((afi!=1&&afi!=2)||safi<1||safi>3)throw UnsupportedUpdate("unsupported_nlri_family");
        if(code==14){mp.take(mp.u(1));mp.u(1);}
        auto nlri=mp.take(value.size()-mp.pos);auto decoded=a->mp_info?(code==14?a->mp_info->announce[afi][safi]:a->mp_info->withdraw[afi][safi]):nullptr;
        routes(nlri,afi,safi,code==14?"announce":"withdraw",decoded?decoded->nlri:nullptr,decoded?decoded->prefix_count:0);
        if(code==15&&nlri.empty())u.eors.emplace_back(afi,safi);
    }
    routes(c.take(frame.size()-c.pos),1,1,"announce",d.announce,d.announce_count);
    if(u.routes.empty()&&u.raw.empty())u.eors.emplace_back(1,1);
}
static uint64_t append_update(BGPDUMP_ENTRY* entry,const std::string& source,const std::string& source_sha,const std::string& source_path,
    uint64_t record,uint64_t offset,uint64_t skip,int part,Index& index,Table& messages,Table& elements,Table& paths,Table& eor,Table& quality,
    double& index_seconds,double& index_cpu,uint64_t& unique){
    UpdateFrame u;std::string status="decoded",reason,detail;
    try{decode_update(entry,u,offset);}
    catch(const UnsupportedUpdate& exc){status="unsupported";reason=exc.what();detail=reason;}
    catch(const std::runtime_error& exc){status="rejected";reason="bgpdump_payload_rejected";detail=exc.what();}
    const char* configured=getenv("DOMEYE_NATIVE_POLICY");std::string policy=configured?configured:"strict/v1";
    if(status=="rejected"&&policy!="isolate-payload/v1")throw std::runtime_error(detail);
    if(status!="decoded"){u.routes.clear();u.eors.clear();u.kind="unknown";u.state=false;}
    if(record<skip)return u.routes.size();auto mid=source+":"+std::to_string(record);
    if(status=="decoded"&&u.kind=="update"){
        double t=now(),ct=cpu();bool fresh=index.add(u.path,u.raw,part);index_seconds+=now()-t;index_cpu+=cpu()-ct;
        if(fresh){unique++;auto& p=u.path;paths.s(0,p.key);paths.b(1,u.raw);paths.s(2,p.digest);paths.i(3,u.width);
            if(p.original)paths.b(4,*p.original);else paths.n(4);if(p.as4)paths.b(5,*p.as4);else paths.n(5);paths.s(6,p.text);
            if(p.as4&&entry->attr->new_aspath&&entry->attr->new_aspath->str)paths.s(7,entry->attr->new_aspath->str);else paths.n(7);
            if(p.raw_origin)paths.l(8,*p.raw_origin);else paths.n(8);if(p.origin)paths.l(9,*p.origin);else paths.n(9);paths.s(10,p.reason);
        }
        for(size_t j=0;j<u.routes.size();j++){auto& r=u.routes[j];elements.s(0,mid+":"+std::to_string(j));elements.s(1,mid);elements.l(2,j);elements.s(3,r.action);elements.i(4,r.afi);elements.i(5,r.safi);elements.s(6,r.prefix);elements.b(7,r.raw);
            if(r.path_id)elements.l(8,*r.path_id);else elements.n(8);elements.flag(9,bool(r.path_id));elements.n(10);elements.s(11,u.peer_ip);elements.l(12,u.peer_asn);elements.n(13);elements.flag(14,false);elements.n(15);elements.n(16);
            elements.s(17,u.path.key);elements.l(18,u.attr_offset);elements.l(19,u.raw.size());elements.s(20,u.path.digest);
        }
        for(auto family:u.eors){eor.s(0,mid);eor.i(1,family.first);eor.i(2,family.second);}
    }
    auto num=[](auto v){return std::to_string(v);};auto nullable=[&](bool present,const std::string& value){return present?value:"null";};
    std::string header="{\"microsecond\":"+(u.microsecond?num(*u.microsecond):"null")+",\"endpoint_trust\":"+quote(u.endpoint?"complete_header":"unknown")+
        ",\"peer_ip\":"+nullable(u.endpoint,quote(u.peer_ip))+",\"peer_asn\":"+nullable(u.endpoint,num(u.peer_asn))+",\"local_ip\":"+nullable(u.endpoint,quote(u.local_ip))+
        ",\"local_asn\":"+nullable(u.endpoint,num(u.local_asn))+",\"interface\":"+nullable(u.endpoint,num(u.interface))+",\"endpoint_afi\":"+nullable(u.endpoint,num(u.afi))+
        ",\"direction\":"+quote(u.endpoint?(u.local?"local":"received"):"unknown")+",\"bgp_type\":"+nullable(u.bgp_header,num(u.bgp_type))+"}";
    auto interpretation="{\"status\":"+quote(status)+",\"policy\":"+quote(policy)+",\"reason_code\":"+(reason.empty()?"null":quote(reason))+
        ",\"header\":"+header+",\"failure\":null,\"interpretation_level\":"+quote(status=="decoded"?(u.kind=="update"?"route_elements":u.state?"state":"bgp_header_only"):u.bgp_header?"bgp_header_only":u.endpoint?"endpoint_header":"frame")+
        ",\"source_path\":"+quote(source_path)+",\"continuation_allowed\":true,\"frame_complete\":true,\"next_record_offset\":"+num(offset+frame.size())+",\"contract_version\":\"mrt-interpretation/bgpdump-v1\"}";
    messages.s(0,mid);messages.s(1,source);messages.s(2,source_sha);messages.l(3,record);messages.l(4,offset);messages.l(5,frame.size());messages.l(6,stamp);
    if(u.microsecond)messages.i(7,*u.microsecond);else messages.n(7);messages.i(8,mrt_type);messages.i(9,subtype);messages.s(10,hash(frame));messages.s(11,u.kind);
    if(reason.empty())messages.n(12);else messages.s(12,reason);
    if(u.endpoint&&status=="decoded"){messages.s(13,u.peer_ip);messages.l(14,u.peer_asn);}else{messages.n(13);messages.n(14);}messages.n(15);messages.flag(16,false);
    if(u.endpoint&&status=="decoded"){messages.s(17,u.local_ip);messages.l(18,u.local_asn);messages.i(19,u.interface);messages.i(20,u.afi);messages.flag(21,u.local);}else for(int j=17;j<=21;j++)messages.n(j);
    if(u.state){messages.i(22,u.old_state);messages.i(23,u.new_state);}else{messages.n(22);messages.n(23);}messages.s(24,interpretation);
    if(!reason.empty()){quality.s(0,source);quality.s(1,mid);quality.s(2,reason);quality.s(3,detail);}
    return u.routes.size();
}
