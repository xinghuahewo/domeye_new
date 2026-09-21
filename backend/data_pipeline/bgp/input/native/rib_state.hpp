// 有界 RIB 列批转唯一工作态负载；字节合同等同 shared_state.py 的 >IHBBI / >B32sqIQIqIIQI。
#include <limits>
#include <unordered_map>

static void state_uint(std::string& value,size_t offset,uint64_t number,size_t width){
    for(size_t i=0;i<width;i++)value[offset+width-i-1]=char(number>>(i*8));
}
static std::shared_ptr<arrow::Array> state_column(const arrow::RecordBatch& batch,const char* name,arrow::Type::type type){
    auto result=batch.GetColumnByName(name);
    if(!result||result->type_id()!=type)throw std::runtime_error(std::string("RIB 打包字段类型不符：")+name);
    return result;
}
extern "C" int domeye_pack_rib(struct ArrowArrayStream* input,struct ArrowArrayStream* endpoint_input,
    struct ArrowArrayStream* path_input,struct ArrowArrayStream* output,
    uint32_t source,uint32_t rank,uint64_t record,int64_t epoch,int64_t previous,char* error){
    try{
        auto reader=take(arrow::ImportRecordBatchReader(input));
        auto endpoints=take(arrow::ImportRecordBatchReader(endpoint_input));
        auto paths=take(arrow::ImportRecordBatchReader(path_input));
        std::unordered_map<std::string,uint32_t> endpoint_ids;
        std::unordered_map<std::string,std::string> path_values;
        while(auto batch=take(endpoints->Next())){
            check(batch->ValidateFull());
            auto ips=state_column(*batch,"peer_ip",arrow::Type::STRING),asns=state_column(*batch,"peer_asn",arrow::Type::INT64),ids=state_column(*batch,"endpoint_id",arrow::Type::INT64);
            for(int64_t i=0;i<batch->num_rows();i++){
                require(!ips->IsNull(i)&&!asns->IsNull(i)&&!ids->IsNull(i),"RIB 端点为空");
                auto id=static_cast<arrow::Int64Array&>(*ids).Value(i);
                require(id>=0&&id<65536,"RIB 端点编号越界");
                auto key=std::string(static_cast<arrow::StringArray&>(*ips).GetView(i))+":"+std::to_string(static_cast<arrow::Int64Array&>(*asns).Value(i));
                require(endpoint_ids.emplace(key,uint32_t(id)).second,"RIB 端点重复");
            }
        }
        while(auto batch=take(paths->Next())){
            check(batch->ValidateFull());
            auto keys=state_column(*batch,"path_key",arrow::Type::STRING),origins=state_column(*batch,"origin",arrow::Type::INT64);
            for(int64_t i=0;i<batch->num_rows();i++){
                require(!keys->IsNull(i),"RIB 路径键为空");
                auto key=std::string(static_cast<arrow::StringArray&>(*keys).GetView(i));
                auto value=binary_key(key);value.resize(40);
                auto origin=origins->IsNull(i)?int64_t(-1):static_cast<arrow::Int64Array&>(*origins).Value(i);
                require(origin>=-1,"RIB 起源编号无效");state_uint(value,32,uint64_t(origin),8);
                require(path_values.emplace(key,value).second,"RIB 路径字典重复");
            }
        }
        arrow::BinaryBuilder keys,values;arrow::UInt32Builder ids;
        int64_t last=previous,count=0;uint64_t last_record=record;
        while(auto batch=take(reader->Next())){
            check(batch->ValidateFull());count+=batch->num_rows();require(count<=8192,"RIB 状态批次超过有界行数");
            auto afi=state_column(*batch,"afi",arrow::Type::INT32),safi=state_column(*batch,"safi",arrow::Type::INT32);
            auto prefix=state_column(*batch,"prefix",arrow::Type::STRING),present=state_column(*batch,"path_id_present",arrow::Type::BOOL),pathid=state_column(*batch,"path_id",arrow::Type::INT64);
            auto ip=state_column(*batch,"peer_ip",arrow::Type::STRING),asn=state_column(*batch,"peer_asn",arrow::Type::INT64);
            auto path=state_column(*batch,"path_key",arrow::Type::STRING),ordinal=state_column(*batch,"ordinal",arrow::Type::INT64);
            auto messages=record==std::numeric_limits<uint64_t>::max()?state_column(*batch,"message_id",arrow::Type::STRING):nullptr;
            for(int64_t i=0;i<batch->num_rows();i++){
                require(!afi->IsNull(i)&&!safi->IsNull(i)&&!prefix->IsNull(i)&&!present->IsNull(i)&&!ip->IsNull(i)&&!asn->IsNull(i)&&!path->IsNull(i)&&!ordinal->IsNull(i),"RIB 工作态必要值为空");
                auto n=static_cast<arrow::Int64Array&>(*ordinal).Value(i);
                uint64_t row_record=record;
                if(messages){
                    require(!messages->IsNull(i),"RIB 消息键为空");
                    auto message=static_cast<arrow::StringArray&>(*messages).GetView(i);auto colon=message.rfind(':');
                    require(colon!=std::string_view::npos&&colon+1<message.size(),"RIB 消息记录号缺失");row_record=0;
                    for(char c:message.substr(colon+1)){
                        require(c>='0'&&c<='9'&&row_record<=2000000,"RIB 消息记录号无效");row_record=row_record*10+(c-'0');
                    }
                    if(row_record!=last_record){
                        require(last_record==std::numeric_limits<uint64_t>::max()||row_record>last_record,"RIB 消息顺序倒退");
                        last=last_record==std::numeric_limits<uint64_t>::max()?previous:-1;last_record=row_record;
                    }
                }
                require(n>last&&uint64_t(n)<=std::numeric_limits<uint32_t>::max(),"RIB 工作态位置重复或乱序");last=n;
                auto af=static_cast<arrow::Int32Array&>(*afi).Value(i),sf=static_cast<arrow::Int32Array&>(*safi).Value(i);
                require(af>=0&&af<=65535&&sf>=0&&sf<=255,"RIB 地址族越界");
                bool has=static_cast<arrow::BooleanArray&>(*present).Value(i);uint64_t pid=0;
                if(has){require(!pathid->IsNull(i),"ADD-PATH 编号缺失");auto p=static_cast<arrow::Int64Array&>(*pathid).Value(i);require(p>=0&&uint64_t(p)<=std::numeric_limits<uint32_t>::max(),"ADD-PATH 编号越界");pid=p;}
                else require(pathid->IsNull(i),"ADD-PATH 未声明却含编号");
                auto endpoint_key=std::string(static_cast<arrow::StringArray&>(*ip).GetView(i))+":"+std::to_string(static_cast<arrow::Int64Array&>(*asn).Value(i));
                auto endpoint=endpoint_ids.find(endpoint_key);require(endpoint!=endpoint_ids.end(),"RIB 端点未准备");
                std::string key(12,0);state_uint(key,0,endpoint->second,4);state_uint(key,4,af,2);key[6]=char(sf);key[7]=char(has);state_uint(key,8,pid,4);
                auto text=static_cast<arrow::StringArray&>(*prefix).GetView(i);
                for(unsigned char c:text)require(c<128,"RIB 网络键非 ASCII");key.append(text.data(),text.size());
                auto p=path_values.find(std::string(static_cast<arrow::StringArray&>(*path).GetView(i)));require(p!=path_values.end(),"RIB 路径引用未准备");
                std::string value(85,0);value[0]=7;value.replace(1,40,p->second);
                state_uint(value,41,source,4);state_uint(value,45,row_record,8);state_uint(value,53,n,4);state_uint(value,57,uint64_t(epoch),8);
                state_uint(value,69,rank,4);state_uint(value,73,row_record,8);state_uint(value,81,n,4);
                check(keys.Append(key));check(values.Append(value));check(ids.Append(endpoint->second));
            }
        }
        auto schema=arrow::schema({arrow::field("key",arrow::binary()),arrow::field("value",arrow::binary()),arrow::field("endpoint_id",arrow::uint32())});
        auto batch=arrow::RecordBatch::Make(schema,count,{take(keys.Finish()),take(values.Finish()),take(ids.Finish())});
        auto result=take(arrow::RecordBatchReader::Make({batch},schema));
        check(arrow::ExportRecordBatchReader(result,output));return 0;
    }catch(const std::exception& e){return failed(e,error);}
}
