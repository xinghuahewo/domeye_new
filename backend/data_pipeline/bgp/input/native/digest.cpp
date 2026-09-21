// v1 typed JSON 的原生并行编码；排序只处理位置键和 32 字节行摘要。
#include "common.hpp"
#include <arrow/c/bridge.h>
#include <cstring>
#include <omp.h>

static void quoted(std::string& out,std::string_view value){
    static const char h[]="0123456789abcdef";out+='"';
    for(unsigned char c:value){switch(c){case '"':out+="\\\"";break;case '\\':out+="\\\\";break;
        case '\b':out+="\\b";break;case '\f':out+="\\f";break;case '\n':out+="\\n";break;
        case '\r':out+="\\r";break;case '\t':out+="\\t";break;
        default:if(c<32){out+="\\u00";out+=h[c>>4];out+=h[c&15];}else out+=char(c);}}
    out+='"';
}
static void encoded(std::string& out,const arrow::Array& a,int64_t row){
    if(a.IsNull(row)){out+="[\"NoneType\",null]";return;}
    switch(a.type_id()){
    case arrow::Type::STRING:out+="[\"str\",";quoted(out,static_cast<const arrow::StringArray&>(a).GetView(row));out+=']';break;
    case arrow::Type::BINARY:{
        static const char h[]="0123456789abcdef";auto v=static_cast<const arrow::BinaryArray&>(a).GetView(row);out+="[\"bytes\",\"";
        for(unsigned char c:v){out+=h[c>>4];out+=h[c&15];}out+="\"]";break;}
    case arrow::Type::INT64:out+="[\"int\",";out+=std::to_string(static_cast<const arrow::Int64Array&>(a).Value(row));out+=']';break;
    case arrow::Type::INT32:out+="[\"int\",";out+=std::to_string(static_cast<const arrow::Int32Array&>(a).Value(row));out+=']';break;
    case arrow::Type::BOOL:out+=static_cast<const arrow::BooleanArray&>(a).Value(row)?"[\"bool\",true]":"[\"bool\",false]";break;
    default:throw std::runtime_error("原生摘要类型不受支持");
    }
}
static int threads(){
    auto env=getenv("DOMEYE_NATIVE_DIGEST_THREADS");int n=env?std::stoi(env):2;require(n>=1&&n<=4,"摘要线程预算只能为 1—4");return n;
}
class Encoder {
    std::vector<int> order;std::vector<std::string> prefixes;
public:
    explicit Encoder(const std::shared_ptr<arrow::Schema>& schema){
        for(int i=0;i<schema->num_fields();i++){
            auto t=schema->field(i)->type()->id();require(t==arrow::Type::NA||t==arrow::Type::STRING||t==arrow::Type::BINARY||t==arrow::Type::INT64||t==arrow::Type::INT32||t==arrow::Type::BOOL,"原生摘要遇到未支持类型");order.push_back(i);
        }
        std::sort(order.begin(),order.end(),[&](int a,int b){return schema->field(a)->name()<schema->field(b)->name();});
        for(int i:order)prefixes.push_back("["+quote(schema->field(i)->name())+",");
    }
    std::vector<unsigned char> hashes(const arrow::RecordBatch& batch){
        check(batch.Validate());auto count=batch.num_rows();std::vector<unsigned char> result(count*32);int nt=threads();
        #pragma omp parallel num_threads(nt) if(count>=4096)
        {
            std::string row;row.reserve(4096);
            #pragma omp for schedule(static)
            for(int64_t r=0;r<count;r++){
                row="[\"dict\",[";for(size_t j=0;j<order.size();j++){if(j)row+=',';row+=prefixes[j];encoded(row,*batch.column(order[j]),r);row+=']';}row+="]]";
                // 每个线程使用栈上的上下文，避免 SHA256 便捷接口逐行获取 provider 的锁。
                SHA256_CTX single;SHA256_Init(&single);SHA256_Update(&single,row.data(),row.size());SHA256_Final(result.data()+r*32,&single);
            }
        }return result;
    }
};
static int failed(const std::exception& e,char* error){std::strncpy(error,e.what(),1023);error[1023]=0;return 1;}
static void finish(SHA256_CTX& hash,char* output){unsigned char bytes[32];SHA256_Final(bytes,&hash);auto text=hex(bytes,32);memcpy(output,text.c_str(),65);}

extern "C" int domeye_digest(struct ArrowArrayStream* input,char* output,int64_t* count,char* error){
    try{
        auto reader=take(arrow::ImportRecordBatchReader(input));Encoder encoder(reader->schema());SHA256_CTX all;SHA256_Init(&all);*count=0;
        while(auto batch=take(reader->Next())){auto hashes=encoder.hashes(*batch);SHA256_Update(&all,hashes.data(),hashes.size());*count+=batch->num_rows();}
        finish(all,output);return 0;
    }catch(const std::exception& e){return failed(e,error);}
}

// 单来源元素的记录号按字节序左对齐：1 < 10 < 100 < 11，保留原 message_id 字典序。
static int64_t record_key(std::string_view mid){
    auto colon=mid.rfind(':');require(colon!=std::string_view::npos,"消息身份缺少来源边界");auto text=mid.substr(colon+1);
    require(!text.empty()&&text.size()<=7&&(text.size()==1||text[0]!='0'),"记录号不符合原生 RIB 范围");uint64_t key=0;
    for(char c:text){require(c>='0'&&c<='9',"非十进制记录号");key=(key<<8)|(unsigned char)c;}return key<<((8-text.size())*8);
}
static std::string binary_key(std::string_view text){
    require(text.size()==64,"路径键长度错误");std::string bytes(32,0);
    auto nibble=[](char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;throw std::runtime_error("路径键不是小写 SHA256");};
    for(int i=0;i<32;i++)bytes[i]=char(nibble(text[2*i])*16+nibble(text[2*i+1]));return bytes;
}
class HashReader:public arrow::RecordBatchReader {
    std::shared_ptr<arrow::RecordBatchReader> input;int mode;Encoder encoder;std::shared_ptr<arrow::Schema> output_schema;
public:
    HashReader(std::shared_ptr<arrow::RecordBatchReader> in,int m):input(in),mode(m),encoder(in->schema()){
        require(mode>=1&&mode<=4,"摘要排序模式错误");output_schema=arrow::schema({arrow::field("k1",mode==3?arrow::binary():arrow::int64()),arrow::field("k2",arrow::int64()),arrow::field("digest",arrow::binary())});
    }
    std::shared_ptr<arrow::Schema> schema()const override{return output_schema;}
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* output)override{
        try{
            auto batch=take(input->Next());if(!batch){*output=nullptr;return arrow::Status::OK();}
            auto hashes=encoder.hashes(*batch);arrow::Int64Builder k1,k2;arrow::BinaryBuilder kb,digests;
            auto named=[&](const char* name){auto a=batch->GetColumnByName(name);require(bool(a),"缺少摘要排序字段");return a;};
            auto primary=named(mode==1?"record":mode==2?"message_id":mode==3?"path_key":"table_record");
            auto secondary=(mode==2||mode==4)?named(mode==2?"ordinal":"index"):nullptr;
            if(mode==1||mode==4)require(primary->type_id()==arrow::Type::INT64,"记录键类型错误");
            else require(primary->type_id()==arrow::Type::STRING,"文本键类型错误");
            if(secondary)require(secondary->type_id()==arrow::Type::INT64,"次级键类型错误");
            for(int64_t r=0;r<batch->num_rows();r++){
                require(!primary->IsNull(r)&&(!secondary||!secondary->IsNull(r)),"摘要排序键为空");
                if(mode==3)check(kb.Append(binary_key(static_cast<const arrow::StringArray&>(*primary).GetView(r))));
                else check(k1.Append(mode==2?record_key(static_cast<const arrow::StringArray&>(*primary).GetView(r)):static_cast<const arrow::Int64Array&>(*primary).Value(r)));
                check(k2.Append(secondary?static_cast<const arrow::Int64Array&>(*secondary).Value(r):0));check(digests.Append(hashes.data()+r*32,32));
            }
            *output=arrow::RecordBatch::Make(output_schema,batch->num_rows(),{mode==3?take(kb.Finish()):take(k1.Finish()),take(k2.Finish()),take(digests.Finish())});return arrow::Status::OK();
        }catch(const std::exception& e){return arrow::Status::Invalid(e.what());}
    }
};
extern "C" int domeye_hash_rows(struct ArrowArrayStream* input,struct ArrowArrayStream* output,int mode,char* error){
    try{auto reader=std::make_shared<HashReader>(take(arrow::ImportRecordBatchReader(input)),mode);check(arrow::ExportRecordBatchReader(reader,output));return 0;}
    catch(const std::exception& e){return failed(e,error);}
}
extern "C" int domeye_combine(struct ArrowArrayStream* input,char* output,int64_t* count,char* error){
    try{
        auto reader=take(arrow::ImportRecordBatchReader(input));SHA256_CTX all;SHA256_Init(&all);*count=0;
        while(auto batch=take(reader->Next())){
            require(batch->num_columns()==1&&batch->column(0)->type_id()==arrow::Type::BINARY,"合并输入必须为摘要列");auto& a=static_cast<const arrow::BinaryArray&>(*batch->column(0));
            for(int64_t r=0;r<batch->num_rows();r++){require(!a.IsNull(r),"摘要为空");auto value=a.GetView(r);require(value.size()==32,"行摘要长度不符");SHA256_Update(&all,value.data(),32);}*count+=batch->num_rows();
        }finish(all,output);return 0;
    }catch(const std::exception& e){return failed(e,error);}
}

#include "owners.hpp"
#include "rib_state.hpp"
