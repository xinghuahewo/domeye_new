// 原生候选共用的 Arrow 构造与严格编码；不定义新的业务归属规则。
#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <openssl/sha.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

inline void check(const arrow::Status& s) { if (!s.ok()) throw std::runtime_error(s.ToString()); }
template<class T> T take(arrow::Result<T> r) { if (!r.ok()) throw std::runtime_error(r.status().ToString()); return std::move(r).ValueUnsafe(); }
inline void require(bool b,const char* text) { if (!b) throw std::runtime_error(text); }
inline std::string hex(const void* data,size_t n) {
    static const char chars[]="0123456789abcdef"; auto p=(const unsigned char*)data;
    std::string r(n*2,'0'); for(size_t i=0;i<n;i++){r[2*i]=chars[p[i]>>4];r[2*i+1]=chars[p[i]&15];} return r;
}
inline std::string hash(const std::string& s) { unsigned char h[32]; SHA256((const unsigned char*)s.data(),s.size(),h);return hex(h,32); }
inline std::string quote(const std::string& s) {
    std::string r="\""; for(unsigned char c:s) {
        switch(c) {case '"':r+="\\\"";break;case '\\':r+="\\\\";break;
        case '\b':r+="\\b";break;case '\f':r+="\\f";break;case '\n':r+="\\n";break;
        case '\r':r+="\\r";break;case '\t':r+="\\t";break;
        default: if(c<32){r+="\\u00";r+=hex(&c,1);}else r+=char(c);}
    } return r+'"';
}
inline void sync_file(const std::filesystem::path& p) {int fd=open(p.c_str(),O_RDONLY);require(fd>=0,"文件无法同步");int code=fsync(fd);close(fd);require(code==0,"文件同步失败");}
inline void marker(const std::filesystem::path& p,const std::string& s) {
    auto tmp=p;tmp+=".partial";{std::ofstream f(tmp);f<<s<<'\n';f.close();require(bool(f),"回执写入失败");}
    sync_file(tmp);std::filesystem::rename(tmp,p);sync_file(p.parent_path());
}
struct Table {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> cols;
    explicit Table(std::shared_ptr<arrow::Schema> s):schema(s) {for(auto& f:s->fields())cols.push_back(take(arrow::MakeBuilder(f->type())));}
    void s(int i,const std::string& v){check(static_cast<arrow::StringBuilder*>(cols[i].get())->Append(v));}
    void b(int i,const std::string& v){check(static_cast<arrow::BinaryBuilder*>(cols[i].get())->Append(v));}
    void l(int i,int64_t v){check(static_cast<arrow::Int64Builder*>(cols[i].get())->Append(v));}
    void i(int i,int32_t v){check(static_cast<arrow::Int32Builder*>(cols[i].get())->Append(v));}
    void flag(int i,bool v){check(static_cast<arrow::BooleanBuilder*>(cols[i].get())->Append(v));}
    void n(int i){check(cols[i]->AppendNull());}
    int64_t rows()const{return cols.empty()?0:cols[0]->length();}
    void write(const std::filesystem::path& path){
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        auto count=rows();for(auto& c:cols){require(c->length()==count,"列长度不一致");arrays.push_back(take(c->Finish()));}
        auto batch=arrow::RecordBatch::Make(schema,count,arrays);check(batch->ValidateFull());
        auto file=take(arrow::io::FileOutputStream::Open(path.string()));
        auto options=arrow::ipc::IpcWriteOptions::Defaults();
        options.codec=take(arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
        auto writer=take(arrow::ipc::MakeFileWriter(file,schema,options));check(writer->WriteRecordBatch(*batch));check(writer->Close());check(file->Close());sync_file(path);
    }
};
