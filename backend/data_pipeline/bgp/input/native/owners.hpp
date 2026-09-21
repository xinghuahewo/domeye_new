// 跨文件路径所有者的临时磁盘索引。正文摘要已经由实际 Parquet 读回产生。
#include <sqlite3.h>
struct OwnerIndex {
    sqlite3* db=nullptr;sqlite3_stmt *insert=nullptr,*lookup=nullptr;
    ~OwnerIndex(){sqlite3_finalize(insert);sqlite3_finalize(lookup);sqlite3_close(db);}
    void exec(const char* sql){require(sqlite3_exec(db,sql,nullptr,nullptr,nullptr)==SQLITE_OK,sqlite3_errmsg(db));}
};
extern "C" int domeye_merge_owners(struct ArrowArrayStream* input,const char* filename,const char* attempt,int64_t* count,char* error){
    try{
        auto reader=take(arrow::ImportRecordBatchReader(input));OwnerIndex index;
        require(sqlite3_open(filename,&index.db)==SQLITE_OK,"所有者索引打开失败");
        // 每次 seal 使用全新临时文件；崩溃后重建。最终权威数据仍由 DuckLake 事务持久化。
        index.exec("PRAGMA journal_mode=OFF;PRAGMA synchronous=OFF;PRAGMA cache_size=-262144;PRAGMA temp_store=FILE;CREATE TABLE IF NOT EXISTS owners(key BLOB PRIMARY KEY,attempt TEXT NOT NULL,digest BLOB NOT NULL) WITHOUT ROWID;BEGIN");
        require(sqlite3_prepare_v2(index.db,"INSERT OR IGNORE INTO owners VALUES(?,?,?)",-1,&index.insert,nullptr)==SQLITE_OK,"所有者插入准备失败");
        require(sqlite3_prepare_v2(index.db,"SELECT digest FROM owners WHERE key=?",-1,&index.lookup,nullptr)==SQLITE_OK,"所有者核对准备失败");*count=0;
        while(auto batch=take(reader->Next())){
            auto keys=batch->GetColumnByName("k1"),digests=batch->GetColumnByName("digest");
            require(keys&&digests&&keys->type_id()==arrow::Type::BINARY&&digests->type_id()==arrow::Type::BINARY,"所有者输入列无效");
            auto& k=static_cast<const arrow::BinaryArray&>(*keys);auto& d=static_cast<const arrow::BinaryArray&>(*digests);
            for(int64_t row=0;row<batch->num_rows();row++){
                require(!k.IsNull(row)&&!d.IsNull(row),"所有者输入为空");auto key=k.GetView(row),digest=d.GetView(row);require(key.size()==32&&digest.size()==32,"所有者摘要长度不符");
                sqlite3_reset(index.insert);sqlite3_bind_blob(index.insert,1,key.data(),32,SQLITE_TRANSIENT);sqlite3_bind_text(index.insert,2,attempt,-1,SQLITE_TRANSIENT);sqlite3_bind_blob(index.insert,3,digest.data(),32,SQLITE_TRANSIENT);
                require(sqlite3_step(index.insert)==SQLITE_DONE,sqlite3_errmsg(index.db));
                if(!sqlite3_changes(index.db)){
                    sqlite3_reset(index.lookup);sqlite3_bind_blob(index.lookup,1,key.data(),32,SQLITE_TRANSIENT);
                    require(sqlite3_step(index.lookup)==SQLITE_ROW&&sqlite3_column_bytes(index.lookup,0)==32&&memcmp(sqlite3_column_blob(index.lookup,0),digest.data(),32)==0,"跨文件相同 path_key 的 typed 正文不符");sqlite3_reset(index.lookup);
                }(*count)++;
            }
        }index.exec("COMMIT");return 0;
    }catch(const std::exception& e){return failed(e,error);}
}
class OwnerReader:public arrow::RecordBatchReader {
    OwnerIndex index;bool done=false;std::shared_ptr<arrow::Schema> columns=arrow::schema({arrow::field("path_key",arrow::utf8()),arrow::field("attempt",arrow::utf8())});
public:
    explicit OwnerReader(const char* filename){
        require(sqlite3_open_v2(filename,&index.db,SQLITE_OPEN_READONLY,nullptr)==SQLITE_OK,"所有者索引只读打开失败");
        require(sqlite3_prepare_v2(index.db,"SELECT key,attempt FROM owners ORDER BY key",-1,&index.lookup,nullptr)==SQLITE_OK,"所有者读取准备失败");
    }
    std::shared_ptr<arrow::Schema> schema()const override{return columns;}
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* output)override{
        try{
            if(done){*output=nullptr;return arrow::Status::OK();}arrow::StringBuilder keys,attempts;int rows=0;
            while(rows<65536){int rc=sqlite3_step(index.lookup);if(rc==SQLITE_DONE){done=true;break;}require(rc==SQLITE_ROW,"所有者索引读取失败");
                require(sqlite3_column_bytes(index.lookup,0)==32,"所有者索引键损坏");check(keys.Append(hex(static_cast<const unsigned char*>(sqlite3_column_blob(index.lookup,0)),32)));
                check(attempts.Append(reinterpret_cast<const char*>(sqlite3_column_text(index.lookup,1))));rows++;
            }
            if(!rows){*output=nullptr;return arrow::Status::OK();}*output=arrow::RecordBatch::Make(columns,rows,{take(keys.Finish()),take(attempts.Finish())});return arrow::Status::OK();
        }catch(const std::exception& e){return arrow::Status::Invalid(e.what());}
    }
};
extern "C" int domeye_owner_rows(const char* filename,struct ArrowArrayStream* output,char* error){
    try{check(arrow::ExportRecordBatchReader(std::make_shared<OwnerReader>(filename),output));return 0;}
    catch(const std::exception& e){return failed(e,error);}
}
