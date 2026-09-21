#!/usr/bin/env python3
"""在显式 Git 外目录构建固定 libbgpdump 与本项目的原生候选。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
import pyarrow as pa
from data_pipeline.bgp.archive.checkpoint import OBS_TABLES


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive',required=True);p.add_argument('--output',required=True)
    p.add_argument('--bz2-prefix',required=True);p.add_argument('--sqlite-prefix',required=True)
    args=p.parse_args();out=Path(args.output).resolve();out.mkdir(parents=True,exist_ok=True)
    archive=Path(args.archive);expected='c9519a520d781a0745c0c01612f3e50ce6685da397fa8397451f7cd5ec22b280'
    if hashlib.sha256(archive.read_bytes()).hexdigest()!=expected:raise ValueError('上游源码归档摘要不符')
    source=out/'bgpdump'
    if source.exists():shutil.rmtree(source)
    with tarfile.open(archive) as tar:
        members=tar.getmembers();prefix=members[0].name.split('/')[0]
        for m in members:
            if m.issym() or m.islnk() or Path(m.name).is_absolute() or '..' in Path(m.name).parts:raise ValueError('归档路径不安全')
        tar.extractall(out)
    (out/prefix).rename(source)
    native=Path(__file__).resolve().parents[2]/'backend/data_pipeline/bgp/input/native'
    schemas=['inline std::map<std::string,std::shared_ptr<arrow::Schema>> schemas(){ return {']
    types={'VARCHAR':'utf8','BIGINT':'int64','INTEGER':'int32','BOOLEAN':'boolean','BLOB':'binary'}
    for name in ('messages','elements','paths','peers','eor','quality'):
        fields=','.join('arrow::field('+json.dumps(n)+',arrow::'+types[t]+'())' for n,t in OBS_TABLES[name])
        schemas.append('{'+json.dumps(name)+',arrow::schema({'+fields+'})},')
    schemas.append('}; }')
    (out/'schema.inc').write_text('\n'.join(schemas)+'\n')
    path=source/'bgpdump_lib.c';text=path.read_text()
    text=text.replace('BGPDUMP_ENTRY*\tbgpdump_read_next(BGPDUMP *dump) {',
        'extern void domeye_header(BGPDUMP_ENTRY*,unsigned,int);\nextern void domeye_capture(BGPDUMP_ENTRY*,const unsigned char*,unsigned);\nBGPDUMP_ENTRY*\tbgpdump_read_next(BGPDUMP *dump) {')
    old='    if (!ok) {\n        if(bytes_read > 0) {'
    if text.count(old)!=1:raise ValueError('固定源码头部接缝漂移')
    text=text.replace(old,'    domeye_header(this_entry,bytes_read,ok);\n'+old)
    old='    mstream_init(&s,buffer,this_entry->length);'
    if text.count(old)!=1:raise ValueError('固定源码正文接缝漂移')
    text=text.replace(old,'    domeye_capture(this_entry,buffer,this_entry->length);\n'+old)
    path.write_text(text)
    # 上游 gzip EOF 分支读取全局 errno，会把 Arrow/SQLite 先前的 errno 误报为压缩错误。
    # 从当前 gzFile 获取真实 zlib 状态，保留 Z_ERRNO 的系统错误。
    path=source/'cfile_tools.c';text=path.read_text()
    old='stream->eof = gzeof(in);\n        stream->error2 = errno;'
    if text.count(old)!=1:raise ValueError('固定源码 gzip 错误接缝漂移')
    text=text.replace(old,'stream->eof = gzeof(in);\n        int zerror; gzerror(in, &zerror);\n        stream->error2 = (zerror == Z_OK || zerror == Z_STREAM_END) ? 0 : zerror;\n        if(zerror == Z_ERRNO) stream->error1 = errno;')
    path.write_text(text)
    bz=Path(args.bz2_prefix).resolve();sq=Path(args.sqlite_prefix).resolve()
    env={**os.environ,'CPPFLAGS':'-I'+str(bz/'usr/include'),'LDFLAGS':'-L'+str(bz/'usr/lib/x86_64-linux-gnu')}
    def run(cmd,cwd=source):subprocess.run(cmd,cwd=cwd,check=True,env=env)
    run(['./bootstrap.sh']);run(['./configure','CPPFLAGS=-I'+str(bz/'usr/include'),'LDFLAGS=-L'+str(bz/'usr/lib/x86_64-linux-gnu')])
    run(['make','-j2','CFLAGS=-O2 -g -fexceptions','libbgpdump.a'])
    # 上游 CLI 仅供回归：直接核对其格式化结果，不经旧 Python parser 转换。
    hooks=out/'reference-hooks.c'
    hooks.write_text('#include "bgpdump_lib.h"\nvoid domeye_header(BGPDUMP_ENTRY* e,unsigned n,int v){}\nvoid domeye_capture(BGPDUMP_ENTRY* e,const unsigned char* p,unsigned n){}\n')
    run(['gcc','-O2','-I'+str(source),str(source/'bgpdump.c'),str(hooks),str(source/'libbgpdump.a'),
         '-L'+str(bz/'usr/lib/x86_64-linux-gnu'),'-lz','-lbz2','-o',str(out/'bgpdump-reference')],out)
    libs=Path(pa.get_library_dirs()[0]);arrow=libs/'libarrow.so.2300'
    if pa.__version__!='23.0.1' or not arrow.is_file():raise ValueError('需锁定的 Linux Arrow 23.0.1')
    sqlite_runtime=out/'libsqlite3.so.0'
    shutil.copy2(Path('/usr/lib/x86_64-linux-gnu/libsqlite3.so.0').resolve(),sqlite_runtime)
    common=['g++','-std=c++17','-O2','-Wno-deprecated-declarations','-I'+pa.get_include(),'-I'+str(native),'-I'+str(out),'-Wl,-rpath,'+str(libs),'-Wl,-rpath,'+str(out)]
    run(common+['-shared','-fPIC','-fopenmp','-I'+str(sq/'usr/include'),str(native/'digest.cpp'),str(arrow),str(sqlite_runtime),'-lcrypto','-o',str(out/'libdomeye_digest.so')],out)
    run(common+['-I'+str(source),'-I'+str(bz/'usr/include'),'-I'+str(sq/'usr/include'),str(native/'rib.cpp'),str(source/'libbgpdump.a'),str(arrow),'-L'+str(bz/'usr/lib/x86_64-linux-gnu'),'-L'+str(sq/'usr/lib/x86_64-linux-gnu'),'-lsqlite3','-lcrypto','-lz','-lbz2','-o',str(out/'domeye-rib')],out)
    files=[*native.glob('*'),out/'schema.inc',out/'domeye-rib',out/'libdomeye_digest.so',out/'bgpdump-reference',sqlite_runtime]
    (out/'build.json').write_text(json.dumps({'upstream_archive_sha256':expected,'pyarrow':pa.__version__,
        'files':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}},indent=2)+'\n')
    print(out/'build.json')


if __name__=='__main__':main()
