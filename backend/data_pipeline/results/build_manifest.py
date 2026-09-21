"""同一发布器的离线完整流准备；无第二publisher、资格补签或正文副本。"""
from contextlib import closing
from pathlib import Path
from copy import deepcopy
import json
import psycopg2
from psycopg2.extras import Json
from data_pipeline.results.manifest_io import require, digest, write_sealed, file_hash, fsync_dir
from data_pipeline.results.component_roles import codec, verify_known_roles
from data_pipeline.results.component_streams import read_required


class WriteScope:
    """只约束本次实际写目录；真实路径和目录身份在每次写入前重验。"""
    def __init__(self,p,request,output):
        self.root=Path(p.root);self.output=Path(output)
        self.identity=(self.root.stat().st_dev,self.root.stat().st_ino)
        protected=[]
        admissions=request['components']+[d['admission'] for d in request['dependencies']]
        for a in admissions:
            protected.append(Path(a['physical']['root']))
            protected.extend(Path(e['path']).parent for e in a['entities'])
            if a['owner']=='resource':
                for entity in a['entities']:
                    path=Path(entity['path'])
                    if path.name!='execution.json':continue
                    with path.open('rb') as handle:raw=handle.read(8*1024**2+1)
                    import hashlib
                    require(len(raw)<=8*1024**2 and hashlib.sha256(raw).hexdigest()==entity['sha256'], '原参考登记元数据大小或SHA不符')
                    registration=json.loads(raw)
                    if 'cache_path' in registration:protected.append(Path(registration['cache_path']).parent)
            if a['owner']=='m2':
                manifest=codec('m2').untyped(a['owner_binding'])['plan']['manifest']
                protected.extend(Path(e['path']).parent for e in [*manifest['inputs'],*manifest['references']])
        if p.combined_input is not None:
            protected.extend((p.combined_input.root,Path(p.combined_input.country_reference['path']).parent))
        self.protected=tuple((path,path.resolve(),(path.stat().st_dev,path.stat().st_ino)) for path in protected)
        self.write_paths=tuple(path for path in (*reversed(self.output.parents),self.output)
                               if path.is_relative_to(self.root) and path!=self.root)
        self.write_identities={path:(path.stat().st_dev,path.stat().st_ino)
                               for path in self.write_paths if path.exists()}
        self.created_bound=False
        self.check()

    def bind_created(self):
        require(not self.created_bound, '组合写目录身份不得重绑定')
        self.check()
        require(all(path.is_dir() for path in self.write_paths), '组合写目录身份缺失')
        for path in self.write_paths:
            if path not in self.write_identities:
                self.write_identities[path]=(path.stat().st_dev,path.stat().st_ino)
        self.created_bound=True
        self.check()

    def check(self):
        require((self.root.stat().st_dev,self.root.stat().st_ino)==self.identity, 'Publication写根身份改变')
        require(self.output.is_relative_to(self.root) and self.output!=self.root, '组合输出越界')
        for path in (self.output,*self.output.parents):
            require(not path.is_symlink(), '组合输出祖先不允许符号链接')
        require(self.output.resolve()==self.output, '组合输出别名漂移')
        for path,identity in self.write_identities.items():
            require(path.is_dir() and (path.stat().st_dev,path.stat().st_ino)==identity, '组合写目录身份漂移')
        for original,expected,identity in self.protected:
            require(original.resolve()==expected, '只读来源别名漂移')
            require((original.stat().st_dev,original.stat().st_ino)==identity, '只读来源目录身份漂移')
            require(not (self.output.is_relative_to(expected) or expected.is_relative_to(self.output)), '组合输出与只读来源交叠')


def code_sha():
    return digest({p.name:file_hash(p) for p in sorted(Path(__file__).parent.glob('*.py'))})


def evidence(p,manifest):
    """重验prepare封存的完整原回执；不将本地回执视为当前owner资格。"""
    path=p.root/'q1-builds'/manifest['build_id']/'composition.json'
    require(file_hash(path,p.guard)==manifest['composition_validation']['typed_digest'], '组合完整流证据漂移')
    with path.open('rb') as handle:raw=handle.read(16*1024**2+1)
    require(len(raw)<=16*1024**2, '组合回执元数据超限')
    value=json.loads(raw)
    require(value['role_graph']==manifest['role_graph'] and value['qualification_summary']==manifest['qualification_summary'], '组合原角色/资格证据不符')
    return value


def prepare(p,request,*,max_rows,max_bytes,progress=None):
    """正文累计量只计量；max_rows兼容，max_bytes仍限制驻留控制元数据。

    原composition整件保护仍为min(max_bytes,16MiB)，不是流读取总预算。
    Limits.max_rows供其他旧接口使用，本路径不以它限制流式总行数。
    """
    from data_pipeline.results import published_reader as combined
    from data_pipeline.results import Token
    request=combined.validate_structure(request)
    require(type(max_rows) is int and max_rows>0
            and type(max_bytes) is int and max_bytes>0, '组合流兼容参数及驻留元数据预算必须为正整数')
    before=code_sha()
    with combined.candidate(p,request) as (build,output,fixed,scope):
        scope.check()
        # 同一原图单锁持续覆盖全部读取、尾current、封存及ready控制事务提交。
        with fixed.locked():
            with read_required(fixed,request['components'],batch_rows=p.limits.batch_rows,
                               batch_bytes=p.limits.batch_bytes,max_rows=max_rows,max_bytes=max_bytes,
                               max_row_bytes=p.limits.max_row_bytes,progress=progress) as streams:
                for owner,req,batch in streams:
                    p.guard();scope.check()
            require(streams.receipts is not None and streams.qualification_summary is not None, '组合完整流屏障未完成')
            require(not verify_known_roles(request), '组合存在未接合角色')
            combined.validate_mode(request['profile'],request['components']+[d['admission'] for d in request['dependencies']],
                                   p.combined,artificial_input=p.combined_input,declared=request.get('artificial_input'))
            require(before==code_sha(), '组合准备期间代码改变')
            summaries=list(streams.qualification_summary)
            proof=dict(role_graph=request['role_graph'],qualification_summary=summaries,reads=list(streams.receipts))
            from data_pipeline.results.manifest_io import encode
            require(len(encode(proof).encode())<=min(max_bytes,16*1024**2), '组合原回执元数据超限')
            scope.check();write_sealed(output/'composition.json',proof)
            manifest={k:deepcopy(request[k]) for k in ('contract','profile','components','dependencies','edges','role_graph')}
            if 'artificial_input' in request:manifest['artificial_input']=deepcopy(request['artificial_input'])
            manifest.update(schema_sha256=combined.schema_sha(request['profile'],derived_reference='trend_reference' in request.get('artificial_input',{})),build_id=build,
                qualification_summary=summaries,composition_validation=dict(validator_version=combined.CONTRACT,
                edges_checked=len(request['edges']),typed_digest=digest(proof)),code_sha256=before)
            combined.validate_manifest(manifest)
            scope.check();artifact=write_sealed(output/'manifest.json',manifest)
            fsync_dir(output.parent);fsync_dir(p.root)
            fixed.current();scope.check()
            evidence(p,manifest)
            require(file_hash(artifact['path'],p.guard)==artifact['sha256'], '组合ready前清单漂移')
            require(before==code_sha(), '组合ready前代码改变')
            publication='q1_'+digest(manifest)
            with closing(psycopg2.connect(p.dsn)) as pg,pg,pg.cursor() as c:
                p._owner(c)
                c.execute("UPDATE publication_q1.builds SET state='ready',manifest=%s,manifest_path=%s,manifest_sha=%s,publication_id=%s WHERE build_id=%s AND state='candidate'",
                          (Json(manifest),artifact['path'],artifact['sha256'],publication,build))
                require(c.rowcount==1,'组合候选状态漂移')
    return Token(publication,build,digest(request['profile']))
