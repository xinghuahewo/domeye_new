"""8233 artificial/v1原元数据接合；只读固定JSON，不读取原始科学正文。"""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
from data_pipeline.results.manifest_io import require
from data_pipeline.results.component_roles import codec

FIELDS = {'profile','description','source_root','manifest_sha256','mapping_sha256','counts'}
CAPS = dict(mrt_sources=32,reference_sources=32,resource_sources=16,calculation_sources=32)


class ArtificialInput:
    def __init__(self, *, manifest, mapping, input, country_reference, trend_reference=None):
        self.manifest_path = Path(manifest)
        self.mapping_path = Path(mapping)
        self.input = deepcopy(input)
        self.country_reference = deepcopy(country_reference)
        self.trend_reference = deepcopy(trend_reference)
        self.derived_identity = None
        self._fixed = json.dumps([self.input,self.country_reference,self.trend_reference],sort_keys=True)
        require(set(self.input) == FIELDS and self.input['profile']=='artificial/v1', '人工input合同字段错误')
        require(type(self.input['description']) is str and bool(self.input['description'].strip()), '人工来源说明缺失')
        self.root = Path(self.input['source_root'])
        require(self.root.is_absolute() and self.root.resolve()==self.root and self.root.is_dir(), '人工来源根必须固定绝对目录')
        self.root_identity = self._root_identity()
        counts = self.input['counts']
        require(type(counts) is dict and set(counts)==set(CAPS)
                and all(type(counts[k]) is int and 1<=counts[k]<=cap for k,cap in CAPS.items()), '人工规模越界')

        if self.trend_reference is not None:
            require(set(self.trend_reference)=={'source_path','source_sha256','derived_root','country_admission_id','binding'}, '人工Trend派生合同字段不符')
            derived=Path(self.trend_reference['derived_root'])
            require(derived.is_absolute() and derived.resolve()==derived and derived.is_dir(), '人工Trend派生根必须固定绝对目录')
            self.derived_identity=(derived.stat().st_dev,derived.stat().st_ino)

    def verify_derived_reference(self, reference, country_admission_id):
        self._configuration_current()
        spec=self.trend_reference
        require(spec is not None, '人工Trend缺显式派生参考来源合同')
        self._inside(spec['source_path'])
        source=self._read(Path(spec['source_path']),spec['source_sha256'])
        require(type(source.get('origin_uri')) is str and source['origin_uri'].startswith('fixture://'), '人工Trend独立原来源标识不符')
        root=Path(spec['derived_root']);path=Path(reference['path'])
        require(root.resolve()==root and root.is_dir() and (root.stat().st_dev,root.stat().st_ino)==self.derived_identity, '人工Trend派生根身份改变')
        require(not (root.is_relative_to(self.root) or self.root.is_relative_to(root)), '人工Trend派生根与冻结原根交叠')
        require(path.is_absolute() and root in path.resolve().parents, '人工Trend派生文件越根')
        require(reference==spec['binding'] and reference['origin_uri'].startswith('fixture://')
                and country_admission_id==spec['country_admission_id'], '人工Trend派生Binding或Country来源不符')
        return deepcopy(spec)|dict(source_origin_uri=source['origin_uri'],derived_root_identity=dict(device=self.derived_identity[0],inode=self.derived_identity[1]))

    def _configuration_current(self):
        require(json.dumps([self.input,self.country_reference,self.trend_reference],sort_keys=True)==self._fixed, '人工输入配置改变')
        require(self._root_identity()==self.root_identity and self.root.resolve()==self.root, '人工来源根身份改变')

    def _root_identity(self):
        st=self.root.stat()
        return dict(device=st.st_dev,inode=st.st_ino)

    def _inside(self, path):
        path=Path(path)
        require(path.is_absolute() and self.root in path.resolve().parents, '人工来源越根')

    def _read(self,path,sha):
        require(type(sha) is str and len(sha)==64 and all(c in '0123456789abcdef' for c in sha), '人工JSON SHA无效')
        with path.open('rb') as f: raw=f.read(8*1024**2+1)
        require(len(raw)<=8*1024**2, '人工元数据大小越界')
        require(hashlib.sha256(raw).hexdigest()==sha, '人工原JSON字节SHA不符')
        return json.loads(raw)

    def verify(self, admissions):
        self._configuration_current()
        derived=None
        manifest=self._read(self.manifest_path,self.input['manifest_sha256'])
        mapping=self._read(self.mapping_path,self.input['mapping_sha256'])
        entries=manifest['inputs'];refs=manifest['references'];counts=self.input['counts']
        ids=[e['source_id'] for e in entries]
        require(len(entries)==len(set(ids))==counts['mrt_sources']
                and len(refs)==len({r['sha256'] for r in refs})==counts['reference_sources'], '人工原清单计数不符')
        for entry in [*entries,*refs]: self._inside(entry['path'])
        require(all(e['origin_uri'].startswith('fixture://') for e in entries), '人工MRT混入非fixture来源')
        for ref in refs:
            require('origin_uri' not in ref or ref['origin_uri'].startswith('fixture://'), '人工参考混入非fixture来源')
        expected=[manifest['baseline_source'],*manifest['update_sources']]
        selections={}
        for owner,count in (('Resource',counts['resource_sources']),('Feature',counts['calculation_sources']),
                            ('canonical',counts['calculation_sources']),('Detection',counts['calculation_sources'])):
            rows=mapping['consumers'][owner]['selected_sources'];selected=[r['source_id'] for r in rows]
            require(len(rows)==len(set(selected))==count, '人工消费数量不符')
            for i,row in enumerate(rows):
                rank=row['joint_mrt_rank_zero_based']
                require(type(rank) is int and 0<=rank<len(ids) and row['selection_sequence_zero_based']==i
                        and ids[rank]==row['source_id'] and row['joint_role']==entries[rank]['role'], '人工消费原rank/角色不符')
            if owner!='Resource': require(selected==expected, '人工F/C/D原顺序不符')
            else:
                from data_pipeline.analysis.detection.result_window import instant
                times=[instant(r['nominal_time_utc']) for r in rows]
                require(all(a<b for a,b in zip(times,times[1:])), '人工Resource时点不递增')
                require(all(r['joint_role'] in ('baseline','snapshot') for r in rows), '人工Resource非RIB')
            selections[owner.lower()]=selected
        m2s=[a for a in admissions if a['owner']=='m2']
        require(bool(m2s), '人工模式缺原M2完整Binding')
        for a in m2s:
            b=codec('m2').untyped(a['owner_binding'])
            require(b['plan']['manifest']==manifest, '人工原manifest与实际M2完整Binding不符')
        for a in admissions:
            owner=a['owner']
            if owner=='trend':
                from data_pipeline.results.trend_metadata import metadata
                trend_inputs=metadata(a);reference=trend_inputs['reference_binding']
                if reference is not None:
                    country=next(d for d in trend_inputs['dependencies'] if d['owner']=='country')
                    derived=self.verify_derived_reference(reference,country['admission_id'])
                continue
            if owner not in ('resource','feature','canonical','detection'): continue
            b=codec(owner).untyped(a['owner_binding'])
            if owner=='resource':
                actual=[s['context']['source_id'] for s in b['binding']['sources']]
                from data_pipeline.analysis.detection.result_window import instant
                require([instant(s['context']['snapshot_time']) for s in b['binding']['sources']]
                        ==[instant(r['nominal_time_utc']) for r in mapping['consumers']['Resource']['selected_sources']], '人工Resource原时点不符')
                ref=b['binding']['country_reference']
                require(ref['origin_uri'].startswith('fixture://'), '人工国家参考混入非fixture来源')
                original=self.country_reference
                self._inside(original['path'])
                require(all(original[k]==ref[k] for k in ('origin_uri','content_sha256')), '人工国家参考原配置不符')
                # 只读取实际AD封存的登记元数据；正文仍由owner原公开路径检验。
                receipts=[e for e in a['entities'] if Path(e['path']).name=='execution.json']
                matched=False
                for entity in receipts:
                    receipt=self._read(Path(entity['path']),entity['sha256'])
                    if receipt.get('reference_id')!=ref['reference_id']: continue
                    require(receipt['cache_path']==str(Path(original['path']).resolve())
                            and receipt['content_sha256']==original['content_sha256']
                            and receipt['origin_uri']==original['origin_uri'], '人工国家参考原登记不符')
                    matched=True
                require(matched, '人工国家参考缺原登记实体')
            elif owner=='feature':actual=[s['source_id'] for s in b['specification']['source_bindings']]
            elif owner=='canonical':actual=b['descriptor']['plan']['selected_sources']
            else:actual=b['identity']['selected_sources']
            require(actual==selections[owner], '人工消费映射与实际完整Binding不符')
        require(self._root_identity()==self.root_identity, '人工来源根身份改变')
        require(self.trend_reference is None or derived is not None, '人工Trend派生合同缺实际绑定')
        result=dict(input=deepcopy(self.input),source_root_identity=dict(self.root_identity),
                    country_reference={k:self.country_reference[k] for k in ('path','origin_uri','content_sha256')})
        if derived is not None:result['trend_reference']=derived
        return result
