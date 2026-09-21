"""H2有限原生清单适配；复用C.1结构载体，不改变旧profile身份。"""
from dataclasses import asdict
from pathlib import Path
import os

from data_pipeline.history.event_collection.freeze import Freezer as CollectionFreezer, safe_path, identity, decode_gzip, valid_sha
from data_pipeline.history.event_collection.model import Binding, Budget, CollectionLimits, FREEZE_RULE, PROFILE_RULE, profile_hash
from data_pipeline.history.event_collection.children import remaining, sqlite_preflight
from data_pipeline.history.database_import import SQLiteSource, freeze_sqlite_source
from data_pipeline.history.database_import.freeze import Limits, read_json, write_json
from data_pipeline.history.database_import.binding import source_label

PROFILES = ('core-rib-consumption/v1', 'rib-comparison-jsonl/v1')
ROOT_SCHEMA = {'core-rib-scale-manifest/v1': 'scale-manifest', 'core-rib-origin-manifest/v1': 'origin-manifest',
               'core-rib-path-package/v1': 'path-package', 'rib-path-comparison-manifest/v1': 'comparison-manifest'}
ROLES = {'summary.json': 'comparison-summary', 'peer-groups.json': 'peer-groups', 'frames.sqlite': 'rib-sqlite',
         'comparisons.jsonl.gz': 'comparisons', 'left.gz': 'mrt-gzip', 'right.gz': 'mrt-gzip', 'left.mrt': 'mrt', 'right.mrt': 'mrt'}


class Freezer(CollectionFreezer):
    def dependency(self, parent, node, ref, role, sha=None):
        local = (Path(self.sources[parent]['path']).parent / ref).absolute().as_uri() if '://' not in ref else ref
        scoped = Path(self.sources[parent]['path']).as_uri() + '/' + ref
        binding_key = scoped if scoped in self.ext else local
        if binding_key in self.ext:
            path, expected = self.ext[binding_key]
            if sha is not None and sha != expected: raise ValueError('H2外部SHA冲突')
            source = self.files[parent]
            try:
                target = self.visit(path, Path(path).as_uri(), role, source['profile'], source['version'], expected, source['root_id'])
            except (OSError, ValueError):
                self.edge(parent, node, ref, role, 'required_file', None, expected, 'failed')
                raise
            self.edge(parent, node, ref, role, 'required_file', target, expected)
            return target
        return super().dependency(parent, node, ref, role, sha)

    def bound(self, parent, node, role):
        reference = node.field('file').text()
        child = self.dependency(parent, node, reference, role, node.field('sha256').text())
        size = node.field('bytes', False)
        if size is not None and self.files[child]['raw_bytes'] != size.integer():
            raise ValueError('H2依赖声明字节不符')
        return child

    def identities(self, doc, file_id, column=None):
        node=self.spool.node(doc)
        if node.row['kind']!='object':return
        contexts=[(node,('schema_version','interpretation_version','comparison_version','observed_at','source_manifest_sha256'))]
        source=node.field('source',False)
        if source is not None and source.row['kind']=='object':contexts.append((source,('sha256','collector_id','coverage')))
        for parent,keys in contexts:
            for key in keys:
                value=parent.field(key,False)
                if value is None:continue
                if value.row['kind']!='string':raise ValueError('H2身份原类型不符')
                self.spool.add('identities',{'identity_ordinal':self.identity_count,'file_id':file_id,'document_id':doc,'node_ordinal':value.ordinal,'name':key,'value':value.text()})
                self.identity_count+=1

    def dispatch(self, node, fid, role):
        if role == 'rib-root':
            schema = node.field('schema_version').text()
            if schema not in ROOT_SCHEMA:
                raise ValueError('H2未知原生根schema')
            role = ROOT_SCHEMA[schema]
            profile = self.files[fid]['profile']
            if (profile == PROFILES[1]) != (role == 'comparison-manifest'):
                raise ValueError('H2根profile与schema冲突')
        if role in ('scale-manifest', 'origin-manifest'):
            stem = role.split('-')[0]
            from data_pipeline.overview.scale import EVIDENCE_NAMES
            expected = {'paths.sqlite', 'origins.json', 'peers.json'} if stem == 'origin' else EVIDENCE_NAMES
            if {n.row['key'] for n in node.field('evidence').children('object')} != expected:
                raise ValueError('H2原生evidence角色集合不符')
            if node.field('summary').field('file').text() != 'summary.json': raise ValueError('H2原summary位置不符')
            self.bound(fid, node.field('summary'), stem + '-summary')
            for item in node.field('evidence').children('object'):
                name = item.row['key']
                kind = {'paths.sqlite': 'rib-sqlite', 'origins.json': 'origins', 'peers.json': 'origin-peers'}.get(name, 'evidence')
                self.bound(fid, item, kind)
            if stem == 'origin':
                summary_id = self.target(fid, 'origin-summary')
                summary = self.spool.node(self.document_id(summary_id))
                self.dependency(fid, node.field('source_local_path'), node.field('source_local_path').text(), 'mrt-gzip', summary.field('source').field('sha256').text())
        elif role == 'scale-summary':
            source = node.field('source')
            self.dependency(fid, source, source.field('path').text(), 'mrt-gzip', source.field('sha256').text())
        elif role == 'path-package':
            evidence = node.field('evidence')
            if evidence.field('file').text() != 'source-manifest.json' or evidence.field('sha256').text() != node.field('source_manifest_sha256').text(): raise ValueError('H2消费源清单声明冲突')
            self.bound(fid, node.field('summary'), 'path-summary')
            self.dependency(fid, node, 'source-manifest.json', 'comparison-manifest', node.field('source_manifest_sha256').text())
        elif role == 'comparison-manifest':
            if node.field('schema_version').text() != 'rib-path-comparison-manifest/v1':
                raise ValueError('H2原comparison schema冲突')
            entries = list(node.field('files').children('object'))
            if {n.row['key'] for n in entries} != set(ROLES):
                raise ValueError('H2 comparison必需文件集合不符')
            for entry in entries:
                name = entry.row['key']
                target = self.dependency(fid, entry, name, ROLES[name], entry.field('sha256').text())
                if self.files[target]['raw_bytes'] != entry.field('bytes').integer():
                    raise ValueError('H2源依赖字节不符')
        # 这里只签结构审计；字段科学性与MRT引用由领域投影核验。
        if role.endswith('manifest') or role == 'path-package':
            self.scope(self.files[fid]['root_id'], role + ':' + str(fid), 'unknown', node)

    def target(self, fid, role):
        rows = self.spool.db.execute('SELECT target_file FROM edges WHERE parent_file=? AND role=?', (fid, role)).fetchall()
        if len(rows) != 1:
            raise ValueError('H2依赖不唯一')
        return rows[0][0]

    def document_id(self, fid):
        rows = self.spool.db.execute('SELECT document_id FROM documents WHERE file_id=?', (fid,)).fetchall()
        if len(rows) != 1:
            raise ValueError('H2原文档不唯一')
        return rows[0][0]

    def sqlite(self, path, fid):
        source = self.files[fid]
        directory = self.out / ('child-' + str(fid))
        limits, _ = remaining(self.budget)
        sqlite_preflight(path, limits)
        manifest = freeze_sqlite_source(path, directory, binding=SQLiteSource(Path(self.sources[fid]['path']).as_uri(), source['version'], source['raw_sha'], True),
                    publication={'status': 'unknown', 'reason': 'H2人工结构'}, availability={'status': 'unknown', 'reason': '尚未领域准入'}, limits=limits)
        self.children.append(str(manifest.relative_to(self.out)))
        source['child_index'] = len(self.children) - 1
        path.unlink()
        source['raw_path'] = str((directory / 'original.sqlite').relative_to(self.out))
        child = read_json(manifest, limits.max_metadata_bytes)
        self.budget.add('child_rows', sum(t['rows'] for t in child['tables']), self.budget.limits.max_total_rows)
        self.budget.add('child_data_bytes', sum(o['bytes'] for o in child['originals']) + sum(b['bytes'] for t in child['tables'] for b in t['blocks']), self.budget.limits.max_total_bytes)
        self.budget.add('temporary_bytes', sum(p.stat().st_size for p in directory.iterdir() if p.is_file()), self.budget.collection_limits.temporary_bytes)
        for p in directory.iterdir():
            if p.is_file(): self.artifact(p)

    def visit(self, path, uri, role, profile, version, sha, root_id):
        path = safe_path(path, self.roots); key = str(path)
        if key in self.active: raise ValueError('H2依赖环')
        if key in self.by_path:
            fid = self.by_path[key]; existing = self.files[fid]
            if (existing['raw_sha'], existing['role']) != (sha, role): raise ValueError('H2共享文件角色/摘要冲突')
            return fid
        if len(self.files) >= self.budget.collection_limits.files: raise ValueError('H2文件数超限')
        if role == 'rib-sqlite':
            if Path(str(path) + '-wal').exists(): raise ValueError('H2原SQLite WAL未关闭')
            self.sqlite_sources.append(path)
        fid = len(self.files); self.by_path[key] = fid; self.active.add(key)
        raw = self.out / ('raw-' + str(fid)); self.copy(path, raw, sha)
        row = dict(file_id=fid, root_id=root_id, uri=uri, role=role, profile=profile, version=version,
                   raw_path=raw.name, raw_sha=sha, raw_bytes=raw.stat().st_size, members=0)
        self.files.append(row)
        try:
            if role == 'rib-sqlite': self.sqlite(raw, fid)
            elif role in ('mrt', 'evidence'): pass
            else:
                entity = raw
                if role in ('mrt-gzip', 'comparisons'):
                    entity = self.out / ('decoded-' + str(fid)); size = decode_gzip(raw, entity, self.budget)
                    row.update(decoded_path=entity.name, decoded_sha=self.budget.hash(entity), decoded_bytes=size, members=1); self.artifact(entity)
                else:
                    row.update(decoded_path=raw.name, decoded_sha=sha, decoded_bytes=row['raw_bytes'])
                    self.budget.add('decoded_bytes', row['raw_bytes'], self.budget.collection_limits.decoded_bytes)
                if role == 'comparisons': row['_document_count'] = self.jsonl(entity, fid, role)
                elif role != 'mrt-gzip':
                    node = self.document(entity, fid)
                    self.dispatch(node, fid, role)
            self.artifact(self.out / row['raw_path']); self.spool.add('files', row)
            return fid
        finally:
            self.active.remove(key)

    def finish(self):
        from data_pipeline.history.rib_index.model import freeze_identity
        code = freeze_identity()
        for index, root in enumerate(self.binding.roots):
            if root.profile not in PROFILES: raise ValueError('未知H2 profile')
            source_label(root.origin_uri, root.source_version)
            self.visit(root.path, root.origin_uri, 'rib-root', root.profile, root.source_version, root.sha256, index)
        for path in self.sqlite_sources:
            if Path(str(path) + '-wal').exists(): raise ValueError('H2原SQLite最终出现WAL')
        for source in self.sources:
            path = safe_path(source['path'], self.roots)
            if identity(path.stat()) != source['identity'] or self.budget.hash(path) != source['sha256']: raise ValueError('H2最终源变化')
        for directory, names in self.inventory.items():
            if self.directory_names(Path(directory)) != names: raise ValueError('H2封闭目录变化')
            if any((Path(directory) / n).is_file() and str(Path(directory) / n) not in self.by_path for n in names): raise ValueError('H2未声明文件')
        self.spool.flush(); self.spool.close(); self.artifact(self.out / 'structure.sqlite')
        for item in self.artifacts.values():
            p = self.out / item['path']
            if p.stat().st_size != item['bytes'] or self.budget.hash(p) != item['sha256']: raise ValueError('H2封存变化')
        if code != freeze_identity(): raise ValueError('H2冻结代码漂移')
        manifest = dict(schema_version=FREEZE_RULE, profile_rule=PROFILE_RULE, collection_id=self.id, profile_sha256=profile_hash(),
                        data_kind='fixture', integrity_state='complete', profile_admission='structure_audit_only',
                        roots=[asdict(r) for r in self.binding.roots], sources=self.sources, closed_directories=self.inventory,
                        children=self.children, artifacts=list(self.artifacts.values()), limits=asdict(self.budget.limits),
                        collection_limits=asdict(self.budget.collection_limits), resources=self.budget.report(), business_publication=None,
                        h2_freezer_code=code)
        from data_pipeline.history.database_import.codec import canonical
        if len(canonical(manifest)) > self.budget.collection_limits.metadata_bytes: raise ValueError('H2清单超限')
        write_json(self.out / 'manifest.json', manifest)
        write_json(self.out / 'COMPLETE.json', {'collection_id': self.id, 'manifest_sha256': self.budget.hash(self.out / 'manifest.json')})
        return self.out / 'manifest.json'


def freeze_collection(binding, destination, *, limits=Limits(), collection_limits=CollectionLimits()):
    freezer = object.__new__(Freezer); freezer._created = False
    try:
        freezer.__init__(binding, destination, limits, collection_limits)
        return freezer.finish()
    except BaseException as error:
        if freezer._created:
            if hasattr(freezer, 'spool'):
                try: freezer.spool.flush()
                except Exception: pass
                try: freezer.spool.close()
                except Exception: pass
            write_json(freezer.out / 'FAILED.json', {'state': 'failed', 'reason': str(error), 'scope': 'H2未签发集合完成'})
        raise
