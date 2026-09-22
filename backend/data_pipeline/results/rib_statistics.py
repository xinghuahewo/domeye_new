"""完整原生 RIB 归档的独立时点统计与显式交付。

复用既有起源和 Resource 规则，读取固定段正文；不恢复归档 PG、不重放 UPDATE。
这是小型统计结果，不冒充带完整观察分页的 rib_snapshot_v1 或 Resource 16 表制品。
"""
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import resource
import shutil
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import Json

from data_pipeline.analysis.resources import compute as resource_compute
from data_pipeline.bgp.snapshots import origin
from utils import prefix_quantity

PROFILE = 'rib-statistics/v1'
FAMILIES = ('all', 'ipv4', 'ipv6')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def bind_archive(source_run, archive_run, ordinal=0):
    """从原封不动的源清单、源完成回执和原段登记提取绑定；不启动旧数据库。"""
    source_run, archive_run = Path(source_run).resolve(), Path(archive_run).resolve()
    manifest_path = source_run / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    entry = manifest['inputs'][ordinal]
    require(entry['role'] in ('baseline', 'snapshot'), '只接受 RIB 来源')
    receipt_path = source_run / f'run/results/file-{ordinal:04d}.json'
    receipt = json.loads(receipt_path.read_text())
    complete = receipt['source_receipt']
    require(complete['kind'] == 'source_complete' and complete['source_id'] == entry['source_id']
            and complete['source_sha'] == entry['sha256'] and complete['ordinal'] == ordinal,
            'RIB 完成回执与来源不一致')
    require(complete['counts']['rejected'] == complete['counts']['unsupported'] == 0,
            '有解析缺口的 RIB 尚不能提供本统计主值')
    old_manifest = json.loads((archive_run / 'manifest.json').read_text())
    candidates = [(i, e) for i, e in enumerate(old_manifest['inputs']) if e['source_id'] == entry['source_id']]
    require(len(candidates) == 1 and candidates[0][1]['sha256'] == entry['sha256'], '归档不属于同一原件')
    native = archive_run / f'run/archive/native-file-{candidates[0][0]:04d}'
    attempt = json.loads((native / 'attempt.json').read_text())
    require(attempt['source_id'] == entry['source_id'], '归档尝试来源不符')
    audit_path = native / 'source-to-columns-audit.json'
    audit = json.loads(audit_path.read_text())
    require(audit['all_message_headers_and_digests'] == complete['counts']['messages']
            and audit['all_element_raw_prefixes'] == complete['counts']['elements'], '归档核对计数不符')
    run_id = json.loads((archive_run / 'run/archive/run.json').read_text())['run_id']
    metadata_root = archive_run / f'run/data/m2_{run_id}/native_segments'
    with duckdb.connect() as db:
        db.execute("SET memory_limit='256MB'"); db.execute('SET threads=2')
        rows = db.execute('SELECT ordinal,payload FROM read_parquet(?) WHERE attempt=? ORDER BY ordinal',
                          [str(metadata_root / '**/*.parquet'), attempt['attempt']]).fetchall()
    segments = []
    for i, payload in rows:
        value = json.loads(payload)
        segments.append({'ordinal': i, **value})
    return {'schema_version': 'retained-native-rib/v1', 'source': entry,
            'collector_id': manifest['collector'], 'source_run': str(source_run), 'archive_run': str(archive_run),
            'archive_attempt': attempt['attempt'], 'source_manifest_sha256': digest_file(manifest_path),
            'source_receipt_sha256': digest_file(receipt_path), 'source_counts': complete['counts'],
            'archive_audit': audit, 'archive_audit_sha256': digest_file(audit_path), 'segments': segments}


def verify_archive(binding, guard=lambda: None):
    require(binding['schema_version'] == 'retained-native-rib/v1', '归档绑定版本不符')
    require(binding['collector_id'] == 'rrc25', '当前只接受 RRC25')
    source = binding['source']
    guard()
    require(digest_file(source['path']) == source['sha256'], '原件摘要与完成回执不符')
    files = defaultdict(list); signatures = []; next_record = 0
    counts = defaultdict(int); stamps = {}
    require(bool(binding['segments']), '归档段缺失')
    for index, segment in enumerate(binding['segments']):
        guard()
        inp = segment['input']
        require(segment['ordinal'] == inp['segment'] == index and inp['first_record'] == next_record,
                '归档段遗漏、重复或次序不连续')
        next_record = inp['next_record']
        require(next_record > inp['first_record'], '归档消息范围无效')
        segment_rows = defaultdict(int)
        for item in segment['files']:
            path = Path(item['path'])
            require(path.is_file() and not path.is_symlink(), '归档正文缺失或类型改变')
            require(binding['archive_attempt'] in path.parts and path.parent.name == f'segment-{index:06d}', '归档段位置不符')
            require(path not in stamps, '同一正文重复登记')
            stamp = path.stat()
            require(stamp.st_size == item['size'] and digest_file(path) == item['sha256'], '归档正文摘要不符')
            # 早期 paths 登记只含摘要和大小；行数仍受下方原段计数约束。
            row_count = pq.ParquetFile(path).metadata.num_rows
            require('rows' not in item or row_count == item['rows'], '归档正文行数不符')
            stamps[path] = (stamp.st_size, stamp.st_mtime_ns, stamp.st_ctime_ns)
            files[item['table']].append(str(path))
            segment_rows[item['table']] += row_count
            counts[item['table']] += row_count
            signatures.append([index, item['table'], item['size'], item['sha256'], row_count])
        for name in ('messages', 'elements', 'paths', 'peers'):
            require(segment_rows[name] == inp['counts'].get(name, 0), '段计数与正文不符：' + name)
    require(next_record == counts['messages'] == binding['source_counts']['messages']
            and counts['elements'] == binding['source_counts']['elements'], '整源段计数未闭合')
    require(counts['elements'] > 0, '空 RIB 闭集未确认，不发布零规模')
    require(not counts['quality'], '归档含质量记录，不能直接提供主值')
    return dict(files), dict(counts), hashlib.sha256(encoded(signatures)).hexdigest(), stamps


def compute_statistics(binding, output, *, memory_limit='4GB', threads=4, max_rss_bytes=8*1024**3,
                       max_temp_bytes=40*1024**3, min_free_bytes=50*1024**3):
    root = Path(output).resolve()
    require(not root.exists(), '输出目录必须是新的独立目录')
    root.mkdir(parents=True)
    started = time.monotonic()
    (root / 'input-binding.json').write_bytes(encoded(binding))
    def stage(name, **details):
        value = {'stage': name, 'seconds': round(time.monotonic()-started, 3), **details}
        (root/'progress.json').write_bytes(encoded(value))
        print(json.dumps(value, ensure_ascii=False), flush=True)
    def guard():
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
        require(rss <= max_rss_bytes, '统计内存保护触发')
        require(shutil.disk_usage(root).free >= min_free_bytes, '可用磁盘保护触发')
    code_files = [Path(__file__), Path(origin.__file__), Path(resource_compute.__file__), Path(prefix_quantity.__file__)]
    code = {p.name: digest_file(p) for p in code_files}
    try:
        stage('archive_verification')
        files, counts, archive_sha, stamps = verify_archive(binding, guard)
        stage('archive_verified', elements=counts['elements'], bytes=sum(s[0] for s in stamps.values()))
        with duckdb.connect(str(root/'working.duckdb')) as db:
            db.execute('SET memory_limit=?', [memory_limit]); db.execute('SET threads=?', [threads])
            db.execute('SET max_temp_directory_size=?', [f'{max_temp_bytes}B'])
            for name in ('messages', 'elements', 'paths'):
                db.from_parquet(files[name]).create_view(name)
            source = binding['source']
            row = db.execute('SELECT min(epoch),max(epoch),count(*),count(DISTINCT record),min(record),max(record),'
                             'count(*) FILTER(WHERE source_id<>? OR content_sha256<>? OR kind NOT IN (\'peer_index_table\',\'rib\')) FROM messages',
                             [source['source_id'], source['sha256']]).fetchone()
            require(row[0] == row[1] and row[2] == row[3] == counts['messages'] and row[4] == 0
                    and row[5] == row[2]-1 and row[6] == 0, 'RIB 消息范围、时点或来源不一致')
            observed_at = datetime.fromtimestamp(row[0], timezone.utc).isoformat()
            require(db.execute("SELECT count(*) FROM elements WHERE action<>'rib_snapshot' OR afi NOT IN (1,2) OR safi<>1 OR path_key IS NULL OR prefix IS NULL").fetchone()[0] == 0,
                    '归档含非单播 RIB 或缺字段')
            require(db.execute('SELECT count(*)-count(DISTINCT path_key) FROM paths').fetchone()[0] == 0, '路径键不唯一')
            # 同一原始路径只解释一次，属性差异不导致重复起源计算。
            db.execute('CREATE TABLE path_variants AS SELECT DISTINCT as_path_raw,as4_path_raw,asn_width FROM paths')
            require(db.execute('SELECT count(*) FROM path_variants WHERE asn_width<>4 OR asn_width IS NULL').fetchone()[0] == 0,
                    'RIB 路径 ASN 宽度不是四字节')
            variants = db.execute('SELECT as_path_raw,as4_path_raw FROM path_variants').fetchall()
            decoded = []
            for index, (raw, as4) in enumerate(variants):
                if index % 8192 == 0: guard()
                value = origin._interpret(raw, as4)
                decoded.append((raw, as4, value['attributed_origin_asn'], value['reason']))
            schema = pa.schema([('as_path_raw', pa.binary()), ('as4_path_raw', pa.binary()), ('origin', pa.int64()), ('reason', pa.string())])
            table = pa.Table.from_pylist([dict(zip(schema.names, row)) for row in decoded], schema=schema)
            db.register('decoded', table)
            db.execute('CREATE TABLE path_origins AS SELECT p.path_key,d.origin,d.reason FROM paths p JOIN decoded d ON '
                       'p.as_path_raw IS NOT DISTINCT FROM d.as_path_raw AND p.as4_path_raw IS NOT DISTINCT FROM d.as4_path_raw')
            db.unregister('decoded'); del table, decoded, variants
            guard(); stage('path_rules_complete')
            rows = db.execute('SELECT e.afi,count(*),count(DISTINCT e.prefix),'
                              'count(DISTINCT CASE WHEN p.origin IS NOT NULL THEN e.prefix END),'
                              'count(*) FILTER(WHERE p.origin IS NULL), count(*) FILTER(WHERE p.path_key IS NULL) '
                              'FROM elements e LEFT JOIN path_origins p USING(path_key) GROUP BY e.afi').fetchall()
            require(sum(r[1] for r in rows) == counts['elements'] and all(r[5] == 0 for r in rows), '元素缺路径关联')
            origin_rows = db.execute('SELECT DISTINCT e.afi,p.origin FROM elements e JOIN path_origins p USING(path_key) WHERE p.origin IS NOT NULL').fetchall()
            family_origins = {a: {o for family,o in origin_rows if family == a} for a in (1,2)}
            by_family = {}
            for afi, family in ((1,'ipv4'),(2,'ipv6')):
                value = next((r for r in rows if r[0] == afi), (afi,0,0,0,0,0))
                by_family[family] = {'visible_prefixes': value[2], 'visible_origin_ases': len(family_origins[afi]),
                                     'attributed_prefixes': value[3], 'unattributed_prefixes': value[2]-value[3],
                                     'rib_entries': value[1], 'unattributed_entries': value[4]}
            by_family['all'] = {k: sum(by_family[f][k] for f in ('ipv4','ipv6')) for k in by_family['ipv4']}
            by_family['all']['visible_origin_ases'] = len(family_origins[1] | family_origins[2])
            guard(); stage('canonical_statistics_complete', families=by_family)
            prefixes = db.execute('SELECT DISTINCT afi,prefix FROM elements').fetchall()
            for afi,prefix in prefixes:
                network = ipaddress.ip_network(prefix, strict=True)
                require(network.version == (4 if afi == 1 else 6), '前缀与地址族不一致')
            v4 = [p for afi,p in prefixes if afi == 1 and p != '0.0.0.0/0']
            v6 = [p for afi,p in prefixes if afi == 2 and p != '::/0']
            values = {'ipv4_prefix_count': len(v4), 'ipv4_address_count': prefix_quantity.calculate_c_segments_count(v4)*256,
                      'ipv6_prefix_count': prefix_quantity.calculate_v6_48_segments_count(v6)}
            values['ipv6_48_count'] = values['ipv6_prefix_count']
            del prefixes,v4,v6
            db.execute("CREATE TABLE resource_path_keys AS SELECT DISTINCT path_key FROM elements WHERE prefix NOT IN ('0.0.0.0/0','::/0')")
            texts = db.execute('SELECT DISTINCT p.as_path_text FROM paths p JOIN resource_path_keys k USING(path_key)').fetchall()
            firsts, public, private = set(), set(), set(); paths = 0
            for (path,) in texts:
                require(path is not None, 'Resource 路径文本缺失')
                first,tail,is_private,status = resource_compute.resource_path_parts(path)
                firsts.add(first)
                if status == 'accepted':
                    paths += 1
                    (private if is_private else public).add(tail)
            values.update(vp_count=len(firsts), public_as_count=len(public), private_as_count=len(private), path_count=paths)
            guard(); stage('resource_statistics_complete', metrics=values)
        for path, expected in stamps.items():
            stat = path.stat()
            require((stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns) == expected, '计算期间归档正文变化')
        require(all(digest_file(p) == code[p.name] for p in code_files), '计算期间规则代码变化')
        body = {'schema_version': PROFILE, 'observed_at': observed_at, 'collector_id': binding['collector_id'],
                'source_id': source['source_id'], 'source_sha256': source['sha256'],
                'canonical': {'rule': origin.RULE, 'by_family': by_family},
                'resource': {'rule': resource_compute.RULE, 'metrics': {k: {'main': v, 'raw': v, 'qualification': 'qualified',
                     'reason': 'verified_independent_rib', 'unit': resource_compute.METRIC_UNITS[k]} for k,v in values.items()}},
                'evidence': {'archive_sha256': archive_sha, 'segment_count': len(binding['segments']), 'counts': counts,
                             'code': code, 'source_receipt_sha256': binding['source_receipt_sha256']},
                'limitations': ['仅此独立 RIB 时点，时点之间未知；不是连续 RouteState 或全天覆盖。',
                                'Canonical 明确起源与 Resource 旧尾 ASN 规则分别统计，不互相替代。',
                                '未计算历史正常范围、国家拓扑或 Peer 明细，不冒充完整 Resource 资格制品。',
                                'RRC25 控制面观察，不代表实际用户连接、断网或原因。']}
        body['snapshot_id'] = 'rib_statistics_v1_' + hashlib.sha256(encoded(body)).hexdigest()
        (root/'statistics.json').write_bytes(encoded(body))
        stage('complete', snapshot_id=body['snapshot_id'])
        return body
    except BaseException as error:
        stage('failed', error_type=type(error).__name__, error=str(error))
        raise


def deliver_statistics(conn, artifact):
    """交付已完成统计；同一来源只能绑定一个明确版本，不覆盖旧结果。"""
    path = Path(artifact)
    body = json.loads(path.read_bytes())
    identifier = body['snapshot_id']
    require(body['schema_version'] == PROFILE and identifier == 'rib_statistics_v1_' + hashlib.sha256(encoded({k:v for k,v in body.items() if k!='snapshot_id'})).hexdigest(), '统计结果摘要不符')
    with conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('domeye_result_delivery'))")
        cur.execute('SELECT receipt FROM result_delivery.files WHERE source_id=%s', (body['source_id'],))
        row = cur.fetchone()
        require(row and row[0]['source_receipt']['source_sha'] == body['source_sha256'], '统计原件不属于本交付库的完成文件')
        cur.execute('''CREATE TABLE IF NOT EXISTS result_delivery.rib_statistics (
            snapshot_id text PRIMARY KEY, observed_at timestamptz NOT NULL,
            source_id text UNIQUE NOT NULL REFERENCES result_delivery.files(source_id), body jsonb NOT NULL)''')
        cur.execute('SELECT snapshot_id,body FROM result_delivery.rib_statistics WHERE source_id=%s', (body['source_id'],))
        old = cur.fetchone()
        if old:
            require(old[0] == identifier and old[1] == body, '同源统计冲突，拒绝覆盖')
            return {'status': 'already_delivered', 'snapshot_id': identifier}
        cur.execute('INSERT INTO result_delivery.rib_statistics VALUES(%s,%s,%s,%s)',
                    (identifier, body['observed_at'], body['source_id'], Json(body)))
    return {'status': 'delivered', 'snapshot_id': identifier}
