"""Trend显式离线/消费运行范围；不从制品授予DSN或文件访问权。"""
from dataclasses import dataclass, asdict, fields, is_dataclass
from copy import deepcopy
from pathlib import Path
import hashlib
import tempfile

from psycopg2.extensions import parse_dsn
from data_pipeline.bgp.archive.admission import Runtime as Paths
from data_pipeline.analysis.country_events.snapshot_store import lake_connect, cleanup
from data_pipeline.analysis.country_trends.snapshot_store import S2Limits
from data_pipeline.analysis.country_trends.stream_schema import encode
from data_pipeline.common import process_resources as resources


def _same_typed(left, right):
    """仅固定Runtime私有比较：不序列化大字符串，不采用bool/int宽松相等。"""
    if type(left) is not type(right): return False
    if isinstance(left, dict):
        if len(left) != len(right): return False
        current = {(type(k), k): v for k, v in left.items()}
        return all((type(k), k) in current and _same_typed(current[type(k), k], v)
                   for k, v in right.items())
    if isinstance(left, (tuple, list)):
        return len(left) == len(right) and all(_same_typed(a,b) for a,b in zip(left,right))
    if is_dataclass(left):
        return all(_same_typed(getattr(left,f.name),getattr(right,f.name)) for f in fields(left))
    return left == right


@dataclass
class ProductionRuntime:
    dsn: str
    output_root: Path
    allowed_roots: tuple
    scratch_root: Path
    dependency_admissions: tuple
    dependency_runtimes: dict
    country_window_us: tuple
    fixture_only: bool = False
    execution_profile: str | None = None
    limits: S2Limits | None = None
    memory_bytes: int | None = None
    max_temp_bytes: int | None = None
    min_free_bytes: int | None = None
    lock_timeout_ms: int | None = None
    audit_sink: object = None
    reference_binding: dict | None = None
    feature_selections: tuple = ()

    def __post_init__(self):
        if type(self.fixture_only) is not bool:
            raise ValueError('trend_runtime_explicit_mode')
        if self.fixture_only:
            if self.execution_profile is not None: raise ValueError('trend_runtime_mixed_mode')
            self._profile = 'synthetic-fixture/v1'
        elif self.execution_profile == 'real-candidate/v1': self._profile = self.execution_profile
        else: raise ValueError('trend_runtime_explicit_mode')
        self._mode = self.fixture_only, self.execution_profile
        for name, value in dict(limits=S2Limits(), memory_bytes=256*1024**2,
                max_temp_bytes=512*1024**2, min_free_bytes=128*1024**2, lock_timeout_ms=2000).items():
            if getattr(self, name) is None:
                if not self.fixture_only: raise ValueError('trend_runtime_missing_budget:' + name)
                setattr(self, name, value)
        self.allowed_roots = tuple(Path(p) for p in self.allowed_roots)
        if not self.allowed_roots: raise ValueError('trend_runtime_roots')
        for path in self.allowed_roots: self.path(path)
        self.output_root = self.path(self.output_root); self.scratch_root = self.path(self.scratch_root)
        if (self.output_root == self.scratch_root or self.output_root in self.scratch_root.parents
                or self.scratch_root in self.output_root.parents):
            raise ValueError('trend_runtime_scratch_isolation')
        parsed = parse_dsn(self.dsn)
        if not parsed.get('host') or not parsed.get('dbname') or parsed.get('service'):
            raise ValueError('trend_runtime_explicit_dsn')
        if self.fixture_only:
            if not parsed['host'].startswith('/'): raise ValueError('trend_runtime_fixture_socket')
            self.path(parsed['host'])
        self._entities = tuple((str(p), p.stat().st_dev, p.stat().st_ino)
                               for p in (*self.allowed_roots, self.output_root, self.scratch_root))
        self._fixed = self.scope_digest()
        self._scope_snapshot = deepcopy(self._scope_state())
        self._runtime_map_type = type(self.dependency_runtimes)
        self._runtime_handles = {(type(k),k):v for k,v in self.dependency_runtimes.items()}
        self.resource_guard()

    path = Paths.path
    event = Paths.event

    def scope_digest(self):
        return hashlib.sha256(encode((self.dsn, tuple(str(p) for p in self.allowed_roots),
            str(self.output_root), str(self.scratch_root), self.dependency_admissions,
            tuple(sorted((k, id(v)) for k, v in self.dependency_runtimes.items())),
            self.country_window_us, asdict(self.limits), self.memory_bytes, self.max_temp_bytes,
            self.min_free_bytes, self.lock_timeout_ms, self.reference_binding, self.feature_selections,
            getattr(self, 'expected_trend_binding', None))).encode()).hexdigest()

    def _scope_state(self):
        return (self.dsn, self.allowed_roots, self.output_root, self.scratch_root,
            self.dependency_admissions, self.country_window_us, self.limits, self.memory_bytes,
            self.max_temp_bytes, self.min_free_bytes, self.lock_timeout_ms,
            self.reference_binding, self.feature_selections, self.fixture_only,
            self.execution_profile, self._profile, getattr(self,'expected_trend_binding',None))

    def _same_scope(self):
        if type(self.dependency_runtimes) is not self._runtime_map_type: return False
        handles = {(type(k),k):v for k,v in self.dependency_runtimes.items()}
        if (handles.keys() != self._runtime_handles.keys()
                or any(value is not self._runtime_handles[key] for key,value in handles.items())):
            return False
        return _same_typed(self._scope_state(), self._scope_snapshot)

    def resource_guard(self):
        if (self.fixture_only, self.execution_profile) != self._mode or type(self.fixture_only) is not bool:
            raise ValueError('trend_runtime_mode_drift')
        if type(self.limits) is not S2Limits or any(type(v) is not int or v < 1 for v in asdict(self.limits).values()):
            raise ValueError('trend_runtime_limits')
        if self.limits.max_batch_rows > 256: raise ValueError('trend_runtime_batch_rows')
        for name in ('memory_bytes', 'max_temp_bytes', 'min_free_bytes', 'lock_timeout_ms'):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError('trend_runtime_budget:' + name)
        if (type(self.country_window_us) is not tuple or len(self.country_window_us) != 2
                or any(type(v) is not int for v in self.country_window_us)
                or self.country_window_us[0] >= self.country_window_us[1]):
            raise ValueError('trend_runtime_result_window')
        if not self._same_scope(): raise ValueError('trend_runtime_scope_drift')
        for name, device, inode in self._entities:
            path = self.path(name); stat = path.stat()
            if not path.is_dir() or (stat.st_dev, stat.st_ino) != (device, inode):
                raise ValueError('trend_runtime_directory_drift')
        if not hasattr(self, '_resource_monitor'):
            roots = [self.output_root, self.scratch_root]
            if resources._binding is not None: roots.append(resources.sqlite_temp())
            self._resource_monitor = resources.Monitor(roots, max_rss=self.limits.max_rss_bytes,
                temp_roots=[r for r in roots if r != self.output_root], max_temp=self.max_temp_bytes, min_free=self.min_free_bytes)
        self._resource_monitor.check()

    def guarded(self, guard):
        if getattr(guard, '_s3_runtime', None) is self: return guard
        def check():
            guard()  # 取消/外部guard不降频。
            self.resource_guard()
        check._s3_runtime = self
        return check

    def dependencies(self, guard):
        from data_pipeline.analysis.country_events import result_admission as country
        from data_pipeline.analysis.features import publication as feature
        if (len({a['admission_id'] for a in self.dependency_admissions}) != len(self.dependency_admissions)
                or set(self.dependency_runtimes) != {a['admission_id'] for a in self.dependency_admissions}
                or sum(a['owner'] == 'country' for a in self.dependency_admissions) != 1
                or sum(a['owner'] == 'feature' for a in self.dependency_admissions) > 1):
            raise ValueError('trend_runtime_actual_dependencies')
        for admission in self.dependency_admissions:
            if admission['owner'] not in ('country', 'feature'): raise ValueError('trend_runtime_dependency_owner')
            runtime = self.dependency_runtimes[admission['admission_id']]
            if (runtime.fixture_only, runtime.execution_profile) != self._mode:
                raise ValueError('trend_runtime_dependency_mode')
            for entity in admission['entities']:
                path = Path(entity['path'])
                if (self.scratch_root == path or self.scratch_root in path.parents
                        or path in self.scratch_root.parents):
                    raise ValueError('trend_runtime_upstream_scratch_overlap')
            (country if admission['owner'] == 'country' else feature).verify_current(runtime, admission, guard=self.guarded(guard))
        if self.reference_binding is not None:
            from data_pipeline.analysis.country_trends.reference_reader import verify_reference
            path = self.path(self.reference_binding['path'])
            if path == self.scratch_root or self.scratch_root in path.parents:
                raise ValueError('trend_runtime_reference_scratch_overlap')
            verify_reference(self, guard=self.guarded(guard))

    def lake(self, *, root=None):
        self.resource_guard()
        if not hasattr(self, '_duckdb_temp'):
            self._duckdb_temp=Path(tempfile.mkdtemp(prefix='trend-duckdb-', dir=self.scratch_root))
            resources.bind_duckdb_temp(self._duckdb_temp)
        db = lake_connect(self.dsn, root, memory_bytes=self.memory_bytes,
                          scratch_root=self._duckdb_temp, max_temp_bytes=self.max_temp_bytes)
        try:
            from data_pipeline.bgp.archive.store import literal
            db.execute('SET temp_directory=' + literal(self._duckdb_temp))
            self.resource_guard()
            return db
        except BaseException as error:
            cleanup((db.close,), error)
            raise


@dataclass
class Runtime(ProductionRuntime):
    expected_trend_binding: dict | None = None

    def __post_init__(self):
        if type(self.expected_trend_binding) is not dict:
            raise ValueError('trend_runtime_requires_complete_binding')
        super().__post_init__()
        from data_pipeline.analysis.country_trends.result_admission import _parts
        _parts(self, self.expected_trend_binding)

    def check_binding(self, binding):
        self.resource_guard()
        if encode(binding) != encode(self.expected_trend_binding):
            raise ValueError('trend_runtime_binding_drift')
