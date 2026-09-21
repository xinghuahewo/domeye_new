"""显式历史源身份；连接凭据仅由调用方在 Git 外提供。"""
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import psycopg2
from psycopg2.extensions import parse_dsn


@dataclass(frozen=True)
class PGIdentity:
    system_identifier: str
    database_oid: int

    def __post_init__(self):
        if not isinstance(self.system_identifier, str) or not self.system_identifier.isdecimal() or type(self.database_oid) is not int or self.database_oid < 1:
            raise ValueError('PG绑定身份无效')

    @classmethod
    def read(cls, connection):
        with connection.cursor() as cursor:
            cursor.execute('SELECT system_identifier::text FROM pg_catalog.pg_control_system()')
            system = cursor.fetchone()[0]
            cursor.execute('SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()')
            return cls(system, cursor.fetchone()[0])


def source_label(uri, version):
    parsed = urlsplit(uri)
    if not parsed.scheme or parsed.username or parsed.password or parsed.query or parsed.fragment or not isinstance(version, str) or not version.strip():
        raise ValueError('来源必须提供不含凭据/查询参数的URI和版本')


@dataclass(frozen=True)
class PGSource:
    identity: PGIdentity
    origin_uri: str
    source_version: str
    dsn: str = field(repr=False, compare=False)

    def __post_init__(self):
        if not isinstance(self.identity, PGIdentity): raise ValueError('缺显式PG源身份')
        source_label(self.origin_uri, self.source_version)
        try:
            parameters = parse_dsn(self.dsn)
        except (psycopg2.Error, TypeError):
            raise ValueError('显式源DSN格式无效') from None
        if not all(parameters.get(key) for key in ('host', 'port', 'dbname', 'user')) or parameters.get('service'):
            raise ValueError('源DSN须显式host/port/dbname/user，不使用环境默认源或service发现')

    def connect(self):
        connection = None
        try:
            connection = psycopg2.connect(self.dsn)
            connection.set_session(readonly=True, isolation_level='REPEATABLE READ')
            if PGIdentity.read(connection) != self.identity: raise ValueError('PG源绑定身份不符')
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_catalog.current_setting('transaction_read_only'),pg_catalog.current_setting('transaction_isolation'),pg_catalog.current_setting('server_encoding')")
                if cursor.fetchone() != ('on', 'repeatable read', 'UTF8'): raise ValueError('PG源只读快照/UTF8能力不符')
            # 不 rollback：身份、后续目录及所有表属于本事务快照。
            return connection
        except BaseException as error:
            if connection is not None: connection.close()
            if isinstance(error, psycopg2.Error):
                raise ValueError('PG源连接/只读身份能力失败（需预先授权pg_control_system及表SELECT）') from None
            raise


@dataclass(frozen=True)
class SQLiteSource:
    origin_uri: str
    source_version: str
    sha256: str
    closed_immutable: bool

    def __post_init__(self):
        source_label(self.origin_uri, self.source_version)
        if self.closed_immutable is not True or len(self.sha256) != 64 or any(c not in '0123456789abcdef' for c in self.sha256):
            raise ValueError('SQLite须显式绑定SHA及关闭写入的独立原件')
