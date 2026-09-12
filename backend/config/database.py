import os
import threading

import psycopg2
from flask import has_request_context, request


def _get_int(name, default):
    value = os.environ.get(name)
    if value is None or value == '':
        return default
    return int(value)

# 数据库配置
USER = os.environ.get('DB_USER', 'postgres')
PASSWORD = os.environ.get('DB_PASSWORD', '')
HOST = os.environ.get('DB_HOST', '127.0.0.1')
PORT = _get_int('DB_PORT', 5432)
DATABASE = os.environ.get('DB_NAME', 'bgp_project')


_CONNECTION_LOCK = threading.Lock()
_CONNECTIONS = {}


def _connect():
    return psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
    )


def _get_connection(name):
    if has_request_context():
        connections = request.environ.setdefault('domeye.database_connections', {})
        connection = connections.get(name)
        if connection is None or getattr(connection, 'closed', 1) != 0:
            connection = _connect()
            connections[name] = connection
        return connection

    connection = _CONNECTIONS.get(name)
    if connection is not None and getattr(connection, 'closed', 1) == 0:
        return connection

    with _CONNECTION_LOCK:
        connection = _CONNECTIONS.get(name)
        if connection is None or getattr(connection, 'closed', 1) != 0:
            connection = _connect()
            _CONNECTIONS[name] = connection
        return connection


class LazyConnection:
    def __init__(self, name):
        self._name = name

    def get_connection(self):
        return _get_connection(self._name)

    def __getattr__(self, item):
        return getattr(self.get_connection(), item)

    def __repr__(self):
        return f"<LazyConnection {self._name}>"


def get_conn_11():
    return _get_connection('conn_11')


def get_conn_13():
    return _get_connection('conn_13')


def get_conn_15():
    return _get_connection('conn_15')


def get_conn_226():
    return _get_connection('conn_226')


def close_all_connections():
    with _CONNECTION_LOCK:
        for connection in _CONNECTIONS.values():
            if getattr(connection, 'closed', 1) == 0:
                connection.close()
        _CONNECTIONS.clear()


def close_request_connections(error=None):
    """请求之间不共享事务；成功或异常结束都回收本请求的连接。"""
    if not has_request_context():
        return
    connections = request.environ.pop('domeye.database_connections', {})
    for connection in connections.values():
        if getattr(connection, 'closed', 1) == 0:
            try:
                connection.rollback()
            except psycopg2.Error:
                # 连接可能已被服务端关闭，仍须释放客户端资源。
                pass
            finally:
                connection.close()


# 数据库连接兼容层
# 旧代码仍然通过 conn_* 使用连接；真正的连接创建推迟到首次使用时发生。
conn_11 = LazyConnection('conn_11')
conn_13 = LazyConnection('conn_13')
conn_15 = LazyConnection('conn_15')
conn_226 = LazyConnection('conn_226')


# ssh数据通道配置 (数据字典)   # 没什么用
SSH_HOST = os.environ.get('SSH_HOST', '')
SSH_USER = os.environ.get('SSH_USER', '')
SSH_PWD = os.environ.get('SSH_PWD', '')
REMOTE_PATH = os.environ.get('REMOTE_PATH', '/home/lgh/fusionprocessing/data/format')



# ssh数据通道配置 (flask后台)
SSH_HOST2 = os.environ.get('SSH_HOST2', '')
SSH_USER2 = os.environ.get('SSH_USER2', '')
SSH_PWD2 = os.environ.get('SSH_PWD2', '')
REMOTE_PATH2 = os.environ.get('REMOTE_PATH2', '/home/bgpdata/bgpc/screen_data/output_data')
