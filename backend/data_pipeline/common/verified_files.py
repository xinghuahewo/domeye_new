"""两个显式离线绑定入口共用的有界清单读取与校验复制；Web不调用复制。"""
import hashlib


def read_manifest(path):
    with path.open('rb') as stream:
        payload = stream.read(65537)
    if len(payload) > 65536:
        raise ValueError('清单超限')
    return payload


def copy_bound(source, destination, expected):
    if source.is_symlink() or not source.is_file():
        raise ValueError('输入必须为普通文件')
    before = source.stat()
    digest = hashlib.sha256()
    with source.open('rb') as reader, destination.open('xb') as writer:
        for block in iter(lambda: reader.read(1024 * 1024), b''):
            digest.update(block)
            writer.write(block)
    after = source.stat()
    if digest.hexdigest() != expected or any(getattr(before, key) != getattr(after, key) for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')):
        raise ValueError('复制摘要不符或输入在复制期间变化')
    destination.chmod(0o400)
