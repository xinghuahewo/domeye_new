"""S3单物理片元数据门禁；逻辑表由独立清单持续跨片。"""
from pathlib import Path
import pyarrow.parquet as pq

# 单文件保护不限制逻辑表的分片总数；达到组数或写出字节目标后滚动。
MAX_ROW_GROUPS = 256
TARGET_FILE_BYTES = 128 * 1024**2  # 软目标；最后一组和footer可使实际文件稍大。
MAX_FOOTER_BYTES = 8 * 1024**2


def open_parquet(path, limits, guard):
    guard()
    with Path(path).open('rb') as source:
        source.seek(0, 2)
        size = source.tell()
        if size < 12:
            raise ValueError('trend_s3_parquet_footer')
        source.seek(-8, 2)
        tail = source.read(8)
        footer_size = int.from_bytes(tail[:4], 'little')
        if tail[4:] != b'PAR1' or footer_size > size - 12:
            raise ValueError('trend_s3_parquet_footer')
        if footer_size > min(MAX_FOOTER_BYTES, limits.max_context_bytes):
            raise ValueError('trend_s3_parquet_footer_representation')
    # Thrift长度声明本身也必须有限，不能仅限制压缩文件/批字节。
    file = pq.ParquetFile(path, thrift_string_size_limit=MAX_FOOTER_BYTES,
                          thrift_container_size_limit=65536)
    try:
        if file.metadata.num_row_groups > MAX_ROW_GROUPS:
            raise ValueError('trend_s3_parquet_groups_representation')
        guard()
        return file
    except BaseException:
        file.close()
        raise
