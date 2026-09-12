#!/usr/bin/env python3
"""将一个已验证单RIB包绑定到新的异常消费副本；不覆盖、无源库或MRT读取。"""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from data_pipeline.core_overview_index import DailyIndex
from data_pipeline.core_overview_scale import EVIDENCE_NAMES, read_scale
from data_pipeline.consumption_files import copy_bound, read_manifest


def bind(args):
    code_paths = [Path(__file__), ROOT / 'backend/data_pipeline/consumption_files.py',
                  ROOT / 'backend/data_pipeline/core_overview_scale.py', ROOT / 'backend/data_pipeline/core_overview_index.py']
    code_hashes = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    original = read_manifest(args.index)
    index = DailyIndex(args.index, original)
    manifest = json.loads(original)
    if 'scale' in manifest:
        raise ValueError('已有规模绑定；本命令仅给无规模版本添加单次绑定')
    scale_payload = read_manifest(args.scale)
    package = json.loads(scale_payload)
    manifest['scale'] = {'file': 'scale/manifest.json', 'sha256': hashlib.sha256(scale_payload).hexdigest()}
    output = args.output
    if output.exists() or output.is_symlink() or output.resolve().is_relative_to(args.index.parent.resolve()) or output.resolve().is_relative_to(args.scale.parent.resolve()):
        raise ValueError('输出必须为独立的新目录')
    # 不回滚删除失败副本；未写最终manifest的目录不能成为消费版本。
    output.mkdir(mode=0o700)
    (output / 'scale').mkdir(mode=0o700)
    (output / 'scale/evidence').mkdir(mode=0o700)
    if package['summary']['file'] != 'summary.json' or set(package['evidence']) != EVIDENCE_NAMES:
        raise ValueError('规模包结构不符')
    files = {'summary.json': package['summary']['sha256']}
    for name, entry in package['evidence'].items():
        if entry['file'] != 'evidence/' + name:
            raise ValueError('规模证据路径不符')
        files[entry['file']] = entry['sha256']
    files['manifest.json'] = manifest['scale']['sha256']
    for filename, expected in files.items():
        source = args.scale.parent / filename
        if source.resolve() != args.scale.parent.resolve() / filename or source.stat().st_size > 8 * 1024 * 1024:
            raise ValueError('规模文件越界或超限')
        copy_bound(source, output / 'scale' / filename, expected)
    summary, scale_version = read_scale(output / 'manifest.json', manifest)
    for entry in [*index.days.values(), *index.diagnostics.values()]:
        source = args.index.parent / entry['file']
        if source.resolve() != args.index.parent.resolve() / entry['file']:
            raise ValueError('异常文件越界')
        copy_bound(source, output / entry['file'], entry['sha256'])
    if read_manifest(args.index) != original or read_manifest(args.scale) != scale_payload:
        raise ValueError('复制期间源清单变化')
    if any(hashlib.sha256(path.read_bytes()).hexdigest() != code_hashes[str(path.relative_to(ROOT))] for path in code_paths):
        raise ValueError('绑定期间执行代码变化')
    manifest['evidence'] = {**manifest.get('evidence', {}), 'scale_binding': {
        'parent_version': index.version, 'binder_sha256': code_hashes[str(Path(__file__).relative_to(ROOT))], 'code_sha256': code_hashes}}
    payload = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    if len(payload) > 65536:
        raise ValueError('新清单超限')
    result = DailyIndex(output / 'manifest.json', payload)
    with (output / 'manifest.json').open('xb') as stream:
        stream.write(payload)
    (output / 'manifest.json').chmod(0o400)
    return {'manifest': str((output / 'manifest.json').resolve()), 'version': result.version,
            'parent_version': index.version, 'scale_version': scale_version, 'observed_at': summary['observed_at'],
            'days': len(index.days), 'diagnostic_days': len(index.diagnostics)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--index', type=Path, required=True)
    parser.add_argument('--scale', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    def timeout(*_):
        raise TimeoutError('绑定复制超过120秒上限')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(120)
    try:
        print(json.dumps(bind(args), ensure_ascii=False))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        parser.exit(1, '规模绑定失败：' + str(error) + '\n')
    finally:
        signal.alarm(0)


if __name__ == '__main__':
    main()
