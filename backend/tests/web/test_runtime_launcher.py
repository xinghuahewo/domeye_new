"""受控启动器的公开配置入口；只用临时配置，不访问真实数据库。"""
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('runtime_launcher', ROOT / 'scripts/run_backend.py')
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


def test_explicit_overview_input_coexists_with_read_only_database(tmp_path):
    runtime = tmp_path / 'backend.env'
    manifest = tmp_path / 'retained' / 'manifest.json'
    runtime.write_text('\n'.join([
        'DB_HOST=127.0.0.1', 'DB_PORT=31627', 'DB_NAME=fixture',
        'DB_USER=fixture', 'DB_PASSWORD=fixture-only', 'SECRET_KEY=fixture-only',
        'PGOPTIONS="-c default_transaction_read_only=off"',
        'AUTO_INIT_DB=true', 'LOAD_CORE_DATA_ON_STARTUP=true',
        f'DOMEYE_CORE_OVERVIEW_MANIFEST={manifest}',
        f'DOMEYE_RIB_SNAPSHOT_REGISTRY={tmp_path / "snapshots"}',
    ]))
    runtime.chmod(0o600)

    environment = launcher.build_environment(runtime)

    assert environment['DOMEYE_CORE_OVERVIEW_MANIFEST'] == str(manifest)
    assert environment['DOMEYE_RIB_SNAPSHOT_REGISTRY'] == str(tmp_path / 'snapshots')
    assert environment['DB_NAME'] == 'fixture'
    assert environment['PGOPTIONS'] == '-c default_transaction_read_only=on'
    assert environment['AUTO_INIT_DB'] == 'false'
    assert environment['LOAD_CORE_DATA_ON_STARTUP'] == 'false'
    assert environment['DOMEYE_CORE_SKIP_LOCAL_ENV'] == 'true'
    assert environment['DOMEYE_ENFORCE_DATA_WINDOW'] == 'true'


def test_legacy_config_does_not_inherit_an_ambient_overview_input(tmp_path, monkeypatch):
    runtime = tmp_path / 'backend.env'
    runtime.write_text('\n'.join([
        'DB_HOST=127.0.0.1', 'DB_PORT=31627', 'DB_NAME=fixture',
        'DB_USER=fixture', 'DB_PASSWORD=fixture-only', 'SECRET_KEY=fixture-only',
    ]))
    runtime.chmod(0o600)
    monkeypatch.setenv('DOMEYE_CORE_OVERVIEW_MANIFEST', '/unselected/manifest.json')
    monkeypatch.setenv('DOMEYE_RIB_SNAPSHOT_REGISTRY', '/unselected/snapshots')

    environment = launcher.build_environment(runtime)

    assert environment['DB_NAME'] == 'fixture'
    assert 'DOMEYE_CORE_OVERVIEW_MANIFEST' not in environment
    assert 'DOMEYE_RIB_SNAPSHOT_REGISTRY' not in environment
    assert environment['PGOPTIONS'] == '-c default_transaction_read_only=on'


def test_retired_p0_configuration_does_not_block_start_or_reactivate_reader(tmp_path, monkeypatch):
    runtime = tmp_path / 'backend.env'
    runtime.write_text('\n'.join([
        'DB_HOST=127.0.0.1', 'DB_PORT=31627', 'DB_NAME=fixture',
        'DB_USER=fixture', 'DB_PASSWORD=fixture-only', 'SECRET_KEY=fixture-only',
        'P0_DATA_RELEASE_DIR=/unused/retired', 'P0_DATA_PRODUCTION_ACTIVE=true',
    ]))
    runtime.chmod(0o600)
    monkeypatch.setenv('P0_DATA_RELEASE_DIR', '/ambient/retired')
    monkeypatch.setenv('P0_DATA_PRODUCTION_ACTIVE', 'true')

    environment = launcher.build_environment(runtime)

    assert environment['DB_NAME'] == 'fixture'
    assert 'P0_DATA_RELEASE_DIR' not in environment
    assert 'P0_DATA_PRODUCTION_ACTIVE' not in environment
