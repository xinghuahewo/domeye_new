#!/usr/bin/env python3
"""加载项目外的受控配置，按唯一数据档启动前台只读 API。"""

from __future__ import annotations

import os
from pathlib import Path
import shlex
import stat
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dev.data_profile import load_data_profile  # noqa: E402


ALLOWED_KEYS = {
    "HOST", "PORT", "DEBUG", "SECRET_KEY", "FLASK_CONFIG",
    "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "INFO_DIR",
    "DOMEYE_LOG_DIR", "DOMEYE_CORE_SKIP_LOCAL_ENV", "DOMEYE_ENFORCE_DATA_WINDOW",
    "DOMEYE_COUNTRY_OUTAGE_GENERAL_READ_MODEL", "DOMEYE_DATA_LAYER_224_310_SELECTION",
    "DOMEYE_COUNTRY_OUTAGE_REGISTRY", "DOMEYE_LEGACY_STORY_REPLAY_DIRECTORY",
    "DOMEYE_RRC25_CONTEMPORANEOUS_REFERENCE", "FEATURE_COUNTRY_TABLE", "FEATURE_OTHER_TABLE",
    "FEATURE_ASN_MONTHLY_ENABLED", "SOURCE", "AUTO_INIT_DB",
    "LOAD_CORE_DATA_ON_STARTUP", "PGOPTIONS", "MAIL_ENABLED",
    "DOMEYE_CORE_OVERVIEW_MANIFEST", "DOMEYE_RESULT_DELIVERY",
    "DOMEYE_RIB_SNAPSHOT_REGISTRY",
}
# 旧配置可以随源码升级，但不再向进程传入已退役能力的开关。
RETIRED_KEYS = {"P0_DATA_RELEASE_DIR", "P0_DATA_PRODUCTION_ACTIVE"}


def load_runtime_env(path: Path) -> dict[str, str]:
    if path.is_symlink() or path.resolve().is_relative_to(ROOT):
        raise RuntimeError("运行配置必须是项目外的独立普通文件")
    with path.open("r", encoding="utf-8") as source:
        metadata = os.fstat(source.fileno())
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise RuntimeError("运行配置必须是权限 0600 的普通文件")
        values = {}
        for line_number, line in enumerate(source, 1):
            fields = shlex.split(line, comments=True, posix=True)
            if not fields:
                continue
            if len(fields) != 1 or "=" not in fields[0]:
                raise RuntimeError(f"运行配置第 {line_number} 行格式无效")
            name, value = fields[0].split("=", 1)
            if name not in ALLOWED_KEYS | RETIRED_KEYS or name in values or "\0" in value:
                raise RuntimeError(f"运行配置第 {line_number} 行包含重复或不允许的键")
            values[name] = value
    if not {"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "SECRET_KEY"}.issubset(values):
        raise RuntimeError("运行配置缺少必要的数据库或应用配置")
    return {name: value for name, value in values.items() if name not in RETIRED_KEYS}


def build_environment(path: Path) -> dict[str, str]:
    environment = {name: os.environ[name] for name in ("PATH", "HOME", "LANG", "LC_ALL")
                   if name in os.environ}
    environment.update(load_runtime_env(path))
    profile = load_data_profile(ROOT / "config/data-profile.json")
    environment.update({
        "DOMEYE_CORE_SKIP_LOCAL_ENV": "true",
        "DOMEYE_ENFORCE_DATA_WINDOW": "true",
        "DOMEYE_DATA_WINDOW_START": profile["local"]["start"],
        "DOMEYE_DATA_WINDOW_END_EXCLUSIVE": profile["local"]["end_exclusive"],
        "DOMEYE_DATA_SNAPSHOT_TIME": profile["local"]["snapshot"],
        "TZ": profile["timezone"],
        "FLASK_CONFIG": "production",
        "DEBUG": "false",
        "AUTO_INIT_DB": "false",
        "LOAD_CORE_DATA_ON_STARTUP": "false",
        "MAIL_ENABLED": "false",
        "PGOPTIONS": "-c default_transaction_read_only=on",
        "PYTHONDONTWRITEBYTECODE": "1",
    })
    environment.setdefault("DOMEYE_LOG_DIR", str(path.parent / "logs"))
    return environment


def configure_environment(path: Path | None = None) -> dict[str, str]:
    """为只读集成验证复用同一个配置入口，不启动服务或连接数据库。"""
    default_path = ROOT.parent / f"{ROOT.name}-runtime" / "backend.env"
    runtime_path = path or Path(os.environ.get("DOMEYE_RUNTIME_ENV", str(default_path)))
    environment = build_environment(runtime_path)
    os.environ.clear()
    os.environ.update(environment)
    return environment


def main() -> None:
    environment = configure_environment()
    python = ROOT / "backend/.venv/bin/python"
    os.chdir(ROOT / "backend")
    os.execve(str(python), [str(python), str(ROOT / "backend/run.py")], environment)


if __name__ == "__main__":
    main()
