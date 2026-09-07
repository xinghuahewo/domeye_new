"""传统数据 API 的只读 Web 入口，不启动数据库初始化或离线检测。"""

import os


def load_local_env():
    if os.getenv('DOMEYE_CORE_SKIP_LOCAL_ENV', '').strip().lower() == 'true':
        return

    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if not os.path.exists(env_path):
        return

    with open(env_path, 'r', encoding='utf-8') as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip()


load_local_env()

from config.config import (  # noqa: E402
    DEBUG,
    HOST,
    PORT,
)
from web.flask_app import create_flask_app  # noqa: E402


def create_app(config_name=None):
    # 保留现有应用工厂签名；所有运行模式都只注册只读数据接口。
    return create_flask_app()


if __name__ == '__main__':
    create_app().run(
        host=HOST,
        port=PORT,
        debug=DEBUG,
        use_reloader=DEBUG,
    )
