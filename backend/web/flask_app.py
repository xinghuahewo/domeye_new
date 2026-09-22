"""Flask 应用构造器。"""

import os

from flask import Flask, g
from flask_cors import CORS

from config.data_window import validate_data_window_config
from config.database import close_request_connections
from .data_window_guard import enforce_request_data_window


def _cors_origins():
    raw_value = os.environ.get('CORS_ORIGINS', '')
    if not raw_value.strip():
        return []
    return [item.strip() for item in raw_value.split(',') if item.strip()]


def create_flask_app():
    validate_data_window_config()
    app = Flask(__name__)
    origins = _cors_origins()
    if origins:
        CORS(
            app,
            origins=origins,
            supports_credentials=False,
            methods=['GET', 'OPTIONS'],
            allow_headers=[
                'Content-Type',
                'If-None-Match',
            ],
            expose_headers=[
                'ETag',
                'Content-Disposition',
            ],
        )

    from .api.route import api_v1_bp
    from .api.v2.route import api_v2_bp

    app.before_request(enforce_request_data_window)
    if os.environ.get('DOMEYE_RESULT_DELIVERY') == 'true':
        @app.before_request
        def bind_result_delivery():
            import psycopg2
            from data_pipeline.overview.input import InputError
            from data_pipeline.results.delivery_read import status
            try:
                g.result_delivery = status()
            except (psycopg2.Error, InputError):
                g.result_delivery = {'state':'unavailable'}

        @app.after_request
        def result_delivery_headers(response):
            bound = getattr(g, 'result_delivery', {})
            response.headers['X-Domeye-Result-State'] = bound.get('state', 'unavailable')
            for field, header in [('version','Version'),('start','Start'),('end_exclusive','End-Exclusive'),('coverage','Coverage')]:
                if field in bound:
                    response.headers['X-Domeye-Result-'+header] = str(bound[field])
            return response
    app.teardown_request(close_request_connections)
    app.register_blueprint(api_v1_bp, url_prefix='/api/v1')
    app.register_blueprint(api_v2_bp, url_prefix='/api/v2')
    return app
