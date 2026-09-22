from datetime import datetime, timezone

import os
from flask import g
import psycopg2
from flask_restful import Resource
from data_pipeline.overview.input import InputError


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class HealthzResource(Resource):
    """
    轻量存活探针
    Endpoint: /api/v1/healthz
    """

    def get(self):
        payload = {
            "status": "ok",
            "service": "domeye-core",
            "time": _utc_now_iso(),
        }
        if os.environ.get("DOMEYE_RESULT_DELIVERY") == "true":
            from data_pipeline.results.delivery_read import status
            try:
                payload["result_delivery"] = getattr(g, "result_delivery", None) or status()
            except (psycopg2.Error, InputError):
                payload["result_delivery"] = {"state":"unavailable", "message":"结果交付库暂不可读"}
        return payload, 200
