"""首页留存异常的独立只读入口，不改写旧事件 API 语义。"""

from flask import request
from flask_restful import Resource
import psycopg2

from services.core_overview_service import OverviewError, get_core_overview, get_core_overview_record


def _query():
    if any(len(request.args.getlist(key)) != 1 for key in request.args):
        raise OverviewError('查询参数不能重复', 400)
    return request.args.to_dict(flat=True)


class CoreOverviewResource(Resource):
    def get(self):
        try:
            return get_core_overview(_query())
        except OverviewError as error:
            return error.payload or {'state': 'unavailable', 'message': str(error)}, error.status
        except psycopg2.Error:
            return {'state': 'unavailable', 'message': '结果查询暂不可用'}, 503


class CoreOverviewRecordResource(Resource):
    def get(self):
        try:
            return get_core_overview_record(_query())
        except OverviewError as error:
            return {'state': 'unavailable', 'message': str(error)}, error.status
        except psycopg2.Error:
            return {'state': 'unavailable', 'message': '结果查询暂不可用'}, 503
