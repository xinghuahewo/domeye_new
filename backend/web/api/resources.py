"""固定 global Resource 指标的只读 HTTP 入口。"""
from flask import request
from flask_restful import Resource

from services.resource_service import ResourceError, query_resources


class ResourceStatisticsResource(Resource):
    def get(self):
        try:
            if any(len(request.args.getlist(key)) != 1 for key in request.args):
                raise ResourceError('查询参数不能重复', 400)
            return query_resources(request.args.to_dict())
        except ResourceError as error:
            return {'state': 'unavailable', 'message': str(error)}, error.status
