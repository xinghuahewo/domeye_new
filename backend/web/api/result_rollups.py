"""已完成结果汇总的 GET 消费入口。"""
from flask import request
from flask_restful import Resource

from services.resource_service import ResourceError
from services.result_rollup_service import query_rollups


class ResultRollupsResource(Resource):
    def get(self):
        try:
            if any(len(request.args.getlist(key)) != 1 for key in request.args):
                raise ResourceError('查询参数不能重复',400)
            return query_rollups(request.args.to_dict())
        except ResourceError as error:
            return {'state':'unavailable','message':str(error)},error.status
