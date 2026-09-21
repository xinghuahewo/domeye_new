"""共享RIB快照只读HTTP入口。"""
from flask import request
from flask_restful import Resource
from services.rib_snapshot_service import SnapshotError, query_snapshots


class RibSnapshotsResource(Resource):
    def get(self, version=None, observations=False, asn=None):
        try:
            if any(len(request.args.getlist(key)) != 1 for key in request.args):
                raise SnapshotError('查询参数不能重复', 400)
            return query_snapshots(request.args.to_dict(), version, observations, asn)
        except SnapshotError as error:
            return {'state': error.state, 'message': str(error)}, error.status


class RibSnapshotObservationsResource(RibSnapshotsResource):
    def get(self, version):
        return super().get(version, observations=True)


class RibSnapshotAsnResource(RibSnapshotsResource):
    def get(self, version, asn):
        return super().get(version, asn=asn)
