"""Domeye Core API 路由白名单。"""

from flask import Blueprint
from flask_restful import Api

from .core_overview import CoreOverviewResource, CoreOverviewRecordResource
from .rib_snapshots import RibSnapshotsResource, RibSnapshotObservationsResource, RibSnapshotAsnResource
from .resources import ResourceStatisticsResource
from .events.api import (
    EventDetailResource,
    EventEvidenceBundleResource,
    EventListResource,
    EventObservationResource,
    EventStoryResource,
    TopEventResource,
)
from .features.api import (
    ASFeatureListResource,
    ASRecentEventsResource,
    ASWorkbenchResource,
    ASPrefixOutageFeatureResource,
    CountryASOutageFeatureResource,
    CountryFeatureListResource,
    CountryPrefixOutageFeatureResource,
    CountryWorkbenchResource,
    GlobalASOutageFeatureResource,
    GlobalPrefixOutageFeatureResource,
    TopFeatureResource,
)
from .health.api import HealthzResource


api_v1_bp = Blueprint('api_v1', __name__)
api = Api(api_v1_bp)

api.add_resource(HealthzResource, '/healthz')
api.add_resource(CoreOverviewResource, '/core-overview')
api.add_resource(CoreOverviewRecordResource, '/core-overview/record')
api.add_resource(RibSnapshotsResource, '/rib-snapshots', '/rib-snapshots/<version>')
api.add_resource(RibSnapshotObservationsResource, '/rib-snapshots/<version>/observations')
api.add_resource(RibSnapshotAsnResource, '/rib-snapshots/<version>/asns/<asn>')
api.add_resource(ResourceStatisticsResource, '/resources')

api.add_resource(EventListResource, '/events')
api.add_resource(TopEventResource, '/events/top')
api.add_resource(
    EventEvidenceBundleResource,
    '/events/evidence-bundle/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
)
api.add_resource(
    EventStoryResource,
    '/events/story/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
)
api.add_resource(
    EventObservationResource,
    '/events/observations/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
)
api.add_resource(
    EventDetailResource,
    '/<event_type>/<start_time>/<problem>/<int:event_id>/<source>',
)

api.add_resource(TopFeatureResource, '/features/top')
api.add_resource(CountryFeatureListResource, '/features/countries')
api.add_resource(CountryWorkbenchResource, '/features/countries/overview')
api.add_resource(ASFeatureListResource, '/features/ases')
api.add_resource(ASWorkbenchResource, '/features/ases/overview')
api.add_resource(ASRecentEventsResource, '/features/ases/events')
api.add_resource(CountryASOutageFeatureResource, '/features/outages/country-as')
api.add_resource(CountryPrefixOutageFeatureResource, '/features/outages/country-prefix')
api.add_resource(ASPrefixOutageFeatureResource, '/features/outages/as-prefix')
api.add_resource(GlobalASOutageFeatureResource, '/features/outages/global-as')
api.add_resource(GlobalPrefixOutageFeatureResource, '/features/outages/global-prefix')
