"""H1人工领域片：History.project_core(CollectionToken, root_id=...) -> CoreToken；
with History.core(CoreToken)提供有界query/references/detail、typed bulk与离线索引重建。
不连接真实源、不接HTTP、不授予H1/H3全族迁完资格。
"""
from data_pipeline.history.event_index.model import CoreToken
from data_pipeline.history.event_index.store import History

__all__ = ['History', 'CoreToken']
