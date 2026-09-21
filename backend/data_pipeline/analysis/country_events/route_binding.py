"""国家 M3 的原全局来源位置；选择子集不重排、不压缩原始 rank。"""
from dataclasses import dataclass, field

from data_pipeline.analysis.country_events.models import Binding


@dataclass(frozen=True)
class M3Binding(Binding):
    # 来自实际 M2 InputBinding 的完整 MRT 来源序，含未选来源，不含参考表。
    global_sources: tuple[str, ...] = field(kw_only=True)
    input_binding_id: str = field(kw_only=True)

    def __post_init__(self):
        super().__post_init__()
        if (type(self.global_sources) is not tuple or not self.global_sources
                or any(type(s) is not str or not s for s in self.global_sources)
                or len(set(self.global_sources)) != len(self.global_sources)
                or not self.input_binding_id):
            raise ValueError('M3 必须保留完整原始来源绑定')
        selected = set(self.ordered_sources)
        if tuple(s for s in self.global_sources if s in selected) != self.ordered_sources:
            raise ValueError('M3 选择必须是原始来源的保序子集')

    def source_at(self, rank):
        if type(rank) is not int or not 0 <= rank < len(self.global_sources):
            return None
        source = self.global_sources[rank]
        return source if source in self.ordered_sources else None

    def rank_of(self, source_id):
        if source_id not in self.ordered_sources:
            raise ValueError('M3 来源不在固定选择中')
        return self.global_sources.index(source_id)
