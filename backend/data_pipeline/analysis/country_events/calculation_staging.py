"""资格推导原科学历史与新增行复用SQLite，逐行保留完整typed对象。"""
from collections.abc import Sequence
import uuid
from data_pipeline.analysis.country_events import qualified_schema as m3_schema
from data_pipeline.analysis.country_events.snapshot_store import cleanup


class ScientificRows(Sequence):
    def __init__(self,store):
        self.store=store;self.key=('qualification',uuid.uuid4().hex);self.owner=None
        store.add_view(self.key,())
    def __len__(self):return len(self.store[self.key])
    def append(self,sequence,item):
        self.store.add_record(self.key,len(self),m3_schema.row_encode(sequence,item))
    def __getitem__(self,index):
        table,row=self.store[self.key][index]
        return m3_schema.row_decode(table,row)
    def items(self):
        for table,row in self.store[self.key]:yield row['_sequence'],table,m3_schema.row_decode(table,row)
    def __iter__(self):
        for _,_,item in self.items():yield item
    def close(self):
        if self.owner is not None:
            owner=self.owner;self.owner=None;owner.close()
        elif not self.store.closed:
            name=self.store.partitions.pop(self.key,None)
            if name is not None:
                self.store.db.execute('DELETE FROM bodies WHERE partition=?',(name,))
                self.store.counts.pop(name,None)
    def __del__(self):
        if getattr(self,'owner',None) is not None:
            try:self.close()
            except BaseException:pass


class QualificationMap:
    def __init__(self,store):self.data=store.new_map()
    def __contains__(self,key):return key in self.data
    def __setitem__(self,key,value):
        from dataclasses import asdict
        self.data[key]=asdict(value)
    def __getitem__(self,key):
        from data_pipeline.analysis.country_events.route_contract import CountryQualification
        return CountryQualification(**self.data[key])
    def values(self):
        from data_pipeline.analysis.country_events.route_contract import CountryQualification
        for value in self.data.values():yield CountryQualification(**value)
    def close(self):self.data.close()
