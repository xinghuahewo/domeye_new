"""业务兼容视图的共享路径文本引用；完整属性仍由 Observation 路径键定位。"""
from collections import OrderedDict
from functools import lru_cache
from collections.abc import MutableMapping, ValuesView, ItemsView
import hashlib
import sys


@lru_cache(maxsize=4096, typed=True)
def vp_text(asn):
    """已核验 ASN 标量的旧 VP 文本；跨前缀共享，不改变 str(asn) 或端点身份。"""
    return str(asn)


class PathText:
    __slots__=('ident','pool')
    def __init__(self,ident,pool):self.ident,self.pool=ident,pool
    @property
    def text(self):return self.pool.value(self.ident)
    def __str__(self):return self.text
    def __repr__(self):return repr(self.text)
    def __contains__(self,value):return value in self.text
    def __len__(self):return len(self.text)
    def __hash__(self):return hash(self.text)
    def __eq__(self,other):return self.ident==other.ident if isinstance(other,PathText) else self.text==other
    def __getattr__(self,name):return getattr(self.text,name)
    def __copy__(self):return self
    def __deepcopy__(self,memo):return self


class TextPool:
    """每种路径文本只存一份；持久证据属于 Observation，本模块不访问数据库。"""
    def __init__(self):
        self.texts={};self.ids={};self.tokens=OrderedDict()
        self.bytes=0;self.max_entries=8192;self.queries=0;self.single_reads=0

    def ident(self,text):
        if isinstance(text,PathText):return text.ident
        known=self.ids.get(text)
        if known is not None:return known
        return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8],'big',signed=True)

    def register(self,texts):
        for text in texts:
            if text in self.ids:continue
            ident=self.ident(text)
            if ident in self.texts and self.texts[ident]!=text:raise ValueError('路径文本编号碰撞')
            self.texts[ident]=text;self.ids[text]=ident
            self.bytes+=sys.getsizeof(text)+sys.getsizeof(ident)

    @property
    def memory_bytes(self):
        return self.bytes+sys.getsizeof(self.texts)+sys.getsizeof(self.ids)

    def token(self,text):return self.by_id(self.ident(text))
    def by_id(self,ident):
        if ident not in self.tokens:
            self.tokens[ident]=PathText(ident,self)
            if len(self.tokens)>self.max_entries:self.tokens.popitem(last=False)
        else:self.tokens.move_to_end(ident)
        return self.tokens[ident]

    def value(self,ident):
        try:return self.texts[ident]
        except KeyError as exc:raise ValueError('共享历史路径引用缺失') from exc


class _SlotValues(ValuesView):
    def __iter__(self):
        paths=self._mapping
        return (paths.pool.by_id(ident) for ident in paths.ids.values())


class _SlotItems(ItemsView):
    def __iter__(self):
        paths=self._mapping
        return ((vp,paths.pool.by_id(ident)) for vp,ident in paths.ids.items())


class SlotPaths(MutableMapping):
    def __init__(self,pool,ids=None):self.pool,self.ids=pool,{} if ids is None else ids
    def __getitem__(self,vp):return self.pool.by_id(self.ids[vp])
    def __setitem__(self,vp,path):self.ids[vp]=self.pool.ident(path)
    def __delitem__(self,vp):del self.ids[vp]
    def __iter__(self):return iter(self.ids)
    def __len__(self):return len(self.ids)
    def __deepcopy__(self,memo):return dict(self.items())
    def snapshot(self):
        """只冻结当前前缀的槽编号；后续写入不改变这条观察的前后证据。"""
        return SlotPaths(self.pool,self.ids.copy())
    def values(self):
        return _SlotValues(self)
    def items(self):
        return _SlotItems(self)


class PrefixSlots(dict):
    def __init__(self,pool):super().__init__();self.pool=pool
    def __setitem__(self,key,value):
        if not isinstance(value,SlotPaths):value=SlotPaths(self.pool,{vp:self.pool.ident(path) for vp,path in value.items()})
        super().__setitem__(key,value)
    def setdefault(self,key,default=None):
        if key not in self:self[key]=default or {}
        return self[key]

