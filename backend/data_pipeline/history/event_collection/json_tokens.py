"""严格UTF8 JSON词法器：有界块/单token，原顺序和十进制词法不经过dict/float。"""
import json
import re

NUMBER = re.compile(rb'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')


def number_parts(raw, limits):
    if not NUMBER.fullmatch(raw): raise ValueError('非法JSON数字')
    text=raw.decode('ascii'); negative=text.startswith('-')
    mantissa, *exponents=re.split('[eE]',text.lstrip('-'))
    digits=mantissa.replace('.','')
    if len(digits)>limits.number_digits: raise ValueError('数字位数超限')
    exp_text=exponents[0] if exponents else '0'
    if len(exp_text.lstrip('+-0'))>len(str(limits.exponent)): raise ValueError('数字指数超限')
    exp=int(exp_text)
    if abs(exp)>limits.exponent: raise ValueError('数字指数超限')
    exp-=len(mantissa.split('.')[1]) if '.' in mantissa else 0
    coefficient=digits.lstrip('0') or '0'
    native=None
    trimmed=coefficient.rstrip('0') if coefficient!='0' else '0'
    normalized_exp=exp+len(coefficient)-len(trimmed)
    decimal_value=None
    if coefficient=='0': decimal_value='0.000000000000000000'
    elif normalized_exp>=-18 and len(trimmed)+normalized_exp<=20:
        from decimal import Decimal, localcontext
        with localcontext() as ctx:
            ctx.prec=38
            value=Decimal((int(negative),tuple(int(c) for c in trimmed),normalized_exp))
            decimal_value=format(value.quantize(Decimal('1e-18')),'f')
    if exp>=0 and len(coefficient)+exp<=19:
        value=int(coefficient)*10**exp*(-1 if negative else 1)
        if -2**63<=value<2**63: native=value
    return dict(number_lexeme=text,sign=-1 if negative else 1,coefficient_digits=coefficient,
                exponent10=exp,negative_zero=negative and coefficient=='0',int64_value=native,decimal128_value=decimal_value)


def compare_numbers(a,b):
    """精确十进制比较，不展开巨大指数、不依赖Decimal上下文。"""
    def norm(n):
        digits=n['coefficient_digits'].lstrip('0') or '0'
        return (0 if digits=='0' else n['sign'],digits,len(digits)+n['exponent10'])
    sa,da,ea=norm(a); sb,db,eb=norm(b)
    if sa!=sb: return (sa>sb)-(sa<sb)
    if sa==0: return 0
    if ea!=eb: return sa*((ea>eb)-(ea<eb))
    length=max(len(da),len(db)); da=da.ljust(length,'0'); db=db.ljust(length,'0')
    return sa*((da>db)-(da<db))


class Parser:
    def __init__(self, stream, budget, emit, start=0):
        self.stream,self.budget,self.emit=stream,budget,emit
        self.buf=b''; self.offset=start; self.position=0; self.count=0; self.document_start=start

    def peek(self):
        if self.position==len(self.buf):
            self.budget.check(); self.offset+=self.position; self.position=0
            self.buf=self.stream.read(self.budget.collection_limits.chunk_bytes)
            self.budget.add('read_blocks',1)
        return self.buf[self.position:self.position+1]

    def tell(self): return self.offset+self.position

    def take(self):
        c=self.peek()
        if not c: raise ValueError('JSON截断')
        if self.tell()-self.document_start>=self.budget.collection_limits.document_bytes:
            raise ValueError('JSON文档字节超限')
        self.position+=1
        return c

    def space(self):
        while self.peek() and self.peek() in b' \t\r\n': self.take()

    def string(self):
        start=self.tell(); raw=bytearray(self.take())
        if raw!=b'"': raise ValueError('JSON成员名必须为字符串')
        escaped=False
        while True:
            if len(raw)>=self.budget.collection_limits.token_bytes: raise ValueError('JSON token字节超限')
            c=self.take(); raw.extend(c)
            if c==b'"' and not escaped: break
            if c==b'\\' and not escaped: escaped=True
            else: escaped=False
        try:
            value=json.loads(raw.decode('utf-8'))
            value.encode('utf-8',errors='strict')
        except (ValueError,UnicodeError): raise ValueError('非法JSON字符串/孤立代理项') from None
        return value,start,self.tell()

    def value(self,parent=None,member=0,key=None,key_start=None,key_end=None,depth=0):
        if depth>self.budget.collection_limits.depth: raise ValueError('JSON深度超限')
        self.space(); ordinal=self.count; self.count+=1
        if self.count>self.budget.collection_limits.document_nodes: raise ValueError('文档节点超限')
        self.budget.add('nodes',1,self.budget.collection_limits.nodes)
        row=dict(node_ordinal=ordinal,parent_ordinal=parent,member_ordinal=member,key=key,key_start=key_start,key_end=key_end,
                 byte_start=self.tell(),child_count=0)
        char=self.peek()
        if char in (b'{',b'['):
            object_=char==b'{'; self.take(); row['kind']='object' if object_ else 'array'; end=b'}' if object_ else b']'
            self.space(); count=0
            if self.peek()!=end:
                while True:
                    self.space(); k=ks=ke=None
                    if object_:
                        k,ks,ke=self.string(); self.space()
                        if self.take()!=b':': raise ValueError('JSON缺冒号')
                    self.value(ordinal,count,k,ks,ke,depth+1); count+=1; self.space()
                    if self.peek()==end: break
                    if self.take()!=b',': raise ValueError('JSON缺逗号')
            self.take(); row['child_count']=count
        elif char==b'"':
            row['kind']='string'; row['text_value']=self.string()[0]
        else:
            raw=bytearray()
            while self.peek() and self.peek() not in b' \t\r\n,]}':
                if len(raw)>=self.budget.collection_limits.token_bytes: raise ValueError('JSON token字节超限')
                raw.extend(self.take())
            raw=bytes(raw)
            if raw==b'null': row['kind']='null'
            elif raw in (b'true',b'false'): row.update(kind='bool',bool_value=raw==b'true')
            else: row.update(kind='number',**number_parts(raw,self.budget.collection_limits))
        row['byte_end']=self.tell(); self.emit(row)
        return row

    def parse(self):
        self.space(); root=self.value(); self.space()
        if self.peek(): raise ValueError('JSON尾随值/垃圾')
        return root,self.count,self.tell()
