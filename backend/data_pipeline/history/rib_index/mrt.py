"""完整物理MRT定位适配：保留原属性；旧端点与起源规则分别解释。"""
import ipaddress
import struct
import json
from data_pipeline.bgp.snapshots import origin as origin, path_comparison as comparison


def require(condition, message):
    if not condition: raise ValueError('H2 ' + message)


def frames(path, budget):
    epoch=None; record=offset=0; peers=None
    with path.open('rb') as stream:
        while True:
            budget.check(); header=stream.read(12)
            if not header: break
            require(len(header)==12,'MRT头截断')
            stamp,kind,subtype,size=origin.HEADER.unpack(header)
            require(kind==13 and subtype in (1,2,4,8,10),'MRT未支持的原范围')
            require(size <= min(16*1024**2,budget.collection_limits.document_bytes),'MRT帧预算')
            budget.add('mrt_bytes',12+size,budget.collection_limits.decoded_bytes)
            body=stream.read(size); require(len(body)==size,'MRT正文截断')
            require(epoch is None or epoch==stamp,'单MRT非固定时点'); epoch=stamp
            budget.add('mrt_frames',1,budget.limits.max_total_rows)
            if subtype==1:
                require(record==0 and peers is None,'MRT首Peer表不唯一')
                peers=origin._peers(body)
                yield {'record':record,'decoded_offset':offset,'body_bytes':size,'epoch':stamp,'subtype':subtype,
                       'afi':None,'safi':None,'prefix':None}, peers, []
            else:
                require(peers is not None,'MRT缺首Peer表')
                afi, packed, cursor=comparison._prefix(body,subtype)
                prefix=str(ipaddress.ip_address(packed[:-1]))+'/'+str(packed[-1])
                count=origin.U16(body,cursor)[0];cursor+=2
                require(count<=budget.limits.batch_rows*1024,'MRT单帧条目预算')
                entries=[]
                old=comparison._entries(body,subtype,peers,{})
                for i in range(count):
                    budget.check(); width=12 if subtype in (8,10) else 8
                    require(cursor+width<=size,'MRT条目头截断')
                    peer,started=struct.unpack_from('!HI',body,cursor)
                    path_id=struct.unpack_from('!I',body,cursor+6)[0] if width==12 else None
                    length=origin.U16(body,cursor+width-2)[0];cursor+=width;end=cursor+length
                    require(peer<len(peers) and end<=size,'MRT条目/Peer越界')
                    path=as4=None
                    while cursor<end:
                        require(cursor+3<=end,'属性头截断')
                        flags,code=body[cursor:cursor+2];cursor+=2
                        n=2 if flags&16 else 1
                        require(cursor+n<=end,'属性长度截断')
                        length=int.from_bytes(body[cursor:cursor+n],'big');cursor+=n
                        require(cursor+length<=end,'属性正文截断')
                        value=body[cursor:cursor+length];cursor+=length
                        if code==2:path=value
                        if code==17:as4=value
                    old_peer,ordinal,(canonical,reasons)=next(old)
                    require((old_peer,ordinal)==(peer,i),'旧端点解码定位不一致')
                    interpreted=origin._interpret(path,as4)
                    budget.add('mrt_observations',1,budget.limits.max_total_rows)
                    entries.append(dict(entry_index=i,peer_index=peer,**{k:peers[peer][k] for k in ('bgp_id','ip','asn')},
                        originated_time_epoch=started,path_id=path_id,as_path=path,as4_path=as4,
                        canonical_path=json.dumps(canonical,separators=(',',':')),comparison_reasons=json.dumps(reasons,separators=(',',':')),
                        raw_origin_asn=interpreted['raw_origin_asn'],attributed_origin_asn=interpreted['attributed_origin_asn'],origin_reason=interpreted['reason']))
                require(next(old,None) is None and cursor==size,'MRT尾部或条目数冲突')
                yield {'record':record,'decoded_offset':offset,'body_bytes':size,'epoch':stamp,'subtype':subtype,
                       'afi':afi,'safi':1,'prefix':prefix}, None, entries
            offset+=12+size;record+=1
        require(peers is not None and record>1,'MRT无完整前缀')
