"""已保存真实字段的版本化解码差异；不是历史原始 stdout。"""
DECODER_VERSION = 'mrt-fields-peer-path-et/v1'
REPRODUCER = 'RIPE-NCC/bgpdump:v1.6.2@63fe1c50c7d07bb4c57d4fcc690696adc9b3c306'
DIRECTION_RULE = 'legacy-feature-bidirectional-original-peer/v1'


def decode_element(row):
    """字段保持不变；输入已经由观察生产者解码。"""
    return {**row, 'decoder_version': DECODER_VERSION, 'direction_rule': DIRECTION_RULE}


def decoding_difference(row):
    """仅生成已通过真实工具证据验证的固定下标推导差异。"""
    reasons = []
    old_path = row['as_path_text']
    old_vp = str(row['peer_asn'])
    old_time_error = None
    subtype, kind = row['mrt_subtype'], row['mrt_type']
    if kind in (16,17) and subtype in (8,9,10,11):
        old_path = str(row['path_id']) if row['action'] != 'withdraw' else ''
        reasons.append('addpath_fixed_path_column')
        if subtype in (10,11):
            old_vp = str(row['local_asn'])
            reasons.append('local_addpath_text_endpoint_swap')
    if kind == 13 and subtype in (8,10):
        old_path = str(row['path_id'])
        reasons.append('rib_addpath_fixed_path_column')
    if kind == 17:
        old_time_error = 'ValueError:int(decimal_timestamp)'
        reasons.append('et_fixed_integer_timestamp')
    if not reasons: return None
    return {'source_id':row['source_id'],'event_id':row['event_id'],'record':row['record'],'ordinal':row['ordinal'],
            'mrt_type':kind,'mrt_subtype':subtype,'peer_asn':row['peer_asn'],'local_asn':row['local_asn'],
            'epoch':row['epoch'],'microsecond':row['microsecond'],'path_id':row['path_id'],
            'path_key':row['path_key'],'as_path_text':row['as_path_text'],
            'old_fixed_path':old_path,'old_fixed_vp':old_vp,'old_time_error':old_time_error,
            'new_vp':str(row['peer_asn']),'reason':reasons,'decoder_version':DECODER_VERSION,
            'reproducer':REPRODUCER,'historical_tool_version':'Unknown',
            'evidence_kind':'fixed_reproducer_field_derivation_not_historical_stdout'}
