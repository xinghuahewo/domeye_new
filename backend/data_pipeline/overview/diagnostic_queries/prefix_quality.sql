-- 以事件开始时间圈定单日总表引用，候选仅按 source/prefix/整数 id 查全；不按候选时间或 ASN 截断。
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='2s';
SET LOCAL work_mem='32MB';
SET LOCAL datestyle='ISO, YMD';
SELECT jsonb_build_object('kind','context','read_at',clock_timestamp(),'database',current_database(),
 'read_only',current_setting('transaction_read_only'),'isolation',current_setting('transaction_isolation'),
 'start',:'start','end_exclusive',:'end','source','r','business_timezone','Asia/Shanghai',
 'scope','prefix_outage 总表引用对应的完整源键候选；源字段预检');
\if :explain
EXPLAIN (COSTS TRUE)
\endif
WITH events AS MATERIALIZED (
 SELECT source,detail_url,s_time,e_time,duration,level,
        string_to_array(detail_url,'/') AS parts,
        replace(split_part(detail_url,'/',3),'-','/') AS ref_prefix,
        split_part(detail_url,'/',4) AS raw_id
 FROM event_table_202603
 WHERE source='r' AND event_type='前缀中断'
   AND s_time >= :'start'::timestamp AND s_time < :'end'::timestamp
), parsed AS (
 SELECT *, CASE WHEN raw_id ~ '^-?[0-9]{1,10}$' THEN
    CASE WHEN raw_id::numeric BETWEEN -2147483648 AND 2147483647 THEN raw_id::integer END
    END AS int_id
 FROM events
), joined AS MATERIALIZED (
 SELECT e.*, d.*
 FROM parsed e
 CROSS JOIN LATERAL (
   SELECT count(*) AS candidates, min(p.s_time) AS detail_s_time,min(p.e_time) AS detail_e_time,
          min(p.duration) AS detail_duration,min(p.outage_level) AS detail_level,
          count(*) FILTER (WHERE p.s_time IS NULL) AS candidate_missing_start,
          count(*) FILTER (WHERE p.outage_level IS NULL OR p.outage_level NOT IN ('low','middle','high')) AS candidate_bad_level,
          count(*) FILTER (WHERE p.e_time<p.s_time OR p.duration<interval '0') AS candidate_bad_order
   FROM prefix_outage_202603 p
   WHERE p.source=e.source AND p.prefix=e.ref_prefix AND p.outage_id=e.int_id
 ) d
), checked AS MATERIALIZED (
 SELECT *,
   (int_id IS NULL OR raw_id IS DISTINCT FROM int_id::text) AS invalid_reference_id,
   (detail_url IS NULL OR cardinality(parts)!=5 OR
    detail_url IS DISTINCT FROM 'prefix_outage/'||to_char(s_time,'YYYY-MM-DD HH24:MI:SS')||'/'||
       replace(ref_prefix,'/','-')||'/'||int_id::text||'/'||source) AS reference_conflict,
   candidates=1 AND s_time IS DISTINCT FROM detail_s_time AS start_conflict,
   candidates=1 AND e_time IS DISTINCT FROM detail_e_time AS end_conflict,
   candidates=1 AND duration IS DISTINCT FROM detail_duration AS duration_conflict,
   candidates=1 AND level IS DISTINCT FROM detail_level AS level_conflict
 FROM joined
)
SELECT jsonb_build_object('kind','prefix_quality','data',to_jsonb(t)) FROM (
 SELECT :'start' AS window_start,:'end' AS window_end_exclusive,count(*) AS records,
   count(DISTINCT detail_url) AS distinct_refs,
   count(*) FILTER (WHERE detail_url IS NULL) AS null_refs,
   count(*) FILTER (WHERE candidates=0) AS missing_candidates,
   count(*) FILTER (WHERE candidates=1) AS unique_candidates,
   count(*) FILTER (WHERE candidates>1) AS multiple_candidates,
   coalesce(sum(candidates),0) AS matched_candidate_rows,
   coalesce(max(candidates),0) AS max_candidates,
   count(*) FILTER (WHERE invalid_reference_id) AS invalid_reference_ids,
   count(*) FILTER (WHERE reference_conflict) AS reference_conflicts,
   count(*) FILTER (WHERE start_conflict) AS start_conflicts,
   count(*) FILTER (WHERE end_conflict) AS end_conflicts,
   count(*) FILTER (WHERE duration_conflict) AS duration_conflicts,
   count(*) FILTER (WHERE level_conflict) AS level_conflicts,
   count(*) FILTER (WHERE candidates=1 AND detail_e_time IS NULL) AS end_not_recorded,
   count(*) FILTER (WHERE candidates=1 AND (detail_level IS NULL OR detail_level NOT IN ('low','middle','high'))) AS level_not_recognized,
   count(*) FILTER (WHERE level IS NULL OR level NOT IN ('low','middle','high')) AS event_level_not_recognized,
   count(*) FILTER (WHERE candidates=1 AND (detail_e_time<detail_s_time OR detail_duration<interval '0')) AS invalid_time_order,
   count(*) FILTER (WHERE e_time<s_time OR duration<interval '0') AS event_invalid_time_order,
   coalesce(sum(candidate_bad_order),0) AS candidate_invalid_time_rows,
   coalesce(sum(candidate_bad_level),0) AS candidate_invalid_level_rows,
   coalesce(sum(candidate_missing_start),0) AS candidate_missing_start_rows,
   count(*) FILTER (WHERE candidates=1 AND detail_e_time IS NOT NULL AND detail_duration IS NOT NULL
       AND detail_duration IS DISTINCT FROM detail_e_time-detail_s_time) AS detail_duration_arithmetic_conflicts,
   min(s_time) AS first_start,max(s_time) AS last_start,
   coalesce((array_agg(detail_url ORDER BY detail_url) FILTER (WHERE candidates!=1 OR invalid_reference_id
       OR reference_conflict OR start_conflict OR end_conflict OR duration_conflict OR level_conflict
       OR candidate_bad_order>0 OR candidate_bad_level>0 OR candidate_missing_start>0
       OR level IS NULL OR level NOT IN ('low','middle','high') OR e_time<s_time OR duration<interval '0'))[1:5],ARRAY[]::text[]) AS conflict_examples
 FROM checked
) t;
SELECT jsonb_build_object('kind','receipt','read_only',current_setting('transaction_read_only'),'finished_at',clock_timestamp());
ROLLBACK;
\echo {"kind":"rollback_ack","status":"ROLLBACK 后客户端到达确认行"}
