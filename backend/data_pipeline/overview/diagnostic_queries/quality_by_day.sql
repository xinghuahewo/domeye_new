-- 三类历史异常按业务日做源键与共同字段质量盘点；不导出路径、不修正等级。
-- 只允许调用脚本传入的 202602/202603 显式表名和窗口。
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
SET LOCAL datestyle = 'ISO, YMD';
SET LOCAL work_mem = '64MB';
SELECT jsonb_build_object('kind','context','read_at',clock_timestamp(),
 'database',current_database(),'read_only',current_setting('transaction_read_only'),
 'isolation',current_setting('transaction_isolation'),'source','r',
 'month',:'month','anomaly_kind',:'anomaly_kind','start',:'start','end_exclusive',:'end',
 'business_timezone','Asia/Shanghai','scope','三类总表引用与完整键候选的按日统计，不是消费准入');
WITH raw_details AS (
 SELECT 'prefix_outage' AS kind, source, prefix AS object, outage_id::text AS record_number,
        s_time,e_time,duration,outage_level AS level FROM :"prefix_table" WHERE source='r' AND :'anomaly_kind'='prefix_outage'
 UNION ALL
 SELECT 'as_outage',source,asn,outage_id::text,s_time,e_time,duration,outage_level
 FROM :"as_table" WHERE source='r' AND :'anomaly_kind'='as_outage'
 UNION ALL
 SELECT 'leak',source,prefix,leak_event_id::text,s_time,NULL::timestamp,NULL::interval,leak_level
 FROM :"leak_table" WHERE source='r' AND :'anomaly_kind'='leak'
), details AS (
 SELECT kind,source,object,record_number,count(*) AS candidates,
        min(s_time) AS s_time,min(e_time) AS e_time,min(duration) AS duration,min(level) AS level
 FROM raw_details GROUP BY kind,source,object,record_number
), events AS (
 SELECT source,detail_url,s_time,e_time,duration,level,
   CASE event_type WHEN '前缀中断' THEN 'prefix_outage' WHEN 'AS中断' THEN 'as_outage' ELSE 'leak' END AS kind
 FROM :"event_table" WHERE source='r' AND s_time >= :'start'::timestamp AND s_time < :'end'::timestamp
 AND event_type IN ('前缀中断','AS中断','路由泄漏')
 AND event_type=CASE :'anomaly_kind' WHEN 'prefix_outage' THEN '前缀中断' WHEN 'as_outage' THEN 'AS中断' ELSE '路由泄漏' END
), joined AS (
 SELECT e.*, d.candidates, d.object AS detail_object, d.record_number,
   d.s_time AS detail_s_time,d.e_time AS detail_e_time,d.duration AS detail_duration,d.level AS detail_level
 FROM events e LEFT JOIN details d ON d.kind=e.kind AND d.source=e.source
  AND d.object=replace(split_part(e.detail_url,'/',3),'-','/')
  AND d.record_number=split_part(e.detail_url,'/',4)
), checked AS (
 SELECT *,
  candidates=1 AND s_time IS DISTINCT FROM detail_s_time AS start_conflict,
  candidates=1 AND level IS DISTINCT FROM detail_level AS level_conflict,
  candidates=1 AND kind!='leak' AND e_time IS DISTINCT FROM detail_e_time AS end_conflict,
  candidates=1 AND kind!='leak' AND duration IS DISTINCT FROM detail_duration AS duration_conflict,
  candidates=1 AND detail_url IS DISTINCT FROM
    kind||'/'||to_char(s_time,'YYYY-MM-DD HH24:MI:SS')||'/'||replace(detail_object,'/','-')||'/'||record_number||'/'||source AS reference_conflict
 FROM joined
), daily AS (
 SELECT s_time::date AS day,kind,count(*) AS records,count(DISTINCT detail_url) AS distinct_refs,
  count(*) FILTER (WHERE candidates IS NULL) AS missing_candidates,
  count(*) FILTER (WHERE candidates>1) AS multiple_candidates,
  count(*) FILTER (WHERE start_conflict) AS start_conflicts,
  count(*) FILTER (WHERE level_conflict) AS level_conflicts,
  count(*) FILTER (WHERE end_conflict) AS end_conflicts,
  count(*) FILTER (WHERE duration_conflict) AS duration_conflicts,
  count(*) FILTER (WHERE reference_conflict) AS reference_conflicts,
  count(*) FILTER (WHERE candidates=1 AND kind!='leak' AND detail_e_time IS NULL) AS end_not_recorded,
  count(*) FILTER (WHERE candidates=1 AND (detail_level IS NULL OR detail_level NOT IN ('low','middle','high'))) AS level_not_recognized,
  count(*) FILTER (WHERE candidates=1 AND (detail_e_time<detail_s_time OR detail_duration<interval '0')) AS invalid_time_order,
  min(s_time) AS first_start,max(s_time) AS last_start,
  (array_agg(detail_url ORDER BY detail_url) FILTER (WHERE candidates IS NULL OR candidates>1 OR start_conflict OR level_conflict OR end_conflict OR duration_conflict OR reference_conflict))[1:5] AS conflict_examples
 FROM checked GROUP BY s_time::date,kind
), calendar AS (
 SELECT day::date AS day,kind FROM generate_series(:'start'::timestamp,:'end'::timestamp-interval '1 day',interval '1 day') day
 CROSS JOIN (VALUES ('prefix_outage'),('as_outage'),('leak')) kinds(kind)
 WHERE kind=:'anomaly_kind'
)
SELECT jsonb_build_object('kind','quality_by_day','items',jsonb_agg(to_jsonb(t) ORDER BY day,kind))
FROM (
 SELECT c.day,c.kind,coalesce(d.records,0) AS records,coalesce(d.distinct_refs,0) AS distinct_refs,
  coalesce(d.missing_candidates,0) AS missing_candidates,coalesce(d.multiple_candidates,0) AS multiple_candidates,
  coalesce(d.start_conflicts,0) AS start_conflicts,coalesce(d.level_conflicts,0) AS level_conflicts,
  coalesce(d.end_conflicts,0) AS end_conflicts,coalesce(d.duration_conflicts,0) AS duration_conflicts,
  coalesce(d.reference_conflicts,0) AS reference_conflicts,coalesce(d.end_not_recorded,0) AS end_not_recorded,
  coalesce(d.level_not_recognized,0) AS level_not_recognized,coalesce(d.invalid_time_order,0) AS invalid_time_order,
  d.first_start,d.last_start,coalesce(d.conflict_examples,ARRAY[]::text[]) AS conflict_examples
 FROM calendar c LEFT JOIN daily d ON d.day=c.day AND d.kind=c.kind
) t;
SELECT jsonb_build_object('kind','receipt','read_only',current_setting('transaction_read_only'),'finished_at',clock_timestamp());
ROLLBACK;
