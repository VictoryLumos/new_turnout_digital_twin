-- ---------------------------------------------------------------------
-- 08. 告警事件段视图 v2（2026-10-04，组长第五轮反馈第1/2条）
--
-- 第1条：恢复时刻不能"最后超限+0.1s"硬编码 → 改为**真实数据帧**：
--        同测点、同来源，最后超限帧之后第一条 value≤阈值 的记录的 ts；
--        其后没有回到阈值以内的数据 → end_ts 为 NULL（未恢复/数据流结束）。
-- 第2条：恢复后再次告警必须分成独立事件 → 分段不再按 30 秒间隔聚合，
--        而是按"非超限帧→超限帧"的**状态跃迁**切段：只要中间出现过一帧
--        不超限（即已恢复），下一次超限就是新事件段。
-- 实现：直接基于 timeseries_data × measurement_point(阈值/单位) 计算，
--        不再依赖 alarm_event 表（该表仍保留作入库自动留痕审计）。
-- 附带：新增 unit 列（第五轮第4条：力/电流等告警单位不能统一标 mm）。
-- 幂等：可重复执行。
-- ---------------------------------------------------------------------

DROP VIEW IF EXISTS v_fault_episodes;

CREATE VIEW v_fault_episodes AS
WITH th AS (          -- 配置了告警阈值的测点（含单位）
    SELECT point_code, alarm_threshold AS threshold, unit
    FROM measurement_point
    WHERE alarm_threshold IS NOT NULL),
s AS (                -- 这些测点的全部时序帧 + 超限判定（严格大于）
    SELECT t.point_code, t.ts, t.value, t.source,
           th.threshold, th.unit,
           (t.value > th.threshold) AS over
    FROM timeseries_data t
    JOIN th ON th.point_code = t.point_code),
k AS (                -- 每帧是否"新进入超限"：前一帧（同测点同来源）不超限
    SELECT *,
           (over AND NOT COALESCE(lag(over) OVER (
                PARTITION BY point_code, source ORDER BY ts), false)) AS is_start
    FROM s),
a AS (                -- 只保留超限帧；累计"进入超限"次数 = 事件段号
    SELECT *,
           sum(CASE WHEN is_start THEN 1 ELSE 0 END) OVER (
                PARTITION BY point_code, source ORDER BY ts) AS seg_id
    FROM k
    WHERE over),
seg AS (
    SELECT point_code, source, seg_id,
           min(ts)  AS start_ts,
           max(ts)  AS last_alarm_ts,
           count(*) AS alarm_points,
           round(max(value)::numeric, 1) AS peak_value,
           threshold, unit
    FROM a
    GROUP BY point_code, source, seg_id, threshold, unit)
SELECT seg.point_code, seg.start_ts, seg.last_alarm_ts, seg.alarm_points,
       seg.peak_value, seg.threshold, seg.unit, seg.source,
       -- 恢复时刻：同测点同来源，最后超限后第一条回到阈值以内的真实数据帧
       rec.ts AS end_ts,
       CASE WHEN rec.ts IS NULL THEN NULL
            ELSE round(extract(epoch FROM (rec.ts - seg.start_ts))::numeric, 1)
       END AS duration_s
FROM seg
LEFT JOIN LATERAL (
    SELECT t.ts
    FROM timeseries_data t
    WHERE t.point_code = seg.point_code
      AND t.source     = seg.source
      AND t.ts > seg.last_alarm_ts
      AND t.value <= seg.threshold
    ORDER BY t.ts
    LIMIT 1) rec ON TRUE
ORDER BY seg.start_ts;

COMMENT ON VIEW v_fault_episodes IS '告警事件段视图v2：按"非超限→超限"状态跃迁分段（恢复后再告警=独立事件）；end_ts=真实恢复帧(同源最后超限后第一条≤阈值记录，无则NULL未恢复)；unit=测点单位；duration_s=超限→恢复全程';
