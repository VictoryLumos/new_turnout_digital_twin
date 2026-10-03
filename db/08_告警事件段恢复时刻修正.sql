-- ---------------------------------------------------------------------
-- 08. 告警事件段"恢复时刻"修正（2026-10-03，组长第四轮反馈第1条）
--
-- 问题：v_fault_episodes 原 end_ts = max(ts) =【最后一次超限帧】的时刻，
--       被页面当"恢复时间"展示——不准确（那一刻设备仍在超限）。
-- 修正：恢复时刻 = 最后超限帧 + 一个数据帧步长（本管道 0.1 秒），
--       即下一帧回到阈值以内的时刻；原值保留在新列 last_alarm_ts。
-- 影响接口：/api/alarm-events、首页"告警变化记录"（自动随视图生效）。
-- 幂等：视图可重复执行（CREATE OR REPLACE）。
-- ---------------------------------------------------------------------

DROP VIEW IF EXISTS v_fault_episodes;

CREATE VIEW v_fault_episodes AS
WITH g AS (
    SELECT point_code, ts, value, threshold, source,
           ts - lag(ts) OVER (PARTITION BY point_code ORDER BY ts) AS gap
    FROM alarm_event),
ep AS (
    SELECT *, sum(CASE WHEN gap > INTERVAL '30 seconds' OR gap IS NULL
                       THEN 1 ELSE 0 END)
           OVER (PARTITION BY point_code ORDER BY ts) AS ep_id
    FROM g)
SELECT point_code,
       min(ts)  AS start_ts,
       -- 恢复时刻：最后一次超限帧 + 一个步长(0.1s) = 下一帧回到阈值以内的时刻
       max(ts) + INTERVAL '0.1 second' AS end_ts,
       -- 保留原语义：最后一次超限时刻（不再冒充恢复时间）
       max(ts)  AS last_alarm_ts,
       count(*) AS alarm_points,
       round(max(value)::numeric, 1) AS peak_value,
       threshold,
       max(source) AS source,
       -- 持续时长改为"超限→恢复"全程（含最后一帧长度）
       round(extract(epoch FROM (max(ts) + INTERVAL '0.1 second' - min(ts)))::numeric,
             1) AS duration_s
FROM ep
GROUP BY point_code, ep_id, threshold
ORDER BY min(ts);

COMMENT ON VIEW v_fault_episodes IS '故障事件段视图：超阈记录按30秒聚合成事件段；end_ts=恢复时刻(最后超限帧+0.1s步长)，last_alarm_ts=最后超限时刻，duration_s=超限→恢复全程时长';
