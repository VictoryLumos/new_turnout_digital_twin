-- =====================================================================
-- 道岔数字孪生 数据库升级脚本 v2（在 01_建表与种子数据.sql 之后执行）
-- 作者：B（数据管道线）  版本：v2.0  日期：2026-09-27
--
-- 内容（四项，全部幂等，可重复执行）：
--   1. 表/列注释（COMMENT ON）——pgAdmin 里直接能看到中文文档
--   2. 查询索引：timeseries_data(source) 与 (ts)——支撑历史查询与来源过滤
--   3. 告警事件审计：alarm_event 表 + 入库触发器——数据一进库就自动留超阈记录，
--      应用层丢了也不怕，数据库层兜底（答辩点：触发器/事务/审计）
--   4. 分析视图：v_point_latest 实时值联表 / v_alarm_records 超阈明细 /
--      v_source_stats 各数据源统计 / v_data_quality 每测点时序连续性
--   附：TimescaleDB 检测块（与 01 相同，装了自动转 hypertable，没装跳过）
--
-- 用法（两种等价）：
--   A. psql -U postgres -d turnout_twin -f 05_数据库升级v2.sql
--      （Windows 中文乱码先 set PGCLIENTENCODING=UTF8）
--   B. python -c "import psycopg2; c=psycopg2.connect(host='localhost',port=5432,
--      user='postgres',password='postgres',dbname='turnout_twin');
--      sql=open('05_数据库升级v2.sql',encoding='utf-8').read();
--      cur=c.cursor(); cur.execute(sql); c.commit(); print('升级完成')"
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. 表与关键列注释（数据库自带文档，pgAdmin/psql \d+ 可见）
-- ---------------------------------------------------------------------
COMMENT ON TABLE  component        IS '构件表：18号道岔 9 个构件，含 glTF 模型节点名（A 导出后确认）与资产ID';
COMMENT ON TABLE  measurement_point IS '测点表：20 个测点（编码映射表），field_name 与数据契约字段一一对应，alarm_threshold 存告警阈值（空=待定）';
COMMENT ON TABLE  timeseries_data   IS '时序数据表：data.json/UM/传感器数据统一入口，主键(测点,时间戳)保证幂等入库，source 区分来源';
COMMENT ON TABLE  work_order        IS '工单表：20 条预留 + 告警自动生成（自动生成：开头），维修闭环后状态转 resolved';
COMMENT ON TABLE  realtime_value    IS '实时值表：每测点恒一行，回放/实时入库脚本按 0.1s 刷新，模拟"实时接入→库"链路';

COMMENT ON COLUMN timeseries_data.source      IS '数据来源：fake=基础版假数据 / 仿真-工况名=四工况数据集 / um=A的UM仿真 / sensor=真实传感器（预留）';
COMMENT ON COLUMN measurement_point.alarm_threshold IS '告警阈值：value 大于该值触发告警；目前仅尖轨/心轨位移两点定阈（150/100mm，《统一标准》第5节），其余待 A 确认编码映射后冻结';

-- ---------------------------------------------------------------------
-- 2. 查询索引（主键已覆盖"按测点+时间"查询；这两个支撑来源过滤与全库时间扫描）
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_timeseries_source ON timeseries_data (source);
CREATE INDEX IF NOT EXISTS idx_timeseries_ts     ON timeseries_data (ts);

-- ---------------------------------------------------------------------
-- 3. 告警事件审计：入库即判阈，超阈值自动落 alarm_event（数据库层兜底）
--    同一(测点,时间戳)唯一约束：重复导入不产生重复审计记录
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alarm_event (
    event_id    SERIAL PRIMARY KEY,
    point_code  TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    value       REAL NOT NULL,
    threshold   REAL,
    source      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (point_code, ts)
);
COMMENT ON TABLE alarm_event IS '告警事件审计：timeseries_data 入库时超阈值自动记录（触发器 trg_timeseries_alarm），应用层告警之外数据库层再兜一层';

CREATE OR REPLACE FUNCTION fn_log_alarm() RETURNS trigger AS $fn$
BEGIN
    INSERT INTO alarm_event (point_code, ts, value, threshold, source)
    SELECT NEW.point_code, NEW.ts, NEW.value, p.alarm_threshold, NEW.source
    FROM measurement_point p
    WHERE p.point_code = NEW.point_code
      AND p.alarm_threshold IS NOT NULL
      AND NEW.value > p.alarm_threshold
    ON CONFLICT (point_code, ts) DO NOTHING;
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;
COMMENT ON FUNCTION fn_log_alarm() IS '入库触发器函数：INSERT timeseries_data 时比对测点阈值，超阈写 alarm_event';

DROP TRIGGER IF EXISTS trg_timeseries_alarm ON timeseries_data;
CREATE TRIGGER trg_timeseries_alarm
    AFTER INSERT ON timeseries_data
    FOR EACH ROW EXECUTE FUNCTION fn_log_alarm();

-- ---------------------------------------------------------------------
-- 4. 分析视图（pgAdmin 直接可查，报告/答辩截图素材）
-- ---------------------------------------------------------------------

-- 4.1 实时值联表（测点信息 + 当前值 + 阈值，一次查询全拿到）
CREATE OR REPLACE VIEW v_point_latest AS
SELECT r.point_code, p.field_name, p.unit, r.value,
       to_char(r.ts, 'HH24:MI:SS') AS ts, r.alarm, p.alarm_threshold,
       to_char(r.updated_at, 'HH24:MI:SS') AS updated_at
FROM realtime_value r JOIN measurement_point p USING (point_code)
ORDER BY r.point_code;
COMMENT ON VIEW v_point_latest IS '实时值联表视图：测点信息+当前值+阈值（/api/realtime 的数据口径）';

-- 4.2 历史超阈明细（阈值判据与前端"超限变红"、API 告警三方同源）
CREATE OR REPLACE VIEW v_alarm_records AS
SELECT d.point_code, p.field_name, to_char(d.ts, 'MM-DD HH24:MI:SS') AS ts,
       round(d.value::numeric, 2) AS value, p.alarm_threshold, p.unit, d.source
FROM timeseries_data d JOIN measurement_point p USING (point_code)
WHERE p.alarm_threshold IS NOT NULL AND d.value > p.alarm_threshold;
COMMENT ON VIEW v_alarm_records IS '历史超阈明细视图：value > alarm_threshold 的全部记录';

-- 4.3 各数据源统计（四工况一屏对比）
CREATE OR REPLACE VIEW v_source_stats AS
SELECT d.source,
       count(*)                    AS rows_total,
       count(DISTINCT d.point_code) AS points,
       to_char(min(d.ts), 'MM-DD HH24:MI') AS ts_min,
       to_char(max(d.ts), 'MM-DD HH24:MI') AS ts_max,
       round(avg(d.value)::numeric, 3)     AS avg_value,
       sum(CASE WHEN d.value > p.alarm_threshold THEN 1 ELSE 0 END) AS alarm_rows
FROM timeseries_data d JOIN measurement_point p USING (point_code)
GROUP BY d.source
ORDER BY min(d.ts);
COMMENT ON VIEW v_source_stats IS '数据源统计视图：每来源行数/测点数/时间范围/均值/超阈行数';

-- 4.4 每测点时序连续性（窗口函数 lag 找时间断点，数据质量检查）
CREATE OR REPLACE VIEW v_data_quality AS
SELECT point_code,
       count(*) AS rows_total,
       to_char(min(ts), 'HH24:MI:SS') AS ts_first,
       to_char(max(ts), 'HH24:MI:SS') AS ts_last,
       sum(CASE WHEN ts - prev_ts > INTERVAL '5 seconds' THEN 1 ELSE 0 END) AS gaps
FROM (
    SELECT point_code, ts,
           lag(ts) OVER (PARTITION BY point_code ORDER BY ts) AS prev_ts
    FROM timeseries_data
) t
GROUP BY point_code
ORDER BY point_code;
COMMENT ON VIEW v_data_quality IS '数据质量视图：每测点行数/首末时间/时间断点数（四工况每小时一段，断点=工况间隔，属预期设计）';

-- 4.5 故障事件段视图：把 alarm_event 的逐条记录聚合成"一段段故障事件"
--     （开始/恢复/持续秒数/测点/峰值/阈值/来源）
--     2026-10-03 第四轮修正：end_ts=恢复时刻(最后超限帧+0.1s步长)，
--     last_alarm_ts=最后超限时刻；已有库请执行 08 迁移脚本同步
CREATE OR REPLACE VIEW v_fault_episodes AS
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
       max(ts) + INTERVAL '0.1 second' AS end_ts,
       max(ts)  AS last_alarm_ts,
       count(*) AS alarm_points,
       round(max(value)::numeric, 1) AS peak_value,
       threshold,
       max(source) AS source,
       round(extract(epoch FROM (max(ts) + INTERVAL '0.1 second' - min(ts)))::numeric,
             1) AS duration_s
FROM ep
GROUP BY point_code, ep_id, threshold
ORDER BY min(ts);
COMMENT ON VIEW v_fault_episodes IS '故障事件段视图：超阈记录按30秒聚合成事件段；end_ts=恢复时刻(最后超限+0.1s步长)，last_alarm_ts=最后超限时刻';

-- ---------------------------------------------------------------------
-- 5. 运行存档表（2026-09-27 追加）：健康度历史 + 维修记录（幂等）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS health_score (
    id             SERIAL PRIMARY KEY,
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 落库时间
    sim_t          REAL,                                -- 回放内仿真时刻（秒）
    score          REAL NOT NULL,                       -- 健康度评分 0~100
    grade          TEXT,                                -- 优/良/预警/故障
    condition_name TEXT                                -- 回放工况
);
CREATE INDEX IF NOT EXISTS idx_health_ts ON health_score (ts);
COMMENT ON TABLE health_score IS '健康度历史存档：服务API回放期间每0.5s自动落一条，支撑健康度曲线的历史回看';

CREATE TABLE IF NOT EXISTS maintenance_log (
    id            SERIAL PRIMARY KEY,
    work_order_id TEXT NOT NULL,                        -- 关联工单号
    point_code    TEXT,                                 -- 维修测点
    problem       TEXT,                                 -- 问题（工单备注原文）
    retest_score  REAL,                                 -- 复测健康度（回放复测后可回填）
    done_at       TIMESTAMPTZ NOT NULL DEFAULT now()    -- 闭环完成时间
);
COMMENT ON TABLE maintenance_log IS '维修记录：工单"维修闭环"时自动写一条，维修动作持久化存档';

-- ---------------------------------------------------------------------
-- 6. 队友只读账号（幂等）：A/C 用 pgAdmin 直连查库，只读防误写
--    连接参数：主机 localhost(或B的IP)、端口5432、库 turnout_twin、
--              用户 turnout_read、密码 turnout_read
-- ---------------------------------------------------------------------
DO $do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'turnout_read') THEN
        CREATE ROLE turnout_read LOGIN PASSWORD 'turnout_read';
        RAISE NOTICE '已创建只读账号 turnout_read / turnout_read';
    END IF;
END
$do$;
GRANT CONNECT ON DATABASE turnout_twin TO turnout_read;
GRANT USAGE ON SCHEMA public TO turnout_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO turnout_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO turnout_read;

-- ---------------------------------------------------------------------
-- 附：TimescaleDB 检测（装了自动转 hypertable；没装按普通表运行，不影响以上全部功能）
-- ---------------------------------------------------------------------
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('timeseries_data', 'ts',
                                  chunk_time_interval => INTERVAL '1 day',
                                  migrate_data => TRUE,
                                  if_not_exists => TRUE);
        RAISE NOTICE 'timeseries_data 已转为 hypertable';
    ELSE
        RAISE NOTICE '未装 TimescaleDB，按普通 PostgreSQL 运行（以上功能全部可用）';
    END IF;
END
$do$;
