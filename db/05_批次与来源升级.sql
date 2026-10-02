-- =====================================================================
-- 道岔数字孪生 05_批次与来源升级（B线整改：历史数据批次能力）
-- 作用：时序表增加 batch 列，区分不同仿真批次；历史存量按来源回填批次。
-- 幂等可重复执行。
-- =====================================================================

ALTER TABLE timeseries_data ADD COLUMN IF NOT EXISTS batch TEXT;

-- 存量回填：按 source 定批次（静态字面量，无外部输入）
UPDATE timeseries_data SET batch = 'B01-基础联调'   WHERE batch IS NULL AND source = 'fake';
UPDATE timeseries_data SET batch = 'B02-四工况-正常转换' WHERE batch IS NULL AND source = '仿真-正常转换';
UPDATE timeseries_data SET batch = 'B02-四工况-卡阻'     WHERE batch IS NULL AND source = '仿真-卡阻';
UPDATE timeseries_data SET batch = 'B02-四工况-密贴不良' WHERE batch IS NULL AND source = '仿真-密贴不良';
UPDATE timeseries_data SET batch = 'B02-四工况-锁闭失败' WHERE batch IS NULL AND source = '仿真-锁闭失败';
UPDATE timeseries_data SET batch = 'B02-四工况-告警演示' WHERE batch IS NULL AND source = '仿真-告警演示';

CREATE INDEX IF NOT EXISTS idx_ts_source ON timeseries_data (source);
CREATE INDEX IF NOT EXISTS idx_ts_batch  ON timeseries_data (batch);
