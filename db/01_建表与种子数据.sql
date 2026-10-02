-- =====================================================================
-- 道岔数字孪生 建库脚本（PostgreSQL 16/17 + TimescaleDB）
-- 作者：B（数据管道线）  版本：v1.0  日期：2026-09-26
-- 表结构详见 docs/数据库表结构设计_v1.0.md，执行前需经 A 确认编码映射
--
-- 用法（装好 PostgreSQL + TimescaleDB 后）：
--   1) psql -U postgres -c "CREATE DATABASE turnout_twin;"
--   2) psql -U postgres -d turnout_twin -f schema.sql
--      Windows 下中文乱码先执行：set PGCLIENTENCODING=UTF8
--   未安装 TimescaleDB 时无需改脚本：下方两个 DO 块会自动检测，
--   检测不到就按普通 PostgreSQL 表运行（见 docs/数据库表结构设计_v1.0.md 选型说明）。
-- =====================================================================

DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        RAISE NOTICE 'TimescaleDB 已启用';
    ELSE
        RAISE NOTICE '未安装 TimescaleDB，按普通 PostgreSQL 运行（不影响建表和数据）';
    END IF;
END
$do$;

-- ---------------------------------------------------------------------
-- 1. 构件表（映射表按构件编码去重，共 9 条）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS component (
    component_code  TEXT PRIMARY KEY,            -- 构件编码，如 T01-SR-01
    turnout_id      TEXT NOT NULL,               -- 道岔编号，如 T01
    component_type  TEXT NOT NULL,               -- 构件类型：SR/PR/SM/FR/GR/BR
    model_node      TEXT,                        -- glTF 几何主节点名（A 导出后确认）
    asset_id        TEXT UNIQUE,                 -- 资产ID，如 AS-002
    remark          TEXT
);

-- ---------------------------------------------------------------------
-- 2. 测点表（对应《完整版》编码映射表 20 行，一行一个测点）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS measurement_point (
    point_code       TEXT PRIMARY KEY,           -- 测点编码，如 T01-SR-01-DISP
    component_code   TEXT NOT NULL REFERENCES component(component_code),
    point_type       TEXT NOT NULL,              -- DISP/FORCE/CURR/VIB/TEMP/...（以后缀为准）
    sensor_id        TEXT UNIQUE,                -- 传感器ID，如 SEN-001
    field_name       TEXT NOT NULL UNIQUE,       -- 数据字段（小驼峰），如 switchRailDisp1
    node_name        TEXT,                       -- 映射表"模型节点名"列原文
    unit             TEXT,                       -- mm/N/A/W/m/s²/℃/kN/0/1
    alarm_threshold  REAL,                       -- 数值大于该值触发告警；空=阈值待定
    remark           TEXT
);

CREATE INDEX IF NOT EXISTS idx_point_component ON measurement_point(component_code);

-- ---------------------------------------------------------------------
-- 3. 时序数据表（data.json / UM / 将来真实传感器数据统一进这张表）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS timeseries_data (
    ts         TIMESTAMPTZ NOT NULL,             -- 时间戳；数据契约 time(秒) 导入时换算
    point_code TEXT NOT NULL REFERENCES measurement_point(point_code),
    value      REAL NOT NULL,                    -- 数值，单位见测点表 unit
    source     TEXT NOT NULL DEFAULT 'fake',     -- fake/um/sensor/...
    PRIMARY KEY (point_code, ts)
);

DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('timeseries_data', 'ts',
                                  chunk_time_interval => INTERVAL '1 day',
                                  migrate_data => TRUE,
                                  if_not_exists => TRUE);
        RAISE NOTICE 'timeseries_data 已转为 hypertable';
    ELSE
        RAISE NOTICE '跳过 hypertable 转换（普通表模式）';
    END IF;
END
$do$;

-- ---------------------------------------------------------------------
-- 4. 工单表（预留，完整版做"告警与工单联动"时启用）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS work_order (
    work_order_id  TEXT PRIMARY KEY,             -- 工单ID，如 WO-2024-001
    point_code     TEXT REFERENCES measurement_point(point_code),
    status         TEXT DEFAULT 'reserved',      -- 预留状态
    remark         TEXT
);

-- ---------------------------------------------------------------------
-- 5. 实时值表（每个测点恒一行，值由 实时数据入库.py 按 0.1s 循环刷新；
--    对应完整版"实时接入→库"链路，基础版用于演示库内实时表格）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS realtime_value (
    point_code  TEXT PRIMARY KEY REFERENCES measurement_point(point_code),
    value       REAL NOT NULL,                   -- 当前值，单位见测点表
    ts          TIMESTAMPTZ NOT NULL,            -- 该值对应的仿真时刻
    alarm       BOOLEAN NOT NULL DEFAULT FALSE,  -- 是否超阈值
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now() -- 本行最后刷新时间
);

-- =====================================================================
-- 种子数据：全部来自《完整版》编码映射表
-- =====================================================================

-- 构件 9 条
INSERT INTO component (component_code, turnout_id, component_type, model_node, asset_id) VALUES
    ('T01-BR-01', 'T01', 'BR', 'StockRail',     'AS-001'),   -- 基本轨
    ('T01-SR-01', 'T01', 'SR', 'SwitchRail',    'AS-002'),   -- 尖轨 牵引点1
    ('T01-SR-02', 'T01', 'SR', 'SwitchRail_2',  'AS-003'),   -- 尖轨 牵引点2
    ('T01-SR-03', 'T01', 'SR', 'SwitchRail_3',  'AS-004'),   -- 尖轨 牵引点3
    ('T01-PR-01', 'T01', 'PR', 'PointRail',     'AS-005'),   -- 心轨 牵引点1
    ('T01-PR-02', 'T01', 'PR', 'PointRail_2',   'AS-006'),   -- 心轨 牵引点2
    ('T01-SM-01', 'T01', 'SM', 'SwitchMachine', 'AS-007'),   -- 转辙机
    ('T01-FR-01', 'T01', 'FR', 'Frog',          'AS-008'),   -- 辙叉
    ('T01-GR-01', 'T01', 'GR', 'GuardRail',     'AS-009')    -- 护轨
ON CONFLICT (component_code) DO NOTHING;

-- 测点 20 条（point_code, component_code, point_type, sensor_id, field_name, node_name, unit, alarm_threshold）
INSERT INTO measurement_point (point_code, component_code, point_type, sensor_id, field_name, node_name, unit, alarm_threshold) VALUES
    -- 尖轨位移（基础版两字段之一；阈值 150mm 来自《统一标准》第 5 节）
    ('T01-SR-01-DISP',  'T01-SR-01', 'DISP',  'SEN-001', 'switchRailDisp1',       'SwitchRail',           'mm',  150),
    ('T01-SR-02-DISP',  'T01-SR-02', 'DISP',  'SEN-002', 'switchRailDisp2',       'SwitchRail_2',         'mm',  NULL),
    ('T01-SR-03-DISP',  'T01-SR-03', 'DISP',  'SEN-003', 'switchRailDisp3',       'SwitchRail_3',         'mm',  NULL),
    -- 尖轨牵引力
    ('T01-SR-01-FORCE', 'T01-SR-01', 'FORCE', 'SEN-004', 'switchRailForce1',      'SwitchRail_1_FORCE',   'N',   NULL),
    ('T01-SR-02-FORCE', 'T01-SR-02', 'FORCE', 'SEN-005', 'switchRailForce2',      'SwitchRail_2_FORCE',   'N',   NULL),
    ('T01-SR-03-FORCE', 'T01-SR-03', 'FORCE', 'SEN-006', 'switchRailForce3',      'SwitchRail_3_FORCE',   'N',   NULL),
    -- 心轨位移（基础版两字段之一；阈值 100mm）
    ('T01-PR-01-DISP',  'T01-PR-01', 'DISP',  'SEN-007', 'pointRailDisp1',        'PointRail',            'mm',  100),
    ('T01-PR-02-DISP',  'T01-PR-02', 'DISP',  'SEN-008', 'pointRailDisp2',        'PointRail_2',          'mm',  NULL),
    -- 心轨牵引力
    ('T01-PR-01-FORCE', 'T01-PR-01', 'FORCE', 'SEN-009', 'pointRailForce1',       'PointRail_1_FORCE',    'N',   NULL),
    ('T01-PR-02-FORCE', 'T01-PR-02', 'FORCE', 'SEN-010', 'pointRailForce2',       'PointRail_2_FORCE',    'N',   NULL),
    -- 转辙机电流/功率/锁闭状态
    ('T01-SM-01-CURR',  'T01-SM-01', 'CURR',  'SEN-011', 'switchMachineCurrent',  'SwitchMachine',        'A',   NULL),
    ('T01-SM-01-POWER', 'T01-SM-01', 'POWER', 'SEN-012', 'switchMachinePower',    'SwitchMachine_Power',  'W',   NULL),
    ('T01-SM-01-LOCK',  'T01-SM-01', 'LOCK',  'SEN-019', 'lockStatus',            'LockStatus',           '0/1', NULL),
    -- 辙叉振动/磨耗/轮轨力
    ('T01-FR-01-VIB',   'T01-FR-01', 'VIB',   'SEN-013', 'frogVibration',         'Frog',                 'm/s²',NULL),
    ('T01-FR-01-WEAR',  'T01-FR-01', 'WEAR',  'SEN-014', 'frogWear',              'Frog_Wear',            'mm',  NULL),
    ('T01-FR-01-WRF-L', 'T01-FR-01', 'WRF-L', 'SEN-017', 'wheelRailForceLateral', 'WheelRailForce_L',     'kN',  NULL),
    ('T01-FR-01-WRF-V', 'T01-FR-01', 'WRF-V', 'SEN-018', 'wheelRailForceVertical','WheelRailForce_V',     'kN',  NULL),
    -- 护轨位移
    ('T01-GR-01-DISP',  'T01-GR-01', 'DISP',  'SEN-015', 'guardRailDisp',         'GuardRail',            'mm',  NULL),
    -- 基本轨温度
    ('T01-BR-01-TEMP',  'T01-BR-01', 'TEMP',  'SEN-016', 'railTemperature',       'RailTemp',             '℃',   NULL),
    -- 密贴状态
    ('T01-SR-01-CLOSE', 'T01-SR-01', 'CLOSE', 'SEN-020', 'closeStatus',           'CloseStatus',          '0/1', NULL)
ON CONFLICT (point_code) DO NOTHING;

-- 工单 20 条（预留，按映射表预置编号）
INSERT INTO work_order (work_order_id, point_code) VALUES
    ('WO-2024-001', 'T01-SR-01-DISP'),
    ('WO-2024-002', 'T01-SR-02-DISP'),
    ('WO-2024-003', 'T01-SR-03-DISP'),
    ('WO-2024-004', 'T01-SR-01-FORCE'),
    ('WO-2024-005', 'T01-SR-02-FORCE'),
    ('WO-2024-006', 'T01-SR-03-FORCE'),
    ('WO-2024-007', 'T01-PR-01-DISP'),
    ('WO-2024-008', 'T01-PR-02-DISP'),
    ('WO-2024-009', 'T01-PR-01-FORCE'),
    ('WO-2024-010', 'T01-PR-02-FORCE'),
    ('WO-2024-011', 'T01-SM-01-CURR'),
    ('WO-2024-012', 'T01-SM-01-POWER'),
    ('WO-2024-013', 'T01-FR-01-VIB'),
    ('WO-2024-014', 'T01-FR-01-WEAR'),
    ('WO-2024-015', 'T01-GR-01-DISP'),
    ('WO-2024-016', 'T01-BR-01-TEMP'),
    ('WO-2024-017', 'T01-FR-01-WRF-L'),
    ('WO-2024-018', 'T01-FR-01-WRF-V'),
    ('WO-2024-019', 'T01-SM-01-LOCK'),
    ('WO-2024-020', 'T01-SR-01-CLOSE')
ON CONFLICT (work_order_id) DO NOTHING;

-- 实时值表初始 2 行（有数据源的两个测点；随后由 实时数据入库.py 持续刷新）
INSERT INTO realtime_value (point_code, value, ts, alarm) VALUES
    ('T01-SR-01-DISP', 0, '2026-09-26 00:00:00+08', FALSE),
    ('T01-PR-01-DISP', 0, '2026-09-26 00:00:00+08', FALSE)
ON CONFLICT (point_code) DO NOTHING;
