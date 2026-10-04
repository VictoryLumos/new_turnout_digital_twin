-- ---------------------------------------------------------------------
-- 09. 只读账号 turnout_read（2026-10-04 找回固化；接口对接指南 §数据库直连）
--
-- 用途：A/C 用 pgAdmin 直连查库做报告/验证，只能 SELECT，杜绝误写。
-- 账号：用户 turnout_read / 密码 turnout_read
-- 幂等：角色存在则跳过创建；授权重复执行无害（换成新库从 01 重建后跑一次即可）。
-- 说明：整库备份恢复时若提示 "role turnout_read does not exist" 警告，
--       执行本脚本后即消失（换机部署指南第 3 步注记对应的就是它）。
-- ---------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'turnout_read') THEN
        CREATE ROLE turnout_read LOGIN PASSWORD 'turnout_read';
    END IF;
END
$$;

-- 密码统一重置（幂等：备份恢复不带角色密码，存在旧角色也以本密码为准）
ALTER ROLE turnout_read WITH LOGIN PASSWORD 'turnout_read';

GRANT USAGE ON SCHEMA public TO turnout_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO turnout_read;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO turnout_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO turnout_read;
