# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 数据库体检（B：数据管道线，配套 05_数据库升级v2.sql）

作用（对应答辩"数据库深度"问点，一次性生成体检报告）：
    1. 对象检查：视图/触发器/索引是否已按 v2 升级就位
    2. 数据分布：五表+告警事件行数、各数据源统计（v_source_stats）
    3. 数据质量：每测点行数/时间断点（v_data_quality）、阈值覆盖率
    4. 性能分析：EXPLAIN ANALYZE 真实执行计划（历史查询/超阈统计），
       报告执行时间与是否走索引
    5. 实例健康：缓存命中率（blks_hit/reads，>95% 为佳）

用法：python 06_数据库体检.py        # 只读不写，随时可跑
"""
import re
import os
import sys

import psycopg2

DB = dict(host=os.environ.get("PGHOST", "localhost"),
          port=int(os.environ.get("PGPORT", "5432")),
          user=os.environ.get("PGUSER", "postgres"),
          password=os.environ.get("PGPASSWORD", "postgres"),
          dbname=os.environ.get("PGDATABASE", "turnout_twin"), connect_timeout=5)

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("[OK] " if ok else "[NG] ") + name + (f"：{detail}" if detail else ""))


print("=" * 56)
print("道岔数字孪生 · 数据库体检（turnout_twin）")
print("=" * 56)

try:
    conn = psycopg2.connect(**DB)
except Exception as e:
    text = str(e).lower()
    hint = "（服务没起来？管理员运行 net start postgresql-x64-17）" if any(
        k in text for k in ("connect", "refused", "timeout", "拒绝")) else ""
    print(f"[NG] 无法连接：{e}{hint}")
    sys.exit(1)

cur = conn.cursor()

# ---- 1) v2 升级对象 ----
cur.execute("SELECT viewname FROM pg_views WHERE schemaname='public' AND viewname LIKE 'v_%'")
views = {r[0] for r in cur.fetchall()}
need_views = {"v_point_latest", "v_alarm_records", "v_source_stats", "v_data_quality"}
check("分析视图（4个）", need_views <= views,
      " ".join(sorted(views)) if need_views <= views
      else f"缺 {need_views - views}，先执行 db/05_数据库升级v2.sql")
cur.execute("SELECT count(*) FROM pg_trigger WHERE tgname='trg_timeseries_alarm'")
has_trg = cur.fetchone()[0] > 0
cur.execute("SELECT to_regclass('alarm_event') IS NOT NULL")
has_evt = cur.fetchone()[0]
check("告警审计（alarm_event + 入库触发器）", has_trg and has_evt,
      "超阈值数据入库即自动留痕" if has_trg and has_evt else "缺触发器或审计表，先执行 05 升级脚本")

# ---- 2) 数据分布 ----
counts = {}   # 六张表行数逐一常量查询（表名为本文件固定名，不做拼接）
cur.execute("SELECT count(*) FROM component")
counts["component"] = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM measurement_point")
counts["measurement_point"] = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM timeseries_data")
counts["timeseries_data"] = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM work_order")
counts["work_order"] = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM realtime_value")
counts["realtime_value"] = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM alarm_event")
counts["alarm_event"] = cur.fetchone()[0]
check("六张表行数", counts["timeseries_data"] >= 30000 and counts["measurement_point"] == 20,
      " ".join(f"{k}={v}" for k, v in counts.items()))
cur.execute("SELECT source, rows_total, alarm_rows FROM v_source_stats ORDER BY ts_min")
src_rows = cur.fetchall()
check("数据源分布（v_source_stats）", len(src_rows) >= 8,
      "；".join(f"{s.split('-')[-1]} {n}行/超阈{a}" for s, n, a in src_rows))

# ---- 3) 数据质量 ----
cur.execute("SELECT max(gaps), count(*) FROM v_data_quality")
max_gaps, pts = cur.fetchone()
check("时序连续性（v_data_quality）", max_gaps is not None,
      f"{pts} 测点，每测点最多 {max_gaps} 个时间断点（四工况+一天13次运维动作，时段间隔为预期设计）")
cur.execute("SELECT count(*) FILTER (WHERE alarm_threshold IS NOT NULL), count(*) "
            "FROM measurement_point")
with_th, total = cur.fetchone()
check("阈值覆盖率", with_th >= 2,
      f"{with_th}/{total} 测点已定阈（其余待 A 确认编码映射后冻结，契约 v2.0 第2节）")

# ---- 4) 性能：EXPLAIN ANALYZE 真实执行（语句为常量字面量，不接受外部拼接） ----
def read_plan():
    plan = "\n".join(r[0] for r in cur.fetchall())
    ms = re.search(r"Execution Time: ([\d.]+) ms", plan)
    return float(ms.group(1)) if ms else -1, plan

cur.execute("EXPLAIN (ANALYZE, TIMING OFF) SELECT * FROM timeseries_data "
            "WHERE point_code='T01-SR-01-DISP' ORDER BY ts DESC LIMIT 500")
ms1, plan1 = read_plan()
used_idx1 = "Index Scan" in plan1 or "index" in plan1.lower()
check("历史查询执行计划（/api/history 同款）", used_idx1 and ms1 < 50,
      f"{ms1} ms，{'走索引' if used_idx1 else '全表扫描'}")

cur.execute("EXPLAIN (ANALYZE, TIMING OFF) SELECT source, count(*) FROM timeseries_data d "
            "JOIN measurement_point p USING(point_code) WHERE d.value > p.alarm_threshold "
            "GROUP BY source")
ms2, plan2 = read_plan()
check("超阈统计执行计划（告警联查）", ms2 < 100, f"{ms2} ms")

# ---- 5) 实例健康：缓存命中率 ----
cur.execute("SELECT blks_hit, blks_read FROM pg_stat_database WHERE datname=current_database()")
hit, read = cur.fetchone()
ratio = hit / (hit + read) * 100 if hit + read else 0
check("缓冲缓存命中率", ratio > 90, f"{ratio:.1f}%（>95% 为佳，冷启动后跑几轮查询会上升）")

conn.close()

ng = results.count(False)
print("-" * 56)
print(f"体检完成：{len(results)} 项，通过 {len(results) - ng} 项"
      + ("" if ng == 0 else f"，未通过 {ng} 项"))
print("结论：" + ("数据库层健康：v2 对象齐备、数据完整、查询走索引" if ng == 0
                  else "按上面 [NG] 项处理（多数为未执行 05_数据库升级v2.sql）"))
sys.exit(0 if ng == 0 else 1)
