# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 四工况扩展数据集入库（B：数据管道线，完整版升级）

作用：
    把 扩展数据集v2/ 的四工况数据（4×100行×20测点）灌入时序表
    timeseries_data，支撑 /api/history 历史查询与趋势回放页面。

时间轴设计（避免主键冲突）：
    每个工况占用一小时：正常转换 00:00 起、卡阻 01:00 起、
    密贴不良 02:00 起、锁闭失败 03:00 起（基准日 2026-09-26）。
    source 字段标记来源（仿真-工况名），与假数据(fake)区分。

用法：python 04_扩展数据入库.py        # 可重复执行，重复行自动跳过
"""
import json
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

HERE = os.path.dirname(os.path.abspath(__file__))
DS_DIR = os.path.join(HERE, "..", "data", "扩展数据集v2")

DB = dict(host="localhost", port=5432, user="postgres",
          password="postgres", dbname="turnout_twin")
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))

# 数据字段 → 测点编码（《完整版》映射表全量）
FIELD_TO_POINT = {
    "switchRailDisp1": "T01-SR-01-DISP", "switchRailDisp2": "T01-SR-02-DISP",
    "switchRailDisp3": "T01-SR-03-DISP",
    "switchRailForce1": "T01-SR-01-FORCE", "switchRailForce2": "T01-SR-02-FORCE",
    "switchRailForce3": "T01-SR-03-FORCE",
    "pointRailDisp1": "T01-PR-01-DISP", "pointRailDisp2": "T01-PR-02-DISP",
    "pointRailForce1": "T01-PR-01-FORCE", "pointRailForce2": "T01-PR-02-FORCE",
    "switchMachineCurrent": "T01-SM-01-CURR", "switchMachinePower": "T01-SM-01-POWER",
    "lockStatus": "T01-SM-01-LOCK", "closeStatus": "T01-SR-01-CLOSE",
    "frogVibration": "T01-FR-01-VIB", "frogWear": "T01-FR-01-WEAR",
    "wheelRailForceLateral": "T01-FR-01-WRF-L",
    "wheelRailForceVertical": "T01-FR-01-WRF-V",
    "guardRailDisp": "T01-GR-01-DISP", "railTemperature": "T01-BR-01-TEMP",
}

CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败"]


def main():
    conn = psycopg2.connect(**DB)
    total = 0
    try:
        for idx, cond in enumerate(CONDITIONS):
            path = os.path.join(DS_DIR, f"工况_{cond}.json")
            rows = json.load(open(path, encoding="utf-8"))
            source = f"仿真-{cond}"
            offset = timedelta(hours=idx)          # 每工况错开 1 小时
            with conn:
                with conn.cursor() as cur:
                    # 先清掉同源旧数据再写入：保证重跑后是最新版本（DO NOTHING 不会更新）
                    cur.execute("DELETE FROM timeseries_data WHERE source = %s", (source,))
            payload = []
            for r in rows:
                ts = BASE_TS + offset + timedelta(seconds=r["time"])
                for field, point in FIELD_TO_POINT.items():
                    payload.append((point, ts, float(r[field]), source))
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur,
                        "INSERT INTO timeseries_data (point_code, ts, value, source) "
                        "VALUES %s ON CONFLICT (point_code, ts) DO NOTHING",
                        payload, template="(%s, %s, %s, %s)")
            total += len(payload)
            print(f"  {cond}: {len(rows)}行 × 20测点 = {len(payload)} 条 "
                  f"（时间轴 {(BASE_TS + offset).strftime('%H:%M')} 起）")
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM timeseries_data")
                n = cur.fetchone()[0]
                cur.execute("SELECT count(DISTINCT point_code) FROM timeseries_data")
                pts = cur.fetchone()[0]
                cur.execute("SELECT count(DISTINCT source) FROM timeseries_data")
                src = cur.fetchone()[0]
        print(f"[完成] 本次写入 {total} 条（重复自动跳过）；"
              f"时序表现有 {n} 条，覆盖 {pts} 个测点、{src} 种数据来源")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
