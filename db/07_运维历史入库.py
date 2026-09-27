# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 一天运维历史数据入库（B：数据管道线）

作用：
    生成"一天运维记录"：2026-09-26 当天 08:00~20:00 每小时一次道岔转换动作，
    共 13 次（10 次正常 + 11:00 卡阻 + 16:00 密贴不良 + 19:00 锁闭失败），
    约 26000 行写入 timeseries_data（source=运维-工况名）。

    价值：库从"4 段孤立演示"变成"一天运维全景"——趋势页"全部"视图
    即一天总览，故障时段一眼可见；触发器自动为故障时段补告警审计；
    报告/答辩可讲"按天回看的运维数据底座"。

    曲线复用 生成扩展数据集.py 的物理模型（同一条 88mm 卡阻曲线），
    每次动作重新播随机种子，噪声互不相同、可复现。

幂等：重复执行先清 source LIKE '运维-%' 再写入，结果一致。
用法：python 07_运维历史入库.py
"""
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "data"))
import 生成扩展数据集 as gen          # 复用四工况物理曲线模型

DB = dict(host="localhost", port=5432, user="postgres",
          password="postgres", dbname="turnout_twin")
DAY = datetime(2026, 9, 26, 8, 0, 0, tzinfo=timezone(timedelta(hours=8)))  # 08:00 起

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

# 13 次动作排班：小时偏移 → 工况（10 正常 + 3 故障，白天运营时段）
SCHEDULE = ["正常转换", "正常转换", "正常转换", "卡阻", "正常转换",
            "正常转换", "正常转换", "正常转换", "密贴不良", "正常转换",
            "正常转换", "锁闭失败", "正常转换"]


def main():
    conn = psycopg2.connect(**DB)
    total = 0
    try:
        with conn:
            with conn.cursor() as cur:
                # 幂等：先清旧运维历史（触发器审计表有 (测点,时间) 唯一约束，重复导入不重复留痕）
                cur.execute("DELETE FROM timeseries_data WHERE source LIKE '运维-%%'")
        for k, cond in enumerate(SCHEDULE):
            random.seed(20260926 * 100 + k)          # 每次动作固定种子：可复现、噪声互异
            rows = gen.build_condition(cond)
            base = DAY + timedelta(hours=k)
            source = f"运维-{cond}"
            payload = [(point, base + timedelta(seconds=r["time"]),
                        float(r[field]), source)
                       for r in rows for field, point in FIELD_TO_POINT.items()]
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur,
                        "INSERT INTO timeseries_data (point_code, ts, value, source) "
                        "VALUES %s ON CONFLICT (point_code, ts) DO NOTHING",
                        payload, template="(%s, %s, %s, %s)")
            total += len(payload)
            mark = "  ← 故障" if cond != "正常转换" else ""
            print(f"  {base.strftime('%H:%M')} {cond}：{len(rows)}行×20测点{mark}")
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM timeseries_data")
                n = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM timeseries_data WHERE source LIKE '运维-%%'")
                n_ops = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM alarm_event")
                n_evt = cur.fetchone()[0]
        print(f"[完成] 运维历史写入 {total} 条；时序表共 {n} 条（其中运维 {n_ops} 条），"
              f"告警审计累计 {n_evt} 条（触发器自动留痕）")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
