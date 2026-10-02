# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 扩展数据集入库 v2（04，整改版）

变化：五工况（新增告警演示）；写入 batch 批次列；按来源替换（不清全表）；
连接失败自动重试；入库前走统一校验。
时间轴：每工况错开 1 小时（正常00:00 卡阻01:00 密贴02:00 锁闭03:00 告警04:00）。

用法：python 04_扩展数据入库.py     # 幂等：重跑先删同来源再插
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), "data")
sys.path.insert(0, DATA_DIR)
from 管道公共 import db_session, validate_rows  # noqa: E402

DS_DIR = os.path.join(DATA_DIR, "扩展数据集v2")
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))

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

CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败", "告警演示"]

INSERT_SQL = ("INSERT INTO timeseries_data (point_code, ts, value, source, batch) "
              "VALUES %s ON CONFLICT (point_code, ts) DO NOTHING")


def main():
    from psycopg2.extras import execute_values
    total = 0
    with db_session() as cur:
        for idx, cond in enumerate(CONDITIONS):
            path = os.path.join(DS_DIR, f"工况_{cond}.json")
            rows = json.load(open(path, encoding="utf-8"))
            good, errors = validate_rows(rows, os.path.basename(path))
            if errors:
                print(f"[校验失败] {path}：{errors[:3]}")
                sys.exit(1)
            source = f"仿真-{cond}"
            batch = f"B02-四工况-{cond}"
            offset = timedelta(hours=idx)
            # 按来源替换旧数据（重跑即更新，绝不清空整表）
            cur.execute("DELETE FROM timeseries_data WHERE source = %s", (source,))
            payload = []
            for r in good:
                ts = BASE_TS + offset + timedelta(seconds=float(r["time"]))
                for field, point in FIELD_TO_POINT.items():
                    payload.append((point, ts, float(r[field]), source, batch))
            execute_values(cur, INSERT_SQL, payload,
                           template="(%s, %s, %s, %s, %s)")
            total += len(payload)
            print(f"  {cond}: {len(good)}行×20测点={len(payload)}条 "
                  f"({(BASE_TS + offset):%H:%M}起, {batch})")
        cur.execute("SELECT count(*), count(DISTINCT batch) "
                    "FROM timeseries_data")  # 字面量SQL
        n, nb = cur.fetchone()
    print(f"[完成] 本次写入 {total} 条；库内共 {n} 条、{nb} 个批次（批次共存）")


if __name__ == "__main__":
    main()
