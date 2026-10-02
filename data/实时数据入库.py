# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 实时数据入库（B：数据管道线）

作用：
    模拟"数据源 → 数据库"的实时写入：按 0.1s 节奏循环播放 data.json，
    把每一时刻的最新值刷进 realtime_value 表。每个测点在这张表里永远
    只有一行，value 随时间不断更新——这就是"数据库里的实时表格"。
    对应完整版架构中"传感器/MQTT 接入 → 库"的那条链路。

怎么看到实时效果（另开一个命令行窗口，二选一）：
    方式A（自动刷新的活表格，推荐）：
        D:\PostgreSQL\bin\psql.exe -U postgres -h localhost -d turnout_twin
        密码输入 postgres，然后执行：
            SELECT point_code 字段, value 值, to_char(ts,'HH24:MI:SS') 仿真时刻,
                   alarm 告警 FROM realtime_value ORDER BY point_code;
            \watch 1
        （\watch 1 = 每秒自动重跑上面那条查询，Ctrl+C 停）
    方式B（pgAdmin）：
        右键 realtime_value 表 → View/Edit Data → All Rows，
        表格不会自动刷新，隔一会儿点一下刷新按钮重查就是新值。

用法：
    python 实时数据入库.py      # Ctrl+C 停止
"""
import json
import os
import sys
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import DB, db_connect  # 统一配置+失败重试

DATA_FILE = os.path.join(HERE, "data.json")
STEP = 0.1

# 数据字段 → 测点编码（与 02_数据导入脚本.py 保持一致）
FIELD_TO_POINT = {
    "switchRailDisp1": "T01-SR-01-DISP",
    "pointRailDisp1": "T01-PR-01-DISP",
}

# 与导入脚本同基准：库里时间轴固定，重复运行结果一致
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))


def main():
    try:
        with open(DATA_FILE, encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        sys.exit(f"[失败] 找不到 {DATA_FILE}")

    conn = db_connect()   # 失败自动重试（管道公共）

    # 告警阈值从测点表读，与前端/监视器同一判据
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT point_code, alarm_threshold FROM measurement_point "
                        "WHERE alarm_threshold IS NOT NULL")
            limits = dict(cur.fetchall())

    print(f"[实时入库] 开始：{len(rows)} 条按 {STEP}s 循环刷新 realtime_value（Ctrl+C 停止）")
    i = 0
    try:
        while True:
            r = rows[i % len(rows)]
            ts = BASE_TS + timedelta(seconds=r["time"])
            with conn:
                with conn.cursor() as cur:
                    for field, point in FIELD_TO_POINT.items():
                        if field not in r:
                            continue
                        alarm = point in limits and r[field] > limits[point]
                        cur.execute(
                            "INSERT INTO realtime_value (point_code, value, ts, alarm, updated_at) "
                            "VALUES (%s, %s, %s, %s, now()) "
                            "ON CONFLICT (point_code) DO UPDATE SET "
                            "value = EXCLUDED.value, ts = EXCLUDED.ts, "
                            "alarm = EXCLUDED.alarm, updated_at = now()",
                            (point, r[field], ts, alarm),
                        )
            if i % 10 == 0:  # 每 1 秒打一行心跳，别刷屏
                print(f"  t={r['time']:>4.1f}s  尖轨={r['switchRailDisp1']:>7.2f}mm  "
                      f"心轨={r['pointRailDisp1']:>6.2f}mm", flush=True)
            i += 1
            time.sleep(STEP)
    except KeyboardInterrupt:
        print("\n[实时入库] 已停止")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
