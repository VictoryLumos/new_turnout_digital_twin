# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 实时数据入库 v2（B：数据管道线，整改第二轮）

作用：
    模拟"数据源 → 数据库"的实时写入：按 0.1s 节奏循环播放 data.json，
    把每一时刻的最新值刷进 realtime_value 表。每个测点在这张表里永远
    只有一行，value 随时间不断更新——这就是"数据库里的实时表格"。
    对应完整版架构中"传感器/MQTT 接入 → 库"的那条链路。

整改第二轮升级（组长反馈第2/3条）：
  · 入库前强制校验：走 管道公共.load_dataset——必填两路 + 五路"出现即
    校验"（NaN/Infinity/字符串/null 拒绝，绝不带病入库）
  · 写入测点扩到五路位移（字段存在才写，缺失不补假值）
  · 运行中断线恢复：长连接换成 管道公共.ResilientDB——数据库重启/连接
    被杀后自动重连续写，进程不再直接崩溃

怎么看到实时效果（另开一个命令行窗口，二选一）：
    方式A（自动刷新的活表格，推荐）：
        D:\PostgreSQL\bin\psql.exe -U postgres -h localhost -d turnout_twin
        密码输入 postgres，然后执行：
            SELECT point_code 字段, value 值, to_char(ts,'HH24:MI:SS') 仿真时刻,
                   alarm 告警 FROM realtime_value ORDER BY point_code;
            \watch 1
    方式B（pgAdmin）：
        右键 realtime_value 表 → View/Edit Data → All Rows，
        表格不会自动刷新，隔一会儿点一下刷新按钮重查就是新值。

用法：
    python 实时数据入库.py      # Ctrl+C 停止
"""
import os
import sys
import time
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import load_dataset, ResilientDB, TZ8  # noqa: E402

DATA_FILE = os.path.join(HERE, "data.json")
STEP = 0.1

# 必填两路 + 出现即校验的三路（与推送服务同一判据）
BASE_REQUIRED = ["time", "switchRailDisp1", "pointRailDisp1"]
ADAPT_FIELDS = ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"]

# 数据字段 → 测点编码（五路位移；与 数据服务API.py 保持一致）
FIELD_TO_POINT = {
    "switchRailDisp1": "T01-SR-01-DISP",
    "switchRailDisp2": "T01-SR-02-DISP",
    "switchRailDisp3": "T01-SR-03-DISP",
    "pointRailDisp1": "T01-PR-01-DISP",
    "pointRailDisp2": "T01-PR-02-DISP",
}

# 与导入脚本同基准：库里时间轴固定，重复运行结果一致
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=TZ8)


def main():
    rows = load_dataset(DATA_FILE, required=BASE_REQUIRED,
                        extra_fields=ADAPT_FIELDS)   # 非法直接 exit 1

    rdb = ResilientDB()   # 长连接：断线自动重连重试（管道公共）

    # 告警阈值从测点表读，与前端/监视器同一判据
    limits = dict(rdb.run(lambda cur: (
        cur.execute("SELECT point_code, alarm_threshold FROM measurement_point "
                    "WHERE alarm_threshold IS NOT NULL"),
        cur.fetchall())[1]))

    n_pt = len(FIELD_TO_POINT)
    print(f"[实时入库] 开始：{len(rows)} 条按 {STEP}s 循环刷新 realtime_value"
          f"（{n_pt} 个位移测点，Ctrl+C 停止；断库自动恢复）")
    i = 0
    try:
        while True:
            r = rows[i % len(rows)]
            ts = BASE_TS + timedelta(seconds=r["time"])
            present = [(p, r[f], p in limits and r[f] > limits[p])
                       for f, p in FIELD_TO_POINT.items() if f in r]

            def _flush(cur, _rows=present, _ts=ts):
                for point, value, alarm in _rows:
                    cur.execute(
                        "INSERT INTO realtime_value (point_code, value, ts, alarm, updated_at) "
                        "VALUES (%s, %s, %s, %s, now()) "
                        "ON CONFLICT (point_code) DO UPDATE SET "
                        "value = EXCLUDED.value, ts = EXCLUDED.ts, "
                        "alarm = EXCLUDED.alarm, updated_at = now()",
                        (point, value, _ts, alarm))
            rdb.run(_flush)
            if i % 10 == 0:  # 每 1 秒打一行心跳，别刷屏
                print(f"  t={r['time']:>4.1f}s  尖轨={r['switchRailDisp1']:>7.2f}mm  "
                      f"心轨={r['pointRailDisp1']:>6.2f}mm"
                      + ("" if len(present) == 5 else f"（本数据含 {len(present)} 路位移）"),
                      flush=True)
            i += 1
            time.sleep(STEP)
    except KeyboardInterrupt:
        print("\n[实时入库] 已停止")
    finally:
        rdb.close()


if __name__ == "__main__":
    main()
