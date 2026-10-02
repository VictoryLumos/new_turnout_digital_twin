# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 联调自检脚本 v2（B：数据管道线，整改版）

一键体检 B 侧管道（对应组长整改令的验收前置）：
  1) data.json 契约（80条/0.1s/三字段）与超限演示段
  2) 五工况数据集文件齐全（含告警演示 120 行）
  3) WebSocket 推送就绪（脚本+库+端口状态，端口走配置默认3003）
  4) 数据库五张表行数与超阈值记录
用法：python 联调自检脚本.py
"""
import json
import os
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import WS_PORT, db_session  # noqa: E402

DATA_FILE = os.path.join(HERE, "data.json")
WS_SCRIPT = os.path.join(HERE, "WebSocket推送服务.py")
DS_DIR = os.path.join(HERE, "扩展数据集v2")
DS_EXPECT = {"工况_正常转换.json": 100, "工况_卡阻.json": 100,
             "工况_密贴不良.json": 100, "工况_锁闭失败.json": 100,
             "工况_告警演示.json": 120}

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("[OK] " if ok else "[NG] ") + name + (f"：{detail}" if detail else ""))


print("=" * 46)
print("道岔数字孪生 · B 侧管道自检 v2（整改版）")
print("=" * 46)

# ---- 1) data.json 契约 ----
try:
    with open(DATA_FILE, encoding="utf-8") as f:
        rows = json.load(f)
    n = len(rows)
    fields = sorted(rows[0].keys()) if rows else []
    steps = {round(rows[i + 1]["time"] - rows[i]["time"], 6) for i in range(n - 1)}
    ok = (n == 80 and fields == ["pointRailDisp1", "switchRailDisp1", "time"]
          and steps == {0.1})
    check("data.json 契约（80条/0.1s/三字段）", ok,
          f"{n}条" if ok else f"异常：{n}条 {fields} {steps}")
    over = sum(r["switchRailDisp1"] > 150 for r in rows)
    check("尖轨超限演示段（>150mm）", over > 0, f"{over} 条")
except Exception as e:  # noqa: BLE001
    check("data.json 读取", False, str(e))

# ---- 2) 五工况数据集 ----
for fname, expect in DS_EXPECT.items():
    p = os.path.join(DS_DIR, fname)
    try:
        with open(p, encoding="utf-8") as f:
            n = len(json.load(f))
        check(f"数据集 {fname}", n == expect, f"{n} 行")
    except Exception as e:  # noqa: BLE001
        check(f"数据集 {fname}", False, str(e))

# ---- 3) WebSocket 就绪 ----
check("WebSocket推送服务.py 存在", os.path.exists(WS_SCRIPT))
try:
    import websockets
    check("websockets 库已安装", True, websockets.__version__)
except ImportError:
    check("websockets 库已安装", False, "pip install websockets")


def _port_listening(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


check(f"WebSocket 端口 {WS_PORT} 状态", True,
      f"服务已在运行（C 连 ws://本机IP:{WS_PORT}）" if _port_listening(WS_PORT)
      else f"未运行（正常；联调时启动 WebSocket推送服务.py 即可）")

# ---- 4) 数据库（统一配置 + 重试） ----
try:
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM component")
        n_comp = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM measurement_point")
        n_point = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM realtime_value")
        n_rt = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data")
        n_ts = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT batch) FROM timeseries_data")
        n_batch = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data d "
                    "JOIN measurement_point p ON p.point_code = d.point_code "
                    "WHERE d.value > p.alarm_threshold")
        n_alarm = cur.fetchone()[0]
    check("数据库 turnout_twin 连接", True)
    check("表行数（构件9/测点20，时序≥160）",
          n_comp == 9 and n_point == 20 and n_ts >= 160,
          f"构件={n_comp} 测点={n_point} 实时={n_rt} 时序={n_ts} 批次={n_batch}")
    check("库内超阈值记录(跨批累计)", n_alarm >= 29, f"{n_alarm} 条")
except Exception as e:  # noqa: BLE001
    hint = "服务没起来？管理员运行：net start postgresql-x64-17"
    check("数据库 turnout_twin 连接", False, f"{e}（{hint}）" if "connect" in str(e).lower() else str(e))

# ---- 汇总 ----
ng = results.count(False)
print("-" * 46)
print(f"自检完成：{len(results)} 项，通过 {len(results) - ng} 项"
      + ("" if ng == 0 else f"，未通过 {ng} 项"))
print("结论：" + ("B 侧管道健康，可以联调" if ng == 0 else "先解决 [NG] 项再联调"))
sys.exit(0 if ng == 0 else 1)
