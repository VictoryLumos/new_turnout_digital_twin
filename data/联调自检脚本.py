# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— 联调自检脚本（B：数据管道线）

作用：
    每周联调前跑一遍，10 秒确认 B 侧整条管道是否健康：
      1) data.json 契约校验（字段 / 条数 / 步长 / 超限演示段）
      2) WebSocket 推送就绪（脚本在 + websockets 库已装）
      3) 数据库 turnout_twin 连通 + 四张表行数 + 超阈值记录
    全部 [OK] = B 这边没问题；联调出问题就去查 C 的页面或 A 的模型/数据。

用法：
    python 联调自检脚本.py
"""
import json
import os
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(HERE, "data.json")
WS_SCRIPT = os.path.join(HERE, "WebSocket推送服务.py")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("[OK] " if ok else "[NG] ") + name + (f"：{detail}" if detail else ""))


print("=" * 46)
print("道岔数字孪生 · B 侧管道自检")
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
          f"{n}条，time {rows[0]['time']}~{rows[-1]['time']}s" if ok
          else f"异常：{n}条，字段{fields}，步长{steps}")
    over = sum(r["switchRailDisp1"] > 150 for r in rows)
    check("尖轨超限演示段（>150mm）", over > 0, f"{over} 条超限，用于演示变红")
except Exception as e:
    check("data.json 读取", False, str(e))

# ---- 2) WebSocket 推送就绪 ----
check("WebSocket推送服务.py 存在", os.path.exists(WS_SCRIPT))
try:
    import websockets
    check("websockets 库已安装", True, websockets.__version__)
except ImportError:
    check("websockets 库已安装", False, "先执行 pip install websockets")


def _port_listening(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


check("WebSocket 端口 8765 状态", True,
      "服务已在运行，C 可直连 ws://localhost:8765" if _port_listening(8765)
      else "未运行（正常；与 C 联调时启动 WebSocket推送服务.py 即可）")

# ---- 3) 数据库 ----
try:
    import psycopg2
    conn = psycopg2.connect(host="localhost", port=5432, user="postgres",
                            password="postgres", dbname="turnout_twin",
                            connect_timeout=5)
    cur = conn.cursor()
    counts = {}
    for t in ("component", "measurement_point", "timeseries_data", "work_order"):
        cur.execute(f"SELECT count(*) FROM {t}")
        counts[t] = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM timeseries_data d "
                "JOIN measurement_point p USING(point_code) "
                "WHERE d.value > p.alarm_threshold")
    alarms = cur.fetchone()[0]
    conn.close()
    check("数据库 turnout_twin 连接", True)
    expect = {"component": 9, "measurement_point": 20,
              "timeseries_data": 160, "work_order": 20}
    check("四张表行数（9/20/160/20）", counts == expect,
          " ".join(f"{k}={v}" for k, v in counts.items()))
    check("库内超阈值记录", alarms > 0, f"{alarms} 条")
except Exception as e:
    hint = ""
    text = str(e).lower()
    if any(k in text for k in ("connect", "refused", "timeout", "拒绝", "无法访问")):
        hint = "（数据库服务没起来？管理员运行：net start postgresql-x64-17）"
    check("数据库 turnout_twin 连接", False, str(e) + hint)

# ---- 汇总 ----
ng = results.count(False)
print("-" * 46)
print(f"自检完成：{len(results)} 项，通过 {len(results) - ng} 项"
      + ("" if ng == 0 else f"，未通过 {ng} 项"))
print("结论：" + ("B 侧管道健康，可以联调" if ng == 0 else "先解决上面 [NG] 项，再开始联调"))
sys.exit(0 if ng == 0 else 1)
