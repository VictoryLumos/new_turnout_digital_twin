# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— 联调自检脚本（B：数据管道线，v2 覆盖完整版全线）

作用：
    每周联调前跑一遍，10 秒确认 B 侧整条管道是否健康：
      1) data.json 契约校验（字段 / 条数 / 步长 / 超限演示段）
      2) 扩展数据集v2（四工况文件齐全、行数 / 字段数 / 步长）
      3) 依赖检查（websockets / fastapi / uvicorn / psycopg2）
      4) WebSocket 8765 与服务API 8000 端口状态（信息性，不判失败）
      5) 数据库 turnout_twin 连通 + 五张表行数（时序≥8000 含四工况）
         + 超阈值记录 + 四工况来源齐全 + 告警→工单联动证据
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
DS_DIR = os.path.join(HERE, "扩展数据集v2")
CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败"]

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("[OK] " if ok else "[NG] ") + name + (f"：{detail}" if detail else ""))


print("=" * 46)
print("道岔数字孪生 · B 侧管道自检（v2）")
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

# ---- 2) 扩展数据集v2（四工况，服务API/历史回放的数据源） ----
try:
    ds_ok, ds_detail = True, []
    for cond in CONDITIONS:
        path = os.path.join(DS_DIR, f"工况_{cond}.json")
        if not os.path.exists(path):
            ds_ok = False
            ds_detail.append(f"{cond}缺失")
            continue
        crows = json.load(open(path, encoding="utf-8"))
        csteps = {round(crows[i + 1]["time"] - crows[i]["time"], 6)
                  for i in range(len(crows) - 1)}
        if not (len(crows) == 100 and len(crows[0]) == 21 and csteps == {0.1}):
            ds_ok = False
            ds_detail.append(f"{cond}异常({len(crows)}行/{len(crows[0])}字段)")
    check("扩展数据集v2（四工况×100行×time+20测点字段×0.1s）", ds_ok,
          " ".join(ds_detail) if ds_detail else "四文件齐全，服务API/趋势页数据源就绪")
except Exception as e:
    check("扩展数据集v2 读取", False, str(e))

# ---- 3) 依赖 ----
check("WebSocket推送服务.py 存在", os.path.exists(WS_SCRIPT))
for mod, hint in (("websockets", "pip install websockets"),
                  ("fastapi", "pip install fastapi uvicorn"),
                  ("uvicorn", "pip install fastapi uvicorn"),
                  ("psycopg2", "pip install psycopg2-binary")):
    try:
        m = __import__(mod)
        check(f"{mod} 库已安装", True, getattr(m, "__version__", ""))
    except ImportError:
        check(f"{mod} 库已安装", False, f"先执行 {hint}")


def _port_listening(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


check("WebSocket 端口 8765 状态", True,
      "服务已在运行，C 可直连 ws://localhost:8765" if _port_listening(8765)
      else "未运行（正常；与 C 联调时启动 WebSocket推送服务.py 即可）")
check("服务API 端口 8000 状态", True,
      "数据服务已在运行，状态板/识别/趋势页可用" if _port_listening(8000)
      else "未运行（正常；演示时双击 服务API演示.bat 或 uvicorn 启动）")

# ---- 4) 数据库 ----
try:
    import psycopg2
    conn = psycopg2.connect(host=os.environ.get("PGHOST", "localhost"),
                            port=int(os.environ.get("PGPORT", "5432")),
                            user=os.environ.get("PGUSER", "postgres"),
                            password=os.environ.get("PGPASSWORD", "postgres"),
                            dbname=os.environ.get("PGDATABASE", "turnout_twin"),
                            connect_timeout=5)
    cur = conn.cursor()
    counts = {}
    for t in ("component", "measurement_point", "timeseries_data",
              "work_order", "realtime_value"):
        cur.execute(f"SELECT count(*) FROM {t}")
        counts[t] = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM timeseries_data d "
                "JOIN measurement_point p USING(point_code) "
                "WHERE d.value > p.alarm_threshold")
    alarms = cur.fetchone()[0]
    cur.execute("SELECT count(DISTINCT source) FROM timeseries_data "
                "WHERE source LIKE '仿真-%'")
    srcs = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM work_order "
                "WHERE remark LIKE '自动生成%'")
    auto_wos = cur.fetchone()[0]
    # 数据库 v2 对象（05_数据库升级v2.sql；旧库缺表时记 0，不中断自检）
    nviews = ntrg = nevts = 0
    try:
        cur.execute("SELECT count(*) FROM pg_views WHERE schemaname='public' "
                    "AND viewname LIKE 'v_%'")
        nviews = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM pg_trigger WHERE tgname='trg_timeseries_alarm'")
        ntrg = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM alarm_event")
        nevts = cur.fetchone()[0]
    except Exception:
        nviews = ntrg = nevts = 0
    conn.close()
    check("数据库 turnout_twin 连接", True)
    # 时序≥30000=四工况8000+一天运维历史26000；工单≥20=种子+联动工单；实时值=20测点
    ok = (counts["component"] == 9 and counts["measurement_point"] == 20
          and counts["timeseries_data"] >= 30000 and counts["work_order"] >= 20
          and counts["realtime_value"] == 20)
    check("五张表行数（9/20/≥30000/≥20/20）", ok,
          " ".join(f"{k}={v}" for k, v in counts.items()))
    check("库内超阈值记录", alarms > 0, f"{alarms} 条")
    check("四工况来源齐全（仿真-×××× ×4）", srcs == 4, f"{srcs} 种")
    check("告警→工单联动证据", auto_wos > 0,
          f"{auto_wos} 张自动生成工单（服务API回放产生）" if auto_wos
          else "尚无（先跑一次 服务API演示 的卡阻回放）")
    # ---- 数据库 v2 升级对象（05_数据库升级v2.sql）----
    check("数据库v2对象（4视图+告警触发器+审计表）", nviews >= 4 and ntrg >= 1,
          f"视图{nviews}个，触发器{'在' if ntrg else '缺'}，告警事件留痕 {nevts} 条"
          if nviews >= 4 and ntrg >= 1
          else "未升级——执行 db/05_数据库升级v2.sql（幂等可重复），再跑 06_数据库体检.py")
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
print("结论：" + ("B 侧管道健康（基础版 + 完整版全线），可以联调" if ng == 0
                  else "先解决上面 [NG] 项，再开始联调"))
sys.exit(0 if ng == 0 else 1)
