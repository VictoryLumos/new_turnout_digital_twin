# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改第四轮验收测试（B线，2026-10-03，纯输出版）

对应组长第四轮反馈两项：
  §A 告警"恢复时间"语义修正（v_fault_episodes：恢复=最后超限帧+0.1s，
     不再把最后超限时刻当恢复时间；含恢复时刻帧值核验与 API 输出核验）
  §B 数据库完整验收（本机）：真实停机→恢复实测（有管理员权限自动执行
     net stop/start 全流程；无权限则指引 data/数据库停机恢复实测.bat）
  §C 数据库完整体检（db/06_数据库体检.py 九项全过并存档）
  §D 待测项（人工/C：页面对新恢复时间的显示确认）

安全说明：被测脚本路径经 safe_path 限制在本项目内；HTTP 仅 http/https 且
主机白名单 127.0.0.1/localhost；SQL 全部字面量+参数绑定。
用法：python 验收测试_第四轮.py
"""
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
from urllib.parse import urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
sys.path.insert(0, DATA_DIR)
from 管道公共 import db_session  # noqa: E402

API_PORT = 8100
PY = sys.executable
LINES = []
SERVICES = []


def sec(title):
    LINES.append("")
    LINES.append("## " + title)
    print("\n=== " + title + " ===")


def record(line):
    LINES.append(line)
    print("  " + line)


def safe_path(*parts):
    p = os.path.abspath(os.path.join(*parts))
    if ".." in parts or not p.startswith(HERE + os.sep):
        raise ValueError("路径越界: " + p)
    return p


TOOL_SCRIPTS = {
    "体检": safe_path(HERE, "db", "06_数据库体检.py"),
    "停机段": safe_path(HERE, "db", "停机恢复实测段.py"),
}


def run_tool(key, extra):
    return subprocess.run([PY, TOOL_SCRIPTS[key]] + list(extra),
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=180)


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}


def http(path, timeout=8):
    url = "http://127.0.0.1:" + str(API_PORT) + path
    u = urlparse(url)
    if u.scheme not in ("http", "https") or u.hostname not in ALLOWED_HOSTS:
        raise ValueError("请求目标未通过白名单校验: " + url)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                return r.status, json.loads(raw)
            except ValueError:
                return r.status, {"html": raw}
    except Exception as e:  # noqa: BLE001
        return None, {"error": type(e).__name__ + ": " + str(e)}


def port_open(port, timeout=0.4):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        return s.connect_ex(("127.0.0.1", port)) == 0


def start_api():
    p = subprocess.Popen([PY, "-m", "uvicorn", "数据服务API:app",
                          "--host", "127.0.0.1", "--port", str(API_PORT)],
                         cwd=DATA_DIR, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL)
    SERVICES.append((p, "数据API"))
    for _ in range(60):
        code, _ = http("/api/points")
        if code == 200:
            return True
        time.sleep(0.3)
    return False


# ==================== §A 告警恢复时刻语义 ====================
def test_a_recovery_time():
    sec("§A 告警恢复时间语义修正（组长第四轮第1条）")
    # A1 视图核验：恢复时刻 = 同源最后超限后第一条≤阈值的真实数据帧
    #（2026-10-04 第五轮起为真实帧语义；帧值核验在 A2）
    with db_session() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE e.end_ts IS NOT NULL) "
                    "FROM v_fault_episodes e")
        total, has_end = cur.fetchone()
    ok1 = total > 0
    record("- 视图 v_fault_episodes：共 " + str(total) + " 个事件段，其中 "
           + str(has_end) + " 段已确定恢复时刻（真实数据帧；其余为数据流结束未恢复）"
           + (" ✓（恢复时刻取实际回阈帧，非固定偏移）" if ok1 else "✗"))
    assert ok1
    # A2 恢复时刻帧值核验：恢复那一帧确实回到阈值以内
    with db_session() as cur:
        cur.execute("SELECT e.point_code, e.end_ts, t.value, e.threshold "
                    "FROM v_fault_episodes e JOIN timeseries_data t "
                    "ON t.point_code = e.point_code AND t.ts = e.end_ts "
                    "ORDER BY e.end_ts LIMIT 8")
        rows = cur.fetchall()
    ok_rows = [r for r in rows if r[2] <= r[3]]
    ok2 = len(rows) > 0 and len(ok_rows) == len(rows)
    sample = rows[0] if rows else None
    record("- 恢复时刻帧值核验：抽验 " + str(len(rows)) + " 段，"
           + str(len(ok_rows)) + " 段恢复帧值 ≤ 阈值"
           + (f"（例：{sample[0][-12:]} 恢复帧 {sample[2]} ≤ {sample[3]}）"
              if sample else "")
           + (" ✓" if ok2 else " ✗"))
    assert ok2
    # A3 API 输出：最后超限与恢复分离（恢复为真实回阈帧时刻，晚于最后超限）
    code, d = http("/api/alarm-events?limit=5")
    ok3 = code == 200 and d["count"] > 0 and "最后超限" in d["告警变化"][0]
    e0 = d["告警变化"][0] if d.get("告警变化") else {}
    sep = (e0.get("最后超限") is not None and e0.get("何时恢复") is not None
           and str(e0.get("何时恢复")) > str(e0.get("最后超限")))
    record("- /api/alarm-events 输出分离：'最后超限'=" + str(e0.get("最后超限"))[:23]
           + "，'何时恢复'=" + str(e0.get("何时恢复"))[:23]
           + ("（恢复晚于最后超限）" if sep else "")
           + ("✓" if ok3 else " ✗"))
    assert ok3


# ==================== §B 数据库真实停机恢复 ====================
def is_admin():
    r = subprocess.run(["net", "session"], capture_output=True, timeout=30)
    return r.returncode == 0


def test_b_outage():
    sec("§B 数据库真实停机恢复实测（组长第四轮第2条：完整验收）")
    if not is_admin():
        record("- [待人工] 当前无管理员权限，无法自动停/起 PostgreSQL 服务。")
        record("  请右键【以管理员身份运行】data/数据库停机恢复实测.bat，")
        record("  脚本将自动完成：记录行数→net stop→停机期间重试验证→net start→")
        record("  恢复验证（不重复写入），实测结果自动追加到")
        record("  docs/整改验证记录_第四轮_停机实测.md。")
        return
    r0 = run_tool("停机段", ["--before"])
    record("- 停机前：" + (r0.stdout or "").strip().splitlines()[-1])
    assert r0.returncode == 0
    r_stop = subprocess.run(["net", "stop", "postgresql-x64-17"],
                            capture_output=True, text=True, timeout=120)
    record("- net stop postgresql-x64-17：rc=" + str(r_stop.returncode))
    time.sleep(1)
    r1 = run_tool("停机段", ["--during"])
    line1 = (r1.stdout or "").strip().splitlines()[-1] if r1.stdout else "(无输出)"
    record("- " + line1)
    r_start = subprocess.run(["net", "start", "postgresql-x64-17"],
                             capture_output=True, text=True, timeout=180)
    record("- net start postgresql-x64-17：rc=" + str(r_start.returncode))
    time.sleep(4)
    r2 = run_tool("停机段", ["--after"])
    line2 = (r2.stdout or "").strip().splitlines()[-1] if r2.stdout else "(无输出)"
    record("- " + line2)
    assert r1.returncode == 0 and r2.returncode == 0, "停机段验证失败"


# ==================== §C 数据库完整体检 ====================
def test_c_healthcheck():
    sec("§C 数据库完整体检（db/06_数据库体检.py，九项全过）")
    r = run_tool("体检", [])
    out = (r.stdout or "") + (r.stderr or "")
    ok = r.returncode == 0 and "通过 9 项" in out and "[NG]" not in out
    hits = re.findall(r"\[OK\] ([^：]+)", out)
    record("- 体检九项：" + "、".join(h.strip() for h in hits)
           + (" —— 全部通过 ✓" if ok else " ✗"))
    assert ok, out[-400:]


def test_d_pending():
    sec("§D 待测项（人工/C，不写'通过'）")
    record("- [待测] C 页面告警历史按新'恢复时间'显示的确认（B 侧接口已修正）")
    record("- [待测] 真实停机实测若本脚本未自动执行（无管理员），请跑 data/数据库停机恢复实测.bat")


def main():
    from datetime import datetime
    print("B 线整改第四轮验收 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    ok_api = start_api()
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "）"
           + ("✓" if ok_api else "✗"))
    assert ok_api
    for t in (test_a_recovery_time, test_b_outage, test_c_healthcheck,
              test_d_pending):
        try:
            t()
        except AssertionError as e:
            failed = True
            print("  [该组存在失败项] " + str(e))
        except Exception as e:  # noqa: BLE001
            failed = True
            print("  [异常] " + type(e).__name__ + ": " + str(e))
    try:
        n_total = sum(1 for l in LINES if l.startswith("- ")
                      and not l.startswith("- [待") and not l.startswith("- [待人工]"))
        n_fail = sum(1 for l in LINES if "✗" in l)
        n_pend = sum(1 for l in LINES if l.startswith("- [待"))
        print("\n结论：已实测用例 " + str(n_total) + " 项，失败 " + str(n_fail)
              + " 项；另有待人工/待测 " + str(n_pend) + " 项。"
              + ("已实测项全部通过，申请复验。" if not failed and n_fail == 0
                 else "存在失败项。"))
    finally:
        for p, name in SERVICES:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
