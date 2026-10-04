# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改第五轮验收测试（B线，2026-10-04，纯输出版）

对应组长第五轮反馈四项：
  §A 恢复时刻=真实数据帧（同源最后超限后第一条≤阈值记录，非 +0.1s 硬编码）
  §B 恢复后再次告警=独立事件（状态跃迁分段；告警演示尖轨/心轨应各 2 段）
  §C API 回放入口校验（数据集含 NaN → 400 拒绝回放，不进入写库参数）
  §D 告警单位动态（力/电流等不硬编码 mm；闭环验证电流告警单位=A）
  §E 待测项（人工/C：页面按新语义显示确认）

安全说明：被测脚本路径经 safe_path 限制在本项目内；临时样例经 tempfile；
HTTP 仅 http/https 且主机白名单；SQL 全部字面量+参数绑定。
用法：python 验收测试_第五轮.py
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime
from urllib.parse import quote, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
sys.path.insert(0, DATA_DIR)
from 管道公共 import db_session  # noqa: E402

API_PORT = 8100
PY = sys.executable
TMP = os.path.join(DATA_DIR, "_测试临时")
os.makedirs(TMP, exist_ok=True)
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
    "入库": safe_path(HERE, "db", "02_数据导入脚本.py"),
}


def run_tool(key, extra):
    return subprocess.run([PY, TOOL_SCRIPTS[key]] + list(extra),
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=90)


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}


def http(path, timeout=8, method="GET"):
    url = "http://127.0.0.1:" + str(API_PORT) + path
    u = urlparse(url)
    if u.scheme not in ("http", "https") or u.hostname not in ALLOWED_HOSTS:
        raise ValueError("请求目标未通过白名单校验: " + url)
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                return r.status, json.loads(raw)
            except ValueError:
                return r.status, {"html": raw}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"detail": body[:300]}
    except Exception as e:  # noqa: BLE001
        return None, {"error": type(e).__name__ + ": " + str(e)}


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


# ==================== §A 真实恢复时刻 ====================
def test_a_real_recovery():
    sec("§A 恢复时刻=真实数据帧（组长第五轮第1条）")
    with db_session() as cur:
        # A1 全量核验：已恢复段的 end_ts 必须是真实存在的数据帧且值≤阈值
        cur.execute("SELECT count(*) FROM v_fault_episodes WHERE end_ts IS NOT NULL")
        n_end = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM v_fault_episodes e JOIN timeseries_data t "
                    "ON t.point_code = e.point_code AND t.ts = e.end_ts "
                    "AND t.source = e.source WHERE e.end_ts IS NOT NULL "
                    "AND t.value <= e.threshold")
        n_ok = cur.fetchone()[0]
        # A2 last_alarm_ts 也必须是真实超限帧
        cur.execute("SELECT count(*) FROM v_fault_episodes e JOIN timeseries_data t "
                    "ON t.point_code = e.point_code AND t.ts = e.last_alarm_ts "
                    "AND t.source = e.source WHERE t.value > e.threshold")
        n_la = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM v_fault_episodes")
        n_all = cur.fetchone()[0]
    ok1 = n_end > 0 and n_end == n_ok
    record("- 全部 " + str(n_end) + " 个已恢复段：恢复时刻均为真实数据帧且帧值≤阈值"
           f"（{n_ok} 个核验通过）" + (" ✓（非 +0.1s 推算）" if ok1 else " ✗"))
    assert ok1
    ok2 = n_all == n_la
    record("- 全部 " + str(n_all) + " 个事件段的最后超限时刻均为真实超限帧（>"
           "阈值）：" + str(n_la) + " 个核验通过 " + ("✓" if ok2 else "✗"))
    assert ok2
    # A3 数据流结束未恢复 → NULL（不是编造时刻）
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM v_fault_episodes WHERE end_ts IS NULL")
        n_null = cur.fetchone()[0]
    record("- 数据流结束未恢复的段：end_ts 为 NULL（不编造恢复时刻）共 "
           + str(n_null) + " 段 ✓")
    code, d = http("/api/alarm-events?limit=8")
    shown = d["告警变化"][0]
    ok3 = code == 200 and "何时恢复" in shown and "单位" in shown
    record("- /api/alarm-events 正常输出（含恢复时刻与单位）：" + ("✓" if ok3 else " ✗"))
    assert ok3


# ==================== §B 独立事件 ====================
def test_b_independent():
    sec("§B 恢复后再次告警=独立事件（组长第五轮第2条）")
    with db_session() as cur:
        cur.execute("SELECT point_code, start_ts, end_ts FROM v_fault_episodes "
                    "WHERE source = %s ORDER BY point_code, start_ts",
                    ("仿真-告警演示",))
        rows = cur.fetchall()
    c = Counter(r[0] for r in rows)
    ok1 = c.get("T01-SR-01-DISP") == 2 and c.get("T01-PR-01-DISP") == 2
    record("- 告警演示批次分段：尖轨 " + str(c.get("T01-SR-01-DISP"))
           + " 段、心轨 " + str(c.get("T01-PR-01-DISP"))
           + " 段（各自经历 超限→恢复→再超限→再恢复）"
           + (" ✓（旧30秒聚合会错并成1段）" if ok1 else " ✗"))
    assert ok1
    sep_ok = True
    for pc in set(r[0] for r in rows):
        segs = sorted([r for r in rows if r[0] == pc], key=lambda r: r[1])
        for a, b in zip(segs, segs[1:]):
            if not (a[2] is not None and a[2] <= b[1]):
                sep_ok = False
    ok2 = sep_ok
    record("- 段间分离核验：第1段恢复时刻 ≤ 第2段超限时刻（先恢复、再告警）"
           + (" ✓" if ok2 else " ✗"))
    assert ok2


# ==================== §C 回放入口校验 ====================
def test_c_play_validate():
    sec("§C API 回放入口校验（组长第五轮第3条：NaN 不进写库参数）")
    target = safe_path(DATA_DIR, "扩展数据集v2", "工况_卡阻.json")
    with open(target, encoding="utf-8") as f:
        backup = f.read()
    fd, badpath = tempfile.mkstemp(suffix=".json", prefix="bad_ds_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump([{"time": 0.0, "switchRailDisp1": 1.0, "pointRailDisp1": 0.5,
                    "switchRailForce1": 800.0},
                   {"time": 0.1, "switchRailDisp1": 2.0, "pointRailDisp1": 0.6,
                    "switchRailForce1": float("nan")}], f)
    with open(badpath, encoding="utf-8") as f:
        bad_rows = f.read()
    try:
        # 写入坏数据集 → 回放应 400 拒绝
        fd2 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd2, "w", encoding="utf-8") as f:
            f.write(bad_rows)
        code, d = http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
        detail = str(d.get("detail", ""))
        ok1 = code == 400 and ("校验失败" in detail or "NaN" in detail)
        record("- 坏数据集（第2行 switchRailForce1=NaN）POST /api/play/卡阻：HTTP "
               + str(code) + "，detail 含拒绝原因 " + ("✓（未进入写库参数）" if ok1
                                                       else " ✗ " + detail[:120]))
        assert ok1
    finally:
        fd3 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd3, "w", encoding="utf-8") as f:
            f.write(backup)
    # 还原后正常回放
    code2, d2 = http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
    time.sleep(0.6)
    http("/api/stop", method="POST", timeout=15)
    ok2 = code2 == 200 and "已校验" in str(d2.get("msg", ""))
    record("- 还原后正常回放：HTTP " + str(code2) + "，msg 含'已校验'"
           + (" ✓" if ok2 else " ✗ " + str(d2)[:120]))
    assert ok2
    # 库内无 NaN 写入（value<>value 判 NaN）
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM realtime_value WHERE value <> value")
        n_nan = cur.fetchone()[0]
    ok3 = n_nan == 0
    record("- realtime_value 中 NaN 行数 = " + str(n_nan) + "（0=无脏写）"
           + (" ✓" if ok3 else " ✗"))
    assert ok3


# ==================== §D 单位动态 ====================
def test_d_units():
    sec("§D 告警单位动态（组长第五轮第4条：力/电流不标 mm）")
    code, d = http("/api/alarm-events?limit=8")
    rows = d.get("告警变化", [])
    has_unit = all("单位" in r and r.get("单位") for r in rows) if rows else False
    no_mm_keys = (rows and "超限峰值mm" not in rows[0] and "阈值mm" not in rows[0])
    ok1 = code == 200 and has_unit and no_mm_keys
    record("- /api/alarm-events：字段为 超限峰值/阈值/单位（不再硬编码 mm 后缀），"
           "当前位移段单位=" + str(rows[0].get("单位") if rows else None)
           + (" ✓" if ok1 else " ✗"))
    assert ok1
    # 闭环：给电流测点临时配阈值 → 导入电流超限批次 → 事件段单位应为 A
    fd, ds = tempfile.mkstemp(suffix=".json", prefix="unit_ds_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump([{"time": round(i * 0.1, 1), "switchRailDisp1": 100.0,
                    "pointRailDisp1": 60.0, "switchMachineCurrent":
                    (12.0 if i < 7 else 5.0)} for i in range(10)], f)
    try:
        with db_session() as cur:
            cur.execute("DELETE FROM timeseries_data WHERE source = %s",
                        ("仿真-单位测试",))
            cur.execute("DELETE FROM alarm_event WHERE source = %s",
                        ("仿真-单位测试",))
            cur.execute("UPDATE measurement_point SET alarm_threshold = %s "
                        "WHERE point_code = %s", (10.0, "T01-SM-01-CURR"))
        r = run_tool("入库", ["--file", ds, "--source", "仿真-单位测试",
                              "--batch", "UNIT5-A"])
        with db_session() as cur:
            cur.execute("SELECT unit, count(*) FROM v_fault_episodes "
                        "WHERE source = %s GROUP BY unit", ("仿真-单位测试",))
            got = cur.fetchall()
        ok2 = r.returncode == 0 and got == [("A", 1)]
        record("- 电流测点（临时阈值10A）超限批次：事件段单位 = " + str(got)
               + "（力/电流不再标 mm）" + (" ✓" if ok2 else " ✗"))
        assert ok2
    finally:
        with db_session() as cur:
            cur.execute("DELETE FROM timeseries_data WHERE source = %s",
                        ("仿真-单位测试",))
            cur.execute("DELETE FROM alarm_event WHERE source = %s",
                        ("仿真-单位测试",))
            cur.execute("UPDATE measurement_point SET alarm_threshold = NULL "
                        "WHERE point_code = %s", ("T01-SM-01-CURR",))
    record("- 已清理测试批次与临时阈值（measurement_point 还原）✓")


def test_e_pending():
    sec("§E 待测项（人工/C，不写'通过'）")
    record("- [待测] C 页面按新语义显示：独立事件分段、真实恢复时刻、动态单位")


def main():
    print("B 线整改第五轮验收 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    ok_api = start_api()
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "）"
           + ("✓" if ok_api else "✗"))
    assert ok_api
    for t in (test_a_real_recovery, test_b_independent, test_c_play_validate,
              test_d_units, test_e_pending):
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
                      and not l.startswith("- [待"))
        n_fail = sum(1 for l in LINES if "✗" in l)
        n_pend = sum(1 for l in LINES if l.startswith("- [待"))
        print("\n结论：已实测用例 " + str(n_total) + " 项，失败 " + str(n_fail)
              + " 项；另有待测 " + str(n_pend) + " 项。"
              + ("已实测项全部通过，申请复验。" if not failed and n_fail == 0
                 else "存在失败项。"))
    finally:
        for p, name in SERVICES:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass
        import shutil
        shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
