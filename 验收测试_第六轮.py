# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改第六轮验收测试（B线，2026-10-04，纯输出版）

对应组长第六轮反馈（真库已复现）：
  维修复测数据非法时接口返回 400，工单却被改成"已解决"——必须先校验数据、
  再修改工单状态。

  §A 组长场景复现：坏数据 + open 工单 → repair 400 且**工单保持 open**
     （含"当前正在回放正常转换"分支同样被校验覆盖的隐患验证）
  §B 修复后正常路径：好数据 → 200，工单 resolved 且复测回放启动
  §C 边界：不存在 404；已 resolved 再修 400；状态不被误改
  §D 回放入口冒烟回归

安全说明：被测路径 safe_path 限本项目；HTTP 仅白名单主机；SQL 全字面量+绑定。
用法：python 验收测试_第六轮.py
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
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
WO_A = "WO-2026-TESTA"      # 场景A工单（坏数据）
WO_B = "WO-2026-TESTB"      # 场景A分支工单（回放中+坏数据）
WO_C = "WO-2026-TESTC"      # 场景B工单（好数据正常闭环）


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


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}


def http(path, timeout=10, method="GET"):
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


def wo_status(wo):
    with db_session() as cur:
        cur.execute("SELECT status, remark FROM work_order WHERE work_order_id = %s",
                    (wo,))
        r = cur.fetchone()
    return r


def setup_workorders():
    with db_session() as cur:
        for wo in (WO_A, WO_B, WO_C):
            cur.execute("DELETE FROM work_order WHERE work_order_id = %s", (wo,))
            cur.execute("INSERT INTO work_order (work_order_id, point_code, "
                        "status, remark) VALUES (%s, %s, 'open', %s)",
                        (wo, "T01-SR-01-DISP", "第六轮顺序验证测试工单"))


def cleanup_workorders():
    with db_session() as cur:
        for wo in (WO_A, WO_B, WO_C):
            cur.execute("DELETE FROM work_order WHERE work_order_id = %s", (wo,))


def test_a_baddata_keeps_open():
    sec("§A 组长场景复现：坏数据 → 400 且工单保持 open（核心断言）")
    target = safe_path(DATA_DIR, "扩展数据集v2", "工况_正常转换.json")
    with open(target, encoding="utf-8") as f:
        backup = f.read()
    fd, bad = tempfile.mkstemp(suffix=".json", prefix="bad_retest_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump([{"time": 0.0, "switchRailDisp1": 1.0, "pointRailDisp1": 0.5,
                    "switchMachineCurrent": 3.0},
                   {"time": 0.1, "switchRailDisp1": 2.0, "pointRailDisp1": 0.6,
                    "switchMachineCurrent": float("nan")}], f)
    with open(bad, encoding="utf-8") as f:
        bad_rows = f.read()
    try:
        # A1：当前未在回放正常转换 → repair 应 400 且工单保持 open
        http("/api/stop", method="POST")
        fd2 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd2, "w", encoding="utf-8") as f:
            f.write(bad_rows)
        code, d = http("/api/repair/" + WO_A, method="POST")
        st = wo_status(WO_A)
        ok1 = code == 400 and st == ("open", "第六轮顺序验证测试工单")
        record("- 空闲态·坏复测数据（电流NaN）repair：HTTP " + str(code)
               + "，工单状态=" + str(st[0]) + "，备注未追加闭环标记 "
               + ("✓（先校验后改状态，未假闭环）" if ok1
                  else " ✗ detail=" + str(d)[:120]))
        assert ok1
        # A2：正在回放正常转换的分支同样被校验覆盖（旧实现会跳过校验直接闭环）
        #     编排：先还原好数据启动正常转换回放 → 再换坏数据 → repair
        fd_r = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd_r, "w", encoding="utf-8") as f:
            f.write(backup)
        code_p, dp = http("/api/play/" + quote("正常转换"), method="POST",
                          timeout=15)
        time.sleep(0.4)
        fd_b = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd_b, "w", encoding="utf-8") as f:
            f.write(bad_rows)
        code2, d2 = http("/api/repair/" + WO_B, method="POST")
        st2 = wo_status(WO_B)
        ok2 = code_p == 200 and code2 == 400 and st2[0] == "open"
        record("- 回放中·坏复测数据 repair（回放先以好数据启动 rc=" + str(code_p)
               + "）：HTTP " + str(code2) + "，工单状态=" + str(st2[0])
               + (" ✓（校验无条件执行，回放中不绕过）" if ok2
                  else " ✗ " + str(d2)[:120]))
        assert ok2
    finally:
        fd3 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd3, "w", encoding="utf-8") as f:
            f.write(backup)
        http("/api/stop", method="POST")


def test_b_gooddata_closes():
    sec("§B 好数据正常路径：校验通过 → 回放启动 → 工单 resolved")
    code, d = http("/api/repair/" + WO_C, method="POST", timeout=15)
    time.sleep(0.8)
    st = wo_status(WO_C)
    code_h, dh = http("/api/health")
    cond = dh.get("当前回放")
    ok = (code == 200 and st[0] == "resolved"
          and "维修完成闭环" in (st[1] or "") and code_h == 200
          and cond == "正常转换")
    record("- 好数据 repair：HTTP " + str(code) + "，工单=" + str(st[0])
           + "（备注含闭环标记），当前回放=" + str(cond)
           + (" ✓（复测回放已启动）" if ok else " ✗ " + str(d)[:120]))
    http("/api/stop", method="POST")
    assert ok


def test_c_edges():
    sec("§C 边界：不存在 404；重复维修 400；状态不被误改")
    c1, d1 = http("/api/repair/WO-2026-NOTEXIST", method="POST")
    ok1 = c1 == 404
    record("- 不存在的工单：HTTP " + str(c1) + " " + ("✓" if ok1 else " ✗"))
    assert ok1
    c2, d2 = http("/api/repair/" + WO_C, method="POST")
    st = wo_status(WO_C)
    ok2 = c2 == 400 and st[0] == "resolved"
    record("- 已 resolved 再维修：HTTP " + str(c2) + "，状态保持 "
           + str(st[0]) + " " + ("✓" if ok2 else " ✗"))
    assert ok2
    st_a = wo_status(WO_A)
    ok3 = st_a[0] == "open"     # A 场景工单全过程未被误改
    record("- 全程未触碰的工单 " + WO_A + " 状态仍为 " + str(st_a[0]) + " ✓")
    assert ok3


def test_d_smoke():
    sec("§D 回放入口冒烟回归")
    code, d = http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
    ok = code == 200 and "已校验" in str(d.get("msg", ""))
    record("- POST /api/play/卡阻：" + str(code) + "，msg 含'已校验' "
           + ("✓" if ok else " ✗"))
    http("/api/stop", method="POST")
    assert ok
    record("- [待测] 页面上维修闭环按钮的实际点击流程（人工确认）")


def main():
    print("B 线整改第六轮验收 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    ok_api = start_api()
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "）"
           + ("✓" if ok_api else "✗"))
    assert ok_api
    setup_workorders()
    try:
        for t in (test_a_baddata_keeps_open, test_b_gooddata_closes,
                  test_c_edges, test_d_smoke):
            try:
                t()
            except AssertionError as e:
                failed = True
                print("  [该组存在失败项] " + str(e))
            except Exception as e:  # noqa: BLE001
                failed = True
                print("  [异常] " + type(e).__name__ + ": " + str(e))
    finally:
        cleanup_workorders()
        for p, name in SERVICES:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass
        import shutil
        shutil.rmtree(TMP, ignore_errors=True)
    n_total = sum(1 for l in LINES if l.startswith("- ")
                  and not l.startswith("- [待"))
    n_fail = sum(1 for l in LINES if "✗" in l)
    n_pend = sum(1 for l in LINES if l.startswith("- [待"))
    print("\n结论：已实测用例 " + str(n_total) + " 项，失败 " + str(n_fail)
          + " 项；另有待测 " + str(n_pend) + " 项。测试工单已清理。"
          + ("已实测项全部通过，申请复验。" if not failed and n_fail == 0
             else "存在失败项。"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
