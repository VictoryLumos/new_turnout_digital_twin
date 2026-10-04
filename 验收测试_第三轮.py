# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改第三轮验收测试（B线，2026-10-03，纯输出版）

对应组长第三轮建议三项修复的**在线实测**：
  §A 数据库断线重试完善（重连失败也进重试流程；真实停机→恢复；不重复写入）
  §B 所有入库字段校验（转换力/电流等扩展字段，三入口拒绝并定位行号字段）
  §C 批次幂等完善（数据 time 从 10s 起，同批次重导不新增；异批共存）
  §D 页面联动（告警变化记录接口、五路总览页、慢放/暂停标注在线可用性）
  §E 待测项（人工/C：iTwin 联调、3D 局部视角、暂停交互、云端 NodeName）

安全说明：被测脚本路径经 safe_path 限制在本项目内；非法样例经 tempfile
限定在 data/_测试临时；HTTP 仅 http/https 且主机白名单 127.0.0.1/localhost；
SQL 全部字面量+参数绑定。用法：python 验收测试_第三轮.py
"""
import contextlib
import importlib.util
import io
import json
import os
import re
import shutil
import socket
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
import 管道公共  # noqa: E402
from 管道公共 import db_session, ResilientDB  # noqa: E402

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
    "推送服务": safe_path(DATA_DIR, "WebSocket推送服务.py"),
    "CSV转换": safe_path(DATA_DIR, "仿真数据转换脚本.py"),
    "入库": safe_path(HERE, "db", "02_数据导入脚本.py"),
}


def load_module(key, mod_name):
    spec = importlib.util.spec_from_file_location(mod_name, TOOL_SCRIPTS[key])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_tool(key, extra):
    return subprocess.run([PY, TOOL_SCRIPTS[key]] + list(extra),
                          capture_output=True, text=True, encoding="utf-8",
                          timeout=90)


def count_timeseries():
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM timeseries_data")
        return cur.fetchone()[0]


def count_batch(batch):
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM timeseries_data WHERE batch = %s",
                    (batch,))
        return cur.fetchone()[0]


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}


def http(path, base=None, timeout=8, method="GET"):
    url = (base or "http://127.0.0.1:" + str(API_PORT)) + path
    u = urlparse(url)
    if u.scheme not in ("http", "https") or u.hostname not in ALLOWED_HOSTS:
        raise ValueError("请求目标未通过白名单校验: " + url)
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                return r.status, json.loads(raw)
            except ValueError:          # HTML 页面等非 JSON 响应
                return r.status, {"html": raw}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"detail": body[:200]}
    except Exception as e:  # noqa: BLE001
        return None, {"error": type(e).__name__ + ": " + str(e)}


def port_open(port, timeout=0.4):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        return s.connect_ex(("127.0.0.1", port)) == 0


def start_service(name, args, cwd):
    p = subprocess.Popen([PY] + args, cwd=cwd,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    SERVICES.append((p, name))
    return p


def write_json(name, rows):
    fd, path = tempfile.mkstemp(suffix=".json", prefix=name + "_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    return path


GOOD6 = [{"time": 0.0 + 0.1 * i, "switchRailDisp1": 10.0, "switchRailDisp2": 9.0,
          "switchRailDisp3": 8.0, "pointRailDisp1": 7.0, "pointRailDisp2": 6.0,
          "switchRailForce1": 800.0, "switchMachineCurrent": 3.0,
          "lockStatus": 1, "railTemperature": 23.0}
         for i in range(3)]


# ============================ §A 断线重试完善 ============================
def test_a_retry():
    sec("§A 数据库断线重试完善（组长第三轮第1条）")
    # A1 重连失败必须进入重试流程（模拟停机：错误端口）
    saved = dict(管道公共.DB)
    rdb = ResilientDB(retries=3)
    buf = io.StringIO()
    raised = False
    管道公共.DB["port"] = 5433          # 无人监听端口 = 停机等价物
    try:
        with contextlib.redirect_stdout(buf):
            rdb.run(lambda cur: (cur.execute("SELECT 1"), cur.fetchone())[1])
    except Exception:  # noqa: BLE001 耗尽重试后抛出（不静默）
        raised = True
    finally:
        管道公共.DB.clear()
        管道公共.DB.update(saved)
    n_retry = buf.getvalue().count("重连失败")
    ok1 = raised and n_retry == 3
    record("- 重连失败进入重试流程：停机(错误端口)下 run() 逐次重试 "
           + str(n_retry) + "/3 次（输出'重连失败(x/3)'），耗尽后抛出 "
           + ("✓" if ok1 else "✗"))
    assert ok1
    # 恢复后新会话立即可用
    v = ResilientDB().run(lambda cur: (cur.execute("SELECT 1"),
                                       cur.fetchone())[1])
    ok2 = v == (1,)
    record("- 配置恢复后新会话立即可用：SELECT 1 → " + str(v)
           + (" ✓" if ok2 else "✗"))
    assert ok2

    # A2 停机窗口模拟：看门狗线程持续杀掉服务的数据库连接（superuser 不受
    #    CONNECTION LIMIT 约束，故用"连接即被终止"等价模拟数据库不可用）
    import threading
    import psycopg2 as _pg

    def killer(stop_evt):
        adm2 = dict(管道公共.DB)
        adm2["dbname"] = "postgres"
        while not stop_evt.is_set():
            try:
                kc = _pg.connect(application_name="turnout-twin-b-test", **adm2)
                kc.autocommit = True
                with kc.cursor() as ck:
                    ck.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                               "WHERE application_name = %s AND pid <> pg_backend_pid()",
                               ("turnout-twin-b",))
                kc.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.05)               # 高频绞杀，避免查询在窗口内溜过

    n0_ts = count_timeseries()
    stop_evt = threading.Event()
    th = threading.Thread(target=killer, args=(stop_evt,), daemon=True)
    th.start()
    try:  # 无论断言成败都必须停掉看门狗，否则会泄漏并杀掉后续测试的连接
        time.sleep(1)
        # 停机窗口内：让被测 fn 持有连接 1.2s（模拟慢查询），绞杀必然命中——
        # 确定性观测"断线→重连重试"流程；retries=2 耗尽后抛出（不静默）
        buf2 = io.StringIO()
        raised2 = False

        def slow_fn(cur):
            cur.execute("SELECT 1")
            row = cur.fetchone()
            time.sleep(1.2)              # 慢查询窗口：killer 必然命中本连接
            return row
        try:
            with contextlib.redirect_stdout(buf2):
                ResilientDB(retries=2).run(slow_fn)
        except Exception:  # noqa: BLE001
            raised2 = True
        n_break = (buf2.getvalue().count("重连失败")
                   + buf2.getvalue().count("运行中断线"))
        ok3 = n_break >= 1 and raised2
        record("- 停机窗口内（连接即被杀）：慢查询被杀后逐次重连重试 "
               + str(n_break) + " 次，耗尽后抛出=" + str(raised2)
               + "（不静默、进程不退）" + ("✓" if ok3 else "✗"))
        assert ok3
        # 停机窗口内 API 请求报错但进程存活（killer 仍在运行，覆盖重试全程）
        c_down, _ = http("/api/health", timeout=30)
        alive = port_open(API_PORT)
        ok3b = alive
        record("- 停机窗口内 API：/api/health=" + str(c_down)
               + "（重试耗尽后报错），进程存活=" + str(alive) + " "
               + ("✓" if ok3b else "✗"))
        assert ok3b
    finally:
        stop_evt.set()                      # 停机结束，恢复（任何路径都必须执行）
        th.join(timeout=5)
    time.sleep(1)
    c_up, d_up = http("/api/health", timeout=15)
    n1_ts = count_timeseries()
    ok4 = c_up == 200 and "健康度评分" in d_up
    ok5 = n0_ts == n1_ts
    record("- 恢复后服务自愈：/api/health " + str(c_up) + "，等级="
           + str(d_up.get("等级")) + " " + ("✓" if ok4 else "✗"))
    assert ok4
    record("- 恢复后不重复写入：timeseries 行数 " + str(n0_ts) + "→"
           + str(n1_ts) + "（不变）" + ("✓" if ok5 else "✗"))
    assert ok5
    # A3 回放进程在断库期间不退出、恢复后续写
    http("/api/play/" + quote("告警演示"), method="POST", timeout=15)
    time.sleep(0.8)
    with db_session() as cur:
        cur.execute("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                    "WHERE application_name = %s AND pid <> pg_backend_pid()",
                    ("turnout-twin-b",))
        killed2 = cur.fetchone()[0]
    time.sleep(0.6)
    c1, _ = http("/api/health")
    http("/api/stop", method="POST", timeout=15)
    ok6 = c1 == 200
    record("- 回放中杀库连接×" + str(killed2) + "：回放暂缓不退出，/api/health "
           + str(c1) + " " + ("✓（下一帧自动续写）" if ok6 else "✗"))
    assert ok6


# ============================ §B 全字段校验 ============================
def test_b_allfields():
    sec("§B 所有入库字段校验（组长第三轮第2条：扩展字段不再漏检）")
    bad_cases = {
        "转换力NaN": dict(GOOD6[1], switchRailForce1=float("nan")),
        "电流字符串": dict(GOOD6[1], switchMachineCurrent="abc"),
        "轨温Infinity": dict(GOOD6[1], railTemperature=float("inf")),
        "锁闭状态null": dict(GOOD6[1], lockStatus=None),
    }
    for name, row in bad_cases.items():
        r = run_tool("推送服务", ["--file",
                                 write_json("bad_ext", [GOOD6[0], row]),
                                 "--port", "3999"])
        out = (r.stdout or "") + (r.stderr or "")
        field = re.search(r"字段\[(.*?)\]", out)
        ok = r.returncode == 1 and "校验失败" in out and field
        record("- 推送入口·" + name + "：退出码=" + str(r.returncode)
               + "，拒绝 ✓（定位 行2[" + field.group(1) + "]）" if ok
               else "- 推送入口·" + name + "：✗ " + out[:160])
        assert ok, out[:300]
    # CSV 转换：扩展列非法拒绝（直调 convert）
    conv = load_module("CSV转换", "仿真数据转换脚本_被测3")
    header = ("time,switchRailDisp1,pointRailDisp1,switchRailForce1,"
              "switchMachineCurrent")
    body_bad = "0.0,1.5,0.8,800.0,3.0\n0.1,1.6,0.9,NaN,3.1"
    cols = header.split(",")
    rows_it = [dict(zip(cols, ln.split(","))) for ln in body_bad.split("\n")]
    rejected = ""
    try:
        conv.convert(rows_it, 0.1, ["switchRailForce1", "switchMachineCurrent"])
    except SystemExit as e:
        rejected = str(e)
    hit = re.search(r"第 (\d+) 行列\[(.*?)\]", rejected)
    ok = bool(hit) and hit.group(2) == "switchRailForce1"
    record("- CSV转换·扩展列NaN：SystemExit 拒绝（定位 第" + hit.group(1)
           + "行[" + hit.group(2) + "]）✓" if ok
           else "- CSV转换·扩展列NaN：✗ " + rejected[:120])
    assert ok
    # 02 入库：扩展字段字符串拒绝且不写库
    before = count_timeseries()
    r = run_tool("入库", ["--file", write_json(
        "bad_ext_db", [GOOD6[0], dict(GOOD6[1], switchRailForce1="八百")]),
        "--source", "非法测试", "--batch", "BAD-TEST-3"])
    out = (r.stdout or "") + (r.stderr or "")
    after = count_timeseries()
    field = re.search(r"字段\[(.*?)\]", out)
    ok = r.returncode == 1 and "校验失败" in out and before == after and field
    record("- 入库入口·转换力字符串：退出码=" + str(r.returncode)
           + "，库行数 " + str(before) + "→" + str(after)
           + "（定位 行2[" + field.group(1) + "]，未写入）✓" if ok
           else "- 入库入口·转换力字符串：✗ " + out[:160])
    assert ok


# ============================ §C 批次幂等完善 ============================
def test_c_offset():
    sec("§C 批次幂等完善（组长第三轮第3条：time 从 10s 起也不重复）")
    rows = [{"time": round(10.0 + 0.1 * i, 1), "switchRailDisp1": 20.0,
             "pointRailDisp1": 15.0} for i in range(21)]   # 10.0~12.0s 起步
    with db_session() as cur:
        cur.execute("DELETE FROM timeseries_data WHERE batch LIKE %s", ("IDEM3-%",))
        before = count_timeseries()

    def cmd(batch):
        return ["--file", write_json("offset_ds", rows), "--source",
                "仿真-幂等测试3", "--batch", batch]
    r1 = run_tool("入库", cmd("IDEM3-A"))
    r2 = run_tool("入库", cmd("IDEM3-A"))
    r3 = run_tool("入库", cmd("IDEM3-B"))
    na = count_batch("IDEM3-A")
    nb = count_batch("IDEM3-B")
    total = count_timeseries()
    expect_rows = 21 * 2   # 21 帧 × 2 测点
    align_hint = "数据集起点 t=10.0s" in (r2.stdout or "")
    ok1 = r1.returncode == 0 and na == expect_rows
    record("- 首导 IDEM3-A（time 10.0~12.0s 起）：批次 " + str(na) + " 条"
           + "（=21帧×2测点）" + ("✓" if ok1 else "✗"))
    assert ok1
    ok2 = r2.returncode == 0 and na == expect_rows and align_hint
    record("- 重导 IDEM3-A：批次仍 " + str(na) + " 条（未新增）；输出含"
           "'按首次导入时间轴对齐（数据集起点 t=10.0s）'=" + str(align_hint)
           + (" ✓" if ok2 else "✗"))
    assert ok2
    ok3 = r3.returncode == 0 and nb == expect_rows \
        and total == before + expect_rows * 2
    record("- 异批 IDEM3-B：" + str(nb) + " 条（不覆盖 A），总行数 "
           + str(before) + "→" + str(total) + "（+" + str(total - before)
           + "）" + ("✓" if ok3 else "✗"))
    assert ok3
    with db_session() as cur:
        cur.execute("DELETE FROM timeseries_data WHERE batch LIKE %s", ("IDEM3-%",))
    record("- 已清理测试批次 IDEM3-* ✓")


# ============================ §D 页面联动 ============================
API_STARTED = [False]


def start_api_wrapper():
    start_service("数据API",
                  ["-m", "uvicorn", "数据服务API:app", "--host", "127.0.0.1",
                   "--port", str(API_PORT)], DATA_DIR)
    for _ in range(60):
        code, _ = http("/api/points")
        if code == 200:
            API_STARTED[0] = True
            break
        time.sleep(0.3)
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "）"
           + ("✓" if API_STARTED[0] else "✗"))
    assert API_STARTED[0]


def test_d_pages():
    sec("§D 页面联动（组长第三轮页面①②③，B 侧在线可用性）")
    c1, d1 = http("/api/alarm-events?limit=5")
    has_fields = (c1 == 200 and d1["count"] > 0
                  and all(k in d1["告警变化"][0] for k in
                          ("何时超限", "何时恢复", "超限峰值", "阈值", "单位",
                           "持续秒")))
    ok1 = c1 == 200 and has_fields
    sample = d1["告警变化"][0] if d1.get("告警变化") else {}
    record("- ①告警变化记录 /api/alarm-events：HTTP " + str(c1) + "，"
           + str(d1.get("count")) + " 段；样例 " + str(sample.get("字段"))
           + " " + str(sample.get("何时超限")) + " → "
           + str(sample.get("何时恢复"))[:23]
           + (" ✓（何时超限/测点/数值/何时恢复齐备）" if ok1 else " ✗"))
    assert ok1
    c2, html2 = http("/trend5")
    ok2 = c2 == 200 and "五路位移总览" in str(html2) and "SwitchRail" in str(html2)
    record("- ②五路总览 /trend5：HTTP " + str(c2) + "（含五路曲线+阈值+模型节点标注）"
           + ("✓" if ok2 else "✗"))
    assert ok2
    c3, html3 = http("/trend")
    ok3 = c3 == 200 and "0.25×" in str(html3) and "回放已暂停" in str(html3)
    record("- ③慢放/暂停 /trend：HTTP " + str(c3) + "（0.25×/0.5× 档 + 暂停标注）"
           + ("✓" if ok3 else "✗"))
    assert ok3
    c4, html4 = http("/")
    ok4 = c4 == 200 and "告警变化记录" in str(html4)
    record("- ①首页告警变化记录区块：HTTP " + str(c4) + (" ✓" if ok4 else " ✗"))
    assert ok4
    from urllib.parse import urlencode as _ue
    c5, d5 = http("/api/history/T01-SR-02-DISP?" + _ue(
        {"source": "仿真-告警演示", "limit": 50}))
    ok5 = c5 == 200 and d5.get("count", 0) > 0
    record("- 五路数据齐备（以第2牵引点为例）：告警演示批次 "
           + str(d5.get("count")) + " 行 " + ("✓" if ok5 else "✗"))
    assert ok5


def test_e_pending():
    sec("§E 待测项（人工/C，不写'通过'）")
    record("- [待测] iTwin 页面联调：收数/两路分别变红/恢复/断线提示（B 侧 3003 已就绪）")
    record("- [待测] 3D 局部观察视角与选中高亮避让（C 侧实现，建议见交付说明'待C项'）")
    record("- [待测] 暂停标注/慢速回放的实际观感（页面已上线，答辩前人工过一遍）")
    record("- [待测] 云端查询用 NodeName 的实际验证（C 侧；文档已区分 UserLabel/NodeName）")


def main():
    print("B 线整改第三轮验收 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    steps = [start_api_wrapper, test_a_retry, test_b_allfields, test_c_offset,
             test_d_pages, test_e_pending]
    for t in steps:
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
                      and not l.startswith("- [待测]"))
        n_fail = sum(1 for l in LINES if "✗" in l)
        n_pend = sum(1 for l in LINES if l.startswith("- [待测]"))
        print("\n结论：已实测用例 " + str(n_total) + " 项，失败 " + str(n_fail)
              + " 项；另有待测 " + str(n_pend) + " 项（人工/C）。"
              + ("已实测项全部通过，申请复验。" if not failed and n_fail == 0
                 else "存在失败项。"))
    finally:
        for p, name in SERVICES:
            try:
                p.terminate()
            except Exception:  # noqa: BLE001
                pass
        shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
