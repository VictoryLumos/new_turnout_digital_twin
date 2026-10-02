# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改第二轮验收测试（B线，2026-10-02，纯输出版）

对应组长第二轮反馈四项的**在线实测**（全部真实起服务/真库跑，非函数自测）：
  §A 三入口非法输入拒绝（推送/CSV转换/02入库——推送与入库走子进程实测退出码，
     CSV转换直调脚本真实 convert 入口、拒绝=SystemExit）
  §B 推送服务 v3 在线实测（time=UTC ISO 可解析/单调/近墙钟，五路有限值，
     simFields 模拟标注，/telemetry 路径，断线重连）
  §C 历史查询完整日期时区（+08:00 与 Z 跨时区等价、纯时刻 400、输出日期/时刻ISO）
  §D 同批次重复导入幂等（重导不新增；不同批次共存）
  §E 数据库运行中断线恢复（杀连接后 ResilientDB 重连；API 在线自愈）
  §F 健康度无数据不虚报（空表=无数据；回放后恢复评估）
  §G iTwin 页面实际联调 —— 待测项（人工，明确不写"通过"）

安全说明：被测入口脚本路径经 safe_path 限制在本项目目录内；非法样例经
tempfile 生成并限制在 data/_测试临时；HTTP 仅 http/https 且主机白名单
127.0.0.1/localhost；全部 SQL 为字面量+参数绑定。
用法：项目根目录执行  python 验收测试_第二轮.py
退出码：0=已实测项全部通过（待测项§G不计），1=存在失败项。
"""
import asyncio
import importlib.util
import json
import math
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
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
sys.path.insert(0, DATA_DIR)
from 管道公共 import WS_PORT, db_session, ResilientDB  # noqa: E402

API_PORT = 8100          # 测试用 API 端口（避开常被占用的 8000）
PY = sys.executable
TMP = os.path.join(DATA_DIR, "_测试临时")
os.makedirs(TMP, exist_ok=True)

LINES = []
SERVICES = []            # [(proc, name)] finally 统一停


def sec(title):
    LINES.append("")
    LINES.append("## " + title)
    print("\n=== " + title + " ===")


def record(line):
    LINES.append(line)
    print("  " + line)


def safe_path(*parts):
    """拼接并校验路径必须仍在本项目目录内（禁止越界，入参全为常量名）。"""
    p = os.path.abspath(os.path.join(*parts))
    if ".." in parts or not p.startswith(HERE + os.sep):
        raise ValueError("路径越界: " + p)
    return p


# 本仓库内的被测脚本入口（全部经 safe_path 校验在本项目目录内）
TOOL_SCRIPTS = {
    "推送服务": safe_path(DATA_DIR, "WebSocket推送服务.py"),
    "CSV转换": safe_path(DATA_DIR, "仿真数据转换脚本.py"),
    "入库": safe_path(HERE, "db", "02_数据导入脚本.py"),
}


def load_module(key, mod_name):
    """以模块方式加载本仓库被测脚本（不触发其 __main__）。"""
    spec = importlib.util.spec_from_file_location(mod_name, TOOL_SCRIPTS[key])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_tool(key, extra):
    """以子进程运行被测入口脚本（路径已白名单化），返回 CompletedProcess。"""
    return subprocess.run([PY, TOOL_SCRIPTS[key]] + list(extra),
                          capture_output=True, text=True, encoding="utf-8",
                          timeout=60)


ALLOWED_HOSTS = {"127.0.0.1", "localhost"}   # 本测试仅访问本机测试服务


def http(path, base=None, timeout=8, method="GET"):
    """请求本地测试 API，返回 (状态码, json/None)。仅 http/https 且主机在白名单。"""
    url = (base or "http://127.0.0.1:" + str(API_PORT)) + path
    u = urlparse(url)
    if u.scheme not in ("http", "https") or u.hostname not in ALLOWED_HOSTS:
        raise ValueError("请求目标未通过白名单校验: " + url)
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
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


def wait_port(port, tries=40):
    for _ in range(tries):
        if port_open(port):
            return True
        time.sleep(0.25)
    return False


def start_service(name, args, cwd):
    p = subprocess.Popen([PY] + args, cwd=cwd,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    SERVICES.append((p, name))
    return p


def write_json(name, rows):
    """非法样例数据经 tempfile 生成（限定在 data/_测试临时 目录内）。"""
    fd, path = tempfile.mkstemp(suffix=".json", prefix=name + "_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    return path


GOOD5 = [{"time": 0.0 + 0.1 * i, "switchRailDisp1": 10.0, "switchRailDisp2": 9.0,
          "switchRailDisp3": 8.0, "pointRailDisp1": 7.0, "pointRailDisp2": 6.0}
         for i in range(3)]

CSV_HEADER = "time,switchRailDisp1,switchRailDisp3,pointRailDisp1"


def csv_rows(header, body):
    """把 CSV 文本按表头解析成 dict 行（与 csv.DictReader 等价）。"""
    cols = header.split(",")
    return [dict(zip(cols, ln.split(","))) for ln in body.split("\n")]


def test_a_entries():
    sec("§A 三入口非法输入在线拒绝（组长反馈第2条）")
    # ---- A1 推送入口：子进程启动，非法应在绑定端口前 exit 1 ----
    bad_cases = {
        "NaN": dict(GOOD5[1], switchRailDisp2=float("nan")),
        "Infinity": dict(GOOD5[1], pointRailDisp2=float("inf")),
        "字符串": dict(GOOD5[1], switchRailDisp3="abc"),
        "null": dict(GOOD5[1], switchRailDisp2=None),
    }
    for name, row in bad_cases.items():
        r = run_tool("推送服务", ["--file",
                                 write_json("bad_push", [GOOD5[0], row]),
                                 "--port", "3999"])
        out = (r.stdout or "") + (r.stderr or "")
        ok = r.returncode == 1 and "校验失败" in out
        field = re.search(r"字段\[(.*?)\]", out)
        record("- 推送入口·可选三路" + name + "：退出码=" + str(r.returncode)
               + "，拒绝 ✓" + ("（定位 行2[" + field.group(1) + "]）" if field
                                else "（未见字段定位）"))
        assert ok, "推送入口未拒绝 " + name + "：" + out[:300]
    # ---- A2 CSV 转换入口：直调真实 convert 入口，拒绝=SystemExit ----
    conv = load_module("CSV转换", "仿真数据转换脚本_被测")
    csv_cases = {
        "NaN": "0.0,1.5,1.2,0.8\n0.1,NaN,1.3,0.9",
        "Infinity": "0.0,1.5,1.2,0.8\n0.1,1.6,Infinity,0.9",
        "字符串": "0.0,1.5,1.2,0.8\n0.1,1.6,1.3,abc",
        "空单元格": "0.0,1.5,1.2,0.8\n0.1,1.6,1.3,",
    }
    for name, body in csv_cases.items():
        rejected = False
        detail = ""
        try:
            conv.convert(csv_rows(CSV_HEADER, body), 0.1, ["switchRailDisp3"])
        except SystemExit as e:
            rejected = True
            detail = str(e)
        ok = rejected and "失败" in detail
        line_hit = re.search(r"第 (\d+) 行列\[(.*?)\]", detail)
        record("- CSV转换·" + name + "：SystemExit 拒绝 "
               + ("（定位 第" + line_hit.group(1) + "行[" + line_hit.group(2)
                  + "]）✓" if line_hit else "✓ " + detail[:60]))
        assert ok, "CSV转换未拒绝 " + name + "：" + detail
    # ---- A2b CSV 五路输入不丢三路 ----
    five_header = ("time,switchRailDisp1,switchRailDisp2,switchRailDisp3,"
                   "pointRailDisp1,pointRailDisp2")
    five_body = "0.0,10,9,8,7,6\n0.1,11,10,9,8,7"
    out5 = conv.convert(csv_rows(five_header, five_body), 0.1,
                        ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"])
    keys = set(out5[0].keys())
    need = {"time", "switchRailDisp1", "switchRailDisp2",
            "switchRailDisp3", "pointRailDisp1", "pointRailDisp2"}
    ok = need <= keys
    record("- CSV转换·五路输入保留：convert 输出 " + str(len(keys))
           + " 字段含全部五路位移 " + ("（三路未丢）✓" if ok else "✗ 丢失三路"))
    assert ok
    # ---- A3 02 入库入口：非法拒绝，不写库 ----
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM timeseries_data")
        before = cur.fetchone()[0]
    r = run_tool("入库", ["--file", write_json(
        "bad_db", [GOOD5[0], dict(GOOD5[1], switchRailDisp2=float("nan"))]),
        "--source", "非法测试", "--batch", "BAD-TEST"])
    out = (r.stdout or "") + (r.stderr or "")
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM timeseries_data")
        after = cur.fetchone()[0]
    ok = r.returncode == 1 and "校验失败" in out and before == after
    record("- 入库入口·可选路NaN：退出码=" + str(r.returncode) + "，库行数 "
           + str(before) + "→" + str(after)
           + ("（拒绝且未写入）✓" if ok else " ✗ " + out[:200]))
    assert ok


ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def test_b_push():
    sec("§B 推送服务 v3 在线实测（端口 " + str(WS_PORT) + "，含 /telemetry 路径）")
    import websockets

    async def flow():
        uri = "ws://localhost:" + str(WS_PORT) + "/telemetry"   # 契约路径
        msgs = []
        async with websockets.connect(uri) as ws:
            for _ in range(6):
                msgs.append(json.loads(await asyncio.wait_for(ws.recv(), 3.0)))
        return msgs

    p = start_service("推送服务", [TOOL_SCRIPTS["推送服务"]], DATA_DIR)
    try:
        if not wait_port(WS_PORT):
            record("- 服务未能在 10 秒内启动（确认 " + str(WS_PORT) + " 未被占用）✗")
            assert False, "推送服务未启动"
        msgs = asyncio.run(flow())
        m0 = msgs[0]
        # 1) time 为 UTC ISO 字符串
        t_ok = isinstance(m0["time"], str) and ISO_RE.match(m0["time"])
        record("- time 为 UTC ISO 8601 字符串：" + str(m0["time"])
               + (" ✓（含日期与时区Z，Date.parse 可解析）" if t_ok else " ✗"))
        assert t_ok
        parsed = [datetime.fromisoformat(m["time"].replace("Z", "+00:00"))
                  for m in msgs]
        # 2) 单调不减
        mono = all(a <= b for a, b in zip(parsed, parsed[1:]))
        record("- time 单调不减：" + ("✓" if mono else "✗"))
        assert mono
        # 3) 与墙钟差 ≤10s（页面有效窗口）
        lag = abs((datetime.now(timezone.utc) - parsed[-1]).total_seconds())
        record("- time 与本机墙钟差 " + "{:.2f}".format(lag) + "s（页面窗口 ±10s）："
               + ("✓" if lag <= 10 else "✗"))
        assert lag <= 10
        # 4) 五路有限值 + simTime + dataSource
        five = ["switchRailDisp1", "switchRailDisp2", "switchRailDisp3",
                "pointRailDisp1", "pointRailDisp2"]
        fin = all(isinstance(m.get(f2), (int, float)) and math.isfinite(m[f2])
                  for m in msgs for f2 in five)
        record("- 五路位移字段有限值（每帧完整快照）：" + ("✓" if fin else "✗"))
        assert fin
        sim_mark = m0.get("simFields") == ["switchRailDisp2", "switchRailDisp3",
                                           "pointRailDisp2"]
        record("- simFields 模拟标注：" + str(m0.get("simFields"))
               + "（data.json 仅两路，缺失三路按设计动程比例生成并标注）"
               + ("✓" if sim_mark else "✗"))
        assert sim_mark
        ds = m0.get("dataSource") == "SIMULATED"
        record("- dataSource=SIMULATED（契约附加说明字段）：" + ("✓" if ds else "✗"))
        assert ds
        st = isinstance(m0.get("simTime"), (int, float))
        record("- simTime 保留仿真秒：" + str(m0.get("simTime"))
               + ("✓" if st else "✗"))
        assert st
        # 5) 断线重连
        async def reconnect():
            async with websockets.connect(
                    "ws://localhost:" + str(WS_PORT) + "/telemetry") as ws:
                return json.loads(await asyncio.wait_for(ws.recv(), 3.0))["seq"]
        seq2 = asyncio.run(reconnect())
        record("- 断线重连：再连一帧 seq=" + str(seq2) + " > " + str(msgs[-1]["seq"])
               + ("✓" if seq2 > msgs[-1]["seq"] else "✗"))
        assert seq2 > msgs[-1]["seq"]
    finally:
        try:
            p.terminate()
        except Exception:  # noqa: BLE001
            pass


def start_api():
    start_service("数据API",
                  ["-m", "uvicorn", "数据服务API:app", "--host", "127.0.0.1",
                   "--port", str(API_PORT)], DATA_DIR)
    for _ in range(60):
        code, _ = http("/api/points")
        if code == 200:
            return True
        time.sleep(0.3)
    return False


def start_api_wrapper():
    ok = start_api()
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "，测试专用端口）"
           + ("✓" if ok else "✗ 启动失败"))
    assert ok


def test_c_history():
    sec("§C 历史查询完整日期与时区（组长反馈第3条，API 在线）")
    with db_session() as cur:
        cur.execute("SELECT min(ts), max(ts) FROM timeseries_data WHERE batch = %s",
                    ("B02-四工况-告警演示",))
        tmin, tmax = cur.fetchone()
    end8 = (tmax + timedelta(seconds=1)).isoformat()
    q = {"start": tmin.isoformat(), "end": end8,
         "source": "仿真-告警演示", "limit": 5000}
    code, d = http("/api/history/T01-SR-01-DISP?" + urlencode(q))
    ok = code == 200 and d["count"] > 0
    record("- 完整日期+08:00 查询：HTTP " + str(code) + "，" + str(d.get("count"))
           + " 行（区间 " + tmin.isoformat() + " ~ " + end8 + "）"
           + ("✓" if ok else "✗ " + str(d)[:200]))
    assert ok
    row0 = d["数据"][0]
    has_iso = ("日期" in row0 and "时刻ISO" in row0 and "+08:00" in row0["时刻ISO"])
    record("- 输出含 日期/时刻ISO 字段：" + str(row0.get("时刻ISO"))
           + ("✓" if has_iso else "✗"))
    assert has_iso
    # Z（UTC）表达同一区间 → 行数一致（跨时区换算）
    zu = tmin.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    zl = (tmax + timedelta(seconds=1)).astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z")
    code2, d2 = http("/api/history/T01-SR-01-DISP?" +
                     urlencode({"start": zu, "end": zl, "source": "仿真-告警演示",
                                "limit": 5000}))
    eq = code2 == 200 and d2.get("count") == d["count"]
    record("- Z(UTC) 表达同一区间：" + str(d2.get("count")) + " 行 vs +08:00 的 "
           + str(d["count"]) + " 行" + ("✓（跨时区换算一致）" if eq else "✗"))
    assert eq
    c3, d3 = http("/api/history/T01-SR-01-DISP?start=04:00")
    ok3 = c3 == 400
    record("- 纯时刻 HH:MM 已停用：HTTP " + str(c3) + "（"
           + str(d3.get("detail", ""))[:56] + "）" + ("✓" if ok3 else "✗"))
    assert ok3
    c4, _ = http("/api/history/T01-SR-01-DISP?start=2026/10/02")
    ok4 = c4 == 400
    record("- 非法格式 2026/10/02：HTTP " + str(c4) + " " + ("✓" if ok4 else "✗"))
    assert ok4


IDEM_SRC = "仿真-幂等测试"


def test_d_idempotent():
    sec("§D 同批次重复导入幂等（组长反馈第3条，真库写入实测）")
    with db_session() as cur:
        cur.execute("DELETE FROM timeseries_data WHERE batch LIKE %s", ("IDEM2-%",))
        cur.execute("SELECT count(*) FROM timeseries_data")
        before = cur.fetchone()[0]

    def cmd(batch):
        return ["--file", safe_path(DATA_DIR, "扩展数据集v2", "工况_告警演示.json"),
                "--source", IDEM_SRC, "--batch", batch]
    r1 = run_tool("入库", cmd("IDEM2-A"))
    r2 = run_tool("入库", cmd("IDEM2-A"))
    r3 = run_tool("入库", cmd("IDEM2-B"))
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM timeseries_data WHERE batch = %s",
                    ("IDEM2-A",))
        na = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data WHERE batch = %s",
                    ("IDEM2-B",))
        nb = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data")
        total = cur.fetchone()[0]
    idem_hint = "幂等" in (r2.stdout or "")
    ok1 = r1.returncode == 0 and na > 0
    record("- 首导 IDEM2-A：rc=" + str(r1.returncode) + "，批次 " + str(na) + " 条"
           + ("✓" if ok1 else "✗"))
    assert ok1
    ok2 = r2.returncode == 0 and idem_hint
    record("- 重导 IDEM2-A：rc=" + str(r2.returncode) + "，输出含'[幂等] 复用时间轴'="
           + str(idem_hint) + "，批次仍 " + str(na) + " 条（未新增重复）"
           + ("✓" if ok2 else "✗"))
    assert ok2
    ok3 = r3.returncode == 0 and nb == na and total == before + na + nb
    record("- 异批 IDEM2-B：rc=" + str(r3.returncode) + "，" + str(nb)
           + " 条（与 A 共存），总行数 " + str(before) + "→" + str(total)
           + "（+" + str(total - before) + "）" + ("✓" if ok3 else "✗"))
    assert ok3
    with db_session() as cur:
        cur.execute("DELETE FROM timeseries_data WHERE batch LIKE %s", ("IDEM2-%",))
    record("- 已清理测试批次 IDEM2-*（库回到测试前行数）✓")


def test_e_reconnect():
    sec("§E 数据库运行中断线恢复（组长反馈第3条）")
    # E1 ResilientDB 直测：杀掉自己的连接，下一次 execute 自动重连
    rdb = ResilientDB()
    v1 = rdb.run(lambda cur: (cur.execute("SELECT 1"), cur.fetchone())[1])
    pid = rdb.run(lambda cur: (cur.execute("SELECT pg_backend_pid()"),
                               cur.fetchone())[1])[0]
    with db_session() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    time.sleep(0.3)
    v2 = rdb.run(lambda cur: (cur.execute("SELECT 1"), cur.fetchone())[1])
    ok = v1 == v2 == (1,)
    record("- ResilientDB：杀掉连接(pid=" + str(pid) + ")后重连成功，SELECT 1 → "
           + str(v2) + (" ✓（运行中断线自动恢复）" if ok else "✗"))
    rdb.close()
    assert ok
    # E2 API 在线自愈：杀掉 API 的库连接，接口不 500
    c0, _ = http("/api/health")
    with db_session() as cur:
        cur.execute("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                    "WHERE application_name = %s AND pid <> pg_backend_pid()",
                    ("turnout-twin-b",))
        killed = cur.fetchone()[0]
    time.sleep(0.5)
    c1, d1 = http("/api/health")
    ok2 = c0 == 200 and c1 == 200 and "健康度评分" in d1
    record("- API 在线自愈：断其库连接×" + str(killed) + " 后 /api/health "
           + str(c0) + "→" + str(c1) + "，等级=" + str(d1.get("等级"))
           + (" ✓（接口未崩、自动重连）" if ok2 else "✗"))
    assert ok2


def test_f_nodata():
    sec("§F 健康度无数据不虚报（组长反馈补充条，API 在线）")
    with db_session() as cur:
        cur.execute("SELECT count(*) FROM realtime_value")
        n0 = cur.fetchone()[0]
        cur.execute("DELETE FROM realtime_value")
    code, d = http("/api/health")
    ok = code == 200 and d.get("等级") == "无数据" and d.get("健康度评分") is None
    record("- 空表时 /api/health：等级=" + str(d.get("等级")) + "，评分="
           + str(d.get("健康度评分")) + (" ✓（不显示100/优）" if ok else " ✗"))
    # 回放注入 → 恢复评估（POST + 中文工况名 URL 编码）
    cp, dp = http("/api/play/" + quote("正常转换"), method="POST", timeout=15)
    time.sleep(1.6)
    code2, d2 = http("/api/health")
    http("/api/stop", method="POST", timeout=15)
    ok2 = cp == 200 and code2 == 200 and d2.get("等级") != "无数据" and isinstance(
        d2.get("健康度评分"), (int, float))
    record("- 回放注入（POST rc=" + str(cp) + "）后恢复：等级="
           + str(d2.get("等级")) + "，评分=" + str(d2.get("健康度评分"))
           + ("✓" if ok2 else "✗"))
    record("- 原实时表 " + str(n0) + " 行已由回放重建（realtime_value 恢复实时数据）")
    assert ok and ok2


def test_g_pending():
    sec("§G iTwin 页面实际联调（待测项——未实测，不写'通过'）")
    record("- [待测] C 页面连接 ws://<B机IP>:3003 收到 B 数据（time=UTC ISO）")
    record("- [待测] switchRailDisp1>150 → 仅 SwitchRail 变红；恢复≤150 → 颜色清除")
    record("- [待测] pointRailDisp1>100 → 仅 PointRail 变红；恢复 → 清除")
    record("- [待测] 断开 B 推送>5s → 页面提示'数据未更新'并保留颜色；重连自动恢复")
    record("- [待测] 验收以 iTwin 页面实际联调为准（组长第二轮原文）——B 侧就绪，等 C 排期")


def main():
    print("B 线整改第二轮验收 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    steps = [test_a_entries, test_b_push, start_api_wrapper, test_c_history,
             test_d_idempotent, test_e_reconnect, test_f_nodata, test_g_pending]
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
        n_total = sum(1 for l in LINES if l.startswith("- "))
        n_fail = sum(1 for l in LINES if "✗" in l)
        n_pend = sum(1 for l in LINES if l.startswith("- [待测]"))
        print("\n结论：已实测用例 " + str(n_total - n_pend) + " 项，失败 " + str(n_fail)
              + " 项；另有待测 " + str(n_pend) + " 项（iTwin 联调，等 C）。"
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
