# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 自查补充测试（B线，2026-10-04 深夜自查，纯输出版）

第六轮修复后主动自查发现的两类潜在问题及修复验证：
  §A 空数据集守卫：推送/入库/api_play 三入口对空数据集明确拒绝
     （原实现：推送服务 IndexError 栈回溯、02 脚本 min() 崩溃、
      api_play 返回"开始回放 0 行"误导）
  §B 回放停止后无残留写入：/api/stop 后 realtime_value 的 updated_at
     不再前进（验证回放线程真正退出；配合"私有停止事件"修复——
     原共享事件在旧线程卡顿超时未退出时会被下一次回放 clear() 复活双写）
  §C 页面冒烟回归

安全说明：被测路径 safe_path 限本项目；临时文件经 tempfile；HTTP 仅白名单；
SQL 全字面量+参数绑定。用法：python 验收测试_自查.py
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
    "入库": safe_path(HERE, "db", "02_数据导入脚本.py"),
}


def run_tool(key, extra):
    return subprocess.run([PY, TOOL_SCRIPTS[key]] + list(extra),
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=90)


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


def max_updated():
    with db_session() as cur:
        cur.execute("SELECT max(updated_at) FROM realtime_value")
        return cur.fetchone()[0]


def test_a_empty_datasets():
    sec("§A 空数据集守卫（三入口明确拒绝，不再崩溃/误导）")
    fd, empty = tempfile.mkstemp(suffix=".json", prefix="empty_", dir=TMP)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("[]")
    r1 = run_tool("推送服务", ["--file", empty, "--port", "3999"])
    out1 = (r1.stdout or "") + (r1.stderr or "")
    ok1 = r1.returncode == 1 and "为空" in out1
    record("- 推送服务·空数据集：退出码=" + str(r1.returncode)
           + "，提示'数据集为空，拒绝推送' " + ("✓（原为 IndexError 栈回溯）"
                                              if ok1 else " ✗ " + out1[:120]))
    assert ok1
    r2 = run_tool("入库", ["--file", empty, "--source", "空测试",
                           "--batch", "EMPTY-TEST"])
    out2 = (r2.stdout or "") + (r2.stderr or "")
    ok2 = r2.returncode != 0 and "为空" in out2
    record("- 02入库·空数据集：退出码=" + str(r2.returncode)
           + "，提示'数据集为空，拒绝入库' " + ("✓（原为 min() 崩溃）" if ok2
                                                else " ✗ " + out2[:120]))
    assert ok2
    # api_play：临时把工况文件换为空数据 → 400 → 还原
    target = safe_path(DATA_DIR, "扩展数据集v2", "工况_卡阻.json")
    with open(target, encoding="utf-8") as f:
        backup = f.read()
    try:
        fd3 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd3, "w", encoding="utf-8") as f:
            f.write("[]")
        code3, d3 = http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
        ok3 = code3 == 400 and "为空" in str(d3.get("detail", ""))
        record("- api_play·空数据集：HTTP " + str(code3) + "，detail 含'为空' "
               + ("✓（原返回'开始回放 0 行'误导）" if ok3
                  else " ✗ " + str(d3)[:120]))
        assert ok3
    finally:
        fd4 = os.open(target, os.O_WRONLY | os.O_TRUNC)
        with os.fdopen(fd4, "w", encoding="utf-8") as f:
            f.write(backup)
    code4, d4 = http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
    ok4 = code4 == 200 and "已校验" in str(d4.get("msg", ""))
    record("- 还原后正常回放：HTTP " + str(code4) + " " + ("✓" if ok4 else " ✗"))
    http("/api/stop", method="POST")
    assert ok4


def test_b_stop_stops():
    sec("§B 回放停止后无残留写入（私有停止事件验证）")
    http("/api/play/" + quote("卡阻"), method="POST", timeout=15)
    time.sleep(1.0)
    http("/api/stop", method="POST", timeout=15)
    time.sleep(0.8)                       # 等在途帧落库
    t1 = max_updated()
    time.sleep(1.2)
    t2 = max_updated()
    ok = t1 == t2
    record("- /api/stop 后 realtime_value.max(updated_at) 两次采样（间隔1.2s）："
           + str(t1) + " → " + str(t2) + ("（不变，回放线程确已退出）✓" if ok
                                           else "（仍在写入 ✗）"))
    assert ok
    # 连续两次快速切换回放后停止，同样无残留
    http("/api/play/" + quote("正常转换"), method="POST", timeout=15)
    http("/api/play/" + quote("告警演示"), method="POST", timeout=15)
    time.sleep(0.8)
    http("/api/stop", method="POST", timeout=15)
    time.sleep(0.8)
    t3 = max_updated()
    time.sleep(1.2)
    t4 = max_updated()
    ok2 = t3 == t4
    record("- 快速切换回放（正常转换→告警演示）再停止：两次采样 "
           + ("不变 ✓（旧线程未复活双写）" if ok2 else "仍在写入 ✗"))
    assert ok2


def test_c_smoke():
    sec("§C 页面冒烟回归")
    for path, mark in [("/", "告警变化记录"), ("/trend5", "五路位移总览"),
                       ("/api/alarm-events?limit=3", "告警变化")]:
        code, d = http(path)
        ok = code == 200 and mark in str(d)
        record("- GET " + path + "：HTTP " + str(code) + "（含'" + mark + "'）"
               + (" ✓" if ok else " ✗"))
        assert ok
    record("- [待测] 以上页面实际观感人工过一遍")


def main():
    print("B 线自查补充测试 · " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    failed = False
    ok_api = start_api()
    record("- 数据服务 API 已启动（127.0.0.1:" + str(API_PORT) + "）"
           + ("✓" if ok_api else "✗"))
    assert ok_api
    try:
        for t in (test_a_empty_datasets, test_b_stop_stops, test_c_smoke):
            try:
                t()
            except AssertionError as e:
                failed = True
                print("  [该组存在失败项] " + str(e))
            except Exception as e:  # noqa: BLE001
                failed = True
                print("  [异常] " + type(e).__name__ + ": " + str(e))
    finally:
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
    print("\n结论：自查补充实测 " + str(n_total) + " 项，失败 " + str(n_fail)
          + " 项。" + ("全部通过。" if not failed and n_fail == 0 else "存在失败项。"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
