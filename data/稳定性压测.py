# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 稳定性压测（B：数据管道线，v2.1）

作用（对应验收项"连续 3 分钟运行不崩"）：
    在数据服务API运行期间，循环注入四工况回放，同时持续请求全部只读接口，
    统计：请求错误数、各接口延迟分位数（p50/p95/max）、工况自动识别准确率。
    结束输出验收结论（0 错误 且 识别全对 = PASS）。

用法（先启动数据服务API，再运行本脚本）：
    python 稳定性压测.py               # 默认 3 分钟（验收口径）
    python 稳定性压测.py --minutes 1   # 快速冒烟 1 分钟

仅用 Python 标准库，无额外依赖。
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import quote, urlparse

BASE = "http://127.0.0.1:8000"
ALLOWED_HOSTS = {"127.0.0.1", "localhost"}   # 压测目标白名单：仅本机数据服务API
CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败"]
READ_PATHS = ["/api/health", "/api/realtime", "/api/alarms",
              "/api/workorders", "/api/identify", "/api/stats",
              "/api/history/T01-SR-01-DISP?limit=500",
              "/api/diagnosis"]

latency = {}       # path -> [ms, ...]
errors = []        # [(path, code/detail)]
identify_hits = 0
identify_total = 0


def build_url(path):
    """拼接并校验请求地址：仅允许 http/https 且主机必须在白名单内（本机API），防 SSRF。"""
    url = BASE + quote(path, safe="/?=&")   # 保留查询串分隔符，只编码中文工况名
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError(f"请求目标未通过白名单校验：{url}")
    return url


def call(path, method="GET"):
    """请求一次，返回 (状态码, json/None, 耗时ms)。异常返回 (None, 详情, 耗时)"""
    url = build_url(path)
    req = urllib.request.Request(url, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = json.load(r)
            return r.status, body, (time.perf_counter() - t0) * 1000
    except urllib.error.HTTPError as e:
        return e.code, None, (time.perf_counter() - t0) * 1000
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", (time.perf_counter() - t0) * 1000


def poll_round(check_identify):
    """把全部只读接口各请求一遍，记延迟/错误；可选用当前识别结果对答案"""
    global identify_hits, identify_total
    for path in READ_PATHS:
        status, body, ms = call(path)
        latency.setdefault(path, []).append(ms)
        if status != 200:
            errors.append((path, status or body))
        elif check_identify and path == "/api/identify":
            identify_total += 1
            if body.get("识别工况") == check_identify:
                identify_hits += 1


def main():
    parser = argparse.ArgumentParser(description="数据服务API稳定性压测")
    parser.add_argument("--minutes", type=float, default=3.0,
                        help="压测时长（分钟），默认 3（验收口径）")
    args = parser.parse_args()
    deadline = time.time() + args.minutes * 60

    # 预检：服务必须在跑
    status, body, _ = call("/api/stats")
    if status != 200:
        print("[NG] 数据服务API(127.0.0.1:8000) 未就绪。先双击 服务API演示.bat，"
              "或 python -m uvicorn 数据服务API:app --port 8000")
        sys.exit(1)
    print(f"[压测] 目标 {BASE} · 时长 {args.minutes} 分钟 · "
          f"每工况回放期间轮询 {len(READ_PATHS)} 个只读接口")
    print("=" * 56)

    cond_idx = 0
    try:
        while time.time() < deadline:
            cond = CONDITIONS[cond_idx % len(CONDITIONS)]
            cond_idx += 1
            status, body, _ = call(f"/api/play/{cond}", "POST")
            if status != 200:
                errors.append((f"/api/play/{cond}", status))
            t_cond = time.time()
            # 一轮回放 10s：前 5s 只记延迟（识别窗口在积累），后 5s 对识别答案
            while time.time() - t_cond < 10 and time.time() < deadline:
                poll_round(cond if time.time() - t_cond > 5 else None)
                time.sleep(0.5)
            print(f"  [周期 {cond_idx}] {cond} 完成，"
                  f"累计 {sum(len(v) for v in latency.values())} 次请求，"
                  f"错误 {len(errors)}，识别 {identify_hits}/{identify_total}")
    except KeyboardInterrupt:
        print("\n[压测] 手动中断")
    finally:
        call("/api/stop", "POST")

    # ---- 报告 ----
    print("=" * 56)
    print(f"稳定性压测报告（实际运行 {cond_idx} 轮工况回放）")
    all_ms = sorted(ms for v in latency.values() for ms in v)
    for path in READ_PATHS:
        s = sorted(latency.get(path, [0]))
        print(f"  {path:<42} p50={s[len(s)//2]:6.1f}ms  "
              f"p95={s[int(len(s)*0.95)]:6.1f}ms  max={s[-1]:7.1f}ms  n={len(s)}")
    acc = identify_hits / identify_total if identify_total else 0
    print(f"  总请求 {len(all_ms)} 次 · 错误 {len(errors)} 次 · "
          f"识别准确率 {identify_hits}/{identify_total}（{acc:.0%}）")
    for p, d in errors[:5]:
        print(f"    错误样例: {p} -> {d}")
    ok = not errors and identify_total > 0 and acc >= 0.95
    print("验收结论：" + ("PASS —— 连续运行无错误，可支撑演示（对应'连续3分钟不崩'验收项）"
                        if ok else "FAIL —— 按上面错误项排查后再演示"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
