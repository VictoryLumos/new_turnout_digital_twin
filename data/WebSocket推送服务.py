# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— WebSocket 数据推送服务 v3（B：数据管道线，整改第二轮）

对应组长第二轮反馈第1/2条与《接口契约》（github_docs/接口契约.md）：
  · time 改为【UTC ISO 8601 字符串】（如 2026-10-02T08:00:00.123Z）：
    iTwin 页面要求 Date.parse 可解析、必须当前时间（与浏览器时差>10s 拒收）、
    单调不减——因此取真实发送时刻；仿真秒保留在 simTime，不再占用 time
  · 五路位移校验升级：必填两路之外，switchRailDisp2/3、pointRailDisp2
    属于"出现即校验"——已提供的 NaN/Infinity/字符串/null 一律拒绝启动，
    绝不把非法值替换成模拟值；仅当字段完全缺失时才由适配层模拟生成
  · 缺失三路的模拟依据：按 A 确认的设计动程等比例换算
    （118/160、75/160、55.5/100.6，编码与映射表.json 第2页8.2/8.5），
    属演示用近似，在 simFields 中显式标注，非设计值或实测值
  · 端口改 3003：3002 已被 C 的 iTwin 本地服务占用（接口契约原文），
    本服务同机不再抢 3002；C 直连 ws://<B机IP>:3003 或由 C 侧转发
  · 消息元信息：seq(全局序号) round(轮次) simTime(仿真秒)
    sendTs(发送时刻毫秒) timeStr(本地时间字符串) dataSource(SIMULATED)
  · 循环边界：按绝对时刻对齐，轮与轮之间保证 ≥ 步长，不会瞬发两帧
  · 断线：服务端支持客户端随时断开重连；客户端自动重连由 C 实现，
    B 的参考实现见 WebSocket测试页面.html

用法：
    python WebSocket推送服务.py                     # 默认 data.json，端口3003
    python WebSocket推送服务.py --file 扩展数据集v2/工况_告警演示.json
    python WebSocket推送服务.py --port 8765         # 临时其他端口
C 端连接：ws://<B的IP>:3003（任意路径均可，含 /telemetry）
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import (CONFIG, STEP, WS_PORT, validate_rows,  # noqa: E402
                     format_errors, now_iso_utc, TZ8,
                     EXTRA_CHECK_FIELDS)

DATA_FILE = os.path.join(HERE, "data.json")
BASE_REQUIRED = ["time", "switchRailDisp1", "pointRailDisp1"]  # 数据最低要求
ADAPT_FIELDS = ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"]
# 缺失三路的模拟依据：A 确认的设计动程（编码与映射表.json，第2页8.2/8.5）
# 尖轨设计动程 160/118/75mm、心轨 100.6/55.5mm——按第一点比例换算，
# 仅为演示用近似（非设计值/实测值），生成通道一律进入 simFields 标注。
STROKE_RATIO = {"switchRailDisp2": 118.0 / 160.0,
                "switchRailDisp3": 75.0 / 160.0,
                "pointRailDisp2": 55.5 / 100.6}
TIME_FMT = CONFIG["适配层"]["时间字符串格式"]

try:
    from websockets.asyncio.server import serve
    from websockets.exceptions import ConnectionClosed
except ImportError:
    sys.exit("缺少依赖：pip install websockets（见 requirements.txt）")

ROWS = []          # 启动时加载并校验
SEQ_TOTAL = 0      # 全局序号（跨轮次累计，证明数据新鲜度）


def load_and_validate(path):
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    if not rows:
        sys.exit(f"[校验失败] {path} 数据集为空，拒绝推送")
    good, errors = validate_rows(rows, os.path.basename(path),
                                 required=BASE_REQUIRED,
                                 extra_fields=EXTRA_CHECK_FIELDS)
    if errors:
        print(f"[校验失败] {path} 共 {len(errors)} 处非法，拒绝推送：")
        print(format_errors(errors))
        sys.exit(1)
    n5 = sum(1 for r in good if all(f in r for f in ADAPT_FIELDS))
    print(f"[自检] {os.path.basename(path)} 校验通过：{len(good)} 条，"
          f"五路齐备 {n5} 条（其余由适配层按设计动程比例模拟并标注），"
          f"time {good[0]['time']}~{good[-1]['time']}s，步长 {STEP}s")
    return good


def adapt_row(row):
    """五路适配：完全缺失的字段按设计动程比例生成，返回(消息dict, 模拟清单)。

    仅当字段【完全缺失】才模拟；已提供 null/NaN/字符串/Infinity 的行
    在 load_and_validate 已被拒绝，绝不替换成模拟值——绝不悄悄补零。
    """
    msg = dict(row)
    sim = []
    sw1, pr1 = row.get("switchRailDisp1", 0.0), row.get("pointRailDisp1", 0.0)
    gen = {"switchRailDisp2": round(sw1 * STROKE_RATIO["switchRailDisp2"], 2),
           "switchRailDisp3": round(sw1 * STROKE_RATIO["switchRailDisp3"], 2),
           "pointRailDisp2": round(pr1 * STROKE_RATIO["pointRailDisp2"], 2)}
    for f in ADAPT_FIELDS:
        if f not in row:            # 只补"缺失"，不覆盖/不替换任何已提供值
            msg[f] = gen[f]
            sim.append(f)
    return msg, sim


def build_message(row, seq, rnd):
    msg, sim = adapt_row(row)
    sim_t = float(row["time"])
    tstr = (datetime.now(TZ8)).strftime(TIME_FMT)[:-3]
    msg.update({
        "seq": seq,                  # 全局递增序号（新鲜度判据）
        "round": rnd,                # 回放轮次（从1起，循环归零不混淆）
        "simTime": sim_t,            # 仿真时间（秒）
        "time": now_iso_utc(),       # UTC ISO 8601 当前时刻（页面契约要求）
        "sendTs": int(time.time() * 1000),   # 发送时刻（墙钟毫秒）
        "timeStr": tstr,             # 本地时间字符串（页面展示格式，配置可调）
        "dataSource": "SIMULATED",   # 契约附加说明字段：当前为模拟回放
    })
    if sim:
        msg["simFields"] = sim       # 明确标注：这些通道为模拟生成
    return msg


async def handler(websocket, path=None):
    """每个客户端独立连接；断开互不影响，可随时重连（整改令第4条）。"""
    global SEQ_TOTAL
    peer = getattr(websocket, "remote_address", None)
    print(f"[连接] 客户端上线：{peer}")
    rnd = 0
    try:
        while True:
            rnd += 1
            print(f"[推送] {peer} 第 {rnd} 轮开始（{len(ROWS)} 条/轮）")
            t0 = asyncio.get_running_loop().time()
            for i, row in enumerate(ROWS):
                # 绝对时刻对齐；轮结束后下一轮首帧至少再等一个步长，
                # 杜绝循环交界两帧瞬发（整改令第3条）
                target = t0 + i * STEP
                now = asyncio.get_running_loop().time()
                if now < target:
                    await asyncio.sleep(target - now)
                else:
                    await asyncio.sleep(0)
                SEQ_TOTAL += 1
                await websocket.send(json.dumps(
                    build_message(row, SEQ_TOTAL, rnd), ensure_ascii=False))
            # 本轮完：强制额外间隔一个步长再进下一轮
            await asyncio.sleep(STEP)
    except ConnectionClosed:
        print(f"[断开] 客户端下线：{peer}（服务保持运行，可重连）")


async def main(port, path):
    stop = asyncio.Event()
    async with serve(handler, "0.0.0.0", port):
        print(f"[服务] 已启动 端口{port} 数据={os.path.basename(path)} "
              f"(Ctrl+C 停止)")
        print(f"[服务] 本机测试：ws://localhost:{port}；跨机用本机IPv4")
        await stop.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据推送服务 v3（默认3003，time为UTC ISO）")
    parser.add_argument("--file", default=DATA_FILE)
    parser.add_argument("--port", type=int, default=WS_PORT)
    args = parser.parse_args()
    ROWS.extend(load_and_validate(args.file))
    try:
        asyncio.run(main(args.port, args.file))
    except KeyboardInterrupt:
        print("\n[服务] 已停止")
