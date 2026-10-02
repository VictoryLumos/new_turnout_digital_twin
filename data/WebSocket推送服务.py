# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— WebSocket 数据推送服务 v2（B：数据管道线，整改版）

对应组长整改令第 1/3/4/5 条：
  · 启动即校验：缺字段/字符串/null/NaN/Infinity/乱序 拒绝启动并定位到 行号+字段
  · 消息带元信息：seq(全局序号) round(轮次) simTime(仿真秒) sendTs(发送时刻毫秒)
    timeStr(时间字符串) —— 区分仿真时间与发送时间，循环归零不会被前端判旧
  · 循环边界：按绝对时刻对齐，轮与轮之间保证 ≥ 步长，不会瞬发两帧
  · 五路位移适配：数据缺 switchRailDisp2/3、pointRailDisp2 时按牵引点
    模型生成，并在消息 simFields 中【显式标注为模拟】，绝不悄悄补零
  · 端口统一为 3002（管道配置.json），--port 可临时覆盖（如旧联调 8765）
  · 断线：服务端支持客户端随时断开重连；客户端自动重连由 C 实现，
    B 的参考实现见 WebSocket测试页面.html（2 秒自动重连）

用法：
    python WebSocket推送服务.py                     # 默认 data.json，端口3002
    python WebSocket推送服务.py --file 扩展数据集v2/工况_告警演示.json
    python WebSocket推送服务.py --port 8765         # 临时旧端口
C 端连接：ws://<B的IP>:3002
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import (CONFIG, STEP, WS_PORT, validate_rows,  # noqa: E402
                     format_errors)

DATA_FILE = os.path.join(HERE, "data.json")
BASE_REQUIRED = ["time", "switchRailDisp1", "pointRailDisp1"]  # 数据最低要求
ADAPT_FIELDS = ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"]
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))
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
    good, errors = validate_rows(rows, os.path.basename(path),
                                 required=BASE_REQUIRED)
    if errors:
        print(f"[校验失败] {path} 共 {len(errors)} 处非法，拒绝推送：")
        print(format_errors(errors))
        sys.exit(1)
    print(f"[自检] {os.path.basename(path)} 校验通过：{len(good)} 条，"
          f"time {good[0]['time']}~{good[-1]['time']}s，步长 {STEP}s")
    return good


def adapt_row(row):
    """五路适配：缺失的三路由牵引点模型生成，返回(消息dict, 模拟字段清单)。
    绝不悄悄补零——凡生成的字段一律进入 simFields 标注。"""
    msg = dict(row)
    sim = []
    sw1, pr1 = row.get("switchRailDisp1", 0.0), row.get("pointRailDisp1", 0.0)
    gen = {"switchRailDisp2": round(max(0.0, sw1 * 0.98 - 2), 2),
           "switchRailDisp3": round(max(0.0, sw1 * 0.96 - 4), 2),
           "pointRailDisp2": round(max(0.0, pr1 * 0.97 - 2), 2)}
    for f in ADAPT_FIELDS:
        if f not in row or row[f] is None:
            msg[f] = gen[f]
            sim.append(f)
    return msg, sim


def build_message(row, seq, rnd):
    msg, sim = adapt_row(row)
    sim_t = float(row["time"])
    tstr = (BASE_TS + timedelta(seconds=sim_t)).strftime(TIME_FMT)[:-3]
    msg.update({
        "seq": seq,                  # 全局递增序号（新鲜度判据）
        "round": rnd,                # 回放轮次（从1起，循环归零不混淆）
        "simTime": sim_t,            # 仿真时间（秒）
        "time": sim_t,               # 兼容旧契约的数值时间
        "sendTs": int(time.time() * 1000),   # 发送时刻（墙钟毫秒）
        "timeStr": tstr,             # 时间字符串（页面格式，配置可调）
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
    parser = argparse.ArgumentParser(description="数据推送服务 v2（3002）")
    parser.add_argument("--file", default=DATA_FILE)
    parser.add_argument("--port", type=int, default=WS_PORT)
    args = parser.parse_args()
    ROWS.extend(load_and_validate(args.file))
    try:
        asyncio.run(main(args.port, args.file))
    except KeyboardInterrupt:
        print("\n[服务] 已停止")
