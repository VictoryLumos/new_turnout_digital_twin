# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— WebSocket 数据推送服务（B：数据管道线）

作用：
    读取同目录下的 data.json，按数据契约的时间步长（0.1 秒）实时逐条
    推送给 C 的网页端；推完自动从头再来（循环播放）。
    每个接入的客户端独立从 t=0 开始收数据。

用法：
    pip install websockets
    python WebSocket推送服务.py              # 默认端口 8765
    python WebSocket推送服务.py --port 9000  # 换端口

C 端连接地址：
    C 和你同一台电脑：ws://localhost:8765
    C 在同一局域网其他电脑：ws://<你的IP>:8765（ipconfig 查你的 IPv4 地址）

说明：
    - 推送内容与 data.json 完全一致（契约字段 time / switchRailDisp1 / pointRailDisp1），
      超限变红由 C 在前端按《统一标准》阈值（尖轨>150mm、心轨>100mm）实现。
    - 按《统一标准》推送方式的约定：若 WebSocket 联调失败，退回 C 直接读本地
      data.json，不影响交付。
    - 启动时先自检数据文件（条数 / 步长 / 时间范围），
      即《分工》第一个环节里"读 JSON、确认能读"这一步。
"""
import argparse
import asyncio
import json
import os
import sys

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
STEP_SECONDS = 0.1  # 《统一标准》数据契约：时间步长 0.1 秒

try:
    from websockets.asyncio.server import serve        # websockets >= 13
    from websockets.exceptions import ConnectionClosed
except ImportError:
    try:
        from websockets import serve                    # 旧版 websockets
        from websockets.exceptions import ConnectionClosed
    except ImportError:
        sys.exit("缺少依赖：请先执行  pip install websockets")

ROWS = []  # 启动时由 load_data() 填充


def load_data():
    """读 data.json 并自检（条数 / 步长 / 时间范围），返回数据列表。"""
    with open(DATA_FILE, encoding="utf-8") as f:
        rows = json.load(f)
    assert rows, "data.json 是空的"
    steps = {round(rows[i + 1]["time"] - rows[i]["time"], 6) for i in range(len(rows) - 1)}
    assert steps == {STEP_SECONDS}, f"步长异常 {steps}，契约要求 0.1 秒"
    print(f"[自检] data.json 读取成功：{len(rows)} 条，"
          f"time {rows[0]['time']}~{rows[-1]['time']}s，步长 {STEP_SECONDS}s")
    return rows


async def handler(websocket, path=None):  # path 参数兼容旧版 websockets
    """单个客户端的推送协程：循环推送，客户端断开后结束。"""
    peer = getattr(websocket, "remote_address", None)
    print(f"[连接] 客户端上线：{peer}")
    try:
        loop_count = 0
        while True:
            loop_count += 1
            print(f"[推送] {peer} 第 {loop_count} 轮开始，共 {len(ROWS)} 条")
            t0 = asyncio.get_running_loop().time()
            for i, row in enumerate(ROWS):
                # 按绝对时刻对齐节奏，抵消 send 本身的耗时，避免越推越慢
                target = t0 + i * STEP_SECONDS
                await asyncio.sleep(max(0, target - asyncio.get_running_loop().time()))
                await websocket.send(json.dumps(row, ensure_ascii=False))
    except ConnectionClosed:
        print(f"[断开] 客户端下线：{peer}")


async def main(port):
    stop = asyncio.Event()
    async with serve(handler, "0.0.0.0", port):
        print(f"[服务] WebSocket 推送服务已启动，端口 {port}")
        print(f"[服务] 本机测试地址 ws://localhost:{port}，等待 C 的页面连接（Ctrl+C 停止）")
        await stop.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="按 0.1s 步长循环推送 data.json")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    ROWS.extend(load_data())
    try:
        asyncio.run(main(args.port))
    except KeyboardInterrupt:
        print("\n[服务] 已停止")
