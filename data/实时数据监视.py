# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— 实时数据监视器（B：数据管道线，调试工具）

作用：
    连接本机的 WebSocket 推送服务，把管道里正在流的数据实时打印成表格，
    用于联调前自证"推出去的数是对的"。注意：这不是项目的演示仪表盘——
    面向验收的仪表盘由 C 的网页实现，这个只是 B 自己的调试窗口。

用法（开两个窗口）：
    窗口1:  python WebSocket推送服务.py
    窗口2:  python 实时数据监视.py                # 默认连 ws://localhost:8765
            python 实时数据监视.py ws://IP:8765   # 指定地址

    Ctrl+C 停止。
"""
import asyncio
import json
import sys

try:
    from websockets.asyncio.client import connect
except ImportError:
    from websockets import connect

SW_LIMIT, PR_LIMIT = 150.0, 100.0  # 《统一标准》告警阈值


async def run(url):
    print(f"正在连接 {url} …（Ctrl+C 停止）")
    async with connect(url) as ws:
        print(f"{'时间s':>6}  {'尖轨mm':>8}  {'心轨mm':>8}  告警")
        print("-" * 42)
        async for msg in ws:
            r = json.loads(msg)
            sw, pr = r["switchRailDisp1"], r["pointRailDisp1"]
            flags = []
            if sw > SW_LIMIT:
                flags.append("尖轨超限!")
            if pr > PR_LIMIT:
                flags.append("心轨超限!")
            print(f"{r['time']:>6.1f}  {sw:>8.2f}  {pr:>8.2f}  {' '.join(flags)}",
                  flush=True)


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8765"
    try:
        asyncio.run(run(url))
    except KeyboardInterrupt:
        print("\n[监视] 已停止")
    except OSError:
        print("\n[失败] 连不上推送服务：ws://localhost:8765 没有响应")
        print("  原因：WebSocket推送服务没在运行（电台没开播，收音机自然收不到）")
        print("  解决：先双击同目录的 WebSocket演示.bat（或运行 WebSocket推送服务.py），")
        print("        等黑窗口显示'已启动'后，再运行本脚本。")
        sys.exit(1)
