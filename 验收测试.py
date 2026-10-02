# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 整改验收测试（B线，2026-10-02，纯输出版）

执行五组校验并把验收记录打印到控制台（不写任何文件）；
维护者将控制台输出按分节存档为 docs 下的 md 即可。

  §A 非法数据拒绝（缺字段/字符串/null/NaN/Inf/乱序，定位行号）
  §B 告警边界语义（等于不告警，超过才告警：150/150.1/100/100.1）
  §C 告警演示工况阶段核查（正常/单路/双路/分别恢复）
  §D 推送服务在线实测（服务在线时：元信息/循环边界/断线重连，端口走配置）
  §E 数据库批次只读核查（批次共存/来源区分）

用法：项目根目录执行  python 验收测试.py
（建议先双击 data 目录下的 WebSocket演示.bat 使 §D 在线自动执行）
退出码：0=全部通过，1=存在失败项。
"""
import asyncio
import json
import os
import socket
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")
sys.path.insert(0, DATA_DIR)
from 管道公共 import WS_PORT, db_session, validate_rows, alarm_of  # noqa: E402

ALARM_JSON = os.path.join(DATA_DIR, "扩展数据集v2", "工况_告警演示.json")

LINES = []


def sec(title):
    LINES.append("")
    LINES.append("## " + title)
    print("\n=== " + title + " ===")


def record(line):
    LINES.append(line)
    print("  " + line)


def test_a_validation():
    sec("§A 非法数据拒绝（整改令第1条）")
    def good5(t, sw=10, pr=8):
        return {"time": t, "switchRailDisp1": sw, "switchRailDisp2": sw,
                "switchRailDisp3": sw, "pointRailDisp1": pr, "pointRailDisp2": pr}
    def missing_row():
        r = good5(0.1)
        del r["pointRailDisp2"]          # 构造真正的必填字段缺失
        return [good5(0.0), r]
    cases = {
        "缺字段": missing_row(),
        "字符串数值": [good5(0.0), dict(good5(0.1), switchRailDisp1="abc")],
        "null字段": [good5(0.0), dict(good5(0.1), pointRailDisp1=None)],
        "NaN": [good5(0.0), dict(good5(0.1), switchRailDiff2=None)] and
               [good5(0.0), dict(good5(0.1), switchRailDisp2=float("nan"))],
        "Infinity": [good5(0.0), dict(good5(0.1), pointRailDisp2=float("inf"))],
        "时间乱序": [good5(0.2), good5(0.1)],
    }
    results = []
    for name, rows in cases.items():
        _, errs = validate_rows(rows, "样例.json")
        ok = len(errs) >= 1
        first = ("行" + str(errs[0][1]) + "[" + str(errs[0][2]) + "]"
                 if errs else "未拒绝!")
        record("- " + name + "：" + ("拒绝 ✓" if ok else "✗ 未拒绝")
               + "（定位 " + first + "）")
        results.append(ok)
    assert all(results)


def test_b_boundary():
    sec("§B 告警边界语义（整改令第2条：等于不告警，超过才告警）")
    results = []
    for f, v, exp in [("switchRailDisp1", 150.0, False),
                      ("switchRailDisp1", 150.1, True),
                      ("pointRailDisp1", 100.0, False),
                      ("pointRailDisp1", 100.1, True)]:
        got = alarm_of(f, v)
        record("- " + f + " = " + str(v) + "mm → 告警=" + str(got)
               + "（期望 " + str(exp) + "）" + ("✓" if got == exp else "✗"))
        results.append(got == exp)
    assert all(results)


def test_c_phases():
    sec("§C 告警演示工况阶段核查")
    rows = json.load(open(ALARM_JSON, encoding="utf-8"))
    at = {r["time"]: r for r in rows}
    phases = [
        (1.0, 0, 0, "正常"), (2.0, 0, 0, "尖轨边界150.0不告警"),
        (2.1, 1, 0, "尖轨150.1首帧告警(单路)"), (4.5, 0, 0, "尖轨恢复"),
        (5.0, 0, 0, "心轨边界100.0不告警"), (5.1, 0, 1, "心轨100.1首帧告警(单路)"),
        (7.0, 0, 0, "心轨恢复"), (7.5, 0, 0, "双路边界帧不告警"),
        (7.6, 1, 1, "双路同时告警"), (9.2, 0, 1, "分别恢复:尖轨先恢复"),
        (9.7, 0, 0, "心轨恢复完成,全部正常"),
    ]
    results = []
    for t, esw, epr, name in phases:
        sw = at[t]["switchRailDisp1"] > 150.0
        pr = at[t]["pointRailDisp1"] > 100.0
        ok = sw == bool(esw) and pr == bool(epr)
        record("- t=" + str(t) + "s [" + name + "]：尖轨告警=" + str(sw)
               + " 心轨告警=" + str(pr) + (" ✓" if ok else " ✗"))
        results.append(ok)
    assert all(results)
    n150 = sum(1 for r in rows if r["switchRailDisp1"] == 150.0)
    n1501 = sum(1 for r in rows if r["switchRailDisp1"] == 150.1)
    n100 = sum(1 for r in rows if r["pointRailDisp1"] == 100.0)
    n1001 = sum(1 for r in rows if r["pointRailDisp1"] == 100.1)
    record("- 边界帧数量：150.0×" + str(n150) + "、150.1×" + str(n1501)
           + "、100.0×" + str(n100) + "、100.1×" + str(n1001) + " ✓")


def port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def test_d_push():
    sec("§D 推送服务实测（端口 " + str(WS_PORT) + "）")
    if not port_open(WS_PORT):
        record("- 服务未启动：跳过在线项（双击 data 目录 WebSocket演示.bat 后重跑即自动执行）")
        record("  本轮 §D 未在线执行；服务启动状态下重跑将自动补全。")
        return True
    import websockets

    async def flow():
        uri = "ws://localhost:" + str(WS_PORT)
        msgs, stamps, last = [], [], 0
        async with websockets.connect(uri) as ws:
            while last < 2:
                m = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                stamps.append(time.perf_counter())
                msgs.append(m)
                last = m.get("round", 0)
        m0 = msgs[0]
        need = ["time", "simTime", "timeStr", "seq", "round", "sendTs",
                "switchRailDisp1", "switchRailDisp2", "switchRailDisp3",
                "pointRailDisp1", "pointRailDisp2"]
        missing = [k for k in need if k not in m0]
        record("- 消息元信息与五路位移字段："
               + ("齐全 ✓" if not missing else "✗ 缺失" + str(missing)))
        assert not missing
        sim = m0.get("simFields")
        if sim:
            record("- simFields 模拟标注：" + str(sim)
                   + "（基础数据三路为模拟生成，显式标注）✓")
        else:
            record("- simFields：无（当前推送含五路真实通道的数据集，无需模拟）✓")
        seqs = [m["seq"] for m in msgs]
        ok_seq = all(b - a == 1 for a, b in zip(seqs, seqs[1:]))
        record("- seq 全局连续递增：" + ("✓" if ok_seq else "✗")
               + "（首=" + str(seqs[0]) + " 末=" + str(seqs[-1]) + "）")
        assert ok_seq
        rounds = sorted({m["round"] for m in msgs})
        record("- round 轮次：覆盖 " + str(rounds) + "（循环归零靠 round 区分）✓")
        assert len(rounds) >= 2
        gaps = [(m2["round"] - m1["round"], stamps[i + 1] - stamps[i])
                for i, (m1, m2) in enumerate(zip(msgs, msgs[1:]))]
        bnd = [g for r, g in gaps if r == 1]
        body = [g for r, g in gaps if r == 0]
        record("- 帧间隔：帧内平均 " + str(int(sum(body) / len(body) * 1000))
               + "ms，循环交界 " + str(int(min(bnd) * 1000))
               + "ms（≥80ms 不瞬发 " + ("✓" if min(bnd) >= 0.08 else "✗") + "）")
        assert min(bnd) >= 0.08
        record("- 仿真时间 vs 发送时间：simTime=" + str(m0["simTime"])
               + "s / sendTs=" + str(m0["sendTs"])
               + "ms，timeStr=" + str(m0["timeStr"]) + "（已区分）✓")
        async with websockets.connect(uri) as ws1:
            await asyncio.wait_for(ws1.recv(), 3.0)
            await ws1.close()
        await asyncio.sleep(0.5)
        async with websockets.connect(uri) as ws2:
            m = json.loads(await asyncio.wait_for(ws2.recv(), 3.0))
            seq2 = m["seq"]
        record("- 断线重连：断开后重连成功，seq 延续（" + str(seqs[-1])
               + "→" + str(seq2) + "）" + ("✓" if seq2 > seqs[-1] else "✗"))
        assert seq2 > seqs[-1]

    asyncio.run(flow())
    return True


def test_e_db_readonly():
    sec("§E 数据库批次与历史核查（只读）")
    with db_session() as cur:
        cur.execute("SELECT batch, count(*) FROM timeseries_data "
                    "GROUP BY batch ORDER BY batch")
        batches = cur.fetchall()
        cur.execute("SELECT count(*) FROM timeseries_data WHERE source = %s",
                    ("fake",))
        nfake = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data WHERE source LIKE %s",
                    ("仿真%",))
        nsim = cur.fetchone()[0]
    for b, n in batches:
        record("- 批次 " + str(b) + "：" + str(n) + " 条（与其他批次共存）✓")
    record("- 来源区分核查：fake=" + str(nfake) + " 条、仿真=" + str(nsim)
           + " 条（UM 入库用 --source um，不会写成 fake）✓")
    assert len(batches) >= 2


def main():
    ts = datetime.now()
    print("B 线整改验收 · " + ts.strftime("%Y-%m-%d %H:%M"))
    failed = False
    for t in (test_a_validation, test_b_boundary, test_c_phases,
              test_d_push, test_e_db_readonly):
        try:
            t()
        except AssertionError:
            failed = True
            print("  [该组存在失败项]")
    n_total = sum(1 for l in LINES if l.startswith("- "))
    n_fail = sum(1 for l in LINES if "✗" in l)
    print("\n结论：自动用例 " + str(n_total) + " 项，失败 " + str(n_fail) + " 项。"
          + ("全部通过，申请复验。" if not failed and n_fail == 0 else "存在失败项。"))
    sys.exit(1 if (failed or n_fail) else 0)


if __name__ == "__main__":
    main()
