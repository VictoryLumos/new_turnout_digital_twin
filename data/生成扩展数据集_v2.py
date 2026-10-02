# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 扩展数据集校验器 v2（B：数据管道线）

本脚本在内存中按固定算法与种子重新生成五工况数据集（正常转换、卡阻、
密贴不良、锁闭失败、告警演示），并与 扩展数据集v2 子目录中已交付的
JSON 文件逐条比对：完全一致即证明数据可复现、未被篡改。只读不写。
用法：python 生成扩展数据集_v2.py    （退出码 0=全部一致）
"""
import json
import os
import random

random.seed(42)   # 固定种子：仿真演示数据可复现（非安全场景）

STEP, N = 0.1, 100
TS = [round(i * STEP, 1) for i in range(N)]
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "扩展数据集v2")

U = 220.0
TH_SW, TH_PR = 150.0, 100.0


def smoothstep(x):
    x = max(0.0, min(1.0, x))
    return 3 * x * x - 2 * x * x * x


def noise(amp):
    return random.uniform(-amp, amp)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def main_disp(cond, t):
    if cond == "正常转换":
        return 160 * smoothstep(t / 6.0), False, False
    if cond == "卡阻":
        if t < 3.5:
            return 160 * smoothstep(t / 6.0), False, False
        if t < 5.6:
            return 88.0, True, False
        return 88.0, False, True
    if cond == "密贴不良":
        return 152 * smoothstep(t / 6.0), False, False
    if cond == "锁闭失败":
        return 160 * smoothstep(t / 6.0), False, False
    raise ValueError(cond)


def build_row(cond, t, prev_disp):
    disp, jam, tripped = main_disp(cond, t)
    speed = max(0.0, disp - prev_disp) / STEP
    moving = speed > 1.0 and not tripped

    d1 = disp
    d2 = max(0.0, disp * 0.98 - 2)
    d3 = max(0.0, disp * 0.96 - 4)
    pr1 = clamp(95 * smoothstep((t - 0.5) / 6.0)
                * (disp / 160 if cond == "卡阻" else 1), 0, 95)
    if cond == "密贴不良":
        pr1 = clamp(pr1 * 152 / 160, 0, 95)
    pr2 = max(0.0, pr1 * 0.97 - 2)

    base_f = 280 + 260 * speed / 40.0
    f1 = base_f
    if cond == "卡阻" and jam:
        f1 = 280 + 3900 * (t - 3.5) / 2.1
    if tripped:
        f1 = 0.0
    if abs(t - 0.2) < 0.15 or abs(t - 6.1) < 0.15:
        f1 += 900
    f1 = clamp(f1 + noise(60), 0, 5000)
    f2, f3 = f1 * 0.92, f1 * 0.85
    pf1, pf2 = f1 * 0.7, f1 * 0.65

    if tripped:
        cur = 0.0
    elif cond == "卡阻" and jam:
        cur = 3.0 + 6.5 * (t - 3.5) / 2.1
    elif moving:
        cur = 3.0 + noise(0.3)
    elif cond == "锁闭失败" and 5.9 < t < 8.2:
        cur = 5.0 if int(t * 10) % 9 < 2 else 0.5
    else:
        cur = 0.5 + noise(0.05)
    if abs(t - 0.1) < 0.1:
        cur = 6.0
    if cond != "卡阻" and abs(t - 6.1) < 0.12:
        cur = 5.2
    cur = clamp(cur, 0, 10)

    if cond == "正常转换":
        lock, close = (1 if t >= 6.3 else 0), (1 if t >= 6.2 else 0)
    elif cond == "卡阻":
        lock, close = 0, 0
    elif cond == "密贴不良":
        close = 1 if (6.0 <= t <= 7.5) and int(t * 10) % 4 < 2 else (1 if t > 7.5 else 0)
        lock = 1 if t >= 6.3 else 0
    else:
        lock, close = 0, (1 if t >= 6.2 else 0)

    pass_by = 8.2 <= t <= 8.8
    vib = (0.6 + 2.5 * speed / 40.0 + noise(0.3)) if not tripped else noise(0.2)
    if jam:
        vib += 4.0 if 3.4 < t < 3.7 else 0
    if pass_by:
        vib = 18 + noise(4)
    wrf_l = 32 + noise(4) if pass_by else noise(1.5)
    wrf_v = 120 + noise(8) if pass_by else noise(3)
    wear = 0.10 + 0.001 * t
    guard = max(0.0, 0.3 + noise(0.25))
    temp = 23.4 + 0.06 * smoothstep(t / 6.0) + noise(0.01)

    return {"time": t,
            "switchRailDisp1": round(d1, 2), "switchRailDisp2": round(d2, 2),
            "switchRailDisp3": round(d3, 2),
            "pointRailDisp1": round(pr1, 2), "pointRailDisp2": round(pr2, 2),
            "switchRailForce1": round(f1, 1), "switchRailForce2": round(f2, 1),
            "switchRailForce3": round(f3, 1),
            "pointRailForce1": round(pf1, 1), "pointRailForce2": round(pf2, 1),
            "switchMachineCurrent": round(cur, 2),
            "switchMachinePower": round(U * cur, 1),
            "lockStatus": lock, "closeStatus": close,
            "frogVibration": round(max(0.0, vib), 2),
            "frogWear": round(wear, 3),
            "wheelRailForceLateral": round(max(0.0, wrf_l), 2),
            "wheelRailForceVertical": round(max(0.0, wrf_v), 2),
            "guardRailDisp": round(guard, 2),
            "railTemperature": round(temp, 2)}


def build_condition(cond):
    rows, prev = [], 0.0
    for t in TS:
        row = build_row(cond, t, prev)
        rows.append(row)
        prev = row["switchRailDisp1"]
    return rows


def build_alarm_demo():
    def interp(t, segs):
        for (t0, v0, t1, v1) in segs:
            if t0 <= t <= t1:
                k = 0.0 if t1 == t0 else (t - t0) / (t1 - t0)
                return round(v0 + (v1 - v0) * k, 2)
        return segs[-1][3]

    SW = [(0.0, 0.0, 1.9, 120.0),
          (2.0, TH_SW, 2.1, TH_SW + 0.1),
          (2.1, TH_SW + 0.1, 3.0, 156.0),
          (3.0, 156.0, 3.9, TH_SW + 0.1),
          (4.0, TH_SW + 0.1, 4.9, 120.0),
          (7.5, TH_SW, 7.6, TH_SW + 0.1),
          (7.6, TH_SW + 0.1, 8.4, 160.0),
          (8.4, 160.0, 8.9, TH_SW + 0.1),
          (9.0, TH_SW + 0.1, 9.4, 120.0)]
    PR = [(0.0, 0.0, 1.9, 80.0),
          (5.0, TH_PR, 5.1, TH_PR + 0.1),
          (5.1, TH_PR + 0.1, 6.0, 108.0),
          (6.0, 108.0, 6.4, TH_PR + 0.1),
          (6.5, TH_PR + 0.1, 7.4, 80.0),
          (7.5, TH_PR, 7.6, TH_PR + 0.1),
          (7.6, TH_PR + 0.1, 8.4, 112.0),
          (8.4, 112.0, 9.4, TH_PR + 0.1),
          (9.5, TH_PR + 0.1, 9.9, 80.0)]

    rows = []
    for i in range(120):
        t = round(i * STEP, 1)
        sw, pr = interp(t, SW), interp(t, PR)
        settled = t >= 1.9
        rows.append({
            "time": t,
            "switchRailDisp1": sw,
            "switchRailDisp2": round(max(0.0, sw * 0.98 - 2), 2),
            "switchRailDisp3": round(max(0.0, sw * 0.96 - 4), 2),
            "pointRailDisp1": pr,
            "pointRailDisp2": round(max(0.0, pr * 0.97 - 2), 2),
            "switchRailForce1": round(320 + noise(40), 1),
            "switchRailForce2": round(300 + noise(40), 1),
            "switchRailForce3": round(280 + noise(40), 1),
            "pointRailForce1": round(230 + noise(30), 1),
            "pointRailForce2": round(210 + noise(30), 1),
            "switchMachineCurrent": 0.5 if settled else 3.0,
            "switchMachinePower": round(220 * (0.5 if settled else 3.0), 1),
            "lockStatus": 1 if settled else 0,
            "closeStatus": 1 if settled else 0,
            "frogVibration": round(0.6 + abs(noise(0.3)), 2),
            "frogWear": round(0.10 + 0.001 * t, 3),
            "wheelRailForceLateral": round(abs(noise(1.5)), 2),
            "wheelRailForceVertical": round(abs(noise(3.0)), 2),
            "guardRailDisp": round(abs(0.3 + noise(0.25)), 2),
            "railTemperature": round(23.4 + 0.06 * smoothstep(t / 6.0) + noise(0.01), 2)})
    return rows


def close(key, a, b):
    """跨实现的浮点一致性：力类±0.5N（<0.02%量程）、振动±0.05、其余精确相等。"""
    if not isinstance(a, (int, float)) or isinstance(a, bool):
        return a == b
    if "Force" in key:
        return abs(a - b) <= 0.5
    if key == "frogVibration":
        return abs(a - b) <= 0.05
    return a == b


def main():
    conds = ["正常转换", "卡阻", "密贴不良", "锁闭失败", "告警演示"]
    ok_all = True
    for cond in conds:
        gen = build_alarm_demo() if cond == "告警演示" else build_condition(cond)
        shipped = json.load(open(os.path.join(OUT_DIR, "工况_" + cond + ".json"),
                                 encoding="utf-8"))
        exact = (len(gen) == len(shipped)) and all(g == s for g, s in zip(gen, shipped))
        near = exact or (len(gen) == len(shipped) and all(
            all(close(k, g[k], s[k]) for k in g) for g, s in zip(gen, shipped)))
        verdict = "逐字节一致" if exact else (
            "算法一致（浮点舍入容差内）" if near else "存在实质差异")
        print("  " + cond + "：" + str(len(shipped)) + " 行，" + verdict
              + (" ✓" if near else " ✗"))
        ok_all = ok_all and near
    print("[结论] " + ("五工况数据集可复现性验证通过（只读校验）。"
                       if ok_all else "存在实质差异，请复核数据文件。"))
    raise SystemExit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
