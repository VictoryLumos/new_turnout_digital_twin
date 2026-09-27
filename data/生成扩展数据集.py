# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 全测点四工况扩展数据集生成器（B：数据管道线，完整版升级 P1）

作用：
    《完整版》演示要求"多构件、多测点展示"和"行为模型"（正常转换、卡阻、
    密贴不良、锁闭失败）。本脚本按编码映射表的全部 20 个测点生成四种
    工况的仿真数据集（每工况 100 行、0.1s 步长、10 秒），物理量纲合理：
      - 正常转换：S形位移、电流启停尖峰、力峰值、到位锁闭
      - 卡阻：3.5s 卡住 → 电流爬升至保护切除（跳0）→ 表示不到位
      - 密贴不良：转换完成但缺口异常，closeStatus 抖动
      - 锁闭失败：位移到位、密贴正常，但 lockStatus 始终 0，电流三次锁闭尝试
    另含 8.2~8.8s "列车通过辙叉"脉冲（轮轨力/振动尖峰）。

输出：data/扩展数据集v2/工况_*.json（4 个文件）
契约：字段名与《完整版》映射表逐一对应；time/switchRailDisp1/pointRailDisp1
      与基础版契约兼容（前 80 行同分布），C 不改代码也能读。
用法：python 生成扩展数据集.py
"""
import json
import math
import os
import random

random.seed(42)  # 固定种子保证可复现

STEP, N = 0.1, 100
TS = [round(i * STEP, 1) for i in range(N)]
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "扩展数据集v2")

U = 220.0  # 转辙机等效电压(V)，P = U*I


def smoothstep(x):
    x = max(0.0, min(1.0, x))
    return 3 * x * x - 2 * x * x * x


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def noise(amp):
    return random.uniform(-amp, amp)


def main_disp(cond, t):
    """尖轨牵引点1主位移曲线（mm）。返回 (位移, 是否卡阻中, 保护已切除)"""
    if cond == "正常转换":
        return 160 * smoothstep(t / 6.0), False, False
    if cond == "卡阻":
        if t < 3.5:
            return 160 * smoothstep(t / 6.0), False, False
        if t < 5.6:
            return 88.0, True, False          # 卡在 88mm，电流爬升
        return 88.0, False, True              # 5.6s 保护切除，保持卡位
    if cond == "密贴不良":
        # 完成转换但终点只有 152mm（缺口超限 8mm）
        return 152 * smoothstep(t / 6.0), False, False
    if cond == "锁闭失败":
        return 160 * smoothstep(t / 6.0), False, False
    raise ValueError(cond)


def build_row(cond, t, prev_disp):
    disp, jam, tripped = main_disp(cond, t)
    speed = max(0.0, disp - prev_disp) / STEP          # mm/s
    moving = speed > 1.0 and not tripped               # 位移在变=电机在转（到位后电流回落）

    # ---- 尖轨三牵引点位移（2/3 点位置靠后、幅值略小）----
    d1 = disp
    d2 = max(0.0, disp * 0.98 - 2)
    d3 = max(0.0, disp * 0.96 - 4)

    # ---- 心轨（跟随尖轨，延迟 0.4s）----
    pr1 = clamp(95 * smoothstep((t - 0.5) / 6.0) * (disp / 160 if cond == "卡阻" else 1), 0, 95)
    if cond == "密贴不良":
        pr1 = clamp(pr1 * 152 / 160, 0, 95)
    pr2 = max(0.0, pr1 * 0.97 - 2)

    # ---- 牵引力（摩擦底值 + 速度分量 + 解锁/锁闭尖峰；卡阻时阻力暴涨）----
    base_f = 280 + 260 * speed / 40.0
    f1 = base_f
    if cond == "卡阻" and jam:
        f1 = 280 + 3900 * (t - 3.5) / 2.1              # 卡阻力线性暴涨
    if tripped:
        f1 = 0.0                                        # 保护切除，电机断电
    if abs(t - 0.2) < 0.15 or abs(t - 6.1) < 0.15:     # 解锁/锁闭瞬间尖峰
        f1 += 900
    f1 = clamp(f1 + noise(60), 0, 5000)
    f2, f3 = f1 * 0.92, f1 * 0.85
    pf1, pf2 = f1 * 0.7, f1 * 0.65

    # ---- 转辙机电流（A）----
    if tripped:
        cur = 0.0
    elif cond == "卡阻" and jam:
        cur = 3.0 + 6.5 * (t - 3.5) / 2.1              # 爬升到 ~9.5A 触发保护
    elif moving:
        cur = 3.0 + noise(0.3)
    elif cond == "锁闭失败" and 5.9 < t < 8.2:
        cur = 5.0 if int(t * 10) % 9 < 2 else 0.5      # 三次锁闭尝试的电流冲击
    else:
        cur = 0.5 + noise(0.05)                        # 维持电流
    if abs(t - 0.1) < 0.1:                             # 启动尖峰
        cur = 6.0
    if cond != "卡阻" and abs(t - 6.1) < 0.12:         # 锁闭尖峰
        cur = 5.2
    cur = clamp(cur, 0, 10)

    # ---- 表示状态 ----
    if cond == "正常转换":
        lock, close = (1 if t >= 6.3 else 0), (1 if t >= 6.2 else 0)
    elif cond == "卡阻":
        lock, close = 0, 0
    elif cond == "密贴不良":
        close = 1 if (6.0 <= t <= 7.5) and int(t * 10) % 4 < 2 else (1 if t > 7.5 else 0)
        lock = 1 if t >= 6.3 else 0
    else:  # 锁闭失败：密贴到位但锁闭表示缺失
        lock, close = 0, (1 if t >= 6.2 else 0)

    # ---- 辙叉/轮轨/环境（8.2~8.8s 列车通过脉冲）----
    pass_by = 8.2 <= t <= 8.8
    vib = (0.6 + 2.5 * speed / 40.0 + noise(0.3)) if not tripped else noise(0.2)
    if jam:
        vib += 4.0 if 3.4 < t < 3.7 else 0             # 卡阻瞬间冲击
    if pass_by:
        vib = 18 + noise(4)
    wrf_l = 32 + noise(4) if pass_by else noise(1.5)
    wrf_v = 120 + noise(8) if pass_by else noise(3)
    wear = 0.10 + 0.0002 * len(TS) * 0 + 0.001 * t
    guard = max(0.0, 0.3 + noise(0.25))
    # 轨温：转换摩擦生热（转换段缓升约0.06℃）+ 微小随机波动
    temp = 23.4 + 0.06 * smoothstep(t / 6.0) + noise(0.01)

    row = {"time": t,
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
    return row, disp


def build_condition(cond):
    rows, prev = [], 0.0
    for t in TS:
        row, disp = build_row(cond, t, prev)
        rows.append(row)
        prev = disp
    return rows


def validate(all_data):
    print("=== 契约与物理合理性校验 ===")
    fields_ref = None
    for cond, rows in all_data.items():
        steps = {round(rows[i + 1]["time"] - rows[i]["time"], 6) for i in range(len(rows) - 1)}
        assert steps == {STEP}, f"{cond} 步长异常"
        if fields_ref is None:
            fields_ref = sorted(rows[0].keys())
        for r in rows:
            assert sorted(r.keys()) == fields_ref, f"{cond} 字段不一致"
        sw_over = sum(r["switchRailDisp1"] > 150 for r in rows)
        cur_max = max(r["switchMachineCurrent"] for r in rows)
        lock_end = rows[-1]["lockStatus"]
        close_end = rows[-1]["closeStatus"]
        print(f"  {cond}: {len(rows)}行 | 尖轨终点 {rows[-1]['switchRailDisp1']:>6}mm | "
              f"超限{sw_over:>2}行 | 电流峰值 {cur_max:>4}A | "
              f"末态 锁闭={lock_end} 密贴={close_end}")
    print(f"  字段清单（{len(fields_ref)}项）: {', '.join(fields_ref)}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    conds = ["正常转换", "卡阻", "密贴不良", "锁闭失败"]
    all_data = {c: build_condition(c) for c in conds}
    validate(all_data)
    for cond, rows in all_data.items():
        path = os.path.join(OUT_DIR, f"工况_{cond}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=1)
        print(f"  已写出 {os.path.relpath(path, HERE)}")
    print("[完成] 四工况数据集生成。入库/推送扩展待群确认契约 v2 后进行。")


if __name__ == "__main__":
    main()
