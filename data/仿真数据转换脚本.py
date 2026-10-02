# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 仿真数据转换脚本 v2（B：数据管道线，stdout 版，整改第二轮）

把 A 的 UM 或 MATLAB 导出的仿真 CSV 转成契约 JSON，结果打印到标准输出；
需要保存时用重定向（示例见下）。

整改第二轮升级（组长反馈第2条）：
  · 五路位移列支持：switchRailDisp2/3、pointRailDisp2 列存在时一并转换输出，
    不再丢掉三路；列不存在时输出两路（下游推送层按设计动程比例模拟并标注）
  · 有限值强制检查：float("NaN")/"Infinity" 在 Python 里能解析成功，
    必须再用 math.isfinite 拒绝——非法行立即报错定位行号，不输出非法 JSON
  · 时间乱序/负数由 管道公共.validate_rows 统一复检（与推送/入库同一判据）

用法（命令行）：
    python 仿真数据转换脚本.py --file 输入.csv > 输出.json
    python 仿真数据转换脚本.py --file 输入.csv --step 0.01 > 输出.json

输入要求：首行为表头，含时间列与位移列（列名在下方 CONFIG 调整）；
时间为秒、位移为毫米、步长均匀。A 首次交付后只需改 CONFIG 列名。
"""
import argparse
import csv
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from 管道公共 import validate_rows  # noqa: E402

# ===== CONFIG：列名映射（按 A 实际导出格式调整，改完群通知）=====
TIME_COL = "time"
SWITCH_COL = "switchRailDisp1"
POINT_COL = "pointRailDisp1"
# 可选三路（有则转换保留，无则跳过——由推送层模拟并标注，转换层不擅自补值）
OPTIONAL_COLS = ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"]

STEP = 0.1
DECIMALS = 2


def _parse_num(raw, col, line):
    """解析单元格为有限数值：字符串/空/NaN/Infinity 一律报错定位行号。"""
    try:
        v = float(raw)
    except (ValueError, TypeError) as e:
        sys.exit(f"[失败] 第 {line} 行列[{col}] 数值解析失败：{raw!r}（{e}）")
    if not math.isfinite(v):
        sys.exit(f"[失败] 第 {line} 行列[{col}] 非有限值：{raw!r}"
                 "（NaN/Infinity 拒绝，不得进入契约 JSON）")
    return v


def convert(rows_iter, step, opt_cols):
    out = []
    prev_t = None
    for i, r in enumerate(rows_iter):
        line = i + 2  # 含表头
        t = round(_parse_num(r[TIME_COL], TIME_COL, line), 3)
        row = {"time": t,
               "switchRailDisp1": round(_parse_num(r[SWITCH_COL], SWITCH_COL, line), DECIMALS),
               "pointRailDisp1": round(_parse_num(r[POINT_COL], POINT_COL, line), DECIMALS)}
        for col in opt_cols:  # 五路 CSV 输入不丢三路：有列就转换保留
            row[col] = round(_parse_num(r[col], col, line), DECIMALS)
        if prev_t is not None:
            delta = round(t - prev_t, 6)
            if abs(delta - step) > 1e-6:
                sys.exit(f"[失败] 第 {line} 行步长 {delta}s 与要求 {step}s 不符"
                         f"（前一行 t={prev_t}）。数据缺行或步长不匀，"
                         f"请 A 重新导出，或用 --step 指定实际步长。")
        prev_t = t
        out.append(row)
    return out


def main():
    parser = argparse.ArgumentParser(
        description="仿真CSV转契约JSON v2（五路保留+有限值校验；stdout输出）")
    parser.add_argument("--file", required=True, help="输入CSV路径")
    parser.add_argument("--step", type=float, default=STEP,
                        help=f"时间步长（默认 {STEP}s）")
    args = parser.parse_args()

    with open(args.file, encoding="utf-8-sig", newline="") as f:  # 只读输入
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        for col in (TIME_COL, SWITCH_COL, POINT_COL):
            if col not in header:
                sys.exit(f"[失败] CSV 缺少列 {col}；实际表头 {header}。"
                         f"请把脚本顶部 CONFIG 改成 A 实际列名。")
        opt_cols = [c for c in OPTIONAL_COLS if c in header]
        skipped = [c for c in OPTIONAL_COLS if c not in header]
        rows = list(reader)
    if not rows:
        sys.exit("[失败] CSV 没有数据行")

    out = convert(rows, args.step, opt_cols)
    # 与推送/入库同一判据复检（时间乱序等；三路已出现则按"出现即校验"把关）
    good, errors = validate_rows(out, os.path.basename(args.file),
                                 required=[TIME_COL, SWITCH_COL, POINT_COL],
                                 extra_fields=OPTIONAL_COLS)
    if errors:
        from 管道公共 import format_errors
        sys.exit("[失败] 转换结果复检未通过（不应发生，请反馈）：\n"
                 + format_errors(errors))
    # 摘要走 stderr，stdout 只输出纯净 JSON（便于重定向与后续校验/入库）
    sw_max = max(r["switchRailDisp1"] for r in out)
    pr_max = max(r["pointRailDisp1"] for r in out)
    print(f"[成功] {len(out)} 条；时间 {out[0]['time']}~{out[-1]['time']}s；"
          f"位移列：两路必带" +
          (f" + 三路保留 {opt_cols}" if opt_cols else
           f"（三路列缺失由推送层按设计动程比例模拟并标注 simFields）"),
          file=sys.stderr)
    if skipped:
        print(f"[提示] CSV 未包含列：{skipped}（五路输入请让 A 带上这三列）",
              file=sys.stderr)
    print(f"[成功] 尖轨峰值 {sw_max}mm（阈值150，{'会超限' if sw_max > 150 else '不超限'}）、"
          f"心轨峰值 {pr_max}mm（阈值100，{'会超限' if pr_max > 100 else '不超限'}）；"
          f"保存示例：python 仿真数据转换脚本.py --file 输入.csv > 目标.json",
          file=sys.stderr)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
