# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 仿真数据转换脚本（B：数据管道线，stdout 版）

把 A 的 UM 或 MATLAB 导出的仿真 CSV 转成契约 JSON，结果打印到标准输出；
需要保存时用重定向（示例见下）。逐行校验字段、数值类型与步长均匀性，
非法行立即报错并定位行号，不输出非法 JSON。

用法（命令行）：
    python 仿真数据转换脚本.py --file 输入.csv > 输出.json
    python 仿真数据转换脚本.py --file 输入.csv --step 0.01 > 输出.json

输入要求：首行为表头，含时间列与两路位移列（列名在下方 CONFIG 调整）；
时间为秒、位移为毫米、步长均匀。A 首次交付后只需改 CONFIG 三个列名。
"""
import argparse
import csv
import json
import sys

# ===== CONFIG：列名映射（按 A 实际导出格式调整，改完群通知）=====
TIME_COL = "time"
SWITCH_COL = "switchRailDisp1"
POINT_COL = "pointRailDisp1"

STEP = 0.1
DECIMALS = 2


def convert(rows_iter, step):
    out = []
    prev_t = None
    for i, r in enumerate(rows_iter):
        line = i + 2  # 含表头
        try:
            t = round(float(r[TIME_COL]), 3)
            sw = round(float(r[SWITCH_COL]), DECIMALS)
            pr = round(float(r[POINT_COL]), DECIMALS)
        except (ValueError, TypeError, KeyError) as e:
            sys.exit(f"[失败] 第 {line} 行数值解析失败：{e}")
        if prev_t is not None:
            delta = round(t - prev_t, 6)
            if abs(delta - step) > 1e-6:
                sys.exit(f"[失败] 第 {line} 行步长 {delta}s 与要求 {step}s 不符"
                         f"（前一行 t={prev_t}）。数据缺行或步长不匀，"
                         f"请 A 重新导出，或用 --step 指定实际步长。")
        prev_t = t
        out.append({"time": t, "switchRailDisp1": sw, "pointRailDisp1": pr})
    return out


def main():
    parser = argparse.ArgumentParser(
        description="仿真CSV转契约JSON（结果打到stdout，用重定向保存）")
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
        rows = list(reader)
    if not rows:
        sys.exit("[失败] CSV 没有数据行")

    out = convert(rows, args.step)
    # 摘要走 stderr，stdout 只输出纯净 JSON（便于重定向与后续校验/入库）
    sw_max = max(r["switchRailDisp1"] for r in out)
    pr_max = max(r["pointRailDisp1"] for r in out)
    print(f"[成功] {len(out)} 条；时间 {out[0]['time']}~{out[-1]['time']}s；"
          f"尖轨峰值 {sw_max}mm（阈值150，{'会超限' if sw_max > 150 else '不超限'}）、"
          f"心轨峰值 {pr_max}mm（阈值100，{'会超限' if pr_max > 100 else '不超限'}）；"
          f"保存示例：python 仿真数据转换脚本.py --file 输入.csv > 目标.json",
          file=sys.stderr)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
