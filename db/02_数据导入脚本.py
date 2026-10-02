# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 数据入库脚本 v2（02，整改版）

对应组长整改令第 1/4/7 条：
  · 入库前强制校验（缺字段/字符串/null/NaN/Inf/乱序 → 拒绝并定位行号）
  · 数据库连接失败自动重试，不直接崩溃（管道公共.db_session）
  · 批次能力：--source 指定来源（um 不再被写死成 fake）、--batch 指定批次、
    时间轴自动错峰（默认排到库中最新数据之后的下一个整点时段），
    换一批数据不再需要清空历史表；--replace-source 仅替换同来源旧数据。

用法：
    python 02_数据导入脚本.py                          # data.json，来源fake
    python 02_数据导入脚本.py --file xx.json --source um --batch UM-1001
    python 02_数据导入脚本.py --file xx.json --source um --base "2026-10-05 08:00:00"
    python 02_数据导入脚本.py --file xx.json --source um --replace-source
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "data"))
from 管道公共 import db_session, validate_rows, format_errors  # noqa: E402

BASE_REQUIRED = ["time", "switchRailDisp1", "pointRailDisp1"]

# 数据字段 → 测点编码（《完整版》映射表全量 20 项）
FIELD_TO_POINT = {
    "switchRailDisp1": "T01-SR-01-DISP", "switchRailDisp2": "T01-SR-02-DISP",
    "switchRailDisp3": "T01-SR-03-DISP",
    "switchRailForce1": "T01-SR-01-FORCE", "switchRailForce2": "T01-SR-02-FORCE",
    "switchRailForce3": "T01-SR-03-FORCE",
    "pointRailDisp1": "T01-PR-01-DISP", "pointRailDisp2": "T01-PR-02-DISP",
    "pointRailForce1": "T01-PR-01-FORCE", "pointRailForce2": "T01-PR-02-FORCE",
    "switchMachineCurrent": "T01-SM-01-CURR", "switchMachinePower": "T01-SM-01-POWER",
    "lockStatus": "T01-SM-01-LOCK", "closeStatus": "T01-SR-01-CLOSE",
    "frogVibration": "T01-FR-01-VIB", "frogWear": "T01-FR-01-WEAR",
    "wheelRailForceLateral": "T01-FR-01-WRF-L",
    "wheelRailForceVertical": "T01-FR-01-WRF-V",
    "guardRailDisp": "T01-GR-01-DISP", "railTemperature": "T01-BR-01-TEMP",
}

INSERT_SQL = ("INSERT INTO timeseries_data (point_code, ts, value, source, batch) "
              "VALUES %s ON CONFLICT (point_code, ts) DO NOTHING")


def next_free_hour():
    """查询库中最大时刻，返回其后推 1 小时的整点作为本批时间轴起点。"""
    with db_session() as cur:
        cur.execute("SELECT max(ts) FROM timeseries_data")  # 字面量SQL
        row = cur.fetchone()
    if row and row[0] is not None:
        nxt = row[0].astimezone(timezone(timedelta(hours=8)))
        nxt = (nxt.replace(minute=0, second=0, microsecond=0)
               + timedelta(hours=1, minutes=0) + timedelta(hours=1))
        return nxt
    return datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))


def main():
    parser = argparse.ArgumentParser(description="数据入库 v2（批次/来源/错峰）")
    parser.add_argument("--file", default=os.path.join(
        os.path.dirname(HERE), "data", "data.json"))
    parser.add_argument("--source", default="fake",
                        help="数据来源：fake / um / 仿真-xxx / 自定义（禁止把UM写成fake）")
    parser.add_argument("--batch", default=None,
                        help="批次号；默认自动 IMP-日期-时分")
    parser.add_argument("--base", default=None,
                        help='本批时间轴起点 "YYYY-MM-DD HH:MM:SS"（默认自动错峰）')
    parser.add_argument("--replace-source", action="store_true",
                        help="先删除同来源旧数据再导入（按来源替换，不清全表）")
    args = parser.parse_args()

    # 1) 读取 + 强制校验
    with open(args.file, encoding="utf-8") as f:
        rows = json.load(f)
    good, errors = validate_rows(rows, os.path.basename(args.file),
                                 required=BASE_REQUIRED)
    if errors:
        print(f"[校验失败] {args.file} 共 {len(errors)} 处非法，拒绝入库：")
        print(format_errors(errors))
        sys.exit(1)
    print(f"[校验] 通过：{len(good)} 条（{args.file}）")

    # 2) 批次与时间轴
    batch = args.batch or datetime.now().strftime("IMP-%Y%m%d-%H%M")
    if args.base:
        base = datetime.strptime(args.base, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone(timedelta(hours=8)))
    else:
        base = next_free_hour()
    print(f"[批次] source={args.source} batch={batch} 时间轴起点={base:%Y-%m-%d %H:%M}")

    # 3) 组装（全部参数绑定，无SQL拼接）
    from psycopg2.extras import execute_values
    payload = []
    for r in good:
        ts = base + timedelta(seconds=float(r["time"]))
        for field, point in FIELD_TO_POINT.items():
            if field in r and r[field] is not None:
                payload.append((point, ts, float(r[field]), args.source, batch))

    # 4) 入库（同来源替换可选；冲突行跳过保证幂等）
    with db_session() as cur:
        if args.replace_source:
            cur.execute("DELETE FROM timeseries_data WHERE source = %s",
                        (args.source,))
            print(f"[替换] 已删除同来源旧数据：{cur.rowcount} 条")
        execute_values(cur, INSERT_SQL, payload,
                       template="(%s, %s, %s, %s, %s)")
        cur.execute("SELECT count(*) FROM timeseries_data "
                    "WHERE batch = %s", (batch,))
        n_batch = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM timeseries_data "
                    "WHERE value > 150 AND point_code = %s",
                    ("T01-SR-01-DISP",))
        n_over = cur.fetchone()[0]
    print(f"[完成] 本批写入 {len(payload)} 条；批次 {batch} 共 {n_batch} 条；"
          f"库内尖轨>150mm 共 {n_over} 条（跨批累计）")


if __name__ == "__main__":
    main()
