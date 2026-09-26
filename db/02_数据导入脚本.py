# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— data.json 导入数据库脚本（B：数据管道线）

用法（装好 PostgreSQL 并执行过 01_建表与种子数据.sql 之后）：
    pip install psycopg2-binary
    python 02_数据导入脚本.py                    # 默认导入 ../data/data.json
    python 02_数据导入脚本.py --file 其他.json   # 导入指定文件（如 UM 导出的同格式数据）
    python 02_数据导入脚本.py --reset            # 先清空时序表再导入（换数据源时必加，见下）

注意：
    时序表主键是 (测点, 时间戳)。假数据和 UM 数据的时间都是 0~7.9s，
    不加 --reset 直接导新数据会因主键相同被"重复跳过"，新数据进不去。
    所以：重复导同一文件 → 不加（自动跳过）；换成新数据源 → 加 --reset。

连接参数默认读环境变量 PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE，
未设置时用下方 DB_CONFIG 默认值（把 password 改成你安装 PostgreSQL 时设的密码）。
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),  # TODO: 改成自己的密码
    "dbname": os.environ.get("PGDATABASE", "turnout_twin"),
}

# 数据字段 → 测点编码 映射（来自《完整版》编码映射表，A 确认后不得随意更改）
FIELD_TO_POINT = {
    "switchRailDisp1": "T01-SR-01-DISP",
    "pointRailDisp1": "T01-PR-01-DISP",
}

# 循环播放由前端处理，库里只存这 8 秒原始段；
# 基准时间固定保证重复导入结果一致，配合 ON CONFLICT 可重复执行。
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))


def main():
    parser = argparse.ArgumentParser(description="导入 data.json 到 timeseries_data 表")
    parser.add_argument("--file", default=os.path.join(os.path.dirname(__file__),
                                                        "..", "data", "data.json"))
    parser.add_argument("--reset", action="store_true",
                        help="导入前清空 timeseries_data（换数据源时使用）")
    args = parser.parse_args()

    try:
        import psycopg2
    except ImportError:
        sys.exit("缺少依赖：请先执行  pip install psycopg2-binary")

    with open(args.file, encoding="utf-8") as f:
        rows = json.load(f)

    records = []
    for row in rows:
        ts = BASE_TS + timedelta(seconds=row["time"])
        for field, point_code in FIELD_TO_POINT.items():
            if field in row:
                records.append((ts, point_code, float(row[field]), "fake"))

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                if args.reset:
                    cur.execute("DELETE FROM timeseries_data")
                    print(f"[--reset] 已清空时序表（原 {cur.rowcount} 条）")
                cur.executemany(
                    "INSERT INTO timeseries_data (ts, point_code, value, source) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (point_code, ts) DO NOTHING",
                    records,
                )
                cur.execute("SELECT count(*) FROM timeseries_data")
                total = cur.fetchone()[0]
                # 验证：超阈值记录数（阈值来自测点表，尖轨>150mm 的那段应被查出）
                cur.execute(
                    "SELECT count(*) FROM timeseries_data d "
                    "JOIN measurement_point p ON p.point_code = d.point_code "
                    "WHERE d.value > p.alarm_threshold"
                )
                alarms = cur.fetchone()[0]
        print(f"导入完成：本次 {len(records)} 条（重复自动跳过），"
              f"库中共 {total} 条，其中超阈值 {alarms} 条")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
