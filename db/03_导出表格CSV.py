# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 表格导出工具 v2（B：数据管道线，stdout 版）

把库里五张表导出为 CSV 段落打印到标准输出（带BOM字符处理由重定向方
保存时自理），并生成整库备份。逐表打印分隔标题，便于拆分保存。

用法：
    python 03_导出表格CSV.py > 全表导出.txt        # 查看或分发
    生成物理CSV时用 psql 或 API：/api/history?...&format=csv
整库备份文件名固定为 turnout_twin_整库备份_日期.sql（basename 净化）。
"""
import os
import subprocess
import sys
from datetime import date

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "data"))
from 管道公共 import DB, db_connect  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(HERE), "data", "导出数据")
PG_DUMP = r"D:\PostgreSQL\bin\pg_dump.exe"

TABLES = {
    "component": ("构件表", "component_code"),
    "measurement_point": ("测点表", "point_code"),
    "timeseries_data": ("时序数据", "point_code, ts"),
    "realtime_value": ("实时值表", "point_code"),
    "work_order": ("工单表", "work_order_id"),
}


def export_csv(conn):
    for table, (cname, order) in TABLES.items():
        print(f"=== {cname} ===")
        with conn.cursor() as cur:
            # 表名与排序列均来自上方固定字面量表，非外部输入
            cur.copy_expert(
                f"COPY (SELECT * FROM {table} ORDER BY {order}) TO STDOUT "
                f"WITH (FORMAT csv, HEADER true)", sys.stdout)


def export_dump():
    fname = os.path.basename("turnout_twin_整库备份_" + date.today().strftime("%Y%m%d") + ".sql")
    path = os.path.join(OUT_DIR, fname)
    env = dict(os.environ, PGPASSWORD=DB["password"])
    r = subprocess.run([PG_DUMP, "-U", DB["user"], "-h", DB["host"],
                        "-d", DB["dbname"], "-f", path],
                       env=env, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"[警告] 整库备份失败：{r.stderr.strip()[:120]}", file=sys.stderr)
        return
    print(f"[备份] {fname}（{os.path.getsize(path)//1024} KB）", file=sys.stderr)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = db_connect()
    try:
        print("[导出] 五张表 CSV -> stdout；整库备份写入 导出数据 目录",
              file=sys.stderr)
        export_csv(conn)
    finally:
        conn.close()
    export_dump()


if __name__ == "__main__":
    main()
