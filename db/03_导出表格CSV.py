# -*- coding: utf-8 -*-
"""
道岔数字孪生 —— 表格导出 CSV（B：数据管道线）

作用：
    把库里的表导出成 CSV 文件（带 BOM，Excel/WPS 双击即开），
    并生成整库备份 .sql。队友拿到仓库后不装数据库也能查看数据；
    备份 .sql 可在有 PostgreSQL 的机器上整库还原。

用法：
    python 03_导出表格CSV.py
    输出到 ../data/导出数据/ ，提交进仓库即可共享。
"""
import csv
import os
import subprocess
import sys
from datetime import date

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "..", "data", "导出数据")

DB = dict(host="localhost", port=5432, user="postgres",
          password="postgres", dbname="turnout_twin")

PG_DUMP = r"D:\PostgreSQL\bin\pg_dump.exe"

# 表名 → (导出文件名, 排序字段)；v2 新表存在才导（旧库不报错）
TABLES = {
    "component":         ("构件表.csv",     "component_code"),
    "measurement_point": ("测点表.csv",     "point_code"),
    "timeseries_data":   ("时序数据.csv",   "point_code, ts"),
    "realtime_value":    ("实时值表.csv",   "point_code"),
    "work_order":        ("工单表.csv",     "work_order_id"),
    "alarm_event":       ("告警事件审计.csv", "ts"),
    "health_score":      ("健康度存档.csv", "id"),
    "maintenance_log":   ("维修记录.csv",   "id"),
}

# 视图 → 导出文件名（05_数据库升级v2 的分析视图，一屏统计给队友/报告用）
VIEWS = {
    "v_source_stats":   "视图_数据源统计.csv",
    "v_alarm_records":  "视图_超阈明细.csv",
    "v_data_quality":   "视图_数据质量.csv",
}


def _has(cur, name):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
    return cur.fetchone()[0]


def export_csv(conn):
    count = 0
    with conn.cursor() as cur:
        for table, (fname, order) in TABLES.items():
            if not _has(cur, table):
                continue
            path = os.path.join(OUT_DIR, fname)
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                cur.copy_expert(
                    f"COPY (SELECT * FROM {table} ORDER BY {order}) TO STDOUT "
                    f"WITH (FORMAT csv, HEADER true)", f)
            n = sum(1 for _ in open(path, encoding="utf-8-sig")) - 1
            print(f"  {fname:<16} {n:>6} 行")
            count += 1
        for view, fname in VIEWS.items():
            if not _has(cur, view):
                continue
            path = os.path.join(OUT_DIR, fname)
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                cur.copy_expert(
                    f"COPY (SELECT * FROM {view}) TO STDOUT "
                    f"WITH (FORMAT csv, HEADER true)", f)
            n = sum(1 for _ in open(path, encoding="utf-8-sig")) - 1
            print(f"  {fname:<16} {n:>6} 行")
            count += 1
    return count


def export_dump():
    path = os.path.join(OUT_DIR, f"turnout_twin_整库备份_{date.today():%Y%m%d}.sql")
    env = dict(os.environ, PGPASSWORD=DB["password"])
    r = subprocess.run([PG_DUMP, "-U", DB["user"], "-h", DB["host"],
                        "-d", DB["dbname"], "-f", path],
                       env=env, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  [警告] 整库备份失败：{r.stderr.strip()[:120]}")
        return
    size = os.path.getsize(path) // 1024
    print(f"  整库备份.sql     {size} KB（还原命令见文件名同目录 README 或设计文档）")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = psycopg2.connect(**DB)
    try:
        print(f"[导出] 目标目录：{os.path.normpath(OUT_DIR)}")
        n = export_csv(conn)
        export_dump()
        print(f"[完成] {n} 张表 CSV + 整库备份，提交进仓库队友即可用 Excel 查看")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
