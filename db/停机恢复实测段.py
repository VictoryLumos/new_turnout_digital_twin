# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 数据库真实停机恢复实测段（组长第四轮"数据库完整验收"）

由 data/数据库停机恢复实测.bat（管理员）按阶段调用：
    --before  停机前：记录 timeseries 行数到 data/_停机行数.txt
    --during  停机中：ResilientDB 建连应失败并进入重试流程（不静默），
              结果追加到 docs/整改验证记录_第四轮_停机实测.md
    --after   恢复后：连接自愈 + 行数与停机前一致（不重复写入），追加记录

独立脚本、只做本阶段的事；SQL 均为字面量；文件写入限制在本项目目录内。
"""
import contextlib
import io
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "data"))
from 管道公共 import ResilientDB, db_session  # noqa: E402

ROOT = os.path.dirname(HERE)
COUNT_FILE = os.path.join(os.path.dirname(HERE), "data", "_停机行数.txt")
REC_FILE = os.path.join(ROOT, "docs", "整改验证记录_第四轮_停机实测.md")


def stamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append(line):
    # 追加写入本项目 docs 下的固定常量路径（禁止越界）
    fd = os.open(REC_FILE, os.O_APPEND | os.O_CREAT | os.O_WRONLY)
    with os.fdopen(fd, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "--before":
        with db_session() as cur:
            cur.execute("SELECT count(*) FROM timeseries_data")
            n = cur.fetchone()[0]
        fd = os.open(COUNT_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(n))
        append(f"- {stamp()} 停机前：timeseries_data 行数 = {n}")
    elif mode == "--during":
        buf = io.StringIO()
        ok_fail = False
        try:
            with contextlib.redirect_stdout(buf):
                ResilientDB(retries=2).run(
                    lambda cur: (cur.execute("SELECT 1"), cur.fetchone())[1])
        except Exception:  # noqa: BLE001 停机中重试耗尽后抛出（预期行为）
            ok_fail = True
        n_retry = (buf.getvalue().count("重连失败")
                   + buf.getvalue().count("连接失败"))
        ok = ok_fail and n_retry >= 1
        append(f"- {stamp()} 停机期间：连接失败进入重试流程（输出 {n_retry} 次"
               f"重试提示）后抛出，不静默 → {'✓' if ok else '✗（异常：停机中竟然连上了？）'}")
        sys.exit(0 if ok else 1)
    elif mode == "--after":
        with open(COUNT_FILE, encoding="utf-8") as f:
            n0 = int(f.read().strip())
        v = ResilientDB().run(lambda cur: (cur.execute("SELECT 1"),
                                           cur.fetchone())[1])
        with db_session() as cur:
            cur.execute("SELECT count(*) FROM timeseries_data")
            n1 = cur.fetchone()[0]
        ok = v == (1,) and n0 == n1
        append(f"- {stamp()} 恢复后：SELECT 1 → {v}，timeseries 行数 {n0}→{n1}"
               f"（不变=不重复写入）→ {'✓' if ok else '✗'}")
        sys.exit(0 if ok else 1)
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
