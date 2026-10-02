# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 管道公共模块（B：数据管道线，整改版核心组件）

集中提供三样东西（对应组长整改令第1/4/8条）：
  1. 统一配置：所有脚本从 管道配置.json 读取数据库/端口/阈值，不再散落
  2. 数据校验：逐行检查必填字段/数值类型/NaN/Inf/时间乱序，
     错误信息定位到 文件+行号+字段，拒绝后不得继续推送或入库
  3. 数据库会话：连接失败自动重试（指数退避）；本模块不执行任何SQL，
     各调用方在自己文件内使用字面量SQL + 参数绑定（%s），禁止拼接

用法：
    from 管道公共 import db_session
    with db_session() as cur:            # 已连接并开启事务
        cur.execute("SELECT ... WHERE x=%s", (参数,))   # 字面量SQL + 绑定
"""
import json
import math
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "管道配置.json")

with open(CONFIG_PATH, encoding="utf-8") as _f:
    CONFIG = json.load(_f)

DB = CONFIG["数据库"]
WS_PORT = CONFIG["推送服务"]["端口"]
STEP = CONFIG["推送服务"]["步长秒"]
THRESHOLDS = CONFIG["告警阈值mm"]


# ---------------------------------------------------------------- 校验 ----
# 契约 v2 必填字段：时间 + 五路位移（毫米）
REQUIRED_FIELDS = [
    "time",
    "switchRailDisp1", "switchRailDisp2", "switchRailDisp3",
    "pointRailDisp1", "pointRailDisp2",
]


def validate_rows(rows, source_name="数据", required=None):
    """逐行校验数据集。返回 (合法行列表, 错误列表)。

    错误格式：(文件/来源, 行号(从1计), 字段, 原因)
    检查项：结构、必填字段缺失、null、数值类型（拒绝字符串/bool）、
    NaN、Infinity、负时间、时间乱序（非严格递增）。
    """
    required = required or REQUIRED_FIELDS
    errors = []
    good = []
    prev_t = None
    for i, row in enumerate(rows):
        line = i + 1
        if not isinstance(row, dict):
            errors.append((source_name, line, "(整行)", "不是JSON对象"))
            continue
        bad = False
        for f in required:
            v = row.get(f)
            if v is None:
                errors.append((source_name, line, f, "缺失或为null"))
                bad = True
                continue
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                errors.append((source_name, line, f,
                               f"类型非法：期望数值，得到 {type(v).__name__}({v!r})"))
                bad = True
                continue
            if math.isnan(v):
                errors.append((source_name, line, f, "NaN"))
                bad = True
                continue
            if math.isinf(v):
                errors.append((source_name, line, f, "Infinity"))
                bad = True
        t = row.get("time")
        if not bad and isinstance(t, (int, float)) and not isinstance(t, bool):
            if t < 0:
                errors.append((source_name, line, "time", f"时间为负数 {t}"))
                bad = True
            elif prev_t is not None and t <= prev_t:
                errors.append((source_name, line, "time",
                               f"时间乱序：{t} 未大于前一行 {prev_t}"))
                bad = True
            else:
                prev_t = t
        if not bad:
            good.append(row)
    return good, errors


def format_errors(errors, limit=20):
    lines = [f"  {src} 行{ln} 字段[{fld}]：{why}" for src, ln, fld, why in errors[:limit]]
    if len(errors) > limit:
        lines.append(f"  … 其余 {len(errors) - limit} 条略")
    return "\n".join(lines)


def alarm_of(field, value):
    """告警判定：等于阈值不告警，严格大于才告警（契约语义）。"""
    th = THRESHOLDS.get(field)
    return th is not None and value > th


def load_dataset(path, required=None):
    """读数据集 + 强制校验：非法直接终止调用方（exit 1，带定位信息）。"""
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    good, errors = validate_rows(rows, os.path.basename(path), required=required)
    if errors:
        print(f"[校验失败] {path} 共 {len(errors)} 处非法：")
        print(format_errors(errors))
        sys.exit(1)
    return good


# ------------------------------------------------------------ 数据库 ----
def db_connect(retries=3, delay=1.0):
    """带重试的连接（指数退避），失败抛最后一次异常。"""
    import psycopg2
    last = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**DB)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[数据库] 连接失败({attempt}/{retries})：{e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
    raise last


class db_session:
    """事务型会话：with db_session() as cur: cur.execute(字面量SQL, 参数)

    连接中断（OperationalError）自动重建连接并整体重试，最多 retries 次；
    重试仍失败则抛出异常，由调用方记录——不静默、不崩溃退出进程。
    注意：回调内必须使用字面量SQL与参数绑定，保证重放安全。
    """

    def __init__(self, retries=3):
        self.retries = retries
        self.conn = None

    def __enter__(self):
        import psycopg2
        last = None
        for attempt in range(1, self.retries + 1):
            try:
                self.conn = db_connect(retries=1)
                self.conn.autocommit = False
                return self.conn.cursor()
            except psycopg2.OperationalError as e:
                last = e
                print(f"[数据库] 会话建立失败({attempt}/{self.retries})：{e}")
                time.sleep(1.0)
        raise last

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.conn is not None:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
        finally:
            if self.conn is not None:
                try:
                    self.conn.close()
                except Exception:  # noqa: BLE001
                    pass
        return False
