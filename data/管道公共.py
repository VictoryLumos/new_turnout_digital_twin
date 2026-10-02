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
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "管道配置.json")

with open(CONFIG_PATH, encoding="utf-8") as _f:
    CONFIG = json.load(_f)

DB = CONFIG["数据库"]
WS_PORT = CONFIG["推送服务"]["端口"]
STEP = CONFIG["推送服务"]["步长秒"]
THRESHOLDS = CONFIG["告警阈值mm"]

TZ8 = timezone(timedelta(hours=8))          # 北京时间
APP_NAME = "turnout-twin-b"                 # 数据库连接名（断线恢复测试按它定位会话）


def now_iso_utc():
    """当前时刻的 UTC ISO 8601 字符串（毫秒 + Z），iTwin 页面 Date.parse 可解析。

    依据《接口契约》：time 为字符串、UTC ISO 8601、必须生成当前时间
    （与浏览器时间差 >10 秒的帧会被拒绝），单调不减。
    """
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------- 校验 ----
# 契约 v2 必填字段：时间 + 五路位移（毫米）
REQUIRED_FIELDS = [
    "time",
    "switchRailDisp1", "switchRailDisp2", "switchRailDisp3",
    "pointRailDisp1", "pointRailDisp2",
]


def validate_rows(rows, source_name="数据", required=None, extra_fields=None):
    """逐行校验数据集。返回 (合法行列表, 错误列表)。

    错误格式：(文件/来源, 行号(从1计), 字段, 原因)
    检查项：结构、必填字段缺失、null、数值类型（拒绝字符串/bool）、
    NaN、Infinity、负时间、时间乱序（非严格递增）。

    required：必填字段（缺失/null 即拒绝）。
    extra_fields：可选字段——键不存在视为缺失（可由适配层模拟并标注），
    但只要出现就必须是有限数值：null、字符串、NaN、Infinity 一律拒绝，
    绝不允许"已提供非法值却被替换成模拟值"。
    """
    required = required or REQUIRED_FIELDS
    extra_fields = extra_fields or []
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
        # 可选字段：出现即校验（已提供非法值必须拒绝，不替换成模拟值）
        for f in extra_fields:
            if f not in row or f in required:
                continue
            v = row[f]
            if v is None:
                errors.append((source_name, line, f, "已提供null（可选字段出现即必须为有限数值）"))
                bad = True
                continue
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                errors.append((source_name, line, f,
                               f"类型非法：期望数值，得到 {type(v).__name__}({v!r})"))
                bad = True
                continue
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                why = "NaN" if math.isnan(v) else "Infinity"
                errors.append((source_name, line, f, why + "（已提供的非法值，拒绝而非模拟替换）"))
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


def load_dataset(path, required=None, extra_fields=None):
    """读数据集 + 强制校验：非法直接终止调用方（exit 1，带定位信息）。"""
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    good, errors = validate_rows(rows, os.path.basename(path),
                                 required=required, extra_fields=extra_fields)
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
            return psycopg2.connect(application_name=APP_NAME, **DB)
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[数据库] 连接失败({attempt}/{retries})：{e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
    raise last


class db_session:
    """事务型会话：with db_session() as cur: cur.execute(字面量SQL, 参数)

    建立连接失败自动重试；适合短请求（脚本、API 只读查询）。
    需要运行中断线恢复的长驻进程请用下方 ResilientDB。
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


class ResilientDB:
    """长连接会话：运行中数据库断线（服务重启/连接被杀/网络中断）自动恢复。

    与 db_session 的区别：db_session 每次调用新建连接（只解决"启动时连不上"），
    本类持有长连接覆盖整个运行周期——run() 中语句遇到
    OperationalError/InterfaceError（连接已死）时：关旧连接 → 重连 → 重放整个
    闭包（指数退避，最多 retries 次），仍失败才抛给调用方记录。

    用法（闭包内必须字面量SQL+参数绑定，且可安全重放；本管道全部为
    SELECT 或 INSERT..ON CONFLICT 幂等写，满足）：
        rdb = ResilientDB()
        rows = rdb.run(lambda cur: (
            cur.execute("SELECT x FROM t WHERE k=%s", (k,)), cur.fetchall())[1])
    """

    def __init__(self, retries=3):
        self.retries = retries
        self.conn = None

    def _ensure_conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = db_connect(retries=1)

    def run(self, fn):
        """执行 fn(cursor) 并提交；连接类异常自动重连重放，返回 fn 的结果。"""
        import psycopg2
        last = None
        for attempt in range(1, self.retries + 1):
            self._ensure_conn()
            try:
                with self.conn.cursor() as cur:
                    result = fn(cur)
                self.conn.commit()
                return result
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last = e
                print(f"[数据库] 运行中断线，重连重试({attempt}/{self.retries})：{e}")
                try:
                    self.conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    self.conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self.conn = None
                time.sleep(min(2.0 ** (attempt - 1), 5.0))
        raise last

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001
                pass
            self.conn = None
