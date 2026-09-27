# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 数据服务 API（B：数据管道线，完整版升级 P3+P4，v2.1 工程化）

对应《完整版》服务层与接入协议条款的实现：
    状态实时监测   GET  /api/realtime          实时值表全量（含测点信息/告警）
    健康度评估     GET  /api/health           规则评分（优/良/预警/故障）+ 扣分明细
    告警监测       GET  /api/alarms           当前超阈值测点
    工单联动       GET  /api/workorders       告警自动生成工单（回放期间）
    历史查询       GET  /api/history/{测点}   时序数据（历史回放的数据源）
    行为模型回放   POST /api/play/{工况}      把四工况数据集实时注入实时值表
                  POST /api/stop            停止回放
    数据集清单     GET  /api/conditions
    工况自动识别   GET  /api/identify         回放窗口统计特征 → 四工况最近邻识别（v2.1）
    故障诊断       GET  /api/diagnosis        规则扣分项 + 自动识别 → 维修建议
    系统状态       GET  /api/stats            表行数 / 查询延迟 / 回放状态（v2.1）

v2.1 工程化升级（答辩"难度与完整度"支撑点）：
    1) 数据库连接池（ThreadedConnectionPool），替代每请求新建连接；
    2) CORS 全开——C 的页面可跨源直接调本 API（联调必需）；
    3) 数据库不可用返回 503 + 排查提示，不再是 500 堆栈；
    4) 工况自动识别：8 维统计特征 + 加权最近邻（同相位前缀参考，回放中途即可
       识别），输出置信度与特征依据，属可解释的统计识别（答辩口径：统计特征
       匹配，非深度学习）。

用法：
    pip install fastapi uvicorn psycopg2-binary
    python -m uvicorn 数据服务API:app --host 0.0.0.0 --port 8000
    浏览器打开 http://localhost:8000/  （首页有可视化状态板和接口清单）

演示剧本（给组长/答辩用）：
    1) 打开首页看到 健康度100/优；
    2) POST /api/play/卡阻  → 实时值开始变化，电流爬升；
    3) 刷新 /api/health → 评分跌到 55/故障，/api/workorders 自动多出工单；
    4) 等 3 秒看"工况自动识别"→ 卡阻（置信度 90%+，数据驱动判工况）；
    5) POST /api/play/正常转换 或点工单"维修闭环" → 恢复。
"""
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

HERE = os.path.dirname(os.path.abspath(__file__))
DS_DIR = os.path.join(HERE, "扩展数据集v2")
CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败"]

DB = dict(host="localhost", port=5432, user="postgres",
          password="postgres", dbname="turnout_twin")
BASE_TS = datetime(2026, 9, 26, 0, 0, 0, tzinfo=timezone(timedelta(hours=8)))

# 数据字段 → 测点编码（《完整版》编码映射表全量 20 项）
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
CODE_OF = {name: code for name, code in FIELD_TO_POINT.items()}

app = FastAPI(title="道岔数字孪生数据服务", version="2.1")
# CORS 全开：C 的页面（file:// 或其他端口/主机）跨源调本 API 不被浏览器拦截
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])
START_AT = datetime.now()
_play = {"thread": None, "stop": threading.Event(), "condition": None, "t": None,
         "window": deque(maxlen=100), "lock": threading.Lock(),
         "health_hist": deque(maxlen=600)}   # (仿真时刻, 评分)，最近60s，状态板画迷你趋势

_pool = None
_pool_lock = threading.Lock()


def q(sql, args=(), fetch=True):
    """经连接池执行 SQL（v2.1 前为每次请求新建连接）"""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ThreadedConnectionPool(1, 8, **DB)
    conn = _pool.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if fetch else None
    finally:
        _pool.putconn(conn)


@app.exception_handler(psycopg2.Error)
def db_down(request, exc):
    """数据库不可用 → 503 + 排查提示（压测/联调时不出现 500 堆栈）"""
    return JSONResponse(status_code=503, content={
        "error": "数据库暂不可用",
        "hint": "先启动 PostgreSQL：管理员运行 net start postgresql-x64-17",
        "detail": str(exc).split("\n")[0]})


def tables_ok(*names):
    """存档表是否已建（05_数据库升级v2.sql）；旧库缺表时相关功能自动降级跳过"""
    try:
        return all(q("SELECT to_regclass(%s) IS NOT NULL", (n,))[0][0] for n in names)
    except psycopg2.Error:
        return False


def latest_values():
    """取实时值表最新值 {point_code: value}"""
    return {p: v for p, v in q("SELECT point_code, value FROM realtime_value")}


def health_check(vals):
    """规则模型：健康度评分 + 扣分明细。返回 (score, grade, issues)

    稳态门：电机运行中（电流≥1A）只判"过程类"故障（过流/超程），
    "结果类"故障（未到位/锁闭/密贴）只在电机空闲（转换动作已结束）时判定，
    避免把转换过程中的正常过渡态误报成故障。
    """
    g = lambda c: vals.get(CODE_OF[c])
    disp, cur = g("switchRailDisp1"), g("switchMachineCurrent")
    lock, close = g("lockStatus"), g("closeStatus")
    steady = cur is not None and cur < 1
    score, issues = 100.0, []
    rules = [
        (cur is not None and cur > 8, 30, "转辙机过流", "T01-SM-01-CURR"),
        (disp is not None and disp > 165, 25, "到位超程", "T01-SR-01-DISP"),
    ]
    if steady and disp is not None:
        rules += [
            (close == 0 and disp > 20, 30, "转换中断", "T01-SR-01-DISP"),
            (disp >= 155 and close == 1 and lock == 0, 35, "锁闭失败", "T01-SM-01-LOCK"),
            (140 <= disp < 158 and lock == 1, 25, "密贴不良/到位不足", "T01-SR-01-CLOSE"),
            (lock == 0 and close == 0 and disp > 50, 15, "未锁闭且未密贴", "T01-SM-01-LOCK"),
        ]
    for fired, penalty, name, point in rules:
        if fired:
            score -= penalty
            issues.append({"规则": name, "扣分": penalty, "关联测点": point})
    score = clamp_score(score)
    grade = "优" if score >= 90 else "良" if score >= 75 else "预警" if score >= 60 else "故障"
    return score, grade, issues


def clamp_score(s):
    return max(0.0, min(100.0, s))


# ===================== 工况自动识别（v2.1：统计特征最近邻） =====================
# 思路：把一段回放窗口压成 8 维可解释统计特征，与四个工况数据集的参考特征
# 做加权距离匹配，取最近者为识别结果，1 - d_min/Σd 为置信度。
# 答辩口径：可解释的统计特征匹配（非深度学习），数据来自四工况仿真数据集。

FEATURE_SCALES = {   # 各特征归一化量纲尺度
    "终点位移": 160.0, "电流均值": 3.0, "电流峰值": 10.0, "末段电流": 3.0,
    "锁闭末态": 1.0, "密贴末态": 1.0, "密贴抖动": 4.0, "位移全程变化": 160.0,
}
FEATURE_WEIGHTS = {  # 权重体现判据重要性：位移结果 > 电流过程 > 表示状态
    "终点位移": 0.25, "电流均值": 0.15, "电流峰值": 0.10, "末段电流": 0.15,
    "锁闭末态": 0.15, "密贴末态": 0.10, "密贴抖动": 0.05, "位移全程变化": 0.05,
}
_REF_FEATURES = None
_PREFIX_REFS = None   # {工况: {前缀长度L: 特征}}：回放进行到第 L 行就和各工况前 L 行比


def extract_features(rows):
    """一段数据窗口 → 8 维统计特征（None 表示窗口为空）"""
    if not rows:
        return None
    disp = [float(r.get("switchRailDisp1", 0.0)) for r in rows]
    curr = [float(r.get("switchMachineCurrent", 0.0)) for r in rows]
    close = [int(r.get("closeStatus", 0)) for r in rows]
    tail_n = max(1, len(curr) // 5)  # 末 20% 电流（锁闭失败的重试脉冲藏在这里）
    return {
        "终点位移": disp[-1],
        "电流均值": sum(curr) / len(curr),
        "电流峰值": max(curr),
        "末段电流": sum(curr[-tail_n:]) / tail_n,
        "锁闭末态": int(rows[-1].get("lockStatus", 0)),
        "密贴末态": close[-1],
        "密贴抖动": sum(1 for i in range(1, len(close)) if close[i] != close[i - 1]),
        "位移全程变化": max(disp) - min(disp),
    }


def feature_distance(a, b):
    return sum(FEATURE_WEIGHTS[k] * ((a[k] - b[k]) / FEATURE_SCALES[k]) ** 2
               for k in FEATURE_WEIGHTS)


def ref_features():
    """四工况全段参考特征（首次调用时从扩展数据集计算并缓存）"""
    global _REF_FEATURES
    if _REF_FEATURES is None:
        refs = {}
        for c in CONDITIONS:
            path = os.path.join(DS_DIR, f"工况_{c}.json")
            if os.path.exists(path):
                feat = extract_features(json.load(open(path, encoding="utf-8")))
                if feat:
                    refs[c] = feat
        _REF_FEATURES = refs
    return _REF_FEATURES


def prefix_ref_features():
    """四工况按前缀长度的参考特征（30..全段）：同相位比对，回放中途也能识别

    例：窗口积到 60 行（t=0~5.9s），就和每个工况数据集的前 60 行比——
    避免拿"转换进行中"的窗口去和"整段已结束"的参考比而误判。
    注意：正常转换与锁闭失败前 6 秒数据本就相同，此阶段两者距离并列、
    置信度趋近 0，属于数据本身不可分，等锁闭状态出现后自然分开。
    """
    global _PREFIX_REFS
    if _PREFIX_REFS is None:
        pre = {}
        for c in CONDITIONS:
            path = os.path.join(DS_DIR, f"工况_{c}.json")
            if not os.path.exists(path):
                continue
            rows = json.load(open(path, encoding="utf-8"))
            pre[c] = {L: extract_features(rows[:L]) for L in range(30, len(rows) + 1)}
        _PREFIX_REFS = pre
    return _PREFIX_REFS


def do_identify():
    """当前回放窗口 → {识别工况, 置信度, 距离, 特征…}；窗口不足时说明原因

    置信度 = 1 - 最近距离/次近距离：两个工况前段数据相同时（本就不可分）
    置信度诚实趋近 0，而不是虚高。
    """
    with _play["lock"]:
        rows = list(_play["window"])
    L = len(rows)
    refs = prefix_ref_features()
    if not refs:
        return {"识别工况": None, "说明": "扩展数据集v2 缺失，无法计算参考特征"}
    if L < 30:
        return {"识别工况": None,
                "说明": f"回放窗口仅 {L} 行（需≥30 行≈3 秒数据），识别待数据积累"}
    feat = extract_features(rows)
    dist = {c: feature_distance(feat, pre[L]) for c, pre in refs.items() if L in pre}
    ranked = sorted(dist, key=dist.get)
    best, second = ranked[0], ranked[1]
    conf = max(0.0, 1.0 - dist[best] / dist[second]) if dist[second] > 0 else 0.0
    return {
        "识别工况": best,
        "置信度": round(min(conf, 0.99), 3),
        "次接近": f"{second}（距离 {dist[second]:.3f}）",
        "特征依据": "终点位移 {:.0f}mm、电流峰值 {:.1f}A、电流均值 {:.2f}A、"
                    "锁闭末态 {}、密贴末态 {}、密贴抖动 {} 次".format(
                        feat["终点位移"], feat["电流峰值"], feat["电流均值"],
                        feat["锁闭末态"], feat["密贴末态"], feat["密贴抖动"]),
        "各工况距离": {c: round(d, 4) for c, d in sorted(dist.items(), key=lambda kv: kv[1])},
        "特征向量": {k: round(v, 3) for k, v in feat.items()},
        "窗口": {"行数": L, "窗口已满": L >= _play["window"].maxlen,
                 "覆盖秒": f"{rows[0]['time']}~{rows[-1]['time']}s"},
    }


def ensure_work_order(point, remark, issued):
    """告警→工单联动：同一测点+同一问题在一次回放中只开一张工单"""
    key = (point, remark)
    if key in issued:
        return
    stored = "自动生成：" + remark
    exists = q("SELECT 1 FROM work_order WHERE point_code=%s AND remark=%s "
               "AND status='open' LIMIT 1", (point, stored))
    if exists:
        issued.add(key)
        return
    (max_id,) = q("SELECT coalesce(max(work_order_id),'WO-2026-000') FROM work_order "
                  "WHERE left(work_order_id, 8) = 'WO-2026-'")[0]
    new_id = f"WO-2026-{int(max_id.split('-')[-1]) + 1:03d}"
    q("INSERT INTO work_order (work_order_id, point_code, status, remark) "
      "VALUES (%s,%s,'open',%s)", (new_id, point, stored), fetch=False)
    issued.add(key)
    print(f"[工单联动] 自动创建 {new_id}：{point} {remark}")


def playback(condition):
    """回放线程：把工况数据集按 0.1s 注入实时值表，并做健康检查+工单联动"""
    path = os.path.join(DS_DIR, f"工况_{condition}.json")
    rows = json.load(open(path, encoding="utf-8"))
    conn = psycopg2.connect(**DB)
    thresholds = {}
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT point_code, alarm_threshold FROM measurement_point "
                        "WHERE alarm_threshold IS NOT NULL")
            thresholds = dict(cur.fetchall())
    print(f"[回放] 开始注入工况「{condition}」（{len(rows)} 行 × 0.1s）")
    issued = set()
    log_health = tables_ok("health_score")   # 存档表未升级时跳过落库，不影响回放
    try:
        for r in rows:
            if _play["stop"].is_set():
                break
            ts = BASE_TS + timedelta(seconds=r["time"])
            vals = {FIELD_TO_POINT[f]: r[f] for f in r if f in FIELD_TO_POINT}
            score, grade, issues = health_check(vals)
            try:
                with conn:
                    with conn.cursor() as cur:
                        upsert = ("INSERT INTO realtime_value (point_code, value, ts, alarm, updated_at) "
                                  "VALUES %s ON CONFLICT (point_code) DO UPDATE SET "
                                  "value=EXCLUDED.value, ts=EXCLUDED.ts, alarm=EXCLUDED.alarm, "
                                  "updated_at=now()")
                        execute_values(cur, upsert, [
                            (point, r[field], ts,
                             point in thresholds and r[field] > thresholds[point])
                            for field, point in FIELD_TO_POINT.items() if field in r],
                            template="(%s, %s, %s, %s, now())")
                        if log_health and int(round(r["time"] * 10)) % 5 == 0:
                            cur.execute("INSERT INTO health_score "
                                        "(sim_t, score, grade, condition_name) "
                                        "VALUES (%s, %s, %s, %s)",
                                        (r["time"], score, grade, condition))
                _play["t"] = r["time"]
                with _play["lock"]:
                    _play["window"].append(r)
                    _play["health_hist"].append((r["time"], score))
                for it in issues:
                    ensure_work_order(it["关联测点"], it["规则"], issued)
            except Exception:
                import traceback
                traceback.print_exc()
            if int(r["time"] * 10) % 10 == 0:
                print(f"  [回放] t={r['time']:>4.1f}s 健康度={score:.0f}({grade})")
            _play["stop"].wait(0.1)
    finally:
        conn.close()
        print(f"[回放] 工况「{condition}」注入结束")


@app.get("/", response_class=HTMLResponse)
def index():
    score, grade, issues = health_check(latest_values())
    color = {"优": "#22c55e", "良": "#eab308", "预警": "#f97316", "故障": "#ef4444"}[grade]
    conds = "".join(
        f'<a href="javascript:play(\'{c}\')" '
        f'style="margin-right:10px">{c}</a>' for c in CONDITIONS)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>道岔数字孪生 · 数据服务</title>
<style>body{{font-family:"Microsoft YaHei";background:#0b1220;color:#e2e8f0;padding:24px}}
.c{{background:#111c30;border:1px solid #1e2f4d;border-left:3px solid #38bdf8;border-radius:12px;padding:16px 20px;margin:12px 0;max-width:640px}}
a{{color:#38bdf8}} code{{background:#1e2f4d;padding:1px 6px;border-radius:4px}}
button{{background:#2563eb;border:0;color:#fff;border-radius:6px;padding:3px 10px;cursor:pointer;font-size:12px}}
.wos{{font-size:13px;margin-top:6px;line-height:1.9}}</style></head><body>
<h1 style="font-size:18px">道岔数字孪生 · 数据服务
<span style="font-size:12px;color:#64748b;font-weight:normal">B线 · 完整版服务层 v2.1</span></h1>
<div class="c" style="display:flex;gap:22px;align-items:center">
<svg width="116" height="116" viewBox="0 0 116 116" style="flex:none">
<circle cx="58" cy="58" r="48" fill="none" stroke="#1e2f4d" stroke-width="11"/>
<circle id="ring" cx="58" cy="58" r="48" fill="none" stroke="{color}" stroke-width="11"
stroke-linecap="round" stroke-dasharray="301.6" stroke-dashoffset="{301.6 * (1 - score / 100):.1f}"
transform="rotate(-90 58 58)" style="transition:stroke-dashoffset .6s,stroke .6s"/>
<text id="score" x="58" y="52" text-anchor="middle" dominant-baseline="middle"
fill="#e2e8f0" font-size="27" font-weight="bold">{score:.0f}</text>
<text x="58" y="76" text-anchor="middle" fill="#64748b" font-size="11">健康度</text>
<text id="grade" x="58" y="94" text-anchor="middle" fill="{color}" font-size="13" font-weight="bold">{grade}</text>
</svg>
<div style="flex:1;min-width:0">
<div id="cond" style="font-size:13px;color:#64748b">当前回放：无</div>
<div id="issues" style="font-size:13px;color:{'#f87171' if issues else '#94a3b8'};margin-top:4px">
{'；'.join(i['规则'] for i in issues) or '无扣分项'}</div>
<canvas id="hspark" style="width:100%;height:56px;display:block;margin-top:8px"></canvas>
<div style="font-size:11px;color:#475569">本轮回放健康度曲线（虚线=60分故障线）— 注入故障看跳水，维修闭环看回升</div>
</div></div>
<div class="c"><b>行为模型回放</b>（点击注入工况，观察健康度与工单联动）：<br><br>{conds}
<a href="javascript:fetch('/api/stop').then(r=>r.json()).then(loadAll)">停止</a></div>
<div class="c"><b>工况自动识别</b>（8维统计特征 → 四工况最近邻 · 数据驱动判工况，v2.1）
<div id="idc" style="font-size:26px;font-weight:bold;margin:6px 0 2px">--</div>
<div id="idf" style="font-size:12px;color:#94a3b8">等待回放数据…（点上方工况后约3秒出结果）</div></div>
<div class="c"><b>工单看板</b>（告警自动生成 → 维修闭环 → 复测恢复）：<div id="wos" class="wos">加载中…</div></div>
<div class="c"><b>接口清单</b><br>
<a href="/trend">📈 趋势回放页（/trend）</a> · <a href="/api/diagnosis">诊断（/api/diagnosis）</a> · <a href="/api/stats">系统状态（/api/stats）</a><br>
GET /api/realtime 实时监测 · GET /api/alarms 告警 · GET /api/health 健康度<br>
GET /api/workorders 工单 · GET /api/history/测点编码?limit=n 历史查询 · GET /api/points 测点清单<br>
GET /api/conditions 数据集 · GET /api/identify 工况自动识别<br>
POST /api/play/工况 · POST /api/repair/工单号 · POST /api/stop（CORS已开，C页面可跨源直调）</div>
<script>
function play(c){{document.getElementById('issues').textContent='指令已发送：'+c+'（转换动作先正常走约6秒才进入故障段）';
document.getElementById('idc').textContent='…';document.getElementById('idf').textContent='识别窗口积累中…';
fetch('/api/play/'+c,{{method:'POST'}}).then(r=>r.json()).then(()=>setTimeout(loadAll,500))
.catch(()=>{{document.getElementById('issues').textContent='服务未响应：请确认数据服务黑窗口开着'}})}}
function repair(id){{fetch('/api/repair/'+id,{{method:'POST'}}).then(r=>r.json()).then(d=>{{
document.getElementById('issues').textContent=d.msg;loadAll()}})}}
function load(){{
fetch('/api/health').then(r=>r.json()).then(d=>{{
const co={{'优':'#22c55e','良':'#eab308','预警':'#f97316','故障':'#ef4444'}};
const sc=d.健康度评分,c=co[d.等级];
document.getElementById('score').textContent=Math.round(sc);
const g=document.getElementById('grade');g.textContent=d.等级;g.setAttribute('fill',c);
const ring=document.getElementById('ring');
ring.style.stroke=c;ring.setAttribute('stroke-dashoffset',(301.6*(1-sc/100)).toFixed(1));
const is=document.getElementById('issues');
is.textContent=d.扣分明细.map(i=>i.规则).join('；')||'无扣分项';
is.style.color=d.扣分明细.length?'#f87171':'#94a3b8';
document.getElementById('cond').textContent='当前回放：'+d.当前回放+(d.仿真时刻!=null?' · 仿真时刻 '+d.仿真时刻+'s':'')}})}}
function loadWos(){{
fetch('/api/workorders').then(r=>r.json()).then(d=>{{
const open=d.工单.filter(w=>w.状态==='open');
document.getElementById('wos').innerHTML=open.length?
open.map(w=>'<div>'+w.工单号+' · '+w.测点+' · '+w.备注+
' <button onclick="repair(\\''+w.工单号+'\\')">维修闭环</button></div>').join('')
:'暂无未闭环工单（全部已闭环或尚未发生告警）'}})}}
function loadId(){{
fetch('/api/identify').then(r=>r.json()).then(d=>{{
const el=document.getElementById('idc'),fl=document.getElementById('idf');
if(d.识别工况){{
el.textContent=d.识别工况+' · 置信度 '+Math.round(d.置信度*100)+'%';
el.style.color=d.识别工况==='正常转换'?'#22c55e':'#f97316';
fl.textContent=d.特征依据+(d.窗口&&!d.窗口.窗口已满?'（窗口未满，结果为阶段性）':'')+
'；次接近：'+d.次接近;}}
else{{el.textContent='--';el.style.color='#64748b';fl.textContent=d.说明||'无回放数据'}}}})
.catch(()=>{{}})}}
function loadH(){{
fetch('/api/health/trend').then(r=>r.json()).then(d=>{{
const cv=document.getElementById('hspark');const dpr=devicePixelRatio||1;
cv.width=cv.clientWidth*dpr;cv.height=cv.clientHeight*dpr;
const ctx=cv.getContext('2d'),W=cv.width,H=cv.height;ctx.clearRect(0,0,W,H);
if(!d.count)return;
const pts=d.数据,n=pts.length;
const X=i=>2+(W-4)*i/Math.max(n-1,1), Y=v=>4+(H-8)*(1-v/100);
ctx.setLineDash([3*dpr,3*dpr]);ctx.strokeStyle='#ef4444';ctx.beginPath();
ctx.moveTo(2,Y(60));ctx.lineTo(W-2,Y(60));ctx.stroke();ctx.setLineDash([]);
ctx.beginPath();ctx.strokeStyle='#38bdf8';ctx.lineWidth=1.5*dpr;
for(let i=0;i<n;i++){{i?ctx.lineTo(X(i),Y(pts[i].健康度评分)):ctx.moveTo(X(i),Y(pts[i].健康度评分))}}
ctx.stroke();
const last=pts[n-1].健康度评分;
ctx.beginPath();ctx.arc(X(n-1),Y(last),3*dpr,0,7);
ctx.fillStyle=last>=90?'#22c55e':last>=75?'#eab308':last>=60?'#f97316':'#ef4444';
ctx.fill();}}).catch(()=>{{}})}}
function loadAll(){{load();loadWos();loadId();loadH()}}
setInterval(load,2000);setInterval(loadWos,5000);setInterval(loadId,1000);setInterval(loadH,2000);loadAll();
</script></body></html>""")


@app.get("/api/realtime")
def api_realtime():
    rows = q("SELECT r.point_code, p.field_name, p.unit, r.value, r.ts, r.alarm, "
             "p.alarm_threshold, r.updated_at FROM realtime_value r "
             "JOIN measurement_point p USING(point_code) ORDER BY r.point_code")
    return {"count": len(rows), "data": [
        {"测点": p, "字段": f, "值": float(v), "单位": u,
         "仿真时刻": t.strftime("%H:%M:%S.%f")[:-4], "告警": a,
         "阈值": th, "刷新时间": upd.strftime("%H:%M:%S")}
        for p, f, u, v, t, a, th, upd in rows]}


@app.get("/api/alarms")
def api_alarms():
    rows = q("SELECT r.point_code, p.field_name, r.value, p.alarm_threshold, p.unit "
             "FROM realtime_value r JOIN measurement_point p USING(point_code) "
             "WHERE r.alarm = TRUE ORDER BY r.point_code")
    out = {"count": len(rows), "告警": [
        {"测点": p, "字段": f, "当前值": float(v), "阈值": th, "单位": u}
        for p, f, v, th, u in rows]}
    # 数据库层告警审计（05_数据库升级v2 的触发器留痕；旧库未升级时自动省略）
    try:
        ev = q("SELECT point_code, to_char(ts,'MM-DD HH24:MI:SS'), value, threshold, source "
               "FROM alarm_event ORDER BY ts DESC, event_id DESC LIMIT 10")
        out["告警事件审计（数据库触发器留痕，最近10条）"] = [
            {"测点": p, "时刻": t, "值": float(v), "阈值": th, "来源": s}
            for p, t, v, th, s in ev]
    except psycopg2.Error:
        out["告警事件审计"] = "未启用（执行 db/05_数据库升级v2.sql 开启数据库层审计）"
    return out


@app.get("/api/health")
def api_health():
    score, grade, issues = health_check(latest_values())
    return {"健康度评分": score, "等级": grade, "扣分明细": issues,
            "当前回放": _play["condition"] or "无",
            "仿真时刻": _play["t"],
            "评估时间": datetime.now().strftime("%H:%M:%S")}


@app.get("/api/health/trend")
def api_health_trend():
    """健康度历史（本轮回放以来的评分曲线，状态板迷你图数据源）"""
    with _play["lock"]:
        hist = list(_play["health_hist"])
    return {"count": len(hist), "当前回放": _play["condition"] or "无",
            "数据": [{"仿真时刻": t, "健康度评分": round(s, 1)} for t, s in hist]}


@app.get("/api/workorders")
def api_workorders():
    rows = q("SELECT work_order_id, point_code, status, remark FROM work_order "
             "ORDER BY work_order_id")
    out = {"count": len(rows), "工单": [
        {"工单号": w, "测点": p, "状态": s, "备注": r} for w, p, s, r in rows]}
    if tables_ok("maintenance_log"):
        logs = q("SELECT work_order_id, point_code, problem, "
                 "to_char(done_at, 'MM-DD HH24:MI:SS') FROM maintenance_log "
                 "ORDER BY id DESC LIMIT 10")
        out["维修记录（最近10条）"] = [
            {"工单号": w, "测点": p, "问题": prob, "闭环时间": t}
            for w, p, prob, t in logs]
    return out


@app.get("/api/history/{point_code}")
def api_history(point_code: str, limit: int = 160):
    limit = max(1, min(limit, 5000))   # 防御性钳制：一次最多取 5000 点
    if point_code not in set(FIELD_TO_POINT.values()):
        raise HTTPException(404, f"未知测点 {point_code}")
    rows = q("SELECT ts, value, source FROM timeseries_data WHERE point_code=%s "
             "ORDER BY ts DESC LIMIT %s", (point_code, limit))
    return {"测点": point_code, "count": len(rows), "数据": [
        {"时刻": t.strftime("%H:%M:%S.%f")[:-4], "值": float(v), "来源": s}
        for t, v, s in reversed(rows)]}


@app.get("/api/conditions")
def api_conditions():
    out = []
    for c in CONDITIONS:
        path = os.path.join(DS_DIR, f"工况_{c}.json")
        rows = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
        out.append({"工况": c, "行数": len(rows),
                    "字段": len(rows[0]) if rows else 0})
    return {"数据集目录": DS_DIR, "工况": out}


@app.get("/api/identify")
def api_identify():
    """工况自动识别（v2.1）：回放窗口统计特征 → 四工况最近邻匹配 + 置信度

    答辩口径：可解释的统计识别——8 维特征、加权距离、无黑盒；
    参考特征取自四工况数据集本身，识别"没见过的数据"不在能力范围内（诚实边界）。
    """
    return do_identify()


@app.get("/api/stats")
def api_stats():
    """系统状态：表行数、查询延迟、回放/识别引擎状态（压测与答辩"运行状态"卡片）"""
    t0 = time.perf_counter()
    counts = {t: q(f"SELECT count(*) FROM {t}")[0][0] for t in
              ("component", "measurement_point", "timeseries_data",
               "work_order", "realtime_value")}
    for t in ("health_score", "maintenance_log"):    # 存档表（未升级自动省略）
        if tables_ok(t):
            counts[t] = q(f"SELECT count(*) FROM {t}")[0][0]
    latency = round((time.perf_counter() - t0) * 1000, 1)
    # v2 数据库对象（体检脚本 06 的口径）
    try:
        views = q("SELECT count(*) FROM pg_views WHERE schemaname='public' "
                  "AND viewname LIKE 'v_%%'")[0][0]
        trg = q("SELECT count(*) FROM pg_trigger WHERE tgname='trg_timeseries_alarm'")[0][0]
        events = q("SELECT count(*) FROM alarm_event")[0][0]
        db_extra = {"分析视图": views, "告警触发器": trg, "告警事件留痕": events}
    except psycopg2.Error:
        db_extra = "未升级（执行 db/05_数据库升级v2.sql 启用视图/触发器/审计）"
    uptime = datetime.now() - START_AT
    with _play["lock"]:
        win_rows = len(_play["window"])
    return {
        "数据库": {"表行数": counts, "本轮5表查询延迟ms": latency, "v2对象": db_extra},
        "回放": {"当前工况": _play["condition"] or "无", "仿真时刻": _play["t"],
                 "识别窗口行数": win_rows},
        "服务": {"版本": "2.1", "启动时间": START_AT.strftime("%Y-%m-%d %H:%M:%S"),
                 "已运行": f"{uptime.days * 24 + uptime.seconds // 3600}h"
                          f"{uptime.seconds % 3600 // 60:02d}m{uptime.seconds % 60:02d}s",
                 "识别引擎": "统计特征最近邻（8维特征 × 4工况同相位前缀参考）"},
    }


ADVICE = {
    "转换中断": "尖轨转换中断：疑似卡阻或电机保护动作。建议检查摩擦联结器状态、尖轨滑床板有无异物、转换阻力是否异常。",
    "未锁闭且未密贴": "尖轨未锁闭且未密贴：检查锁闭铁与表示缺口，确认尖轨到位行程。",
    "锁闭失败": "锁闭表示缺失：转辙机已到位但锁闭检查器未给出表示，建议检查锁闭检查器接点与缺口标定。",
    "密贴不良/到位不足": "密贴不良（行程不足）：尖轨未达设计行程，建议检查密贴调整量与基本轨密贴面。",
    "转辙机过流": "转辙机过流：电机电流超限，建议检查摩擦联结器压力、转换阻力及电机回路。",
    "到位超程": "位移超出设计行程：建议检查缺口标定与锁闭量。",
}


@app.get("/api/diagnosis")
def api_diagnosis():
    """故障诊断（v2.1）：规则扣分项 + 工况自动识别 → 诊断结论 + 维修建议"""
    score, grade, issues = health_check(latest_values())
    ident = do_identify()
    if issues:
        concl = [{"问题": i["规则"], "关联测点": i["关联测点"],
                  "维修建议": ADVICE.get(i["规则"], "建议现场检查")}
                 for i in issues]
    else:
        concl = "各测点正常，设备健康"
    return {"健康度": score, "等级": grade,
            "自动识别": {"识别工况": ident.get("识别工况"),
                        "置信度": ident.get("置信度"),
                        "特征依据": ident.get("特征依据")},
            "诊断结论": concl}


@app.post("/api/repair/{work_order_id}")
def api_repair(work_order_id: str):
    """维修闭环：工单完结 → 自动重演正常转换复测 → 健康度回升（维修后数据自动更新）"""
    rows = q("SELECT point_code, status, remark FROM work_order WHERE work_order_id=%s",
             (work_order_id,))
    if not rows:
        raise HTTPException(404, f"工单 {work_order_id} 不存在")
    point, status, remark = rows[0]
    if status != "open":
        raise HTTPException(400, f"工单 {work_order_id} 状态为 {status}，无需维修")
    q("UPDATE work_order SET status='resolved', remark=%s WHERE work_order_id=%s",
      (remark + "（维修完成闭环）", work_order_id), fetch=False)
    if tables_ok("maintenance_log"):    # 维修动作存档（表未升级时跳过）
        q("INSERT INTO maintenance_log (work_order_id, point_code, problem) "
          "VALUES (%s, %s, %s)",
          (work_order_id, point, remark.replace("自动生成：", "")), fetch=False)
    if _play["condition"] != "正常转换":
        _stop_playback()
        _play["stop"].clear()
        _play["condition"] = "正常转换"
        _play["t"] = None
        with _play["lock"]:
            _play["window"].clear()
            _play["health_hist"].clear()
        _play["thread"] = threading.Thread(target=playback, args=("正常转换",), daemon=True)
        _play["thread"].start()
    return {"msg": f"{work_order_id} 已维修闭环；系统自动重演正常转换复测，健康度将回升"}


@app.get("/api/points")
def api_points():
    rows = q("SELECT point_code, field_name, unit, alarm_threshold "
             "FROM measurement_point ORDER BY point_code")
    return {"count": len(rows), "测点": [
        {"编码": p, "字段": f, "单位": u, "阈值": t} for p, f, u, t in rows]}


@app.get("/api/model")
def api_model():
    """测点↔构件↔3D模型节点 映射总表（C 端驱动模型的一站式对照接口）

    C 的用法：拿测点编码或数据字段名 → 查 node_name/model_node 找到 glTF 节点 →
    按 unit 换算位移 → 按 alarm_threshold 判超限变红（阈值与库内同源）。
    """
    comps = q("SELECT c.component_code, c.turnout_id, c.component_type, "
              "c.model_node, c.asset_id, count(p.point_code) AS pts "
              "FROM component c LEFT JOIN measurement_point p USING (component_code) "
              "GROUP BY c.component_code, c.turnout_id, c.component_type, "
              "c.model_node, c.asset_id ORDER BY c.component_code")
    pts = q("SELECT point_code, field_name, node_name, unit, alarm_threshold, "
            "component_code FROM measurement_point ORDER BY point_code")
    return {
        "构件": [{"编码": c, "道岔": t, "类型": ty, "模型节点": n, "资产ID": a, "测点数": int(p)}
                for c, t, ty, n, a, p in comps],
        "测点映射": [{"测点": p, "数据字段": f, "模型节点": n, "单位": u, "阈值": th, "所属构件": c}
                    for p, f, n, u, th, c in pts],
        "用法": "字段值→按测点映射找模型节点→毫米×0.001换算移动模型→value>阈值变红；"
                "A 确认导出后 model_node 以 /api/model 实时返回为准（库内可改，无需改页面）",
    }


@app.post("/api/ingest")
def api_ingest(payload: dict):
    """统一数据入库接口（传感器/仿真数据直推，契约 v2.0 第 6 节的 HTTP 实装）

    请求体：{"source": "sensor-01", "rows": [{"time": 0.0, "switchRailDisp1": 1.2,
              "pointRailDisp1": 0.5, "switchMachineCurrent": 3.0, ...}, ...]}
    - 字段按契约 v2.0（必选3+可选17），未知字段忽略并在响应中列明
    - 时间二选一：time=秒数（基准 2026-09-26 00:00+08）或 ts=ISO8601 字符串
    - 入时序表（主键去重幂等）→ 触发器自动告警审计 → 刷新实时值 → /api/health 立即生效
    """
    src = str(payload.get("source") or "sensor")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(400, "请求体需为 {'source':..., 'rows':[{...}]} 且 rows 非空")
    if len(rows) > 5000:
        raise HTTPException(400, "单次最多 5000 行，请分批")
    thresholds = {p: th for p, th in q(
        "SELECT point_code, alarm_threshold FROM measurement_point "
        "WHERE alarm_threshold IS NOT NULL")}
    payload_rows, latest, unknown = [], {}, set()
    for r in rows:
        try:
            if r.get("ts") is not None:
                ts_dt = datetime.fromisoformat(str(r["ts"]))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone(timedelta(hours=8)))
            else:
                ts_dt = BASE_TS + timedelta(seconds=float(r["time"]))
            for field, point in FIELD_TO_POINT.items():
                if field in r and r[field] is not None:
                    v = float(r[field])
                    payload_rows.append((point, ts_dt, v, src))
                    latest[point] = (v, ts_dt)
            unknown |= {k for k in r if k not in FIELD_TO_POINT and k not in ("time", "ts")}
        except (TypeError, ValueError) as e:
            raise HTTPException(400, f"行解析失败（{e}）：{json.dumps(r, ensure_ascii=False)[:120]}")
    if not payload_rows:
        raise HTTPException(400, f"rows 里没有契约字段（字段清单见 /api/points）；"
                                 f"收到的键：{sorted({k for r in rows for k in r})[:15]}")
    conn = _pool.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur,
                    "INSERT INTO timeseries_data (point_code, ts, value, source) VALUES %s "
                    "ON CONFLICT (point_code, ts) DO NOTHING", payload_rows,
                    template="(%s, %s, %s, %s)")
                execute_values(cur,
                    "INSERT INTO realtime_value (point_code, value, ts, alarm, updated_at) "
                    "VALUES %s ON CONFLICT (point_code) DO UPDATE SET "
                    "value=EXCLUDED.value, ts=EXCLUDED.ts, alarm=EXCLUDED.alarm, "
                    "updated_at=now()",
                    [(p, v, t, p in thresholds and v > thresholds[p])
                     for p, (v, t) in latest.items()],
                    template="(%s, %s, %s, %s, now())")
    finally:
        _pool.putconn(conn)
    return {"消息": f"已入库 {len(payload_rows)} 条（来源 {src}，重复自动跳过），"
                   f"实时值已更新 {len(latest)} 个测点，/api/health 与 /api/identify 立即生效",
            "入库条数": len(payload_rows),
            "忽略的未知字段": sorted(unknown) or "无",
            "覆盖测点": sorted(latest)}


@app.get("/trend", response_class=HTMLResponse)
def trend():
    points = q("SELECT point_code, field_name, unit, alarm_threshold "
               "FROM measurement_point ORDER BY point_code")
    ordered = sorted(points, key=lambda r: r[0] != "T01-SR-01-DISP")  # 默认首选尖轨位移
    opts = "".join(
        f'<option value="{p}"{" selected" if p == "T01-SR-01-DISP" else ""}>{p} · {f} ({u})</option>'
        for p, f, u, t in ordered)
    opts2 = "".join(
        f'<option value="{p}">{p} · {f} ({u})</option>' for p, f, u, t in ordered)
    th = {p: t for p, f, u, t in points if t is not None}
    th_json = json.dumps(th, ensure_ascii=False)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>道岔数字孪生 · 趋势回放</title>
<style>body{{font-family:"Microsoft YaHei";background:#0b1220;color:#e2e8f0;padding:20px 24px}}
.c{{background:#111c30;border:1px solid #1e2f4d;border-radius:12px;padding:14px 18px;max-width:920px;margin-bottom:12px}}
select,button{{background:#0b1220;color:#e2e8f0;border:1px solid #1e2f4d;border-radius:6px;padding:5px 10px}}
canvas{{width:100%;height:340px;display:block}}
#info{{font-size:13px;color:#64748b;margin-top:8px}}
#cur{{font-size:15px;color:#fbbf24;font-weight:bold}}</style></head><body>
<h1 style="font-size:19px">道岔数字孪生 · 历史趋势回放（20测点 · 四工况）</h1>
<div class="c">测点：<select id="pt" onchange="load()">{opts}</select>
叠加：<select id="pt2" onchange="load()"><option value="">（不叠加）</option>{opts2}</select>（粉色副轴）
时段：<select id="src" onchange="load()"><option value="all">全部（演示四工况+一天运维）</option><option value="运维-正常转换">一天运维·正常</option><option value="运维-卡阻">一天运维·卡阻</option><option value="运维-密贴不良">一天运维·密贴不良</option><option value="运维-锁闭失败">一天运维·锁闭失败</option><option value="仿真-正常转换">正常转换</option><option value="仿真-卡阻">卡阻</option><option value="仿真-密贴不良">密贴不良</option><option value="仿真-锁闭失败">锁闭失败</option><option value="fake">基础版假数据</option></select>
<button onclick="load()">刷新</button>
<button onclick="exportCSV()">⬇ 导出CSV</button>
<button id="pb" onclick="togglePlay()">▶ 回放</button>
<select id="spd"><option value="1">1×</option><option value="2" selected>2×</option><option value="4">4×</option></select>
<span id="cur">--</span>
<div id="info">加载中…</div></div>
<div class="c"><canvas id="cv"></canvas></div>
<div style="font-size:12px;color:#475569">回放=按时间轴逐点重演历史（黄色光标处为当前时刻）；蓝色=主测点（左轴刻度）、粉色=叠加测点（独立量程）；红/粉虚线=各自告警阈值；背景色带=数据来源分段（四工况各一段，段名标在上方）；⬇ 导出当前数据为 CSV（带 BOM，Excel 直接打开）。</div>
<script>const TH={th_json};
const cv=document.getElementById('cv');
let ROWS=[],ROWS2=[],TMIN=[],TMIN2=[],SEGS=[],T0=0,SPAN=1,LO=0,HI=1,LO2=0,HI2=1,idx=0,timer=null;
const MINUTES=x=>{{const a=x.时刻.split(':');return +a[0]*60+ +a[1]+ +a[2]/60}};
function fit(){{cv.width=cv.clientWidth*(devicePixelRatio||1);cv.height=cv.clientHeight*(devicePixelRatio||1)}}
addEventListener('resize',()=>{{fit();draw()}});
async function load(){{
fit();
const p=document.getElementById('pt').value, p2=document.getElementById('pt2').value;
const f=document.getElementById('src').value;
const d=await(await fetch('/api/history/'+p+'?limit=2000')).json();
ROWS=d.数据||[];
if(f!=="all")ROWS=ROWS.filter(x=>x.来源===f);
ROWS2=[];
if(p2){{const d2=await(await fetch('/api/history/'+p2+'?limit=2000')).json();
ROWS2=d2.数据||[];if(f!=="all")ROWS2=ROWS2.filter(x=>x.来源===f);}}
if(!ROWS.length){{document.getElementById('info').textContent=p+' 在该时段暂无数据';return}}
TMIN=ROWS.map(MINUTES);TMIN2=ROWS2.map(MINUTES);
SEGS=[];let s=0;
for(let i=1;i<=ROWS.length;i++){{if(i===ROWS.length||ROWS[i].来源!==ROWS[s].来源){{
SEGS.push({{a:TMIN[s],b:TMIN[i-1],src:ROWS[s].来源}});s=i;}}}}
T0=TMIN[0];SPAN=Math.max(TMIN[TMIN.length-1]-T0,0.1);
const vals=ROWS.map(x=>x.值);let lo=Math.min(...vals),hi=Math.max(...vals);
const thv=TH[p];if(thv!==undefined){{lo=Math.min(lo,thv);hi=Math.max(hi,thv)}}
const pad=(hi-lo)*0.15||5;LO=lo-pad;HI=hi+pad;
if(ROWS2.length){{const v2=ROWS2.map(x=>x.值);let l2=Math.min(...v2),h2=Math.max(...v2);
const t2=TH[p2];if(t2!==undefined){{l2=Math.min(l2,t2);h2=Math.max(h2,t2)}}
const pd2=(h2-l2)*0.15||5;LO2=l2-pd2;HI2=h2+pd2;}}
stopPlay();idx=ROWS.length-1;draw();
document.getElementById('info').textContent=p+(p2?' ＋ '+p2.split('-').pop():'')+
' · 共'+ROWS.length+'点 · '+ROWS[0].时刻+' ~ '+ROWS[ROWS.length-1].时刻+' · '+SEGS.length+' 个数据段（背景色区分）';
}}
function draw(){{
const ctx=cv.getContext('2d'),W=cv.width,H=cv.height,dpr=devicePixelRatio||1;
ctx.clearRect(0,0,W,H);
if(!ROWS.length)return;
const PL=52,PR=12,PT=24,PB=24,pw=W-PL-PR,ph=H-PT-PB;
const X=m=>PL+pw*(m-T0)/SPAN, Y=v=>PT+ph*(1-(v-LO)/(HI-LO)), Y2=v=>PT+ph*(1-(v-LO2)/(HI2-LO2||1));
const BAND=['rgba(56,189,248,0.07)','rgba(251,191,36,0.07)','rgba(52,211,153,0.07)','rgba(244,114,182,0.07)'];
SEGS.forEach((sg,i)=>{{const x0=Math.max(PL,X(sg.a)),x1=Math.min(W-PR,X(sg.b));
ctx.fillStyle=BAND[i%4];ctx.fillRect(x0,PT,x1-x0,ph);
if(x1-x0>30*dpr){{ctx.fillStyle='#64748b';ctx.font=(10*dpr)+'px sans-serif';ctx.textAlign='center';
ctx.fillText(sg.src.replace('仿真-',''),(x0+x1)/2,PT-9*dpr);}}}});
ctx.strokeStyle='#1e2f4d';ctx.fillStyle='#64748b';ctx.font=(10*dpr)+'px sans-serif';
for(let i=0;i<=4;i++){{const v=LO+(HI-LO)*i/4;ctx.beginPath();ctx.moveTo(PL,Y(v));ctx.lineTo(W-PR,Y(v));ctx.stroke();ctx.textAlign='right';ctx.fillText(v.toFixed(1),PL-6,Y(v)+3*dpr)}}
const p=document.getElementById('pt').value,thv=TH[p];
const p2=document.getElementById('pt2').value,thv2=TH[p2];
if(thv!==undefined){{ctx.setLineDash([6,4]);ctx.strokeStyle='#ef4444';ctx.beginPath();ctx.moveTo(PL,Y(thv));ctx.lineTo(W-PR,Y(thv));ctx.stroke();ctx.setLineDash([]);
ctx.textAlign='left';ctx.fillStyle='#ef4444';ctx.fillText('主阈值 '+thv,PL+4*dpr,Y(thv)-4*dpr);}}
if(ROWS2.length&&thv2!==undefined){{ctx.setLineDash([6,4]);ctx.strokeStyle='#f472b6';ctx.beginPath();ctx.moveTo(PL,Y2(thv2));ctx.lineTo(W-PR,Y2(thv2));ctx.stroke();ctx.setLineDash([]);
ctx.textAlign='right';ctx.fillStyle='#f472b6';ctx.fillText('副阈值 '+thv2,W-PR-4*dpr,Y2(thv2)-4*dpr);}}
ctx.beginPath();ctx.strokeStyle='#38bdf8';ctx.lineWidth=1.6*dpr;
for(let i=0;i<=idx&&i<ROWS.length;i++){{const gap=i>0&&TMIN[i]-TMIN[i-1]>2;
i?(gap?ctx.moveTo(X(TMIN[i]),Y(ROWS[i].值)):ctx.lineTo(X(TMIN[i]),Y(ROWS[i].值))):ctx.moveTo(X(TMIN[i]),Y(ROWS[i].值))}}
ctx.stroke();
if(ROWS2.length){{let k2=-1;const tCur=idx<ROWS.length?TMIN[idx]:TMIN[ROWS.length-1];
ctx.beginPath();ctx.strokeStyle='#f472b6';ctx.lineWidth=1.4*dpr;
for(let i=0;i<ROWS2.length;i++){{
if(TMIN2[i]>tCur)break;
const gap=i>0&&TMIN2[i]-TMIN2[i-1]>2;
i?(gap?ctx.moveTo(X(TMIN2[i]),Y2(ROWS2[i].值)):ctx.lineTo(X(TMIN2[i]),Y2(ROWS2[i].值))):ctx.moveTo(X(TMIN2[i]),Y2(ROWS2[i].值));
k2=i;}}
ctx.stroke();
if(k2>=0){{ctx.beginPath();ctx.arc(X(TMIN2[k2]),Y2(ROWS2[k2].值),3*dpr,0,7);
ctx.fillStyle='#f472b6';ctx.fill();ctx.strokeStyle='#0b1220';ctx.lineWidth=1;ctx.stroke();}}
window._k2=k2;}}
if(ROWS[idx]){{const cx=X(TMIN[idx]),cy=Y(ROWS[idx].值);
ctx.beginPath();ctx.arc(cx,cy,4*dpr,0,7);ctx.fillStyle='#fbbf24';ctx.fill();
ctx.strokeStyle='#0b1220';ctx.lineWidth=1;ctx.stroke();
let label2='';
if(ROWS2.length&&window._k2>=0){{const o=document.getElementById('pt2').selectedOptions[0];
label2=' · '+(o&&o.text.split(' · ')[1]||'叠加')+' '+ROWS2[window._k2].值;}}
document.getElementById('cur').textContent='当前值 '+ROWS[idx].值+'（'+ROWS[idx].时刻+'）'+label2;}}
ctx.textAlign='center';ctx.fillStyle='#475569';
[0,.5,1].forEach(f=>{{const m=T0+SPAN*f;ctx.fillText(((m/60)|0)+':'+String(Math.round(m%60)).padStart(2,'0'),X(m),H-8)}});
}}
function exportCSV(){{
if(!ROWS.length)return;
const p=document.getElementById('pt').value,p2=document.getElementById('pt2').value;
const m2=new Map();if(ROWS2.length)ROWS2.forEach(x=>m2.set(x.时刻,x.值));
let csv='\\ufeff仿真时刻,来源,'+p+(p2?','+p2:'')+'\\n';
ROWS.forEach(r=>{{csv+=r.时刻+','+r.来源+','+r.值+(p2?','+(m2.has(r.时刻)?m2.get(r.时刻):''):'')+'\\n'}});
const blob=new Blob([csv],{{type:'text/csv;charset=utf-8'}});
const a=document.createElement('a');a.href=URL.createObjectURL(blob);
a.download=p+(p2?'_vs_'+p2:'')+'.csv';a.click();URL.revokeObjectURL(a.href);
}}
function togglePlay(){{
if(timer){{stopPlay();return}}
if(!ROWS.length)return;
idx=0;const spd=+document.getElementById('spd').value||2;
document.getElementById('pb').textContent='⏸ 暂停';
timer=setInterval(()=>{{
idx++;
if(idx>=ROWS.length-1){{idx=ROWS.length-1;draw();stopPlay();return}}
draw();
}},100/spd);
}}
function stopPlay(){{if(timer){{clearInterval(timer);timer=null}}
const b=document.getElementById('pb');if(b)b.textContent='▶ 回放'}}
window.addEventListener('load',load);
fit();load();
</script></body></html>""")


@app.post("/api/play/{condition}")
def api_play(condition: str):
    if condition not in CONDITIONS:
        raise HTTPException(404, f"未知工况，可选：{CONDITIONS}")
    _stop_playback()
    _play["stop"].clear()
    _play["condition"] = condition
    _play["t"] = None
    with _play["lock"]:
        _play["window"].clear()   # 识别窗口重置：新一轮回放从头积累
        _play["health_hist"].clear()
    _play["thread"] = threading.Thread(target=playback, args=(condition,), daemon=True)
    _play["thread"].start()
    return {"msg": f"开始回放「{condition}」，实时值表每0.1s刷新，"
                   f"观察 /api/health 与 /api/workorders 联动；"
                   f"约3秒后 /api/identify 给出自动识别结果"}


@app.post("/api/stop")
def api_stop():
    _stop_playback()
    return {"msg": "回放已停止"}


def _stop_playback():
    _play["stop"].set()
    if _play["thread"] and _play["thread"].is_alive():
        _play["thread"].join(timeout=3)
