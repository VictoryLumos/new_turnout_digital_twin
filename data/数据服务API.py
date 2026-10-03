# -*- coding: utf-8 -*-
r"""
道岔数字孪生 —— 数据服务 API（B：数据管道线，完整版升级 P3+P4）

对应《完整版》服务层与接入协议条款的最小实现：
    状态实时监测   GET  /api/realtime          实时值表全量（含测点信息/告警）
    健康度评估     GET  /api/health           规则评分（优/良/预警/故障）+ 扣分明细
    告警监测       GET  /api/alarms           当前超阈值测点
    工单联动       GET  /api/workorders       告警自动生成工单（回放期间）
    历史查询       GET  /api/history/{测点}   时序数据（历史回放的数据源）
    行为模型回放   POST /api/play/{工况}      把四工况数据集实时注入实时值表
                  POST /api/stop            停止回放
    数据集清单     GET  /api/conditions

用法：
    pip install fastapi uvicorn psycopg2-binary
    python -m uvicorn 数据服务API:app --host 0.0.0.0 --port 8000
    浏览器打开 http://localhost:8000/  （首页有可视化状态板和接口清单）

演示剧本（给组长/答辩用）：
    1) 打开首页：未回放且实时值表为空时显示"无数据"（不虚报 100/优）；
    2) POST /api/play/卡阻  → 实时值开始变化，电流爬升；
    3) 刷新 /api/health → 评分跌到 55/故障，/api/workorders 自动多出工单；
    4) POST /api/play/正常转换 或 /api/stop → 恢复。
"""
import json
import os
import threading
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse

HERE = os.path.dirname(os.path.abspath(__file__))
DS_DIR = os.path.join(HERE, "扩展数据集v2")
CONDITIONS = ["正常转换", "卡阻", "密贴不良", "锁闭失败", "告警演示"]

from 管道公共 import db_session, DB, ResilientDB  # 统一配置 + 断线恢复
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
RDB = ResilientDB()   # 长连接：运行中数据库断线自动重连恢复（整改第二轮）
_play = {"thread": None, "stop": threading.Event(), "condition": None, "t": None}


def q(sql, args=(), fetch=True):
    """字面量SQL + 参数绑定执行；运行中断线由 ResilientDB 自动重连恢复。"""
    def _op(cur):
        cur.execute(sql, args)
        return cur.fetchall() if fetch else None
    return RDB.run(_op)


def latest_values():
    """取实时值表最新值 {point_code: value}"""
    return {p: v for p, v in q("SELECT point_code, value FROM realtime_value")}


def health_check(vals):
    """规则模型：健康度评分 + 扣分明细。返回 (score, grade, issues)

    无数据不评估：vals 为空时返回 (None, "无数据", [])——绝不显示
    "健康度100/优"（整改第二轮：没有数据不能虚报健康）。

    稳态门：电机运行中（电流≥1A）只判"过程类"故障（过流/超程），
    "结果类"故障（未到位/锁闭/密贴）只在电机空闲（转换动作已结束）时判定，
    避免把转换过程中的正常过渡态误报成故障。
    """
    if not vals:
        return None, "无数据", []
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
    """回放线程：把工况数据集按 0.1s 注入实时值表，并做健康检查+工单联动。

    数据库走 ResilientDB 长连接：注入过程中数据库重启/连接被杀，
    当前帧自动重连重放，回放不中断（整改第二轮第3条）。
    """
    path = os.path.join(DS_DIR, f"工况_{condition}.json")
    rows = json.load(open(path, encoding="utf-8"))
    rdb = ResilientDB()
    thresholds = dict(rdb.run(lambda cur: (
        cur.execute("SELECT point_code, alarm_threshold FROM measurement_point "
                    "WHERE alarm_threshold IS NOT NULL"),
        cur.fetchall())[1]))
    print(f"[回放] 开始注入工况「{condition}」（{len(rows)} 行 × 0.1s）")
    issued = set()
    try:
        for r in rows:
            if _play["stop"].is_set():
                break
            ts = BASE_TS + timedelta(seconds=r["time"])
            score, grade, issues = 100.0, "优", []
            try:
                def _flush(cur, _r=r, _ts=ts):
                    execute_values(cur,
                        "INSERT INTO realtime_value (point_code, value, ts, alarm, updated_at) "
                        "VALUES %s ON CONFLICT (point_code) DO UPDATE SET "
                        "value=EXCLUDED.value, ts=EXCLUDED.ts, alarm=EXCLUDED.alarm, "
                        "updated_at=now()",
                        [(point, _r[field], _ts,
                          point in thresholds and _r[field] > thresholds[point])
                         for field, point in FIELD_TO_POINT.items() if field in _r],
                        template="(%s, %s, %s, %s, now())")
                rdb.run(_flush)
                vals = {FIELD_TO_POINT[f]: r[f] for f in r if f in FIELD_TO_POINT}
                score, grade, issues = health_check(vals)
                _play["t"] = r["time"]
                for it in issues:
                    ensure_work_order(it["关联测点"], it["规则"], issued)
            except Exception as e:  # noqa: BLE001 停机等异常：本帧暂缓，下一帧续写
                print(f"  [回放暂缓] {type(e).__name__}: {e}（自动重试，回放不中断）")
            if int(r["time"] * 10) % 10 == 0:
                mark = f"{score:.0f}({grade})" if score is not None else "(无数据)"
                print(f"  [回放] t={r['time']:>4.1f}s 健康度={mark}")
            _play["stop"].wait(0.1)
    finally:
        rdb.close()
        print(f"[回放] 工况「{condition}」注入结束")


@app.get("/", response_class=HTMLResponse)
def index():
    score, grade, issues = health_check(latest_values())
    color = {"优": "#22c55e", "良": "#eab308", "预警": "#f97316",
             "故障": "#ef4444", "无数据": "#64748b"}[grade]
    score_html = f"{score:.0f}" if score is not None else "--"
    conds = "".join(
        f'<a href="javascript:play(\'{c}\')" '
        f'style="margin-right:10px">{c}</a>' for c in CONDITIONS)
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>道岔数字孪生 · 数据服务</title>
<style>body{{font-family:"Microsoft YaHei";background:#0b1220;color:#e2e8f0;padding:24px}}
.c{{background:#111c30;border:1px solid #1e2f4d;border-radius:12px;padding:16px 20px;margin:12px 0;max-width:640px}}
a{{color:#38bdf8}} code{{background:#1e2f4d;padding:1px 6px;border-radius:4px}}
button{{background:#2563eb;border:0;color:#fff;border-radius:6px;padding:3px 10px;cursor:pointer;font-size:12px}}
.wos{{font-size:13px;margin-top:6px;line-height:1.9}}</style></head><body>
<h1>道岔数字孪生 · 数据服务（B线 · 完整版服务层）</h1>
<div class="c">健康度 <span id="score" style="font-size:34px;font-weight:bold;color:{color}">{score_html}</span>
<span id="grade" style="color:{color}">{grade}</span>
<div id="issues" style="font-size:13px;color:#94a3b8;margin-top:6px">
{'；'.join(i['规则'] for i in issues) or ('暂无实时数据：不虚报100/优，点击下方工况开始回放' if score is None else '无扣分项')}</div>
<div id="cond" style="font-size:13px;color:#64748b;margin-top:4px">当前回放：无</div></div>
<div class="c"><b>行为模型回放</b>（点击注入工况，观察健康度与工单联动）：<br><br>{conds}
<a href="javascript:fetch('/api/stop').then(r=>r.json()).then(loadAll)">停止</a></div>
<div class="c"><b>工单看板</b>（告警自动生成 → 维修闭环 → 复测恢复）：<div id="wos" class="wos">加载中…</div></div>
<div class="c"><b>告警变化记录</b>（何时超限 → 何测点 → 峰值 → 何时恢复；事件段视图）：<div id="aev" class="wos">加载中…</div>
<div style="font-size:12px;color:#475569;margin-top:4px">数据来自 v_fault_episodes（超阈按30秒聚合），JSON 接口：<a href="/api/alarm-events">/api/alarm-events</a></div></div>
<div class="c"><b>接口清单</b><br>
<a href="/trend">📈 趋势回放页（/trend）</a> · <a href="/trend5">🛤 五路位移总览（/trend5）</a> · <a href="/api/diagnosis">诊断（/api/diagnosis）</a> · <a href="/api/alarm-events">告警变化（/api/alarm-events）</a><br>
GET /api/realtime 实时监测 · GET /api/alarms 告警 · GET /api/health 健康度<br>
GET /api/workorders 工单 · GET /api/history/测点编码?limit=n 历史查询 · GET /api/points 测点清单<br>
GET /api/conditions 数据集 · POST /api/play/工况 · POST /api/repair/工单号 · POST /api/stop</div>
<script>
function play(c){{document.getElementById('issues').textContent='指令已发送：'+c+'（转换动作先正常走约6秒才进入故障段）';
fetch('/api/play/'+c,{{method:'POST'}}).then(r=>r.json()).then(()=>setTimeout(loadAll,500))
.catch(()=>{{document.getElementById('issues').textContent='服务未响应：请确认数据服务黑窗口开着'}})}}
function repair(id){{fetch('/api/repair/'+id,{{method:'POST'}}).then(r=>r.json()).then(d=>{{
document.getElementById('issues').textContent=d.msg;loadAll()}})}}
function load(){{
fetch('/api/health').then(r=>r.json()).then(d=>{{
const co={{'优':'#22c55e','良':'#eab308','预警':'#f97316','故障':'#ef4444','无数据':'#64748b'}};
document.getElementById('score').textContent=(d.健康度评分==null?'--':d.健康度评分);
document.getElementById('score').style.color=co[d.等级];
document.getElementById('grade').textContent=d.等级;
document.getElementById('grade').style.color=co[d.等级];
document.getElementById('issues').textContent=(d.健康度评分==null)?(d.说明||'暂无实时数据'):(d.扣分明细.map(i=>i.规则).join('；')||'无扣分项');
document.getElementById('cond').textContent='当前回放：'+d.当前回放+(d.仿真时刻!=null?' · 仿真时刻 '+d.仿真时刻+'s':'')}})}}
function loadWos(){{
fetch('/api/workorders').then(r=>r.json()).then(d=>{{
const open=d.工单.filter(w=>w.状态==='open');
document.getElementById('wos').innerHTML=open.length?
open.map(w=>'<div>'+w.工单号+' · '+w.测点+' · '+w.备注+
' <button onclick="repair(\\''+w.工单号+'\\')">维修闭环</button></div>').join('')
:'暂无未闭环工单（全部已闭环或尚未发生告警）'}})}}
function loadAev(){{
fetch('/api/alarm-events?limit=8').then(r=>r.json()).then(d=>{{
const list=d.告警变化||[];
document.getElementById('aev').innerHTML=list.length?
list.map(a=>'<div>'+String(a.何时超限).replace('T',' ').slice(5,23)+' 超限 <b>'+a.字段+
'</b> 峰值 '+a.超限峰值mm+'mm（阈值'+a.阈值mm+'） → '+
(a.何时恢复==='未恢复（仍在超限）'?'<span style="color:#f87171">未恢复</span>':
String(a.何时恢复).replace('T',' ').slice(5,23)+' 恢复（'+a.持续秒.toFixed(1)+'s）')+'</div>').join('')
:'暂无告警记录（未发生超限）'}}).catch(()=>{{}})}}
function loadAll(){{load();loadWos();loadAev()}}
setInterval(load,2000);setInterval(loadWos,5000);setInterval(loadAev,5000);loadAll();
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
    return {"count": len(rows), "告警": [
        {"测点": p, "字段": f, "当前值": float(v), "阈值": th, "单位": u}
        for p, f, v, th, u in rows]}


@app.get("/api/alarm-events")
def api_alarm_events(point_code: str = None, limit: int = 100):
    """告警变化记录（整改第三轮页面①）：何时超限、哪个测点、当时数值(峰值)、何时恢复。

    数据来自 v_fault_episodes（05 升级视图：超阈记录按 30 秒聚合为事件段），
    每段 = 一次"超限 → 恢复"全过程；end_ts 为空表示仍在超限（未恢复）。
    """
    cond = ["1=1"]
    params = []
    if point_code:
        cond.append("point_code = %s")
        params.append(point_code)
    params.append(max(1, min(limit, 500)))
    # WHERE 片段为固定字面量集合，筛选值全部经 %s 参数绑定
    sql_txt = ("SELECT point_code, start_ts, end_ts, last_alarm_ts, peak_value, "
               "threshold, source, duration_s FROM v_fault_episodes WHERE "
               + " AND ".join(cond) + " ORDER BY start_ts DESC LIMIT %s")
    rows = q(sql_txt, params)
    name_of = {v: k for k, v in FIELD_TO_POINT.items()}
    tz8 = timezone(timedelta(hours=8))

    def fmt(t):
        if t is None:
            return None
        if t.tzinfo is None:
            t = t.replace(tzinfo=tz8)
        return t.astimezone(tz8).isoformat(timespec="milliseconds")

    return {"count": len(rows), "告警变化": [
        {"测点": p, "字段": name_of.get(p, p),
         "何时超限": fmt(s),
         "最后超限": fmt(la),          # 最后一次仍超限的时刻（整改第四轮：与恢复分离）
         "超限峰值mm": float(v), "阈值mm": float(th),
         "何时恢复": fmt(e) if e is not None else "未恢复（仍在超限）",
         "持续秒": float(d) if d is not None else None, "来源": src}
        for p, s, e, la, v, th, src, d in rows]}


@app.get("/api/health")
def api_health():
    score, grade, issues = health_check(latest_values())
    return {"健康度评分": score, "等级": grade, "扣分明细": issues,
            "说明": ("实时值表暂无数据：不虚报健康度，等待回放或数据接入"
                     if score is None else None),
            "当前回放": _play["condition"] or "无",
            "仿真时刻": _play["t"],
            "评估时间": datetime.now(timezone(timedelta(hours=8)))
                        .isoformat(timespec="seconds")}


@app.get("/api/workorders")
def api_workorders():
    rows = q("SELECT work_order_id, point_code, status, remark FROM work_order "
             "ORDER BY work_order_id")
    return {"count": len(rows), "工单": [
        {"工单号": w, "测点": p, "状态": s, "备注": r} for w, p, s, r in rows]}


@app.get("/api/history/{point_code}")
def api_history(point_code: str, start: str = None, end: str = None,
                source: str = None, batch: str = None,
                limit: int = 160, format: str = None):
    """历史查询/导出：按测点+时间范围(HH:MM或HH:MM:SS)+来源+批次筛选；
    format=csv 导出文件。所有筛选值均经参数绑定，SQL 片段为固定字面量。"""
    if point_code not in set(FIELD_TO_POINT.values()):
        raise HTTPException(404, f"未知测点 {point_code}")
    tz8 = timezone(timedelta(hours=8))

    def parse_t(s):
        """完整日期时间解析（整改第二轮：不再写死2026-09-26）。

        支持：ISO 8601（含时区，如 2026-10-02T14:30:05+08:00 / ...Z）、
        "YYYY-MM-DD HH:MM[:SS]"、"YYYY-MM-DD"（当日00:00起）。
        无时区按北京时间；跨时区自动换算后与库内时间轴比较。
        纯 HH:MM[:SS] 已停用（历史数据跨多日，纯时刻有歧义）→ 400 提示。
        """
        txt = s.strip()
        try:
            d = datetime.fromisoformat(txt.replace("Z", "+00:00"))
            return d.astimezone(tz8)
        except ValueError:
            pass
        for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(txt, f).replace(tzinfo=tz8)
            except ValueError:
                pass
        if len(txt) <= 8 and ":" in txt:
            raise HTTPException(
                400, "纯时刻(HH:MM[:SS])已停用：历史数据已跨多日存在歧义，"
                     "请传完整日期时间（含时区），如 2026-10-02T14:30:00+08:00")
        raise HTTPException(400, f"时间格式应为完整日期时间（ISO 8601），收到 {s}")

    cond = ["point_code = %s"]
    params = [point_code]
    if start:
        cond.append("ts >= %s"); params.append(parse_t(start))
    if end:
        cond.append("ts <= %s"); params.append(parse_t(end))
    if source:
        cond.append("source = %s"); params.append(source)
    if batch:
        cond.append("batch = %s"); params.append(batch)
    params.append(max(1, min(limit, 5000)))
    # WHERE 片段为固定字面量集合，筛选值全部经 %s 参数绑定
    sql_txt = "SELECT ts, value, source, batch FROM timeseries_data WHERE "
    sql_txt += " AND ".join(cond)
    sql_txt += " ORDER BY ts DESC LIMIT %s"
    rows = q(sql_txt, params)
    data = []
    for t, v, s, b in reversed(rows):
        if t.tzinfo is None:            # 兼容无时区列：统一按北京时间补齐
            t = t.replace(tzinfo=tz8)
        data.append({"日期": t.strftime("%Y-%m-%d"),
                     "时刻": t.strftime("%H:%M:%S.%f")[:-4],
                     "时刻ISO": t.astimezone(tz8).isoformat(timespec="milliseconds"),
                     "值": float(v), "来源": s, "批次": b or "(未标注)"})
    if format == "csv":
        import csv as _csv, io as _io
        buf = _io.StringIO()
        w = _csv.writer(buf)
        w.writerow(["日期", "时刻", "时刻ISO", "值", "来源", "批次"])
        for d in data:
            w.writerow([d["日期"], d["时刻"], d["时刻ISO"],
                        d["值"], d["来源"], d["批次"]])
        return Response(content="﻿" + buf.getvalue(),
                        media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition":
                                 "attachment; filename=history.csv"})
    return {"测点": point_code, "count": len(data), "数据": data,
            "筛选": {"start": start, "end": end, "source": source, "batch": batch}}


@app.get("/api/conditions")
def api_conditions():
    out = []
    for c in CONDITIONS:
        path = os.path.join(DS_DIR, f"工况_{c}.json")
        rows = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else []
        out.append({"工况": c, "行数": len(rows),
                    "字段": len(rows[0]) if rows else 0})
    return {"数据集目录": DS_DIR, "工况": out}


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
    """故障诊断（规则版）：健康度扣分项 → 诊断结论 + 维修建议"""
    score, grade, issues = health_check(latest_values())
    if score is None:
        concl = "实时值表暂无数据，不做诊断（不虚报'设备健康'）"
    elif issues:
        concl = [{"问题": i["规则"], "关联测点": i["关联测点"],
                  "维修建议": ADVICE.get(i["规则"], "建议现场检查")}
                 for i in issues]
    else:
        concl = "各测点正常，设备健康"
    return {"健康度": score, "等级": grade, "诊断结论": concl}


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
    if _play["condition"] != "正常转换":
        _stop_playback()
        _play["stop"].clear()
        _play["condition"] = "正常转换"
        _play["thread"] = threading.Thread(target=playback, args=("正常转换",), daemon=True)
        _play["thread"].start()
    return {"msg": f"{work_order_id} 已维修闭环；系统自动重演正常转换复测，健康度将回升"}


@app.get("/api/points")
def api_points():
    rows = q("SELECT point_code, field_name, unit, alarm_threshold "
             "FROM measurement_point ORDER BY point_code")
    return {"count": len(rows), "测点": [
        {"编码": p, "字段": f, "单位": u, "阈值": t} for p, f, u, t in rows]}


@app.get("/trend", response_class=HTMLResponse)
def trend():
    points = q("SELECT point_code, field_name, unit, alarm_threshold "
               "FROM measurement_point ORDER BY point_code")
    ordered = sorted(points, key=lambda r: r[0] != "T01-SR-01-DISP")  # 默认首选尖轨位移
    opts = "".join(
        f'<option value="{p}"{" selected" if p == "T01-SR-01-DISP" else ""}>{p} · {f} ({u})</option>'
        for p, f, u, t in ordered)
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
时段：<select id="src" onchange="load()"><option value="all">全部（四工况）</option><option value="仿真-正常转换">正常转换</option><option value="仿真-卡阻">卡阻</option><option value="仿真-密贴不良">密贴不良</option><option value="仿真-锁闭失败">锁闭失败</option><option value="仿真-告警演示">告警演示</option><option value="fake">基础版假数据</option></select>
<button onclick="load()">刷新</button>
<button id="pb" onclick="togglePlay()">▶ 回放</button>
<select id="spd"><option value="0.25">0.25×</option><option value="0.5">0.5×</option><option value="1">1×</option><option value="2" selected>2×</option><option value="4">4×</option></select>
<span id="cur">--</span>
<div id="info">加载中…</div></div>
<div class="c"><canvas id="cv"></canvas></div>
<div style="font-size:12px;color:#475569">回放=按时间轴逐点重演历史（黄色光标处为当前时刻）；红色虚线=告警阈值；数据来源含 fake 与 仿真-四工况，每工况间隔1小时。</div>
<script>const TH={th_json};
const cv=document.getElementById('cv');
let ROWS=[],TMIN=[],T0=0,SPAN=1,LO=0,HI=1,idx=0,timer=null;
function fit(){{cv.width=cv.clientWidth*(devicePixelRatio||1);cv.height=cv.clientHeight*(devicePixelRatio||1)}}
addEventListener('resize',()=>{{fit();draw()}});
async function load(){{
fit();
const p=document.getElementById('pt').value;
const d=await(await fetch('/api/history/'+p+'?limit=2000')).json();
ROWS=d.数据||[];
const f=document.getElementById('src').value;
if(f!=="all")ROWS=ROWS.filter(x=>x.来源===f);
if(!ROWS.length){{document.getElementById('info').textContent=p+' 在该时段暂无数据';return}}
TMIN=ROWS.map(x=>{{const a=x.时刻.split(':');return +a[0]*60+ +a[1]+ +a[2]/60}});
T0=TMIN[0];SPAN=Math.max(TMIN[TMIN.length-1]-T0,0.1);
const vals=ROWS.map(x=>x.值);let lo=Math.min(...vals),hi=Math.max(...vals);
const thv=TH[p];if(thv!==undefined){{lo=Math.min(lo,thv);hi=Math.max(hi,thv)}}
const pad=(hi-lo)*0.15||5;LO=lo-pad;HI=hi+pad;
stopPlay();idx=ROWS.length-1;draw();
document.getElementById('info').textContent=p+' · 共'+ROWS.length+'点 · '+ROWS[0].时刻+' ~ '+ROWS[ROWS.length-1].时刻+'（四工况分四段）';
}}
function draw(){{
const ctx=cv.getContext('2d'),W=cv.width,H=cv.height,dpr=devicePixelRatio||1;
ctx.clearRect(0,0,W,H);
if(!ROWS.length)return;
const PL=52,PR=12,PT=12,PB=24,pw=W-PL-PR,ph=H-PT-PB;
const X=m=>PL+pw*(m-T0)/SPAN, Y=v=>PT+ph*(1-(v-LO)/(HI-LO));
ctx.strokeStyle='#1e2f4d';ctx.fillStyle='#64748b';ctx.font=(10*dpr)+'px sans-serif';
for(let i=0;i<=4;i++){{const v=LO+(HI-LO)*i/4;ctx.beginPath();ctx.moveTo(PL,Y(v));ctx.lineTo(W-PR,Y(v));ctx.stroke();ctx.textAlign='right';ctx.fillText(v.toFixed(1),PL-6,Y(v)+3*dpr)}}
const p=document.getElementById('pt').value,thv=TH[p];
if(thv!==undefined){{ctx.setLineDash([6,4]);ctx.strokeStyle='#ef4444';ctx.beginPath();ctx.moveTo(PL,Y(thv));ctx.lineTo(W-PR,Y(thv));ctx.stroke();ctx.setLineDash([])}}
ctx.beginPath();ctx.strokeStyle='#38bdf8';ctx.lineWidth=1.6*dpr;
for(let i=0;i<=idx&&i<ROWS.length;i++){{const gap=i>0&&TMIN[i]-TMIN[i-1]>2;
i?(gap?ctx.moveTo(X(TMIN[i]),Y(ROWS[i].值)):ctx.lineTo(X(TMIN[i]),Y(ROWS[i].值))):ctx.moveTo(X(TMIN[i]),Y(ROWS[i].值))}}
ctx.stroke();
if(ROWS[idx]){{const cx=X(TMIN[idx]),cy=Y(ROWS[idx].值);
ctx.beginPath();ctx.arc(cx,cy,4*dpr,0,7);ctx.fillStyle='#fbbf24';ctx.fill();
ctx.strokeStyle='#0b1220';ctx.lineWidth=1;ctx.stroke();
document.getElementById('cur').textContent='当前值 '+ROWS[idx].值+'（'+ROWS[idx].时刻+'）';}}
ctx.textAlign='center';ctx.fillStyle='#475569';
[0,.5,1].forEach(f=>{{const m=T0+SPAN*f;ctx.fillText(((m/60)|0)+':'+String(Math.round(m%60)).padStart(2,'0'),X(m),H-8)}});
}}
function togglePlay(){{
if(timer){{stopPlay();
document.getElementById('cur').textContent='⏸ 回放已暂停 · '+ROWS[idx].值+'mm @ '+ROWS[idx].时刻+'（点 ▶ 继续）';return}}
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


@app.get("/trend5", response_class=HTMLResponse)
def trend5():
    """五路位移趋势总览（整改第三轮页面②）：五条曲线 + 演示阈值 + 模型节点标注。"""
    th_json = json.dumps({"switch": 150.0, "point": 100.0})
    return HTMLResponse(f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>道岔数字孪生 · 五路位移总览</title>
<style>body{{font-family:"Microsoft YaHei";background:#0b1220;color:#e2e8f0;padding:20px 24px}}
.c{{background:#111c30;border:1px solid #1e2f4d;border-radius:12px;padding:14px 18px;max-width:1100px;margin-bottom:12px}}
select,button{{background:#0b1220;color:#e2e8f0;border:1px solid #1e2f4d;border-radius:6px;padding:5px 10px}}
canvas{{width:100%;height:400px;display:block}}
#info{{font-size:13px;color:#64748b;margin-top:8px}}
#cur{{font-size:15px;color:#fbbf24;font-weight:bold}}
.lg{{font-size:13px;margin-right:14px}}</style></head><body>
<h1 style="font-size:19px">道岔数字孪生 · 五路位移总览（尖轨3路 + 心轨2路 · 含演示阈值与模型节点）</h1>
<div class="c">时段：<select id="src" onchange="load()">
<option value="仿真-告警演示" selected>告警演示（推荐讲解）</option>
<option value="仿真-正常转换">正常转换</option><option value="仿真-卡阻">卡阻</option>
<option value="仿真-密贴不良">密贴不良</option><option value="仿真-锁闭失败">锁闭失败</option>
<option value="all">全部来源</option></select>
<button onclick="load()">刷新</button>
<button id="pb" onclick="togglePlay()">▶ 回放</button>
<select id="spd"><option value="0.25">0.25×</option><option value="0.5" selected>0.5×</option><option value="1">1×</option><option value="2">2×</option></select>
<span id="cur">--</span>
<div id="info" style="margin-top:6px">
<span class="lg" style="color:#38bdf8">━ SwitchRail·第1牵引点 160mm</span>
<span class="lg" style="color:#60a5fa">━ SwitchRail·第2牵引点 118mm</span>
<span class="lg" style="color:#93c5fd">━ SwitchRail·第3牵引点 75mm</span>
<span class="lg" style="color:#fbbf24">━ PointRail·第1牵引点 100.6mm</span>
<span class="lg" style="color:#f59e0b">━ PointRail·第2牵引点 55.5mm</span>
<span class="lg" style="color:#ef4444">╌ 演示阈值 尖轨&gt;150 / 心轨&gt;100（严格大于才告警）</span></div>
<div id="info2" style="font-size:12px;color:#475569;margin-top:4px">五路=同一根尖轨的三个牵引点+同一根心轨的两个牵引点（第二牵引点≠第二根钢轨）；设计动程为设计参数，非故障限值。粗字标注=模拟通道来源见推送 simFields。</div>
<div id="info">加载中…</div></div>
<div class="c"><canvas id="cv"></canvas></div>
<script>const TH={th_json};
const CH=[
{{p:'T01-SR-01-DISP',f:'switchRailDisp1',c:'#38bdffaa',w:2.2,node:'SwitchRail·第1牵引点',th:150}},
{{p:'T01-SR-02-DISP',f:'switchRailDisp2',c:'#60a5fa99',w:1.6,node:'SwitchRail·第2牵引点',th:null}},
{{p:'T01-SR-03-DISP',f:'switchRailDisp3',c:'#93c5fd88',w:1.6,node:'SwitchRail·第3牵引点',th:null}},
{{p:'T01-PR-01-DISP',f:'pointRailDisp1',c:'#fbbf24aa',w:2.2,node:'PointRail·第1牵引点',th:100}},
{{p:'T01-PR-02-DISP',f:'pointRailDisp2',c:'#f59e0b88',w:1.6,node:'PointRail·第2牵引点',th:null}}];
const cv=document.getElementById('cv');
let S=[],T0=0,SPAN=1,idx=0,timer=null;
function fit(){{cv.width=cv.clientWidth*(devicePixelRatio||1);cv.height=cv.clientHeight*(devicePixelRatio||1)}}
addEventListener('resize',()=>{{fit();draw()}});
async function load(){{
fit();stopPlay();
const s=document.getElementById('src').value;
S=await Promise.all(CH.map(ch=>fetch('/api/history/'+ch.p+'?limit=2000'+(s!=='all'?'&source='+encodeURIComponent(s):'')).then(r=>r.json()).then(d=>d.数据||[])));
if(!S[0].length){{document.getElementById('info').textContent='该时段暂无数据';S=[];return}}
const mm=x=>{{const a=x.时刻.split(':');return +a[0]*60+ +a[1]+ +a[2]/60}};
const all=S.flat().map(x=>mm(x));
T0=Math.min(...all);SPAN=Math.max(Math.max(...all)-T0,0.1);
idx=S[0].length-1;draw();
document.getElementById('info').textContent='共 '+S[0].length+' 帧 · '+S[0][0].日期+' '+S[0][0].时刻+' ~ '+S[0][S[0].length-1].时刻+'（五路同轴）';
}}
function draw(){{
const ctx=cv.getContext('2d'),W=cv.width,H=cv.height,dpr=devicePixelRatio||1;
ctx.clearRect(0,0,W,H);if(!S.length||!S[0].length)return;
const PL=52,PR=12,PT=14,PB=26,pw=W-PL-PR,ph=H-PT-PB,YM=185;
const X=t=>PL+pw*(t-T0)/SPAN,Y=v=>PT+ph*(1-v/YM);
ctx.strokeStyle='#1e2f4d';ctx.fillStyle='#64748b';ctx.font=(10*dpr)+'px sans-serif';
for(let v=0;v<=180;v+=30){{ctx.beginPath();ctx.moveTo(PL,Y(v));ctx.lineTo(W-PR,Y(v));ctx.stroke();ctx.textAlign='right';ctx.fillText(v+'',PL-6,Y(v)+3*dpr)}}
ctx.setLineDash([6,4]);ctx.strokeStyle='#ef4444';
[TH.switch,TH.point].forEach(v=>{{ctx.beginPath();ctx.moveTo(PL,Y(v));ctx.lineTo(W-PR,Y(v));ctx.stroke();ctx.fillText('阈值'+v,PL+4*dpr,Y(v)-4*dpr)}});
ctx.setLineDash([]);ctx.textAlign='left';
S.forEach((rows,si)=>{{
const ch=CH[si];const mm=x=>{{const a=x.时刻.split(':');return +a[0]*60+ +a[1]+ +a[2]/60}};
ctx.beginPath();ctx.strokeStyle=ch.c;ctx.lineWidth=ch.w*dpr;
for(let i=0;i<=Math.min(idx,rows.length-1);i++){{const x=X(mm(rows[i])),y=Y(rows[i].值);i?ctx.lineTo(x,y):ctx.moveTo(x,y)}}
ctx.stroke();
if(si===0&&rows[idx]){{const x=X(mm(rows[idx])),y=Y(rows[idx].值);
ctx.beginPath();ctx.arc(x,y,4.5*dpr,0,7);ctx.fillStyle='#fde047';ctx.fill();
ctx.strokeStyle='#0b1220';ctx.lineWidth=1;ctx.stroke();}}
}});
const cur=document.getElementById('cur');
cur.textContent='光标帧：'+CH.map((ch,i)=>S[i][idx]?ch.node.split('·')[1]+(S[i][idx].值).toFixed(1):'--').join(' / ')+' mm';
ctx.textAlign='center';ctx.fillStyle='#475569';
[0,.25,.5,.75,1].forEach(f=>{{const m=T0+SPAN*f;const h=(m/60)|0,mi=Math.round(m%60);ctx.fillText(h+':'+String(mi).padStart(2,'0'),X(m),H-8)}});
}}
function togglePlay(){{
if(timer){{stopPlay();
document.getElementById('cur').textContent='⏸ 回放已暂停 · '+document.getElementById('cur').textContent.slice(4)+'（点 ▶ 继续）';return}}
if(!S.length)return;idx=0;const spd=+document.getElementById('spd').value||0.5;
document.getElementById('pb').textContent='⏸ 暂停';
timer=setInterval(()=>{{idx++;if(idx>=S[0].length-1){{idx=S[0].length-1;draw();stopPlay();return}}draw()}},100/spd);}}
function stopPlay(){{if(timer){{clearInterval(timer);timer=null}}
const b=document.getElementById('pb');if(b)b.textContent='▶ 回放'}}
window.addEventListener('load',load);fit();load();
</script></body></html>""")


@app.post("/api/play/{condition}")
def api_play(condition: str):
    if condition not in CONDITIONS:
        raise HTTPException(404, f"未知工况，可选：{CONDITIONS}")
    _stop_playback()
    _play["stop"].clear()
    _play["condition"] = condition
    _play["thread"] = threading.Thread(target=playback, args=(condition,), daemon=True)
    _play["thread"].start()
    return {"msg": f"开始回放「{condition}」，实时值表每0.1s刷新，"
                   f"观察 /api/health 与 /api/workorders 联动"}


@app.post("/api/stop")
def api_stop():
    _stop_playback()
    return {"msg": "回放已停止"}


def _stop_playback():
    _play["stop"].set()
    if _play["thread"] and _play["thread"].is_alive():
        _play["thread"].join(timeout=3)
