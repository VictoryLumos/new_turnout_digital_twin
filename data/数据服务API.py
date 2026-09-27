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
    1) 打开首页看到 健康度100/优；
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
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

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

app = FastAPI(title="道岔数字孪生数据服务", version="2.0")
_play = {"thread": None, "stop": threading.Event(), "condition": None, "t": None}


def q(sql, args=(), fetch=True):
    conn = psycopg2.connect(**DB)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if fetch else None
    finally:
        conn.close()


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
    try:
        for r in rows:
            if _play["stop"].is_set():
                break
            ts = BASE_TS + timedelta(seconds=r["time"])
            score, grade, issues = 100.0, "优", []
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
                vals = {FIELD_TO_POINT[f]: r[f] for f in r if f in FIELD_TO_POINT}
                score, grade, issues = health_check(vals)
                _play["t"] = r["time"]
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
.c{{background:#111c30;border:1px solid #1e2f4d;border-radius:12px;padding:16px 20px;margin:12px 0;max-width:640px}}
a{{color:#38bdf8}} code{{background:#1e2f4d;padding:1px 6px;border-radius:4px}}
button{{background:#2563eb;border:0;color:#fff;border-radius:6px;padding:3px 10px;cursor:pointer;font-size:12px}}
.wos{{font-size:13px;margin-top:6px;line-height:1.9}}</style></head><body>
<h1>道岔数字孪生 · 数据服务（B线 · 完整版服务层）</h1>
<div class="c">健康度 <span id="score" style="font-size:34px;font-weight:bold;color:{color}">{score:.0f}</span>
<span id="grade" style="color:{color}">{grade}</span>
<div id="issues" style="font-size:13px;color:#94a3b8;margin-top:6px">
{'；'.join(i['规则'] for i in issues) or '无扣分项'}</div>
<div id="cond" style="font-size:13px;color:#64748b;margin-top:4px">当前回放：无</div></div>
<div class="c"><b>行为模型回放</b>（点击注入工况，观察健康度与工单联动）：<br><br>{conds}
<a href="javascript:fetch('/api/stop').then(r=>r.json()).then(loadAll)">停止</a></div>
<div class="c"><b>工单看板</b>（告警自动生成 → 维修闭环 → 复测恢复）：<div id="wos" class="wos">加载中…</div></div>
<div class="c"><b>接口清单</b><br>
<a href="/trend">📈 趋势回放页（/trend）</a> · <a href="/api/diagnosis">诊断（/api/diagnosis）</a><br>
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
const co={{'优':'#22c55e','良':'#eab308','预警':'#f97316','故障':'#ef4444'}};
document.getElementById('score').textContent=d.健康度评分;
document.getElementById('score').style.color=co[d.等级];
document.getElementById('grade').textContent=d.等级;
document.getElementById('grade').style.color=co[d.等级];
document.getElementById('issues').textContent=d.扣分明细.map(i=>i.规则).join('；')||'无扣分项';
document.getElementById('cond').textContent='当前回放：'+d.当前回放+(d.仿真时刻!=null?' · 仿真时刻 '+d.仿真时刻+'s':'')}})}}
function loadWos(){{
fetch('/api/workorders').then(r=>r.json()).then(d=>{{
const open=d.工单.filter(w=>w.状态==='open');
document.getElementById('wos').innerHTML=open.length?
open.map(w=>'<div>'+w.工单号+' · '+w.测点+' · '+w.备注+
' <button onclick="repair(\\''+w.工单号+'\\')">维修闭环</button></div>').join('')
:'暂无未闭环工单（全部已闭环或尚未发生告警）'}})}}
function loadAll(){{load();loadWos()}}
setInterval(load,2000);setInterval(loadWos,5000);loadAll();
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


@app.get("/api/health")
def api_health():
    score, grade, issues = health_check(latest_values())
    return {"健康度评分": score, "等级": grade, "扣分明细": issues,
            "当前回放": _play["condition"] or "无",
            "仿真时刻": _play["t"],
            "评估时间": datetime.now().strftime("%H:%M:%S")}


@app.get("/api/workorders")
def api_workorders():
    rows = q("SELECT work_order_id, point_code, status, remark FROM work_order "
             "ORDER BY work_order_id")
    return {"count": len(rows), "工单": [
        {"工单号": w, "测点": p, "状态": s, "备注": r} for w, p, s, r in rows]}


@app.get("/api/history/{point_code}")
def api_history(point_code: str, limit: int = 160):
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
    if issues:
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
时段：<select id="src" onchange="load()"><option value="all">全部（四工况）</option><option value="仿真-正常转换">正常转换</option><option value="仿真-卡阻">卡阻</option><option value="仿真-密贴不良">密贴不良</option><option value="仿真-锁闭失败">锁闭失败</option><option value="fake">基础版假数据</option></select>
<button onclick="load()">刷新</button>
<button id="pb" onclick="togglePlay()">▶ 回放</button>
<select id="spd"><option value="1">1×</option><option value="2" selected>2×</option><option value="4">4×</option></select>
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
