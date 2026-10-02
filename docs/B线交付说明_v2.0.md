# B 线交付说明 v2.0（2026-10-02 整改版）

> 本文档是组长整改令（八条）的正式答复与接口文档，取代 v1《数据格式约定》中冲突的内容。

## 一、整改对照表（组长条目 → 落地位置）

| # | 组长要求 | 落地 |
|---|---|---|
| 1 | 数据校验（缺字段/字符串/null/NaN/Inf/乱序，定位行号） | `data/管道公共.py` validate_rows；推送启动、02/04 入库前强制校验，非法**拒绝推送/入库**并打印 文件+行号+字段 |
| 2 | 告警演示数据（单路/双路/分别恢复+边界样例） | 工况_告警演示.json（120 行，16 阶段校验通过）；150.0/150.1/100.0/100.1 边界帧齐备 |
| 3 | 循环推送（0.1s/不瞬发/区分时间/轮次序号） | 推送服务 v2：绝对时刻对齐 + 轮间强制一个步长间隔；消息含 `seq/round/simTime/sendTs` |
| 4 | 断线恢复（服务端重连/客户端分工/DB重试） | 服务端每连接独立、断开不影响；**客户端自动重连由 C 实现**（B 参考实现：WebSocket测试页面.html，2 秒重连）；DB 全部走 db_session 失败重试 |
| 5 | 时间/字段/地址统一（时间字符串+五路+3002） | 消息同时含 `timeStr`（时间字符串，格式配置可调）+ 数值 `time`；五路位移齐备，缺失三路**模拟生成并在 `simFields` 显式标注，绝不悄悄补零**；端口统一 **3002**（配置文件） |
| 6 | 编码与模型关联表 | `docs/编码与模型关联表_v2.0.md`（含"第二牵引点≠第二根尖轨"红线批注；T18 资产编码待 A 提供） |
| 7 | 历史读取/回放/批次 | API `/api/history/{测点}?start=&end=&source=&batch=&format=csv`；时序表新增 `batch` 列（05 迁移），批次共存，换批不清表（02 自动错峰时间轴 / --replace-source 按来源替换） |
| 8 | 文档/启动/配置统一/更正旧说法 | 本文档 + requirements.txt + `data/管道配置.json`（DB/端口/阈值唯一来源）；**Y 轴更正**：iModel 横移为 Y 轴，旧"沿X轴"作废 |

## 二、依赖清单与安装

```
pip install -r requirements.txt
# websockets>=13.0  fastapi>=0.110  uvicorn>=0.29  psycopg2-binary>=2.9
```

数据库：PostgreSQL 17 + TimescaleDB 2.28.3（已装于 D:\PostgreSQL，服务 postgresql-x64-17 开机自启）。

## 三、统一连接参数（唯一来源：data/管道配置.json）

| 项 | 值 | 说明 |
|---|---|---|
| 数据库 | localhost:5432，postgres/postgres，库 turnout_twin | 所有脚本/API 从配置文件读取，改一处全局生效 |
| WebSocket | **ws://localhost:3002**（跨机用 B 的 IPv4） | 原 8765 可用 --port 8765 临时启用 |
| 数据服务 API | http://localhost:8000 | 健康度/工单/诊断/趋势页/历史 |
| 告警阈值 | 尖轨>150mm、心轨>100mm | **等于不告警，严格大于才告警** |

## 四、WebSocket 消息契约 v2（C 对接用）

每 0.1s 一条 JSON（round 循环，从 round=1 递增）：

```json
{
  "time": 2.1,  "simTime": 2.1,
  "timeStr": "00:00:02.100",
  "seq": 23, "round": 1, "sendTs": 1759382400000,
  "switchRailDisp1": 150.1, "switchRailDisp2": 145.1, "switchRailDisp3": 140.1,
  "pointRailDisp1": 80.0,  "pointRailDisp2": 75.6,
  "simFields": ["switchRailDisp2", "switchRailDisp3", "pointRailDisp2"]
}
```

- 五路位移单位均 mm；推送 扩展数据集v2 工况时含全部 21 字段（无 simFields）；
- `simFields`：**该消息中由适配层模拟生成的通道清单**（来源=基础两路数据推导），A 提供真实三路后自动消失；
- 新鲜度判据：`seq` 全局递增；循环归零时 `round+1`，前端不会误判旧数据；
- 断线：客户端自动重连由 **C 实现**（参考代码见 WebSocket测试页面.html 的 onclose→2s 重连）；
- 告警由前端按阈值判定（等于不告警）。

## 五、启动命令一览

| 用途 | 命令（在 data/ 或项目根） |
|---|---|
| 推送服务（3002） | `python data/WebSocket推送服务.py`（可 `--file 扩展数据集v2/工况_告警演示.json`） |
| 数据服务 API（8000） | `python -m uvicorn 数据服务API:app --port 8000`（cd data） |
| 一键演示 | 双击 `data/WebSocket演示.bat` / `data/服务API演示.bat` / `data/实时表格演示.bat` |
| 数据入库 | `python db/02_数据导入脚本.py --file xx.json --source um --batch UM-1001 [--replace-source] [--base "YYYY-MM-DD HH:MM:SS"]` |
| 五工况入库 | `python db/04_扩展数据入库.py`（幂等，按来源替换） |
| 自检 | `python data/联调自检脚本.py` |
| 验收测试 | `python data/验收测试.py`（生成 docs/整改验收记录） |

## 六、历史/回放接口（C 调用）

```
GET /api/history/T01-SR-01-DISP?start=04:00&end=04:12&source=仿真-告警演示&batch=B02-四工况-告警演示
GET /api/history/T01-SM-01-CURR?source=um&format=csv      # 导出CSV
GET /trend                                                    # 趋势回放页（含告警演示时段）
```

批次命名：`B01-基础联调` / `B02-四工况-*` / `IMP-日期-时分`（02 默认）/ 自定义 `--batch`。UM 数据请 `--source um`，**不会被写成 fake**。

## 七、验收材料对照（对应组长四类材料）

1. 更新后的程序/数据/接口文档/依赖清单 —— 本文档 + 全部 v2 脚本 + requirements.txt
2. 正常/超限/恢复三类与边界测试结果 —— `python data/验收测试.py` §B/§C，记录写入 docs/整改验收记录_*.md
3. 非法数据被拒绝的测试结果 —— 同上 §A（六类非法样例逐一拒绝并定位行号）
4. 与 C 页面联调记录（收数/变红/恢复/断线重连） —— 待 C 页面接入 3002 后补充（联调记录模板在验收记录文档末尾）
5. 数据库建表/入库/重复导入/批次保存/历史查询运行记录 —— 同上 §E
6. 共享仓库地址与提交版本 —— 待 VictoryLumos 开协作者权限后推送（本地提交已就绪），届时补填

## 八、遗留事项（不阻塞复验，需三方确认）

- T18 资产编码：待 A 从 iModel 提供 → B 回填关联表；
- 契约 v2（五路+元信息+3002）需 A/C 群确认后转正；
- 与 C 的联调记录：约定时间后执行并回填验收记录。
