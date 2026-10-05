-- ---------------------------------------------------------------------
-- 10. 模型节点映射修正（2026-10-05，组长反馈"还需B修改"）
--
-- 问题：/api/model 沿用整改前假设的节点名，把牵引点编号当成了不同的
--       钢轨节点（SwitchRail_2/_3、PointRail_2、*_N_FORCE），与 A 交付
--       的模型不一致。
-- 依据：《T18资产对应关系_v1.0.md》——A 交付《编码与映射表.json》
--       （5 测点 CONFIRMED）：尖轨三个牵引点 → 单一节点 SwitchRail；
--       心轨两个牵引点 → 单一节点 PointRail。第二牵引点≠第二根钢轨。
-- 范围：component.model_node 3 行 + measurement_point.node_name 8 行。
--       其余 15 测点（转辙机/辙叉/护轨/轨温等）的节点名不含牵引点编号，
--       不在本次裁定范围，仍按 T18 文档存疑清单第 2 项待 A 确认。
-- 幂等：UPDATE 带 node_name 旧值白名单，可重复执行；
--       已手工改过 / 未来 A 另行确认的值不会被覆盖。
-- 用法：psql -U postgres -d turnout_twin -f "db/10_模型节点映射修正.sql"
-- ---------------------------------------------------------------------

-- ① 尖轨三个牵引点（同一根尖轨）→ SwitchRail
UPDATE component SET model_node = 'SwitchRail'
 WHERE component_code IN ('T01-SR-01', 'T01-SR-02', 'T01-SR-03')
   AND model_node IN ('SwitchRail_2', 'SwitchRail_3');

-- ② 心轨两个牵引点（同一根心轨）→ PointRail
UPDATE component SET model_node = 'PointRail'
 WHERE component_code IN ('T01-PR-01', 'T01-PR-02')
   AND model_node IN ('PointRail_2');

-- ③ 尖轨测点（位移 3 + 牵引力 3）→ SwitchRail
UPDATE measurement_point SET node_name = 'SwitchRail'
 WHERE component_code IN ('T01-SR-01', 'T01-SR-02', 'T01-SR-03')
   AND node_name IN ('SwitchRail_2', 'SwitchRail_3',
                     'SwitchRail_1_FORCE', 'SwitchRail_2_FORCE', 'SwitchRail_3_FORCE');

-- ④ 心轨测点（位移 2 + 牵引力 2）→ PointRail
UPDATE measurement_point SET node_name = 'PointRail'
 WHERE component_code IN ('T01-PR-01', 'T01-PR-02')
   AND node_name IN ('PointRail_2', 'PointRail_1_FORCE', 'PointRail_2_FORCE');

-- 核对（执行后应各只剩一种节点名）：
SELECT component_code, model_node FROM component
 WHERE component_type IN ('SR', 'PR') ORDER BY component_code;
SELECT point_code, node_name FROM measurement_point
 WHERE component_code LIKE 'T01-SR-0%' OR component_code LIKE 'T01-PR-0%'
 ORDER BY point_code;
