--
-- PostgreSQL database dump
--

\restrict k95vC1k7iGJEN9GUuI0ycqXoIlJcNkJWyuImC2KWTNysxRYJk1rDMAqPXzpbzEf

-- Dumped from database version 17.9
-- Dumped by pg_dump version 17.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: component; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.component (
    component_code text NOT NULL,
    turnout_id text NOT NULL,
    component_type text NOT NULL,
    model_node text,
    asset_id text,
    remark text
);


ALTER TABLE public.component OWNER TO postgres;

--
-- Name: measurement_point; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.measurement_point (
    point_code text NOT NULL,
    component_code text NOT NULL,
    point_type text NOT NULL,
    sensor_id text,
    field_name text NOT NULL,
    node_name text,
    unit text,
    alarm_threshold real,
    remark text
);


ALTER TABLE public.measurement_point OWNER TO postgres;

--
-- Name: realtime_value; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.realtime_value (
    point_code text NOT NULL,
    value real NOT NULL,
    ts timestamp with time zone NOT NULL,
    alarm boolean DEFAULT false NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.realtime_value OWNER TO postgres;

--
-- Name: timeseries_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.timeseries_data (
    ts timestamp with time zone NOT NULL,
    point_code text NOT NULL,
    value real NOT NULL,
    source text DEFAULT 'fake'::text NOT NULL
);


ALTER TABLE public.timeseries_data OWNER TO postgres;

--
-- Name: work_order; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.work_order (
    work_order_id text NOT NULL,
    point_code text,
    status text DEFAULT 'reserved'::text,
    remark text
);


ALTER TABLE public.work_order OWNER TO postgres;

--
-- Data for Name: component; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.component (component_code, turnout_id, component_type, model_node, asset_id, remark) FROM stdin;
T01-BR-01	T01	BR	StockRail	AS-001	\N
T01-SR-01	T01	SR	SwitchRail	AS-002	\N
T01-SR-02	T01	SR	SwitchRail_2	AS-003	\N
T01-SR-03	T01	SR	SwitchRail_3	AS-004	\N
T01-PR-01	T01	PR	PointRail	AS-005	\N
T01-PR-02	T01	PR	PointRail_2	AS-006	\N
T01-SM-01	T01	SM	SwitchMachine	AS-007	\N
T01-FR-01	T01	FR	Frog	AS-008	\N
T01-GR-01	T01	GR	GuardRail	AS-009	\N
\.


--
-- Data for Name: measurement_point; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.measurement_point (point_code, component_code, point_type, sensor_id, field_name, node_name, unit, alarm_threshold, remark) FROM stdin;
T01-SR-01-DISP	T01-SR-01	DISP	SEN-001	switchRailDisp1	SwitchRail	mm	150	\N
T01-SR-02-DISP	T01-SR-02	DISP	SEN-002	switchRailDisp2	SwitchRail_2	mm	\N	\N
T01-SR-03-DISP	T01-SR-03	DISP	SEN-003	switchRailDisp3	SwitchRail_3	mm	\N	\N
T01-SR-01-FORCE	T01-SR-01	FORCE	SEN-004	switchRailForce1	SwitchRail_1_FORCE	N	\N	\N
T01-SR-02-FORCE	T01-SR-02	FORCE	SEN-005	switchRailForce2	SwitchRail_2_FORCE	N	\N	\N
T01-SR-03-FORCE	T01-SR-03	FORCE	SEN-006	switchRailForce3	SwitchRail_3_FORCE	N	\N	\N
T01-PR-01-DISP	T01-PR-01	DISP	SEN-007	pointRailDisp1	PointRail	mm	100	\N
T01-PR-02-DISP	T01-PR-02	DISP	SEN-008	pointRailDisp2	PointRail_2	mm	\N	\N
T01-PR-01-FORCE	T01-PR-01	FORCE	SEN-009	pointRailForce1	PointRail_1_FORCE	N	\N	\N
T01-PR-02-FORCE	T01-PR-02	FORCE	SEN-010	pointRailForce2	PointRail_2_FORCE	N	\N	\N
T01-SM-01-CURR	T01-SM-01	CURR	SEN-011	switchMachineCurrent	SwitchMachine	A	\N	\N
T01-SM-01-POWER	T01-SM-01	POWER	SEN-012	switchMachinePower	SwitchMachine_Power	W	\N	\N
T01-SM-01-LOCK	T01-SM-01	LOCK	SEN-019	lockStatus	LockStatus	0/1	\N	\N
T01-FR-01-VIB	T01-FR-01	VIB	SEN-013	frogVibration	Frog	m/s²	\N	\N
T01-FR-01-WEAR	T01-FR-01	WEAR	SEN-014	frogWear	Frog_Wear	mm	\N	\N
T01-FR-01-WRF-L	T01-FR-01	WRF-L	SEN-017	wheelRailForceLateral	WheelRailForce_L	kN	\N	\N
T01-FR-01-WRF-V	T01-FR-01	WRF-V	SEN-018	wheelRailForceVertical	WheelRailForce_V	kN	\N	\N
T01-GR-01-DISP	T01-GR-01	DISP	SEN-015	guardRailDisp	GuardRail	mm	\N	\N
T01-BR-01-TEMP	T01-BR-01	TEMP	SEN-016	railTemperature	RailTemp	℃	\N	\N
T01-SR-01-CLOSE	T01-SR-01	CLOSE	SEN-020	closeStatus	CloseStatus	0/1	\N	\N
\.


--
-- Data for Name: realtime_value; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.realtime_value (point_code, value, ts, alarm, updated_at) FROM stdin;
T01-SR-01-DISP	160	2026-09-26 00:00:07.4+08	t	2026-09-26 19:20:52.961587+08
T01-PR-01-DISP	95	2026-09-26 00:00:07.4+08	f	2026-09-26 19:20:52.961587+08
\.


--
-- Data for Name: timeseries_data; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.timeseries_data (ts, point_code, value, source) FROM stdin;
2026-09-26 00:00:00+08	T01-SR-01-DISP	0	fake
2026-09-26 00:00:00+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.1+08	T01-SR-01-DISP	0.13	fake
2026-09-26 00:00:00.1+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.2+08	T01-SR-01-DISP	0.52	fake
2026-09-26 00:00:00.2+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.3+08	T01-SR-01-DISP	1.16	fake
2026-09-26 00:00:00.3+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.4+08	T01-SR-01-DISP	2.04	fake
2026-09-26 00:00:00.4+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.5+08	T01-SR-01-DISP	3.15	fake
2026-09-26 00:00:00.5+08	T01-PR-01-DISP	0	fake
2026-09-26 00:00:00.6+08	T01-SR-01-DISP	4.48	fake
2026-09-26 00:00:00.6+08	T01-PR-01-DISP	0.08	fake
2026-09-26 00:00:00.7+08	T01-SR-01-DISP	6.03	fake
2026-09-26 00:00:00.7+08	T01-PR-01-DISP	0.31	fake
2026-09-26 00:00:00.8+08	T01-SR-01-DISP	7.77	fake
2026-09-26 00:00:00.8+08	T01-PR-01-DISP	0.69	fake
2026-09-26 00:00:00.9+08	T01-SR-01-DISP	9.72	fake
2026-09-26 00:00:00.9+08	T01-PR-01-DISP	1.21	fake
2026-09-26 00:00:01+08	T01-SR-01-DISP	11.85	fake
2026-09-26 00:00:01+08	T01-PR-01-DISP	1.87	fake
2026-09-26 00:00:01.1+08	T01-SR-01-DISP	14.16	fake
2026-09-26 00:00:01.1+08	T01-PR-01-DISP	2.66	fake
2026-09-26 00:00:01.2+08	T01-SR-01-DISP	16.64	fake
2026-09-26 00:00:01.2+08	T01-PR-01-DISP	3.58	fake
2026-09-26 00:00:01.3+08	T01-SR-01-DISP	19.28	fake
2026-09-26 00:00:01.3+08	T01-PR-01-DISP	4.62	fake
2026-09-26 00:00:01.4+08	T01-SR-01-DISP	22.07	fake
2026-09-26 00:00:01.4+08	T01-PR-01-DISP	5.77	fake
2026-09-26 00:00:01.5+08	T01-SR-01-DISP	25	fake
2026-09-26 00:00:01.5+08	T01-PR-01-DISP	7.04	fake
2026-09-26 00:00:01.6+08	T01-SR-01-DISP	28.07	fake
2026-09-26 00:00:01.6+08	T01-PR-01-DISP	8.41	fake
2026-09-26 00:00:01.7+08	T01-SR-01-DISP	31.25	fake
2026-09-26 00:00:01.7+08	T01-PR-01-DISP	9.88	fake
2026-09-26 00:00:01.8+08	T01-SR-01-DISP	34.56	fake
2026-09-26 00:00:01.8+08	T01-PR-01-DISP	11.45	fake
2026-09-26 00:00:01.9+08	T01-SR-01-DISP	37.97	fake
2026-09-26 00:00:01.9+08	T01-PR-01-DISP	13.1	fake
2026-09-26 00:00:02+08	T01-SR-01-DISP	41.48	fake
2026-09-26 00:00:02+08	T01-PR-01-DISP	14.84	fake
2026-09-26 00:00:02.1+08	T01-SR-01-DISP	45.08	fake
2026-09-26 00:00:02.1+08	T01-PR-01-DISP	16.66	fake
2026-09-26 00:00:02.2+08	T01-SR-01-DISP	48.76	fake
2026-09-26 00:00:02.2+08	T01-PR-01-DISP	18.56	fake
2026-09-26 00:00:02.3+08	T01-SR-01-DISP	52.51	fake
2026-09-26 00:00:02.3+08	T01-PR-01-DISP	20.52	fake
2026-09-26 00:00:02.4+08	T01-SR-01-DISP	56.32	fake
2026-09-26 00:00:02.4+08	T01-PR-01-DISP	22.55	fake
2026-09-26 00:00:02.5+08	T01-SR-01-DISP	60.19	fake
2026-09-26 00:00:02.5+08	T01-PR-01-DISP	24.63	fake
2026-09-26 00:00:02.6+08	T01-SR-01-DISP	64.09	fake
2026-09-26 00:00:02.6+08	T01-PR-01-DISP	26.77	fake
2026-09-26 00:00:02.7+08	T01-SR-01-DISP	68.04	fake
2026-09-26 00:00:02.7+08	T01-PR-01-DISP	28.95	fake
2026-09-26 00:00:02.8+08	T01-SR-01-DISP	72.01	fake
2026-09-26 00:00:02.8+08	T01-PR-01-DISP	31.18	fake
2026-09-26 00:00:02.9+08	T01-SR-01-DISP	76	fake
2026-09-26 00:00:02.9+08	T01-PR-01-DISP	33.44	fake
2026-09-26 00:00:03+08	T01-SR-01-DISP	80	fake
2026-09-26 00:00:03+08	T01-PR-01-DISP	35.73	fake
2026-09-26 00:00:03.1+08	T01-SR-01-DISP	84	fake
2026-09-26 00:00:03.1+08	T01-PR-01-DISP	38.06	fake
2026-09-26 00:00:03.2+08	T01-SR-01-DISP	87.99	fake
2026-09-26 00:00:03.2+08	T01-PR-01-DISP	40.4	fake
2026-09-26 00:00:03.3+08	T01-SR-01-DISP	91.96	fake
2026-09-26 00:00:03.3+08	T01-PR-01-DISP	42.76	fake
2026-09-26 00:00:03.4+08	T01-SR-01-DISP	95.91	fake
2026-09-26 00:00:03.4+08	T01-PR-01-DISP	45.13	fake
2026-09-26 00:00:03.5+08	T01-SR-01-DISP	99.81	fake
2026-09-26 00:00:03.5+08	T01-PR-01-DISP	47.5	fake
2026-09-26 00:00:03.6+08	T01-SR-01-DISP	103.68	fake
2026-09-26 00:00:03.6+08	T01-PR-01-DISP	49.87	fake
2026-09-26 00:00:03.7+08	T01-SR-01-DISP	107.49	fake
2026-09-26 00:00:03.7+08	T01-PR-01-DISP	52.24	fake
2026-09-26 00:00:03.8+08	T01-SR-01-DISP	111.24	fake
2026-09-26 00:00:03.8+08	T01-PR-01-DISP	54.6	fake
2026-09-26 00:00:03.9+08	T01-SR-01-DISP	114.92	fake
2026-09-26 00:00:03.9+08	T01-PR-01-DISP	56.94	fake
2026-09-26 00:00:04+08	T01-SR-01-DISP	118.52	fake
2026-09-26 00:00:04+08	T01-PR-01-DISP	59.27	fake
2026-09-26 00:00:04.1+08	T01-SR-01-DISP	122.03	fake
2026-09-26 00:00:04.1+08	T01-PR-01-DISP	61.56	fake
2026-09-26 00:00:04.2+08	T01-SR-01-DISP	125.44	fake
2026-09-26 00:00:04.2+08	T01-PR-01-DISP	63.82	fake
2026-09-26 00:00:04.3+08	T01-SR-01-DISP	128.75	fake
2026-09-26 00:00:04.3+08	T01-PR-01-DISP	66.05	fake
2026-09-26 00:00:04.4+08	T01-SR-01-DISP	131.93	fake
2026-09-26 00:00:04.4+08	T01-PR-01-DISP	68.23	fake
2026-09-26 00:00:04.5+08	T01-SR-01-DISP	135	fake
2026-09-26 00:00:04.5+08	T01-PR-01-DISP	70.37	fake
2026-09-26 00:00:04.6+08	T01-SR-01-DISP	137.93	fake
2026-09-26 00:00:04.6+08	T01-PR-01-DISP	72.45	fake
2026-09-26 00:00:04.7+08	T01-SR-01-DISP	140.72	fake
2026-09-26 00:00:04.7+08	T01-PR-01-DISP	74.48	fake
2026-09-26 00:00:04.8+08	T01-SR-01-DISP	143.36	fake
2026-09-26 00:00:04.8+08	T01-PR-01-DISP	76.44	fake
2026-09-26 00:00:04.9+08	T01-SR-01-DISP	145.84	fake
2026-09-26 00:00:04.9+08	T01-PR-01-DISP	78.34	fake
2026-09-26 00:00:05+08	T01-SR-01-DISP	148.15	fake
2026-09-26 00:00:05+08	T01-PR-01-DISP	80.16	fake
2026-09-26 00:00:05.1+08	T01-SR-01-DISP	150.28	fake
2026-09-26 00:00:05.1+08	T01-PR-01-DISP	81.9	fake
2026-09-26 00:00:05.2+08	T01-SR-01-DISP	152.23	fake
2026-09-26 00:00:05.2+08	T01-PR-01-DISP	83.55	fake
2026-09-26 00:00:05.3+08	T01-SR-01-DISP	153.97	fake
2026-09-26 00:00:05.3+08	T01-PR-01-DISP	85.12	fake
2026-09-26 00:00:05.4+08	T01-SR-01-DISP	155.52	fake
2026-09-26 00:00:05.4+08	T01-PR-01-DISP	86.59	fake
2026-09-26 00:00:05.5+08	T01-SR-01-DISP	156.85	fake
2026-09-26 00:00:05.5+08	T01-PR-01-DISP	87.96	fake
2026-09-26 00:00:05.6+08	T01-SR-01-DISP	157.96	fake
2026-09-26 00:00:05.6+08	T01-PR-01-DISP	89.23	fake
2026-09-26 00:00:05.7+08	T01-SR-01-DISP	158.84	fake
2026-09-26 00:00:05.7+08	T01-PR-01-DISP	90.38	fake
2026-09-26 00:00:05.8+08	T01-SR-01-DISP	159.48	fake
2026-09-26 00:00:05.8+08	T01-PR-01-DISP	91.42	fake
2026-09-26 00:00:05.9+08	T01-SR-01-DISP	159.87	fake
2026-09-26 00:00:05.9+08	T01-PR-01-DISP	92.34	fake
2026-09-26 00:00:06+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06+08	T01-PR-01-DISP	93.13	fake
2026-09-26 00:00:06.1+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.1+08	T01-PR-01-DISP	93.79	fake
2026-09-26 00:00:06.2+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.2+08	T01-PR-01-DISP	94.31	fake
2026-09-26 00:00:06.3+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.3+08	T01-PR-01-DISP	94.69	fake
2026-09-26 00:00:06.4+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.4+08	T01-PR-01-DISP	94.92	fake
2026-09-26 00:00:06.5+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.5+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:06.6+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.6+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:06.7+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.7+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:06.8+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.8+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:06.9+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:06.9+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.1+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.1+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.2+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.2+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.3+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.3+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.4+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.4+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.5+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.5+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.6+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.6+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.7+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.7+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.8+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.8+08	T01-PR-01-DISP	95	fake
2026-09-26 00:00:07.9+08	T01-SR-01-DISP	160	fake
2026-09-26 00:00:07.9+08	T01-PR-01-DISP	95	fake
\.


--
-- Data for Name: work_order; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.work_order (work_order_id, point_code, status, remark) FROM stdin;
WO-2024-001	T01-SR-01-DISP	reserved	\N
WO-2024-002	T01-SR-02-DISP	reserved	\N
WO-2024-003	T01-SR-03-DISP	reserved	\N
WO-2024-004	T01-SR-01-FORCE	reserved	\N
WO-2024-005	T01-SR-02-FORCE	reserved	\N
WO-2024-006	T01-SR-03-FORCE	reserved	\N
WO-2024-007	T01-PR-01-DISP	reserved	\N
WO-2024-008	T01-PR-02-DISP	reserved	\N
WO-2024-009	T01-PR-01-FORCE	reserved	\N
WO-2024-010	T01-PR-02-FORCE	reserved	\N
WO-2024-011	T01-SM-01-CURR	reserved	\N
WO-2024-012	T01-SM-01-POWER	reserved	\N
WO-2024-013	T01-FR-01-VIB	reserved	\N
WO-2024-014	T01-FR-01-WEAR	reserved	\N
WO-2024-015	T01-GR-01-DISP	reserved	\N
WO-2024-016	T01-BR-01-TEMP	reserved	\N
WO-2024-017	T01-FR-01-WRF-L	reserved	\N
WO-2024-018	T01-FR-01-WRF-V	reserved	\N
WO-2024-019	T01-SM-01-LOCK	reserved	\N
WO-2024-020	T01-SR-01-CLOSE	reserved	\N
\.


--
-- Name: component component_asset_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.component
    ADD CONSTRAINT component_asset_id_key UNIQUE (asset_id);


--
-- Name: component component_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.component
    ADD CONSTRAINT component_pkey PRIMARY KEY (component_code);


--
-- Name: measurement_point measurement_point_field_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement_point
    ADD CONSTRAINT measurement_point_field_name_key UNIQUE (field_name);


--
-- Name: measurement_point measurement_point_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement_point
    ADD CONSTRAINT measurement_point_pkey PRIMARY KEY (point_code);


--
-- Name: measurement_point measurement_point_sensor_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement_point
    ADD CONSTRAINT measurement_point_sensor_id_key UNIQUE (sensor_id);


--
-- Name: realtime_value realtime_value_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.realtime_value
    ADD CONSTRAINT realtime_value_pkey PRIMARY KEY (point_code);


--
-- Name: timeseries_data timeseries_data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.timeseries_data
    ADD CONSTRAINT timeseries_data_pkey PRIMARY KEY (point_code, ts);


--
-- Name: work_order work_order_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.work_order
    ADD CONSTRAINT work_order_pkey PRIMARY KEY (work_order_id);


--
-- Name: idx_point_component; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_point_component ON public.measurement_point USING btree (component_code);


--
-- Name: measurement_point measurement_point_component_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.measurement_point
    ADD CONSTRAINT measurement_point_component_code_fkey FOREIGN KEY (component_code) REFERENCES public.component(component_code);


--
-- Name: realtime_value realtime_value_point_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.realtime_value
    ADD CONSTRAINT realtime_value_point_code_fkey FOREIGN KEY (point_code) REFERENCES public.measurement_point(point_code);


--
-- Name: timeseries_data timeseries_data_point_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.timeseries_data
    ADD CONSTRAINT timeseries_data_point_code_fkey FOREIGN KEY (point_code) REFERENCES public.measurement_point(point_code);


--
-- Name: work_order work_order_point_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.work_order
    ADD CONSTRAINT work_order_point_code_fkey FOREIGN KEY (point_code) REFERENCES public.measurement_point(point_code);


--
-- PostgreSQL database dump complete
--

\unrestrict k95vC1k7iGJEN9GUuI0ycqXoIlJcNkJWyuImC2KWTNysxRYJk1rDMAqPXzpbzEf

