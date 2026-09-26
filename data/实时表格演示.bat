@echo off
chcp 65001 >nul
title 道岔数字孪生 · 数据库实时表格演示
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 · 数据库实时表格 一键演示
echo ================================================
echo.
echo [1/2] 正在启动实时数据写入（会开一个新窗口跑脚本）...
start "实时数据入库（别关这个窗口）" cmd /k python 实时数据入库.py
echo [2/2] 正在打开实时表格（每秒自动刷新）...
echo       密码已自动带上，不用输。Ctrl+C 退出刷新，关掉新窗口=停止写入
echo.
set PGPASSWORD=postgres
"D:\PostgreSQL\bin\psql.exe" -U postgres -h localhost -d turnout_twin -c "SELECT point_code AS dian, value AS zhi, to_char(ts,'HH24:MI:SS') AS sim_time, alarm FROM realtime_value ORDER BY point_code" -c "\watch 1"
echo.
echo （已退出刷新。想再看点一遍本文件即可）
pause
