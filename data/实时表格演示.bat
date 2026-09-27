@echo off
title 道岔数字孪生 - 数据库实时表格 一键演示
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 - 数据库实时表格 一键演示
echo ================================================
echo.
echo [1/2] 启动实时数据写入（新窗口，演示期间别关它）...
start "rt-writer" cmd /k python 实时数据入库.py
echo [2/2] 打开实时表格（每秒自动刷新）...
timeout /t 2 >nul
set PGPASSWORD=postgres
"D:\PostgreSQL\bin\psql.exe" -U postgres -h localhost -d turnout_twin -c "SELECT point_code AS dian, value AS zhi, to_char(ts,'HH24:MI:SS') AS sim_time, alarm FROM realtime_value ORDER BY point_code" -c "\watch 1"
echo.
echo （已退出刷新。想再看就重新双击本文件）
pause
