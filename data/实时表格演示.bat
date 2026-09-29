@echo off
title 道岔数字孪生 - 数据库实时表格 一键演示
cd /d %~dp0
rem Python 探测：PATH 里没有就用本机安装路径
set "PY=python"
where python >nul 2>nul || if exist "D:\PY\python.exe" set "PY=D:\PY\python.exe"
echo ================================================
echo   道岔数字孪生 - 数据库实时表格 一键演示
echo ================================================
echo.
echo [1/2] 启动实时数据写入（新窗口，演示期间别关它）...
start "rt-writer" cmd /k %PY% 实时数据入库.py
echo [2/2] 打开实时表格（每秒自动刷新）...
timeout /t 2 >nul
set PGPASSWORD=postgres
rem psql 自动探测：PATH → 本机D盘 → 默认安装目录
set "PSQL=psql"
where psql >nul 2>nul || if exist "D:\PostgreSQL\bin\psql.exe" (set "PSQL=D:\PostgreSQL\bin\psql.exe")
where psql >nul 2>nul || if not exist "D:\PostgreSQL\bin\psql.exe" (set "PSQL=C:\Program Files\PostgreSQL\17\bin\psql.exe")
"%PSQL%" -U postgres -h localhost -d turnout_twin -c "SELECT point_code AS dian, value AS zhi, to_char(ts,'HH24:MI:SS') AS sim_time, alarm FROM realtime_value ORDER BY point_code" -c "\watch 1"
echo.
echo （已退出刷新。想再看就重新双击本文件）
pause
