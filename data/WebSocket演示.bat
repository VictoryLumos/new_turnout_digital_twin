@echo off
title 道岔数字孪生 - WebSocket 一键演示
cd /d %~dp0
rem Python 探测：PATH 里没有就用本机安装路径
set "PY=python"
where python >nul 2>nul || if exist "D:\PY\python.exe" set "PY=D:\PY\python.exe"
echo ================================================
echo   道岔数字孪生 - WebSocket 一键演示
echo ================================================
echo.
echo [1/2] 启动WebSocket推送服务（新窗口，演示期间别关它）...
start "ws-service" cmd /k %PY% WebSocket推送服务.py
echo [2/2] 打开测试页面（实时曲线，超限变红）...
timeout /t 2 >nul
start "" "%~dp0WebSocket测试页面.html"
echo.
echo 完成。浏览器数值每0.1秒刷新、8秒一轮循环。
echo 停止演示：关闭ws-service黑窗口即可。
pause
