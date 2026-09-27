@echo off
title 道岔数字孪生 - 数据服务API 一键演示
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 - 数据服务API 一键演示
echo ================================================
echo.
echo [1/2] 启动数据服务API（新窗口，演示期间别关它）...
start "data-api-service" cmd /k python -m uvicorn 数据服务API:app --host 127.0.0.1 --port 8000
echo [2/2] 打开可视化状态板...
timeout /t 3 >nul
start "" "http://localhost:8000/"
echo.
echo 完成。页面上点工况名看健康度变化和自动工单。
echo 停止演示：关闭数据服务API黑窗口即可。
pause
