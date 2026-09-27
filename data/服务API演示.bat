@echo off
title 道岔数字孪生 - 数据服务API 一键演示
cd /d %~dp0
rem Python 探测：PATH 里没有就用本机安装路径（不依赖环境变量配置）
set "PY=python"
where python >nul 2>nul || if exist "D:\PY\python.exe" set "PY=D:\PY\python.exe"
echo ================================================
echo   道岔数字孪生 - 数据服务API 一键演示
echo ================================================
echo.
echo 演示动线（详见 docs/演示剧本_三分钟_v1.0.md）：
echo   1. 点"卡阻"   - 健康度跳水 + 工单自动生成
echo   2. 点"维修闭环" - 自动复测恢复
echo   3. 看"工况自动识别"卡片（约3秒出结果，v2.1 新增）
echo   4. 打开 /trend  - 历史趋势回放
echo.
echo [1/2] 启动数据服务API（新窗口，演示期间别关它）...
echo       绑定 0.0.0.0：局域网内 C 可直接访问 http://本机IP:8000
echo       （首次运行 Windows 防火墙弹窗请点"允许访问"）
start "data-api-service" cmd /k %PY% -m uvicorn 数据服务API:app --host 0.0.0.0 --port 8000
echo [2/2] 打开可视化状态板...
timeout /t 3 >nul
start "" "http://localhost:8000/"
echo.
echo 完成。停止演示：关闭 data-api-service 黑窗口即可。
pause
