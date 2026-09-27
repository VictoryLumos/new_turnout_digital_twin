@echo off
title 道岔数字孪生 - 项目命令行
cd /d %~dp0
echo ================================================================
echo   道岔数字孪生 · 项目命令行（已定位到项目根目录）
echo ================================================================
echo.
echo   当前目录：%cd%
echo   文档里的命令（如 python data/联调自检脚本.py）直接粘贴即可运行。
echo.
echo   常用命令速查：
echo     python data/联调自检脚本.py     自检 16 项
echo     python db/06_数据库体检.py      数据库体检 9 项
echo     python data/稳定性压测.py       3 分钟压测（先开 服务API演示.bat）
echo     python db/03_导出表格CSV.py     导出 11 项 CSV+备份
echo     exit                           关闭本窗口
echo.
echo   完整体验动线见 docs\体验路线_v1.0.md
echo ================================================================
cmd /k
