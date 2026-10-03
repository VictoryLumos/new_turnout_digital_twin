@echo off
chcp 65001 >nul
title 数据库停机恢复实测（需管理员）
net session >nul 2>&1
if %errorlevel% neq 0 (
  echo [失败] 本脚本需要管理员权限：请右键选择"以管理员身份运行"
  pause
  exit /b 1
)
cd /d "%~dp0.."
echo ============================================================
echo  数据库停机恢复实测（组长第四轮：数据库完整验收）
echo  流程：记录行数 - 停库 - 停机期间验证 - 起库 - 恢复验证存档
echo ============================================================
echo [1/5] 记录停机前行数...
"D:\PY\python.exe" "db\停机恢复实测段.py" --before
echo [2/5] 停止 PostgreSQL 服务（停机开始）...
net stop postgresql-x64-17
echo [3/5] 停机期间验证（连接应失败并进入重试流程，不静默）...
"D:\PY\python.exe" "db\停机恢复实测段.py" --during
echo [4/5] 重启数据库...
net start postgresql-x64-17
echo [5/5] 恢复后验证（服务自愈 + 行数不变=不重复写入）并写入记录...
"D:\PY\python.exe" "db\停机恢复实测段.py" --after
echo.
echo 完成。实测记录已追加到 docs\整改验证记录_第四轮_停机实测.md
pause
