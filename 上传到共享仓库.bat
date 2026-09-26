@echo off
chcp 65001 >nul
title 道岔数字孪生 · 一键同步到共享仓库
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 · 一键上传到 Gitee 共享仓库
echo ================================================
echo.

where git >nul 2>nul
if errorlevel 1 (
    echo [错误] 电脑上没装 Git，先装：https://git-scm.com/download/win
    pause
    exit /b
)

rem ---- 首次运行：初始化仓库 ----
if not exist .git (
    echo [首次] 初始化本地仓库...
    git init >nul
    git branch -M master
)

rem ---- 首次运行：询问 Gitee 地址 ----
git remote get-url origin >nul 2>nul
if errorlevel 1 (
    echo 还没绑定 Gitee 仓库地址。
    echo 请先在 gitee.com 建好私有仓库，然后复制它的地址
    echo 形如 https://gitee.com/你的用户名/仓库名.git
    set /p REPO=把地址粘贴到这里后回车：
    git remote add origin "!REPO!"
    echo 已绑定：!REPO!
)

rem ---- 提交并推送 ----
echo.
echo [1/2] 提交本地改动...
git add .
git commit -m "更新 %date% %time%" >nul 2>nul
if errorlevel 1 echo        （本次没有新改动，直接推送）

echo [2/2] 推送到 Gitee...
git push -u origin master
if errorlevel 1 (
    echo.
    echo [推送失败] 常见原因：
    echo   1. 地址粘错了 → 删掉重新运行本脚本，先执行:
    echo      git remote remove origin
    echo   2. 第一次推送要登录 Gitee 账号（会弹浏览器或要输密码）
) else (
    echo.
    echo [完成] 已同步到共享仓库，去群里说一声吧！
)
echo.
pause
