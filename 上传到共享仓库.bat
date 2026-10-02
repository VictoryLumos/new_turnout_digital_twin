@echo off
setlocal EnableDelayedExpansion
title 道岔数字孪生 - 一键同步到共享仓库
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 - 一键上传到共享仓库
echo ================================================
echo.
where git >nul 2>nul
if errorlevel 1 (
    echo [错误] 电脑上没装 Git
    pause
    exit /b
)
if not exist .git (
    echo [首次] 初始化本地仓库...
    git init >nul
    git branch -M master
)
git remote get-url origin >nul 2>nul
if errorlevel 1 (
    echo 还没绑定仓库地址。先建好 Gitee/GitHub 私有仓库，
    echo 复制地址（形如 https://gitee.com/用户名/仓库名.git）
    set /p REPO=把地址粘贴到这里后回车：
    git remote add origin "!REPO!"
    echo 已绑定：!REPO!
)
echo.
echo [1/2] 提交本地改动...
git add .
git commit -m "更新 %date% %time%" >nul 2>nul
if errorlevel 1 echo        （本次没有新改动，直接推送）
echo [2/2] 推送...
git push -u origin master
if errorlevel 1 (
    echo.
    echo [推送失败] 常见原因：地址粘错 / 首次推送要登录 / 没有仓库权限
) else (
    echo.
    echo [完成] 已同步到共享仓库，去群里说一声吧！
)
echo.
pause
