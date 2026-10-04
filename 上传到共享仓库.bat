@echo off
chcp 65001 >nul
title 道岔数字孪生 - 一键同步到共享仓库
cd /d %~dp0
echo ================================================
echo   道岔数字孪生 - 一键上传到共享仓库（main 分支）
echo ================================================
echo.
where git >nul 2>nul
if errorlevel 1 (
    echo [错误] 电脑上没装 Git
    pause
    exit /b
)
git remote get-url origin >nul 2>nul
if errorlevel 1 (
    echo [错误] 还没绑定远程仓库地址，请联系 B 线负责人配置
    pause
    exit /b
)
echo [0/3] 先拉取远端最新（合并组长/他人网页上传的文件）...
git pull origin main --no-edit
if errorlevel 1 (
    echo.
    echo [提示] 拉取失败：网络不通（代理工具开了吗）或有冲突文件。
    echo        有冲突时联系 B 线负责人处理，不要强推。
    pause
    exit /b
)
echo.
echo [1/3] 提交本地改动...
git add .
git commit -m "bat同步：更新 %date% %time%（一键上传自动提交）" >nul 2>nul
if errorlevel 1 echo        （本次没有新改动，直接推送）
echo [2/3] 推送 main 分支...
git push -u origin main
if errorlevel 1 (
    echo.
    echo [推送失败] 常见原因：
    echo   1. 网络不通——GitHub 需走代理（本机已配 git 代理 127.0.0.1:7890，
    echo      请确认代理工具正在运行）；
    echo   2. 首次推送要登录 GitHub 账号（弹窗输一次即可）；
    echo   3. 没有仓库权限——找组长开协作者邀请并接受邮件邀请。
) else (
    echo.
    echo [3/3] [完成] 已同步到共享仓库 main 分支，去群里说一声吧！
)
echo.
pause
