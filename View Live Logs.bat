@echo off
title KrowSec AutoToken Bot - LIVE LOGS
echo ============================================
echo   KrowSec AutoToken Bot - LIVE LOGS
echo   (Ctrl+C closes viewer only, not the bot)
echo ============================================
powershell -NoProfile -Command "Get-Content -LiteralPath '%~dp0bot_live.log' -Wait -Tail 40"
pause
