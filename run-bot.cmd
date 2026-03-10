@echo off
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
"D:\Program Files\nodejs\node.exe" "%SCRIPT_DIR%dist\index.js"
