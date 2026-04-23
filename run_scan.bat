@echo off
REM Windows Task Scheduler entry point — launches the Julia scan inside WSL.
REM Edit WSL_DISTRO and PROJECT_DIR below if your layout differs.

set WSL_DISTRO=Ubuntu
set PROJECT_DIR=/home/jeepee/repos/StockMonitor.jl

wsl.exe -d %WSL_DISTRO% -e bash -lc "cd %PROJECT_DIR% && ~/.juliaup/bin/julia --project scripts/scan.jl"
exit /b %ERRORLEVEL%
