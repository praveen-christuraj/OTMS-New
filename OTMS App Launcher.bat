@echo off
title Oil Terminal Management System - OTMS
color 0A

echo ========================================
echo   Oil Terminal Management System
echo ========================================
echo.

cd /d "D:\Project OTMS-New"

echo Starting OTMS Server...
echo.
echo The application will open in your default browser.
echo Please wait...
echo.

REM Start Streamlit and automatically open browser
start /B python -m streamlit run oil_app_ui.py --server.headless true

REM Wait for server to start
timeout /t 3 /nobreak >nul

REM Open browser
start http://localhost:8501

echo.
echo ========================================
echo   OTMS is running
echo   Browser should open automatically
echo   Close this window to stop the server
echo ========================================
echo.
echo Press any key to stop the server...
pause >nul

REM Kill Streamlit process on exit
taskkill /F /IM streamlit.exe >nul 2>&1
taskkill /F /IM python.exe /FI "WINDOWTITLE eq *streamlit*" >nul 2>&1

echo Server stopped.
timeout /t 2 >nul