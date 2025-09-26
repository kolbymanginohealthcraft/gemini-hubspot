@echo off
echo HubSpot Data Updater
echo ===================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.7+ and try again
    pause
    exit /b 1
)

REM Install requirements if needed
echo Installing required packages...
pip install -r requirements.txt

echo.
echo Starting data update process...
echo.

REM Run the data updater
python run_data_update.py

echo.
echo Process completed. Check the 'hubspot_updates' folder for results.
pause
