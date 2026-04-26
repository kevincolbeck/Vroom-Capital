@echo off
echo ============================================
echo  Legion Bot - Installation
echo ============================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Please install Python 3.11+
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Check Node.js
node --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Node.js not found. Please install Node.js 18+
    echo Download: https://nodejs.org/
    pause
    exit /b 1
)

echo [1/4] Installing Python dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install Python packages
    pause
    exit /b 1
)

echo.
echo [2/4] Installing Node.js dependencies...
cd frontend
call npm install
if errorlevel 1 (
    echo ERROR: Failed to install Node packages
    cd ..
    pause
    exit /b 1
)
cd ..

echo.
echo [3/4] Setting up .env file...
if not exist ".env" (
    copy .env.example .env
    echo Created .env from template.
) else (
    echo .env already exists, skipping.
)

echo.
echo [4/4] Building frontend...
cd frontend
call npm run build
cd ..

echo.
echo ============================================
echo  Installation Complete!
echo ============================================
echo.
echo Next steps:
echo  1. Edit .env with your API keys and password
echo  2. Run start.bat to launch the bot
echo.
pause
