@echo off
echo ============================================
echo  Legion Bot - Starting Services
echo ============================================

:: Check for .env
if not exist ".env" (
    echo Creating .env from example...
    copy .env.example .env
    echo Please edit .env with your settings before running.
    pause
    exit /b 1
)

:: Start backend in background
echo Starting Backend API (port 8000)...
start "Legion Bot Backend" cmd /k "cd /d %~dp0 && python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload"

:: Wait for backend
timeout /t 3 /nobreak > nul

:: Start frontend dev server
echo Starting Frontend (port 5173)...
cd frontend
start "Legion Bot Frontend" cmd /k "npm run dev"
cd ..

echo.
echo ============================================
echo  Services Started!
echo  Backend API:  http://localhost:8000
echo  Frontend UI:  http://localhost:5173
echo  API Docs:     http://localhost:8000/docs
echo ============================================
echo.
echo Default password: admin123 (change in .env)
echo.
pause
