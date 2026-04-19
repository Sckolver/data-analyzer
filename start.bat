@echo off
setlocal
chcp 65001 >nul
title T-Analyze launcher

REM Работаем из папки скрипта (важно при двойном клике).
cd /d "%~dp0"

echo ============================================
echo   T-Analyze - launcher (Windows)
echo ============================================
echo.

REM 1) Проверяем Python
where python >nul 2>nul
if errorlevel 1 (
    echo [ОШИБКА] Python не найден в PATH.
    echo Установи Python 3.10+ с https://www.python.org/downloads/
    echo и при установке отметь галочку "Add Python to PATH".
    echo.
    pause
    exit /b 1
)

REM 2) Создаём venv при первом запуске
if not exist "venv\Scripts\python.exe" (
    echo [1/3] Создаю виртуальное окружение venv...
    python -m venv venv
    if errorlevel 1 (
        echo [ОШИБКА] Не удалось создать venv.
        pause
        exit /b 1
    )
) else (
    echo [1/3] venv уже существует - пропускаю.
)

REM 3) Проверяем ключевые зависимости и при необходимости ставим
echo [2/3] Проверяю зависимости...
venv\Scripts\python.exe -c "import fastapi, uvicorn, pandas, duckdb, sqlalchemy, psycopg2, pymysql" >nul 2>nul
if errorlevel 1 (
    echo     Устанавливаю пакеты из requirements.txt (первый раз может занять 1-3 минуты)...
    venv\Scripts\python.exe -m pip install --upgrade pip >nul
    venv\Scripts\python.exe -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ОШИБКА] Установка зависимостей не удалась.
        pause
        exit /b 1
    )
) else (
    echo     Все зависимости уже установлены.
)

REM 4) .env
if not exist ".env" (
    if exist ".env.example" (
        echo     Создаю .env из шаблона .env.example
        copy /Y ".env.example" ".env" >nul
        echo     [ВНИМАНИЕ] Впиши свой OPENROUTER_KEY в файл .env, иначе AI-чат Насти работать не будет.
    ) else (
        echo     [ВНИМАНИЕ] Нет .env - AI-чат Насти будет недоступен, пока не создашь .env с OPENROUTER_KEY.
    )
)

echo.
echo [3/3] Запускаю бэкенд и фронтенд...
echo.

REM 5) Бэкенд и простой HTTP-сервер для фронта - каждый в своём окне
start "T-Analyze backend"  cmd /k "venv\Scripts\python.exe backend.py"
start "T-Analyze frontend" cmd /k "venv\Scripts\python.exe -m http.server 8080"

REM Даём серверам подняться
timeout /t 3 /nobreak >nul

REM 6) Открываем UI в браузере
start "" http://localhost:8080

echo ============================================
echo   Frontend:  http://localhost:8080
echo   Backend:   http://localhost:8765
echo   API docs:  http://localhost:8765/docs
echo ============================================
echo.
echo Чтобы остановить - закрой окна "T-Analyze backend" и "T-Analyze frontend".
echo.
pause
endlocal
