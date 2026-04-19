#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "============================================"
echo "  T-Analyze - launcher (macOS/Linux)"
echo "============================================"
echo ""

# 1) Проверка Python 3
if ! command -v python3 &> /dev/null; then
    echo "[ОШИБКА] python3 не найден. Установи Python 3.10+."
    exit 1
fi

# 2) venv
if [ ! -x "venv/bin/python" ]; then
    echo "[1/3] Создаю виртуальное окружение venv..."
    python3 -m venv venv
else
    echo "[1/3] venv уже существует - пропускаю."
fi

PY="venv/bin/python"
PIP="venv/bin/pip"

# 3) Зависимости
echo "[2/3] Проверяю зависимости..."
if ! "$PY" -c "import fastapi, uvicorn, pandas, duckdb, sqlalchemy, psycopg2, pymysql" &> /dev/null; then
    echo "    Устанавливаю пакеты из requirements.txt..."
    "$PY" -m pip install --upgrade pip > /dev/null
    "$PIP" install -r requirements.txt
else
    echo "    Все зависимости уже установлены."
fi

# 4) .env
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "    Создаю .env из шаблона .env.example"
        cp .env.example .env
        echo "    [ВНИМАНИЕ] Впиши свой OPENROUTER_KEY в файл .env, иначе AI-чат Насти работать не будет."
    else
        echo "    [ВНИМАНИЕ] Нет .env - AI-чат Насти будет недоступен."
    fi
fi

echo ""
echo "[3/3] Запускаю бэкенд и фронтенд..."
echo ""

# Бэкенд в фоне
"$PY" backend.py &
BACKEND_PID=$!

# Простой HTTP-сервер для фронта в фоне
"$PY" -m http.server 8080 > /dev/null 2>&1 &
FRONTEND_PID=$!

# Остановка обоих по Ctrl+C / exit
trap 'echo ""; echo "Останавливаю сервера..."; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true; exit 0' INT TERM EXIT

# Дать серверам стартануть и открыть браузер
sleep 2
if command -v open &> /dev/null; then
    open http://localhost:8080
elif command -v xdg-open &> /dev/null; then
    xdg-open http://localhost:8080
fi

echo "============================================"
echo "  Frontend:  http://localhost:8080"
echo "  Backend:   http://localhost:8765"
echo "  API docs:  http://localhost:8765/docs"
echo "============================================"
echo ""
echo "Нажми Ctrl+C чтобы остановить оба сервера."
echo ""

wait $BACKEND_PID
