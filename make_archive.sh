#!/bin/bash
# Собирает zip-архив проекта для переноса на Windows / другой компьютер.
# Исключает venv, кэши Python, .git, локальный .env (там ключ от OpenRouter)
# и прочий мусор. На стороне получателя достаточно распаковать архив
# и запустить start.bat (Windows) или start.sh (macOS/Linux).

set -e
cd "$(dirname "$0")"

NAME="t-analyze"
OUT="$NAME.zip"

if ! command -v zip &> /dev/null; then
    echo "[ERROR] утилита 'zip' не найдена. Установи zip или используй финдер/архиватор вручную."
    exit 1
fi

rm -f "$OUT"

zip -r "$OUT" . \
    -x "venv/*" \
    -x "__pycache__/*" "__pycache__" \
    -x "**/__pycache__/*" "**/__pycache__" \
    -x "*.pyc" \
    -x ".git/*" \
    -x ".env" \
    -x ".DS_Store" \
    -x "*.zip" \
    -x ".cursor/*" \
    -x ".vscode/*" \
    -x ".idea/*" \
    -x "t-analyze/*" \
    > /dev/null

SIZE=$(du -h "$OUT" | cut -f1)
echo "Готово: $OUT ($SIZE)"
echo ""
echo "Как запустить на Windows:"
echo "  1) Распакуй архив"
echo "  2) Двойной клик на start.bat"
echo "  3) При первом запуске появится файл .env - впиши в него OPENROUTER_KEY"
