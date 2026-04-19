# T-Analyze

Сервис для анализа данных: загружай CSV/Excel/JSON или подключайся к базе (PostgreSQL / MySQL / SQLite) — смотри метрики качества, строй визуализации, общайся с AI-ассистентом Настей.

## Быстрый запуск

### 1. Клонируй репозиторий

```bash
git clone https://github.com/Sckolver/data-analyzer.git
cd data-analyzer
```

### 2. Создай `.env` с ключом OpenRouter

```bash
cp .env.example .env
# открой .env и впиши свой OPENROUTER_KEY
```

Без ключа метрики и визуализации работают, но AI-чат Насти — нет.

### 3. Запусти

**macOS / Linux:**

```bash
./start.sh
```

**Windows:**

Двойной клик на `start.bat` — или из cmd:

```cmd
start.bat
```

Скрипт сам создаст `venv`, поставит зависимости и откроет браузер на `http://localhost:8080`. Бэкенд поднимется на `http://localhost:8765`.

### Ручной запуск

Если не хочется скрипт:

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python backend.py &              # бэкенд 127.0.0.1:8765
python -m http.server 8080       # фронт
```

Затем открой `http://localhost:8080`.

## Как работать

**Датасет:** на главном экране выбери «Датасет», загрузи `.csv`, `.xlsx`, `.xls` или `.json`. Для быстрой проверки есть `sample_data.csv`.

**База данных:** выбери «База данных», укажи СУБД и параметры подключения. Для SQLite — путь к `.db`-файлу. Бэкенд читает все таблицы в память снэпшотом и собирает схему (PK, FK). В визуализациях появится селектор таблицы; Настя видит всю схему и умеет делать JOIN через `CREATE TEMP VIEW`.

## Стек

Backend: FastAPI, pandas, DuckDB, SQLAlchemy. Frontend: vanilla JS + Chart.js. AI: OpenRouter с tool-calls (`run_sql`, `run_pandas`) в изолированной DuckDB-песочнице.

## Перенести на другую машину

```bash
./make_archive.sh
```

Соберёт `t-analyze.zip` без `venv`, кэша и `.env` — можно переслать и распаковать где угодно.
