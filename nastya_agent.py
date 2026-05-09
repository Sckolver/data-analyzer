"""LLM-агент Настя: цикл вызова инструментов через OpenRouter.

- Системный промпт и описания инструментов подгружаются из system_prompt.txt / tools.json.
- Статическая часть промпта — персоналия и правила, динамическая — контекст датасета.
- Агент крутится в цикле tool-calls, пока не отдаст обычный ответ (без tool_calls).
"""
from __future__ import annotations

import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from sandbox import run_pandas, run_sql

load_dotenv()

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = os.getenv("NASTYA_MODEL", "qwen/qwen3.5-27b")
MAX_TOOL_ITERATIONS = 8
REQUEST_TIMEOUT = 120

BASE_DIR = Path(__file__).resolve().parent
SYSTEM_PROMPT_PATH = BASE_DIR / "system_prompt.txt"
SYSTEM_PROMPT_DB_PATH = BASE_DIR / "system_prompt_db.txt"
TOOLS_PATH = BASE_DIR / "tools.json"

_WEEKDAYS_RU = [
    "понедельник", "вторник", "среда", "четверг",
    "пятница", "суббота", "воскресенье",
]


def _load_tools() -> List[Dict[str, Any]]:
    with open(TOOLS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_system_template() -> str:
    with open(SYSTEM_PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _load_system_template_db() -> str:
    with open(SYSTEM_PROMPT_DB_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _df_head_text(df: pd.DataFrame, n: int = 5) -> str:
    buf = io.StringIO()
    with pd.option_context(
        "display.max_columns", 50,
        "display.width", 200,
        "display.max_colwidth", 60,
    ):
        df.head(n).to_string(buf, index=False)
    return buf.getvalue()


def _columns_info(df: pd.DataFrame) -> str:
    lines = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        lines.append(f"- {col}: {dtype}")
    return "\n".join(lines)


def build_system_prompt(df: pd.DataFrame, filename: str) -> str:
    tpl = _load_system_template()
    now = datetime.now()
    return tpl.format(
        now=now.strftime("%Y-%m-%d %H:%M:%S"),
        weekday=_WEEKDAYS_RU[now.weekday()],
        filename=filename or "(без имени)",
        rows=len(df),
        cols=len(df.columns),
        columns_info=_columns_info(df),
        head=_df_head_text(df, n=5),
    )


def _tables_overview(tables: Dict[str, pd.DataFrame]) -> str:
    lines: List[str] = []
    for name, tbl in tables.items():
        lines.append(f"### {name} ({len(tbl)} строк, {len(tbl.columns)} колонок)")
        for col in tbl.columns:
            lines.append(f"  - {col}: {tbl[col].dtype}")
    return "\n".join(lines)


def _pk_fk_block(schema: Dict[str, Any]) -> str:
    lines: List[str] = []
    for tname, meta in (schema.get("tables") or {}).items():
        pk = meta.get("primary_key") or []
        fks = meta.get("foreign_keys") or []
        if not pk and not fks:
            continue
        lines.append(f"- {tname}:")
        if pk:
            lines.append(f"    PK: {', '.join(pk)}")
        for fk in fks:
            cols = ", ".join(fk.get("constrained_columns") or [])
            ref_table = fk.get("referred_table") or "?"
            ref_cols = ", ".join(fk.get("referred_columns") or [])
            lines.append(f"    FK: ({cols}) → {ref_table}({ref_cols})")
    return "\n".join(lines) if lines else "(первичные/внешние ключи не обнаружены)"


def _sample_rows_block(tables: Dict[str, pd.DataFrame], n: int = 3) -> str:
    chunks: List[str] = []
    for name, tbl in tables.items():
        chunks.append(f"### {name}\n{_df_head_text(tbl, n=n)}")
    return "\n\n".join(chunks)


def build_system_prompt_db(
    tables: Dict[str, pd.DataFrame],
    schema: Dict[str, Any],
    db_name: str,
    current_table: str,
) -> str:
    tpl = _load_system_template_db()
    now = datetime.now()
    return tpl.format(
        now=now.strftime("%Y-%m-%d %H:%M:%S"),
        weekday=_WEEKDAYS_RU[now.weekday()],
        dialect=schema.get("dialect") or "SQL",
        db_name=db_name or "(без имени)",
        current_table=current_table,
        tables_overview=_tables_overview(tables),
        pk_fk_block=_pk_fk_block(schema),
        sample_rows_block=_sample_rows_block(tables),
    )


# ---------------------------------------------------------------------------

class NastyaAgent:
    """Держит историю диалога и гоняет цикл тул-коллов.

    Режим 'dataset': работает с одним DataFrame.
    Режим 'database': работает со словарём таблиц + схемой (PK/FK).
    """

    def __init__(self, df: pd.DataFrame, filename: str) -> None:
        self.mode: str = "dataset"
        self.df = df
        self.filename = filename
        self.tables: Dict[str, pd.DataFrame] = {}
        self.schema: Dict[str, Any] = {}
        self.db_name: str = ""
        self.current_table: str = ""
        self.tools = _load_tools()
        self.history: List[Dict[str, Any]] = []
        self._refresh_system_prompt()

    @classmethod
    def from_db(
        cls,
        tables: Dict[str, pd.DataFrame],
        schema: Dict[str, Any],
        db_name: str,
        current_table: str,
    ) -> "NastyaAgent":
        if not tables:
            raise ValueError("Нельзя создать агента: нет таблиц в БД.")
        if current_table not in tables:
            current_table = next(iter(tables.keys()))
        # Инициализируем «пустым» dataset, затем перенастроим в DB-режим.
        self = cls.__new__(cls)
        self.mode = "database"
        self.df = tables[current_table]
        self.filename = db_name
        self.tables = tables
        self.schema = schema
        self.db_name = db_name
        self.current_table = current_table
        self.tools = _load_tools()
        self.history = []
        self._refresh_system_prompt()
        return self

    def set_current_table(self, name: str) -> None:
        if self.mode != "database":
            return
        if name in self.tables:
            self.current_table = name
            self.df = self.tables[name]
            self._refresh_system_prompt()

    def _refresh_system_prompt(self) -> None:
        if self.mode == "database":
            self.system_prompt = build_system_prompt_db(
                self.tables, self.schema, self.db_name, self.current_table
            )
        else:
            self.system_prompt = build_system_prompt(self.df, self.filename)

    def reset(self) -> None:
        self.history = []
        self._refresh_system_prompt()

    def set_history(self, messages: List[Dict[str, Any]]) -> None:
        """Подменяет историю диалога (используется при работе с многочатами)."""
        self.history = list(messages or [])
        self._refresh_system_prompt()

    def get_history(self) -> List[Dict[str, Any]]:
        """Возвращает копию текущей истории."""
        return list(self.history)

    # ------------------------------------------------------------------
    def _call_openrouter(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        api_key = os.getenv("OPENROUTER_KEY") or os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_KEY не задан в .env")

        payload = {
            "model": MODEL,
            "messages": messages,
            "tools": self.tools,
            "tool_choice": "auto",
            "temperature": 0.3,
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        r = requests.post(OPENROUTER_URL, headers=headers, data=json.dumps(payload),
                          timeout=REQUEST_TIMEOUT)
        if not r.ok:
            raise RuntimeError(f"OpenRouter {r.status_code}: {r.text[:500]}")
        data = r.json()
        if "choices" not in data:
            raise RuntimeError(f"Неожиданный ответ OpenRouter: {str(data)[:500]}")
        return data["choices"][0]["message"]

    # ------------------------------------------------------------------
    def _exec_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        if self.mode == "database":
            source: Any = self.tables
            current = self.current_table
        else:
            source = self.df
            current = None
        if name == "run_sql":
            return run_sql(source, arguments.get("query", ""), current_table=current)
        if name == "run_pandas":
            return run_pandas(source, arguments.get("code", ""), current_table=current)
        return {"ok": False, "error": f"Неизвестный инструмент: {name}"}

    # ------------------------------------------------------------------
    def chat(self, user_message: str) -> Dict[str, Any]:
        """Обрабатывает одно сообщение пользователя. Возвращает ответ + трейс тул-коллов."""
        self._refresh_system_prompt()  # обновим now/weekday
        self.history.append({"role": "user", "content": user_message})

        trace: List[Dict[str, Any]] = []

        for _ in range(MAX_TOOL_ITERATIONS):
            messages = [{"role": "system", "content": self.system_prompt}] + self.history
            assistant_msg = self._call_openrouter(messages)

            tool_calls = assistant_msg.get("tool_calls") or []
            content = assistant_msg.get("content")

            # Сохраняем ассистентское сообщение в историю как есть (с tool_calls, если есть)
            stored: Dict[str, Any] = {"role": "assistant", "content": content or ""}
            if tool_calls:
                stored["tool_calls"] = tool_calls
            self.history.append(stored)

            if not tool_calls:
                # Финальный ответ — текстовое сообщение пользователю.
                return {
                    "reply": content or "",
                    "trace": trace,
                }

            # Выполняем все вызванные инструменты и возвращаем tool-респонсы.
            for call in tool_calls:
                call_id = call.get("id") or ""
                fn = call.get("function", {}) or {}
                name = fn.get("name", "")
                raw_args = fn.get("arguments", "{}")
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
                except Exception:
                    args = {}
                    tool_result = {"ok": False,
                                   "error": f"Не удалось распарсить arguments как JSON: {raw_args[:300]}"}
                else:
                    tool_result = self._exec_tool(name, args)

                trace.append({"name": name, "arguments": args, "result": tool_result})

                self.history.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "name": name,
                    "content": json.dumps(tool_result, ensure_ascii=False,
                                          default=_json_default)[:8000],
                })

        # Лимит итераций исчерпан.
        final = (
            "Ой, кажется, я застряла в цикле инструментов и не смогла собрать финальный ответ. "
            "Попробуй переформулировать вопрос ✿"
        )
        self.history.append({"role": "assistant", "content": final})
        return {"reply": final, "trace": trace}


def _json_default(o: Any) -> Any:
    try:
        import numpy as np
        if isinstance(o, np.generic):
            return o.item()
    except Exception:
        pass
    if isinstance(o, (pd.Timestamp, datetime)):
        return o.isoformat()
    return str(o)
