"""Файловое хранилище переписок с Настей.

Структура chats.json:
{
  "<session_key>": {
    "chats": [
      { "id": "<uuid>", "name": "Новый чат", "created_at": "...", "updated_at": "...", "history": [...] }
    ]
  }
}

session_key формируется на стороне backend.py через `build_session_key()` —
для dataset-режима это "dataset:<filename>", для database-режима —
"db:<dialect>://<user>@<host>:<port>/<dbname>". Так у одной БД (= одних
кредов подключения) свой набор сохранённых диалогов.
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_CHATS_PATH = Path(__file__).resolve().parent / "chats.json"
_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load() -> Dict[str, Any]:
    if not _CHATS_PATH.exists():
        return {}
    try:
        with open(_CHATS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {}
            return data
    except (json.JSONDecodeError, OSError):
        return {}


def _save(data: Dict[str, Any]) -> None:
    tmp = _CHATS_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(_CHATS_PATH)


def _ensure_bucket(data: Dict[str, Any], session_key: str) -> Dict[str, Any]:
    bucket = data.get(session_key)
    if not bucket or not isinstance(bucket, dict):
        bucket = {"chats": []}
        data[session_key] = bucket
    if "chats" not in bucket or not isinstance(bucket["chats"], list):
        bucket["chats"] = []
    return bucket


def list_chats(session_key: str) -> List[Dict[str, Any]]:
    """Список чатов для сессии — без истории, только метаданные."""
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        out = []
        for chat in bucket["chats"]:
            out.append({
                "id": chat.get("id"),
                "name": chat.get("name") or "Без названия",
                "created_at": chat.get("created_at"),
                "updated_at": chat.get("updated_at"),
                "messages_count": len(chat.get("history") or []),
            })
        # Свежие сверху
        out.sort(key=lambda c: c.get("updated_at") or c.get("created_at") or "", reverse=True)
        return out


def create_chat(session_key: str, name: Optional[str] = None) -> Dict[str, Any]:
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        chat_id = str(uuid.uuid4())
        now = _now_iso()
        chat = {
            "id": chat_id,
            "name": (name or "").strip() or "Новый чат",
            "created_at": now,
            "updated_at": now,
            "history": [],
        }
        bucket["chats"].append(chat)
        _save(data)
        return {
            "id": chat["id"],
            "name": chat["name"],
            "created_at": chat["created_at"],
            "updated_at": chat["updated_at"],
            "messages_count": 0,
        }


def get_chat(session_key: str, chat_id: str) -> Optional[Dict[str, Any]]:
    """Полный чат с историей или None, если не найден."""
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        for chat in bucket["chats"]:
            if chat.get("id") == chat_id:
                return {
                    "id": chat.get("id"),
                    "name": chat.get("name"),
                    "created_at": chat.get("created_at"),
                    "updated_at": chat.get("updated_at"),
                    "history": list(chat.get("history") or []),
                }
        return None


def rename_chat(session_key: str, chat_id: str, new_name: str) -> Optional[Dict[str, Any]]:
    new_name = (new_name or "").strip()
    if not new_name:
        return None
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        for chat in bucket["chats"]:
            if chat.get("id") == chat_id:
                chat["name"] = new_name
                chat["updated_at"] = _now_iso()
                _save(data)
                return {
                    "id": chat["id"],
                    "name": chat["name"],
                    "updated_at": chat["updated_at"],
                }
        return None


def delete_chat(session_key: str, chat_id: str) -> bool:
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        before = len(bucket["chats"])
        bucket["chats"] = [c for c in bucket["chats"] if c.get("id") != chat_id]
        if len(bucket["chats"]) == before:
            return False
        _save(data)
        return True


def update_chat_history(session_key: str, chat_id: str, history: List[Dict[str, Any]]) -> bool:
    """Перезаписывает историю существующего чата."""
    with _LOCK:
        data = _load()
        bucket = _ensure_bucket(data, session_key)
        for chat in bucket["chats"]:
            if chat.get("id") == chat_id:
                chat["history"] = list(history or [])
                chat["updated_at"] = _now_iso()
                _save(data)
                return True
        return False
