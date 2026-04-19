"""Песочницы для безопасного исполнения SQL и pandas кода от LLM.

- SQL: только читающие запросы (SELECT/WITH/подзапросы), плюс CREATE TEMP VIEW/TABLE AS SELECT
  для промежуточных подвыборок. Любые модификации источника запрещены.
- Pandas: ограниченный exec с белым списком AST, без импортов, файлов, сети, приватных атрибутов.
  df передаётся как deepcopy — мутации не затрагивают исходный DataFrame.

Поддерживаются два режима источника данных:
- Одиночный DataFrame: регистрируется в DuckDB как `df`, в pandas-песочнице доступен как `df`.
- Словарь таблиц (режим БД): каждая таблица регистрируется под своим именем, плюс алиас `df`
  указывает на «текущую» (или первую) таблицу для обратной совместимости кода.
"""
from __future__ import annotations

import ast
import io
from typing import Any, Dict, Optional, Tuple, Union

import duckdb
import numpy as np
import pandas as pd
import sqlglot
from sqlglot import exp


MAX_PREVIEW_ROWS = 50

DataSource = Union[pd.DataFrame, Dict[str, pd.DataFrame]]


def _normalize_source(
    source: DataSource,
    current_table: Optional[str] = None,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """Приводит источник к виду (tables_dict, current_df)."""
    if isinstance(source, pd.DataFrame):
        return {"df": source}, source
    if isinstance(source, dict) and source:
        if current_table and current_table in source:
            current = source[current_table]
        else:
            current = next(iter(source.values()))
        return source, current
    raise ValueError("Источник данных должен быть DataFrame или непустым dict[str, DataFrame].")


# ---------------------------------------------------------------------------
# SQL sandbox
# ---------------------------------------------------------------------------

_FORBIDDEN_SQL_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Alter,
    exp.AlterColumn,
    exp.Merge,
    exp.Copy,
    exp.Pragma,
    exp.Set,
    exp.Attach,
    exp.Detach,
    exp.TruncateTable,
)


def _validate_sql(query: str) -> Tuple[bool, str]:
    """Разрешаем только чтение и создание временных view/table AS SELECT."""
    try:
        statements = sqlglot.parse(query, read="duckdb")
    except Exception as e:
        return False, f"Не удалось распарсить SQL: {e}"

    if not statements:
        return False, "Пустой SQL."

    for stmt in statements:
        if stmt is None:
            continue

        if isinstance(stmt, _FORBIDDEN_SQL_NODES):
            return False, (
                f"Запрещённая операция: {type(stmt).__name__}. "
                "Разрешены только SELECT/WITH и CREATE TEMP VIEW|TABLE AS SELECT."
            )

        if isinstance(stmt, exp.Create):
            kind = (stmt.args.get("kind") or "").upper()
            if kind not in ("VIEW", "TABLE"):
                return False, f"CREATE {kind or '?'} запрещён."
            if not stmt.args.get("properties") and not stmt.args.get("temporary"):
                props = stmt.args.get("properties")
                temp_flag = False
                if props is not None:
                    for p in props.expressions:
                        if isinstance(p, exp.TemporaryProperty):
                            temp_flag = True
                            break
                if not temp_flag:
                    return False, (
                        "Разрешены только временные объекты: "
                        "используй CREATE TEMP VIEW ... AS SELECT ..."
                    )
            inner = stmt.expression
            if inner is None or not isinstance(inner, (exp.Select, exp.Union, exp.With)):
                return False, "CREATE ... должен быть AS SELECT."

        for bad in stmt.find_all(_FORBIDDEN_SQL_NODES):
            return False, f"Запрещённая операция внутри запроса: {type(bad).__name__}"

    return True, ""


def run_sql(
    source: DataSource,
    query: str,
    current_table: Optional[str] = None,
) -> Dict[str, Any]:
    ok, reason = _validate_sql(query)
    if not ok:
        return {"ok": False, "error": reason}

    tables, current_df = _normalize_source(source, current_table)

    con = duckdb.connect(database=":memory:")
    try:
        # Регистрируем все таблицы под исходными именами.
        for name, tbl in tables.items():
            con.register(name, tbl)
        # Алиас `df` для обратной совместимости — всегда указывает на текущую таблицу.
        if "df" not in tables:
            con.register("df", current_df)
        statements = [s for s in sqlglot.parse(query, read="duckdb") if s is not None]
        last_result: pd.DataFrame | None = None
        for stmt in statements:
            sql_text = stmt.sql(dialect="duckdb")
            res = con.execute(sql_text)
            try:
                last_result = res.fetch_df()
            except Exception:
                last_result = None
        if last_result is None:
            return {"ok": True, "rows": 0, "columns": [], "preview": "(нет табличного результата)"}

        total = len(last_result)
        preview = last_result.head(MAX_PREVIEW_ROWS)
        return {
            "ok": True,
            "rows": int(total),
            "columns": [str(c) for c in last_result.columns],
            "truncated": bool(total > MAX_PREVIEW_ROWS),
            "preview": _df_to_text(preview),
        }
    except Exception as e:
        return {"ok": False, "error": f"Ошибка выполнения SQL: {e}"}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Pandas sandbox
# ---------------------------------------------------------------------------

_ALLOWED_AST_NODES = {
    ast.Module, ast.Expr, ast.Assign, ast.AugAssign, ast.AnnAssign,
    ast.Name, ast.Load, ast.Store, ast.Del,
    ast.Constant, ast.JoinedStr, ast.FormattedValue,
    ast.List, ast.Tuple, ast.Set, ast.Dict, ast.Starred,
    ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
    ast.LShift, ast.RShift, ast.BitOr, ast.BitXor, ast.BitAnd, ast.MatMult,
    ast.And, ast.Or, ast.Not, ast.Invert, ast.UAdd, ast.USub,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Is, ast.IsNot, ast.In, ast.NotIn,
    ast.Call, ast.keyword,
    ast.Attribute, ast.Subscript, ast.Slice, ast.Index if hasattr(ast, "Index") else ast.Slice,
    ast.IfExp, ast.If, ast.For, ast.While, ast.Break, ast.Continue, ast.Pass,
    ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp, ast.comprehension,
    ast.Lambda, ast.arguments, ast.arg,
    ast.Return, ast.FunctionDef,
    ast.Try, ast.ExceptHandler,
}

_BLOCKED_NAMES = {
    "eval", "exec", "compile", "open", "input", "__import__",
    "globals", "locals", "vars", "getattr", "setattr", "delattr",
    "exit", "quit", "help", "breakpoint",
    "os", "sys", "subprocess", "shutil", "pathlib", "socket",
    "importlib", "builtins", "ctypes",
}


def _validate_pandas(code: str) -> Tuple[bool, str]:
    try:
        tree = ast.parse(code, mode="exec")
    except SyntaxError as e:
        return False, f"Синтаксическая ошибка: {e}"

    for node in ast.walk(tree):
        if type(node) not in _ALLOWED_AST_NODES:
            return False, f"Запрещённая конструкция: {type(node).__name__}"
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return False, "Импорты запрещены."
        if isinstance(node, ast.Attribute):
            if isinstance(node.attr, str) and node.attr.startswith("_"):
                return False, f"Обращение к приватному атрибуту запрещено: .{node.attr}"
        if isinstance(node, ast.Name) and node.id in _BLOCKED_NAMES:
            return False, f"Имя '{node.id}' запрещено в песочнице."
    return True, ""


_SAFE_BUILTINS = {
    "abs": abs, "min": min, "max": max, "sum": sum, "len": len,
    "range": range, "round": round, "sorted": sorted, "reversed": reversed,
    "enumerate": enumerate, "zip": zip, "map": map, "filter": filter,
    "any": any, "all": all, "int": int, "float": float, "str": str,
    "bool": bool, "list": list, "dict": dict, "set": set, "tuple": tuple,
    "print": lambda *a, **k: None,
}


def run_pandas(
    source: DataSource,
    code: str,
    current_table: Optional[str] = None,
) -> Dict[str, Any]:
    ok, reason = _validate_pandas(code)
    if not ok:
        return {"ok": False, "error": reason}

    tables, current_df = _normalize_source(source, current_table)
    # Глубокие копии, чтобы код от LLM не мог замутировать исходные данные.
    local_tables = {name: tbl.copy(deep=True) for name, tbl in tables.items()}
    local_current = local_tables.get(current_table) if current_table else None
    if local_current is None:
        # current_df — это один из исходных; найдём соответствующую копию по ссылке на имя.
        for name, tbl in tables.items():
            if tbl is current_df:
                local_current = local_tables[name]
                break
        if local_current is None:
            local_current = current_df.copy(deep=True)

    ns: Dict[str, Any] = {
        "df": local_current,
        "tables": local_tables,
        "pd": pd,
        "np": np,
        "__builtins__": _SAFE_BUILTINS,
    }
    try:
        exec(compile(code, "<nastya-pandas>", "exec"), ns, ns)
    except Exception as e:
        return {"ok": False, "error": f"Ошибка исполнения pandas-кода: {type(e).__name__}: {e}"}

    if "result" not in ns:
        return {"ok": False, "error": "Код должен присвоить итог переменной `result`."}

    result = ns["result"]
    return {"ok": True, **_format_result(result)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df_to_text(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    with pd.option_context(
        "display.max_rows", MAX_PREVIEW_ROWS,
        "display.max_columns", 50,
        "display.width", 200,
        "display.max_colwidth", 60,
    ):
        df.to_string(buf, index=False)
    return buf.getvalue()


def _format_result(result: Any) -> Dict[str, Any]:
    if isinstance(result, pd.DataFrame):
        total = len(result)
        return {
            "type": "dataframe",
            "shape": [int(total), int(result.shape[1])],
            "columns": [str(c) for c in result.columns],
            "truncated": bool(total > MAX_PREVIEW_ROWS),
            "preview": _df_to_text(result.head(MAX_PREVIEW_ROWS)),
        }
    if isinstance(result, pd.Series):
        total = len(result)
        return {
            "type": "series",
            "name": str(result.name) if result.name is not None else None,
            "length": int(total),
            "truncated": bool(total > MAX_PREVIEW_ROWS),
            "preview": _df_to_text(result.head(MAX_PREVIEW_ROWS).to_frame()),
        }
    if isinstance(result, (np.generic,)):
        return {"type": "scalar", "value": result.item()}
    if isinstance(result, (int, float, bool, str)) or result is None:
        return {"type": "scalar", "value": result}
    if isinstance(result, dict):
        return {"type": "dict", "value": {str(k): _coerce(v) for k, v in result.items()}}
    if isinstance(result, (list, tuple, set)):
        return {"type": "list", "value": [_coerce(v) for v in list(result)[:MAX_PREVIEW_ROWS]]}
    return {"type": "repr", "value": repr(result)[:4000]}


def _coerce(v: Any) -> Any:
    if isinstance(v, np.generic):
        return v.item()
    if isinstance(v, (pd.Timestamp,)):
        return v.isoformat()
    if isinstance(v, (pd.DataFrame, pd.Series)):
        return _df_to_text(v.head(MAX_PREVIEW_ROWS) if isinstance(v, pd.DataFrame)
                          else v.head(MAX_PREVIEW_ROWS).to_frame())
    try:
        import json
        json.dumps(v)
        return v
    except Exception:
        return repr(v)[:500]
