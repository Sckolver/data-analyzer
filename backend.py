from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import numpy as np
from scipy import stats
import io
import json
import math
import os
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from urllib.parse import quote_plus
import warnings

import sqlalchemy
from sqlalchemy import create_engine, inspect as sa_inspect
from sqlalchemy.engine import Engine

from nastya_agent import NastyaAgent

warnings.filterwarnings('ignore')


def to_py(o: Any) -> Any:
    """Рекурсивно приводит numpy/pandas типы к чистому Python для JSON-сериализации."""
    if o is None:
        return None
    if isinstance(o, dict):
        return {to_py(k): to_py(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [to_py(v) for v in o]
    if isinstance(o, np.ndarray):
        return to_py(o.tolist())
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        v = float(o)
        return None if math.isnan(v) or math.isinf(v) else v
    if isinstance(o, float):
        return None if math.isnan(o) or math.isinf(o) else o
    if isinstance(o, (pd.Timestamp, datetime, date)):
        return o.isoformat()
    if isinstance(o, np.dtype):
        return str(o)
    try:
        if pd.isna(o):
            return None
    except (TypeError, ValueError):
        pass
    return o

app = FastAPI(title="Dataset Analyzer API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global storage for current dataset / database
current_dataset: Optional[pd.DataFrame] = None
dataset_metadata: Dict[str, Any] = {}
nastya_agent: Optional[NastyaAgent] = None

# Режим источника: 'dataset' (файл) или 'database' (подключение к БД).
data_source_mode: Optional[str] = None
# В режиме database: словарь таблиц и собранная схема.
db_tables: Dict[str, pd.DataFrame] = {}
db_schema: Dict[str, Any] = {}
db_meta: Dict[str, Any] = {}
current_table_name: Optional[str] = None

BASE_DIR = Path(__file__).resolve().parent
CHATS_PATH = BASE_DIR / "chats.json"
SUPPORT_PATH = BASE_DIR / "support.json"


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Атомарно пишет JSON через временный файл + os.replace."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_chats() -> Dict[str, Any]:
    if not CHATS_PATH.exists():
        return {"chats": {}}
    try:
        with open(CHATS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "chats" not in data:
            return {"chats": {}}
        if not isinstance(data["chats"], dict):
            data["chats"] = {}
        return data
    except (json.JSONDecodeError, OSError):
        return {"chats": {}}


def _save_chats(data: Dict[str, Any]) -> None:
    _atomic_write_json(CHATS_PATH, data)


def _current_source_id() -> str:
    """Идентификатор текущего источника данных для группировки чатов."""
    if data_source_mode == "database":
        dialect = (db_meta or {}).get("dialect") or "db"
        host = (db_meta or {}).get("host") or "local"
        dbname = (db_meta or {}).get("dbname") or "?"
        return f"db:{dialect}@{host}/{dbname}"
    if data_source_mode == "dataset":
        fname = (dataset_metadata or {}).get("filename") or "dataset"
        return f"dataset:{fname}"
    return "none"


def _select_dataset(table: Optional[str]) -> pd.DataFrame:
    """Возвращает DataFrame для анализа: либо текущий датасет, либо указанную таблицу БД.

    Побочный эффект: в database-режиме синхронизирует current_dataset и current_table_name
    с выбором, чтобы последующие эндпоинты (и агент Насти) работали с той же таблицей.
    """
    global current_dataset, current_table_name

    if data_source_mode == "database":
        if not db_tables:
            raise HTTPException(status_code=400, detail="База данных не подключена")
        target = table or current_table_name or next(iter(db_tables.keys()))
        if target not in db_tables:
            raise HTTPException(status_code=404, detail=f"Таблица '{target}' не найдена в БД")
        current_table_name = target
        current_dataset = db_tables[target]
        if nastya_agent is not None:
            nastya_agent.set_current_table(target)
        return current_dataset

    if current_dataset is None:
        raise HTTPException(status_code=400, detail="Датасет не загружен")
    return current_dataset


def detect_column_type(series: pd.Series) -> str:
    """Определяет тип данных столбца"""
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return "empty"
    
    # Numeric
    if pd.api.types.is_numeric_dtype(series):
        if series.nunique() == 2:
            return "binary"
        elif len(non_null.unique()) / len(non_null) < 0.05:
            return "categorical"
        else:
            return "numeric"
    
    # Boolean
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    
    # Datetime
    try:
        pd.to_datetime(series, errors='raise')
        return "datetime"
    except:
        pass
    
    # Text/Categorical
    unique_ratio = len(non_null.unique()) / len(non_null)
    if unique_ratio < 0.5:
        return "categorical"
    else:
        return "text"


def calculate_outliers(series: pd.Series) -> Dict[str, Any]:
    """Вычисляет выбросы методом IQR"""
    if not pd.api.types.is_numeric_dtype(series):
        return {"count": 0, "percentage": 0, "method": "N/A"}
    
    non_null = series.dropna()
    if len(non_null) == 0:
        return {"count": 0, "percentage": 0, "method": "N/A"}
    
    Q1 = non_null.quantile(0.25)
    Q3 = non_null.quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = ((non_null < lower_bound) | (non_null > upper_bound)).sum()
    
    return {
        "count": int(outliers),
        "percentage": round(outliers / len(non_null) * 100, 2),
        "lower_bound": float(lower_bound),
        "upper_bound": float(upper_bound),
        "method": "IQR (1.5)"
    }


def calculate_skewness_kurtosis(series: pd.Series) -> Dict[str, float]:
    """Вычисляет асимметрию и эксцесс"""
    if not pd.api.types.is_numeric_dtype(series):
        return {"skewness": None, "kurtosis": None}
    
    non_null = series.dropna()
    if len(non_null) < 3:
        return {"skewness": None, "kurtosis": None}
    
    return {
        "skewness": round(float(stats.skew(non_null)), 4),
        "kurtosis": round(float(stats.kurtosis(non_null)), 4)
    }


def calculate_normality_test(series: pd.Series) -> Dict[str, Any]:
    """Тест на нормальность распределения"""
    if not pd.api.types.is_numeric_dtype(series):
        return {"test": "N/A", "statistic": None, "p_value": None, "is_normal": None}
    
    non_null = series.dropna()
    if len(non_null) < 8:
        return {"test": "N/A", "statistic": None, "p_value": None, "is_normal": None}
    
    try:
        statistic, p_value = stats.shapiro(non_null[:5000])  # Limit to 5000 for performance
        return {
            "test": "Shapiro-Wilk",
            "statistic": round(float(statistic), 6),
            "p_value": round(float(p_value), 6),
            "is_normal": bool(p_value > 0.05)
        }
    except:
        return {"test": "Failed", "statistic": None, "p_value": None, "is_normal": None}


def calculate_entropy(series: pd.Series) -> float:
    """Вычисляет энтропию Шеннона"""
    value_counts = series.value_counts()
    probabilities = value_counts / len(series)
    entropy = -np.sum(probabilities * np.log2(probabilities + 1e-9))
    return round(float(entropy), 4)


def detect_constant_columns(df: pd.DataFrame) -> List[str]:
    """Находит столбцы с константными значениями"""
    constant_cols = []
    for col in df.columns:
        if df[col].nunique() <= 1:
            constant_cols.append(col)
    return constant_cols


def detect_high_cardinality(df: pd.DataFrame, threshold: float = 0.9) -> List[str]:
    """Находит столбцы с высокой кардинальностью"""
    high_card_cols = []
    for col in df.columns:
        if df[col].nunique() / len(df) > threshold:
            high_card_cols.append(col)
    return high_card_cols


def analyze_missing_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    """Анализирует паттерны пропущенных значений"""
    missing_matrix = df.isnull()
    
    # Полностью пустые строки
    completely_empty_rows = missing_matrix.all(axis=1).sum()
    
    # Строки с хотя бы одним пропуском
    rows_with_missing = missing_matrix.any(axis=1).sum()
    
    # Корреляция пропусков между столбцами
    if len(df.columns) > 1:
        missing_corr = missing_matrix.corr()
        high_corr_pairs = []
        
        for i in range(len(missing_corr.columns)):
            for j in range(i + 1, len(missing_corr.columns)):
                corr_val = missing_corr.iloc[i, j]
                if abs(corr_val) > 0.5 and not np.isnan(corr_val):
                    high_corr_pairs.append({
                        "col1": missing_corr.columns[i],
                        "col2": missing_corr.columns[j],
                        "correlation": round(float(corr_val), 3)
                    })
    else:
        high_corr_pairs = []
    
    return {
        "completely_empty_rows": int(completely_empty_rows),
        "rows_with_missing": int(rows_with_missing),
        "missing_correlation_pairs": high_corr_pairs
    }


def calculate_correlation_matrix(df: pd.DataFrame) -> Dict[str, Any]:
    """Вычисляет корреляционную матрицу для числовых столбцов"""
    numeric_df = df.select_dtypes(include=[np.number])
    
    if len(numeric_df.columns) < 2:
        return {"available": False, "reason": "Недостаточно числовых столбцов"}
    
    corr_matrix = numeric_df.corr()
    
    # Найти сильные корреляции
    strong_correlations = []
    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            corr_val = corr_matrix.iloc[i, j]
            if abs(corr_val) > 0.7 and not np.isnan(corr_val):
                strong_correlations.append({
                    "col1": corr_matrix.columns[i],
                    "col2": corr_matrix.columns[j],
                    "correlation": round(float(corr_val), 3),
                    "strength": "strong" if abs(corr_val) > 0.9 else "moderate"
                })
    
    return {
        "available": True,
        "matrix": {k: {k2: float(v2) if not np.isnan(v2) else None for k2, v2 in v.items()} 
                   for k, v in corr_matrix.to_dict().items()},
        "strong_correlations": strong_correlations
    }


def detect_duplicates_detailed(df: pd.DataFrame) -> Dict[str, Any]:
    """Детальный анализ дубликатов"""
    total_duplicates = df.duplicated().sum()
    
    # Дубликаты с учетом всех столбцов
    duplicate_rows = df[df.duplicated(keep=False)]
    
    # Дубликаты по подмножествам столбцов
    subset_duplicates = {}
    for col in df.columns:
        col_dups = df.duplicated(subset=[col], keep=False).sum()
        if col_dups > 0:
            subset_duplicates[col] = int(col_dups)
    
    return {
        "total_duplicate_rows": int(total_duplicates),
        "percentage": round(total_duplicates / len(df) * 100, 2),
        "duplicate_groups": int(len(duplicate_rows) / 2) if len(duplicate_rows) > 0 else 0,
        "column_duplicates": subset_duplicates
    }


def calculate_data_quality_score(column_info: Dict[str, Any]) -> float:
    """Вычисляет общую оценку качества данных (0-100)"""
    score = 100.0
    
    # Штраф за пропуски
    missing_penalty = column_info['missing_percentage'] * 0.5
    score -= missing_penalty
    
    # Штраф за выбросы (если есть)
    if 'outliers' in column_info and column_info['outliers']['percentage'] > 0:
        outlier_penalty = min(column_info['outliers']['percentage'] * 0.3, 15)
        score -= outlier_penalty
    
    # Бонус за разнообразие данных (но не слишком высокое)
    if 0.1 < column_info['unique_ratio'] < 0.8:
        score += 5
    
    return max(0, min(100, round(score, 2)))


@app.post("/api/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """Загрузка и первичный анализ датасета"""
    global current_dataset, dataset_metadata, data_source_mode
    global db_tables, db_schema, db_meta, current_table_name
    
    try:
        # Read file
        contents = await file.read()
        file_extension = file.filename.split('.')[-1].lower()
        
        if file_extension == 'csv':
            df = pd.read_csv(io.BytesIO(contents))
        elif file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(io.BytesIO(contents))
        elif file_extension == 'json':
            df = pd.read_json(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Неподдерживаемый формат файла")
        
        current_dataset = df
        # Переключаемся в dataset-режим и чистим DB-состояние, если было.
        data_source_mode = "dataset"
        db_tables = {}
        db_schema = {}
        db_meta = {}
        current_table_name = None

        # Reset Nastya chat for a fresh dataset
        global nastya_agent
        nastya_agent = NastyaAgent(df, file.filename)

        # Basic info
        dataset_metadata = {
            "filename": file.filename,
            "rows": len(df),
            "columns": len(df.columns),
            "size_bytes": len(contents),
            "uploaded_at": datetime.now().isoformat()
        }
        
        return JSONResponse(to_py({
            "success": True,
            "message": "Датасет успешно загружен",
            "metadata": dataset_metadata
        }))
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки файла: {str(e)}")


@app.get("/api/analysis/full")
async def get_full_analysis(table: Optional[str] = Query(default=None)):
    """Полный анализ качества данных (для выбранной таблицы в DB-режиме)."""
    df = _select_dataset(table)
    
    # Column-level analysis
    columns_analysis = []
    
    for col in df.columns:
        series = df[col]
        col_type = detect_column_type(series)
        missing_count = series.isnull().sum()
        missing_pct = (missing_count / len(series)) * 100
        unique_count = series.nunique()
        unique_ratio = unique_count / len(series)
        
        col_info = {
            "name": col,
            "type": col_type,
            "missing_count": int(missing_count),
            "missing_percentage": round(missing_pct, 2),
            "unique_count": int(unique_count),
            "unique_ratio": round(unique_ratio, 4),
            "entropy": calculate_entropy(series.fillna('__NA__'))
        }
        
        # Statistics for numeric columns
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if len(non_null) > 0:
                col_info["statistics"] = {
                    "mean": round(float(non_null.mean()), 4),
                    "median": round(float(non_null.median()), 4),
                    "std": round(float(non_null.std()), 4),
                    "min": round(float(non_null.min()), 4),
                    "max": round(float(non_null.max()), 4),
                    "q25": round(float(non_null.quantile(0.25)), 4),
                    "q75": round(float(non_null.quantile(0.75)), 4)
                }
                col_info["outliers"] = calculate_outliers(series)
                col_info["distribution"] = calculate_skewness_kurtosis(series)
                col_info["normality_test"] = calculate_normality_test(series)
        
        # Top values for categorical
        if col_type in ["categorical", "text", "boolean"]:
            top_values = series.value_counts().head(10)
            col_info["top_values"] = [
                {"value": str(k), "count": int(v), "percentage": round(v / len(series) * 100, 2)}
                for k, v in top_values.items()
            ]
        
        # Quality score
        col_info["quality_score"] = calculate_data_quality_score(col_info)
        
        columns_analysis.append(col_info)
    
    # Dataset-level metrics
    total_cells = len(df) * len(df.columns)
    total_missing = df.isnull().sum().sum()
    completeness = ((total_cells - total_missing) / total_cells) * 100
    
    # Duplicates analysis
    duplicates_info = detect_duplicates_detailed(df)
    
    # Missing patterns
    missing_patterns = analyze_missing_patterns(df)
    
    # Correlation matrix
    correlation_info = calculate_correlation_matrix(df)
    
    # Special columns detection
    constant_cols = detect_constant_columns(df)
    high_cardinality_cols = detect_high_cardinality(df)
    
    # Memory usage
    memory_usage = df.memory_usage(deep=True).sum()
    
    # Data types distribution
    dtype_counts = df.dtypes.value_counts().to_dict()
    dtype_distribution = {str(k): int(v) for k, v in dtype_counts.items()}
    
    # Overall quality score
    avg_quality_score = float(np.mean([col['quality_score'] for col in columns_analysis]))
    
    return JSONResponse(to_py({
        "success": True,
        "dataset_info": {
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "total_cells": int(total_cells),
            "memory_usage_bytes": int(memory_usage),
            "memory_usage_mb": round(float(memory_usage / (1024 * 1024)), 2)
        },
        "quality_metrics": {
            "completeness_percentage": round(float(completeness), 2),
            "missing_cells": int(total_missing),
            "missing_percentage": round(float((total_missing / total_cells) * 100), 2),
            "overall_quality_score": round(float(avg_quality_score), 2)
        },
        "duplicates": duplicates_info,
        "missing_patterns": missing_patterns,
        "correlation": correlation_info,
        "data_types": dtype_distribution,
        "columns": columns_analysis,
        "warnings": {
            "constant_columns": constant_cols,
            "high_cardinality_columns": high_cardinality_cols
        }
    }))


@app.get("/api/analysis/summary")
async def get_summary(table: Optional[str] = Query(default=None)):
    """Краткая сводка по датасету / таблице БД."""
    df = _select_dataset(table)
    
    return JSONResponse(to_py({
        "success": True,
        "summary": {
            "rows": len(df),
            "columns": len(df.columns),
            "missing_percentage": round((df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2),
            "duplicates": int(df.duplicated().sum()),
            "numeric_columns": len(df.select_dtypes(include=[np.number]).columns),
            "categorical_columns": len(df.select_dtypes(exclude=[np.number]).columns)
        }
    }))


@app.get("/api/visualization/distribution/{column}")
async def get_distribution(
    column: str,
    bins: int = 30,
    table: Optional[str] = Query(default=None),
):
    """Получить данные для визуализации распределения."""
    df = _select_dataset(table)
    if column not in df.columns:
        raise HTTPException(status_code=404, detail="Столбец не найден")
    series = df[column].dropna()
    
    if pd.api.types.is_numeric_dtype(series):
        # Histogram data
        hist, bin_edges = np.histogram(series, bins=bins)
        
        return JSONResponse(to_py({
            "success": True,
            "type": "numeric",
            "data": {
                "counts": hist.tolist(),
                "bin_edges": bin_edges.tolist(),
                "bins": bins
            }
        }))
    else:
        # Frequency data
        value_counts = series.value_counts().head(20)
        
        return JSONResponse(to_py({
            "success": True,
            "type": "categorical",
            "data": {
                "labels": value_counts.index.tolist(),
                "counts": value_counts.values.tolist()
            }
        }))


_DYNAMICS_AGG_LABELS = {
    "sum": "Сумма",
    "mean": "Среднее",
    "median": "Медиана",
    "min": "Минимум",
    "max": "Максимум",
    "count": "Количество",
    "nunique": "Уникальных",
}


@app.get("/api/visualization/dynamics")
async def get_dynamics(
    date_column: str,
    value_column: str,
    agg: str = Query(default="sum"),
    table: Optional[str] = Query(default=None),
):
    """Динамика: агрегация value_column по дням из date_column.

    agg: sum | mean | median | min | max | count | nunique
    Для count/nunique значения колонки могут быть нечисловыми.
    """
    df = _select_dataset(table)

    agg_key = (agg or "sum").lower().strip()
    if agg_key not in _DYNAMICS_AGG_LABELS:
        raise HTTPException(status_code=400, detail=f"Неизвестная агрегация: {agg}")

    if date_column not in df.columns:
        raise HTTPException(status_code=404, detail=f"Столбец даты '{date_column}' не найден")
    if value_column not in df.columns:
        raise HTTPException(status_code=404, detail=f"Столбец значений '{value_column}' не найден")

    parsed_dates = pd.to_datetime(df[date_column], errors='coerce')

    # Для count/nunique числовое приведение не нужно — считаем по сырым значениям
    if agg_key in ("count", "nunique"):
        raw_values = df[value_column]
        work = pd.DataFrame({"_date": parsed_dates, "_value": raw_values}).dropna(subset=["_date"])
    else:
        numeric_values = pd.to_numeric(df[value_column], errors='coerce')
        work = pd.DataFrame({"_date": parsed_dates, "_value": numeric_values}).dropna()

    if work.empty:
        return JSONResponse(to_py({
            "success": True,
            "data": {"labels": [], "values": []},
            "agg": agg_key,
            "agg_label": _DYNAMICS_AGG_LABELS[agg_key],
            "warning": "Нет валидных пар (дата, значение) для построения динамики",
        }))

    work["_date"] = work["_date"].dt.date
    grouped_obj = work.groupby("_date", as_index=True)["_value"]

    if agg_key == "sum":
        grouped = grouped_obj.sum()
    elif agg_key == "mean":
        grouped = grouped_obj.mean()
    elif agg_key == "median":
        grouped = grouped_obj.median()
    elif agg_key == "min":
        grouped = grouped_obj.min()
    elif agg_key == "max":
        grouped = grouped_obj.max()
    elif agg_key == "count":
        grouped = grouped_obj.count()
    elif agg_key == "nunique":
        grouped = grouped_obj.nunique()
    else:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемая агрегация: {agg_key}")

    grouped = grouped.sort_index()

    return JSONResponse(to_py({
        "success": True,
        "agg": agg_key,
        "agg_label": _DYNAMICS_AGG_LABELS[agg_key],
        "data": {
            "labels": [d.isoformat() for d in grouped.index],
            "values": [float(v) if pd.notna(v) else None for v in grouped.values],
        }
    }))


@app.get("/api/visualization/missing")
async def get_missing_visualization(table: Optional[str] = Query(default=None)):
    """Данные для визуализации пропущенных значений."""
    df = _select_dataset(table)
    missing_data = []
    
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        missing_data.append({
            "column": col,
            "count": int(missing_count),
            "percentage": round((missing_count / len(df)) * 100, 2)
        })
    
    return JSONResponse(to_py({
        "success": True,
        "data": missing_data
    }))


@app.get("/api/columns")
async def get_columns(table: Optional[str] = Query(default=None)):
    """Получить список столбцов датасета / выбранной таблицы БД."""
    df = _select_dataset(table)
    return JSONResponse(to_py({
        "success": True,
        "columns": df.columns.tolist()
    }))


@app.get("/api/health")
async def health_check():
    """Проверка состояния API"""
    return JSONResponse(to_py({
        "status": "healthy",
        "dataset_loaded": current_dataset is not None,
        "mode": data_source_mode,
        "dialect": db_meta.get("dialect"),
        "version": "1.1.0"
    }))


@app.get("/api/source")
async def get_source_info():
    """Текущий источник данных + его идентификатор для группировки чатов."""
    return JSONResponse(to_py({
        "success": True,
        "mode": data_source_mode,
        "source_id": _current_source_id(),
        "dataset_filename": (dataset_metadata or {}).get("filename"),
        "db": {
            "dialect": (db_meta or {}).get("dialect"),
            "dbname": (db_meta or {}).get("dbname"),
            "host": (db_meta or {}).get("host"),
        } if data_source_mode == "database" else None,
    }))


class ChatRequest(BaseModel):
    message: str
    chat_id: Optional[str] = None


class ChatCreateRequest(BaseModel):
    name: Optional[str] = None
    source_id: Optional[str] = None


class ChatRenameRequest(BaseModel):
    name: str


class SupportTicket(BaseModel):
    subject: str
    description: str
    email: Optional[str] = None


class DbConnectRequest(BaseModel):
    dialect: str
    dbname: str
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None


# ---------------------------------------------------------------------------
# Database connection helpers
# ---------------------------------------------------------------------------

def _build_db_url(req: DbConnectRequest) -> str:
    """Собирает SQLAlchemy URL под выбранный диалект."""
    dialect = (req.dialect or "").lower().strip()
    if dialect == "postgresql":
        host = req.host or "localhost"
        port = req.port or 5432
        user = quote_plus(req.user or "")
        password = quote_plus(req.password or "")
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{req.dbname}"
    if dialect == "mysql":
        host = req.host or "localhost"
        port = req.port or 3306
        user = quote_plus(req.user or "")
        password = quote_plus(req.password or "")
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{req.dbname}"
    if dialect == "sqlite":
        # Для SQLite dbname = путь к файлу .db
        return f"sqlite:///{req.dbname}"
    raise HTTPException(status_code=400, detail=f"Неподдерживаемый тип СУБД: {req.dialect}")


def _collect_db_snapshot(engine: Engine, dialect: str) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    """Возвращает (tables_dict, schema_dict). Схема содержит колонки, PK, FK."""
    insp = sa_inspect(engine)
    # Для PostgreSQL явно используем public, для остальных — дефолтную схему.
    schema_arg: Optional[str] = "public" if dialect == "postgresql" else None
    try:
        table_names = insp.get_table_names(schema=schema_arg)
    except Exception:
        table_names = insp.get_table_names()

    tables: Dict[str, pd.DataFrame] = {}
    schema_tables: Dict[str, Any] = {}
    with engine.connect() as conn:
        for name in table_names:
            try:
                df = pd.read_sql_table(name, conn, schema=schema_arg)
            except Exception:
                # Фолбэк на сырой SELECT (например, SQLite не понимает schema=).
                try:
                    df = pd.read_sql(f'SELECT * FROM "{name}"', conn)
                except Exception as e:
                    schema_tables[name] = {"error": f"Не удалось прочитать: {e}"}
                    continue

            columns_info: List[Dict[str, Any]] = []
            try:
                for col in insp.get_columns(name, schema=schema_arg):
                    columns_info.append({
                        "name": col.get("name"),
                        "type": str(col.get("type")),
                        "nullable": bool(col.get("nullable", True)),
                    })
            except Exception:
                columns_info = [{"name": c, "type": str(df[c].dtype)} for c in df.columns]

            try:
                pk_info = insp.get_pk_constraint(name, schema=schema_arg) or {}
                primary_key = list(pk_info.get("constrained_columns") or [])
            except Exception:
                primary_key = []

            try:
                fks_raw = insp.get_foreign_keys(name, schema=schema_arg) or []
                foreign_keys = [
                    {
                        "constrained_columns": list(fk.get("constrained_columns") or []),
                        "referred_table": fk.get("referred_table"),
                        "referred_columns": list(fk.get("referred_columns") or []),
                    }
                    for fk in fks_raw
                ]
            except Exception:
                foreign_keys = []

            tables[name] = df
            schema_tables[name] = {
                "columns": columns_info,
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
                "rows": int(len(df)),
            }

    return tables, {"dialect": dialect, "tables": schema_tables}


@app.post("/api/connect/db")
async def connect_db(req: DbConnectRequest):
    """Подключение к БД через SQLAlchemy, снэпшот всех таблиц и переключение режима."""
    global current_dataset, dataset_metadata, nastya_agent
    global data_source_mode, db_tables, db_schema, db_meta, current_table_name

    url = _build_db_url(req)
    try:
        engine = create_engine(url)
        # Короткая проверка коннекта.
        with engine.connect() as _:
            pass
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось подключиться: {e}")

    try:
        tables, schema = _collect_db_snapshot(engine, req.dialect.lower())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения схемы/данных: {e}")
    finally:
        engine.dispose()

    if not tables:
        raise HTTPException(status_code=400, detail="В базе не найдено ни одной таблицы")

    first_table = next(iter(tables.keys()))

    data_source_mode = "database"
    db_tables = tables
    db_schema = schema
    db_meta = {
        "dialect": req.dialect.lower(),
        "dbname": req.dbname,
        "host": req.host,
        "port": req.port,
        "connected_at": datetime.now().isoformat(),
    }
    current_table_name = first_table
    current_dataset = tables[first_table]
    dataset_metadata = {
        "filename": req.dbname,
        "rows": len(current_dataset),
        "columns": len(current_dataset.columns),
        "size_bytes": 0,
        "uploaded_at": datetime.now().isoformat(),
    }

    nastya_agent = NastyaAgent.from_db(tables, schema, req.dbname, first_table)

    return JSONResponse(to_py({
        "success": True,
        "message": "База данных подключена",
        "dialect": req.dialect.lower(),
        "dbname": req.dbname,
        "tables_count": len(tables),
        "current_table": first_table,
    }))


@app.get("/api/db/tables")
async def get_db_tables():
    """Список таблиц текущей БД с PK/FK и размерами."""
    if data_source_mode != "database":
        raise HTTPException(status_code=400, detail="Активный источник — не БД")
    schema_tables = (db_schema or {}).get("tables") or {}
    out = []
    for name, df in db_tables.items():
        meta = schema_tables.get(name) or {}
        out.append({
            "name": name,
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "primary_key": meta.get("primary_key", []),
            "foreign_keys": meta.get("foreign_keys", []),
        })
    return JSONResponse(to_py({
        "success": True,
        "mode": data_source_mode,
        "dialect": db_meta.get("dialect"),
        "dbname": db_meta.get("dbname"),
        "current_table": current_table_name,
        "tables": out,
    }))


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """Диалог с Настей: крутим tool-call цикл до финального текстового ответа.

    Если передан chat_id — история подгружается/сохраняется в chats.json,
    что обеспечивает persist между перезапусками сервера.
    """
    global nastya_agent
    if nastya_agent is None or current_dataset is None:
        raise HTTPException(status_code=400, detail="Датасет не загружен")
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Пустое сообщение")

    chats_data: Optional[Dict[str, Any]] = None
    chat_record: Optional[Dict[str, Any]] = None

    if req.chat_id:
        chats_data = _load_chats()
        chat_record = chats_data["chats"].get(req.chat_id)
        if chat_record is None:
            raise HTTPException(status_code=404, detail="Чат не найден")
        nastya_agent.set_history(chat_record.get("history") or [])

    try:
        result = nastya_agent.chat(req.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка LLM: {e}")

    if chats_data is not None and chat_record is not None and req.chat_id:
        now_iso = datetime.now().isoformat()
        chat_record["history"] = nastya_agent.get_history()
        chat_record["updated_at"] = now_iso
        if not chat_record.get("name") or chat_record["name"] == "Новый чат":
            short = req.message.strip().splitlines()[0][:40]
            if short:
                chat_record["name"] = short
        chats_data["chats"][req.chat_id] = chat_record
        _save_chats(chats_data)

    return JSONResponse(to_py({"success": True, **result}))


@app.post("/api/chat/reset")
async def chat_reset():
    """Сброс истории чата с Настей (датасет не меняется)."""
    global nastya_agent
    if nastya_agent is None:
        raise HTTPException(status_code=400, detail="Датасет не загружен")
    nastya_agent.reset()
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# Чаты: множественные диалоги с persist в chats.json
# ---------------------------------------------------------------------------

@app.get("/api/chats")
async def list_chats(source_id: Optional[str] = Query(default=None)):
    """Список чатов. Если задан source_id — только чаты этого источника."""
    data = _load_chats()
    chats = list(data.get("chats", {}).values())
    if source_id:
        chats = [c for c in chats if c.get("source_id") == source_id]
    chats.sort(key=lambda c: c.get("updated_at") or c.get("created_at") or "", reverse=True)
    summaries = [
        {
            "id": c.get("id"),
            "name": c.get("name") or "Новый чат",
            "source_id": c.get("source_id"),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
            "messages_count": len([m for m in (c.get("history") or [])
                                   if m.get("role") in ("user", "assistant")]),
        }
        for c in chats
    ]
    return JSONResponse({"success": True, "chats": summaries})


@app.post("/api/chats")
async def create_chat(req: ChatCreateRequest):
    """Создаёт пустой чат, привязанный к источнику данных."""
    data = _load_chats()
    chat_id = uuid.uuid4().hex
    now_iso = datetime.now().isoformat()
    source_id = req.source_id or _current_source_id()
    record = {
        "id": chat_id,
        "name": (req.name or "Новый чат").strip() or "Новый чат",
        "source_id": source_id,
        "history": [],
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    data["chats"][chat_id] = record
    _save_chats(data)
    return JSONResponse({"success": True, "chat": {
        "id": record["id"],
        "name": record["name"],
        "source_id": record["source_id"],
        "created_at": record["created_at"],
        "updated_at": record["updated_at"],
        "messages_count": 0,
    }})


@app.get("/api/chats/{chat_id}")
async def get_chat(chat_id: str):
    """Полный чат с историей (для рендера на фронте)."""
    data = _load_chats()
    record = data.get("chats", {}).get(chat_id)
    if not record:
        raise HTTPException(status_code=404, detail="Чат не найден")
    return JSONResponse({"success": True, "chat": record})


@app.patch("/api/chats/{chat_id}")
async def rename_chat(chat_id: str, req: ChatRenameRequest):
    """Переименование чата."""
    new_name = (req.name or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Пустое название")
    data = _load_chats()
    record = data.get("chats", {}).get(chat_id)
    if not record:
        raise HTTPException(status_code=404, detail="Чат не найден")
    record["name"] = new_name
    record["updated_at"] = datetime.now().isoformat()
    data["chats"][chat_id] = record
    _save_chats(data)
    return JSONResponse({"success": True})


@app.delete("/api/chats/{chat_id}")
async def delete_chat(chat_id: str):
    """Удаление чата."""
    data = _load_chats()
    if chat_id not in data.get("chats", {}):
        raise HTTPException(status_code=404, detail="Чат не найден")
    del data["chats"][chat_id]
    _save_chats(data)
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# Поддержка: сохраняем тикеты в support.json
# ---------------------------------------------------------------------------

@app.post("/api/support")
async def create_support_ticket(t: SupportTicket):
    """Принимает обращение в поддержку и сохраняет в support.json."""
    subject = (t.subject or "").strip()
    description = (t.description or "").strip()
    email = (t.email or "").strip() or None
    if not subject:
        raise HTTPException(status_code=400, detail="Укажите тему обращения")
    if not description:
        raise HTTPException(status_code=400, detail="Опишите проблему")

    if SUPPORT_PATH.exists():
        try:
            with open(SUPPORT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "tickets" not in data:
                data = {"tickets": []}
        except (json.JSONDecodeError, OSError):
            data = {"tickets": []}
    else:
        data = {"tickets": []}

    ticket = {
        "id": uuid.uuid4().hex,
        "subject": subject,
        "description": description,
        "email": email,
        "source_id": _current_source_id(),
        "created_at": datetime.now().isoformat(),
    }
    data["tickets"].append(ticket)
    _atomic_write_json(SUPPORT_PATH, data)

    return JSONResponse({"success": True, "ticket_id": ticket["id"]})


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Dataset Analyzer Backend...")
    print("📊 API Documentation: http://localhost:8765/docs")
    uvicorn.run(app, host="127.0.0.1", port=8765)

