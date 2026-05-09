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
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from urllib.parse import quote_plus
import warnings

import sqlalchemy
from sqlalchemy import create_engine, inspect as sa_inspect
from sqlalchemy.engine import Engine

from nastya_agent import NastyaAgent
import chats_store

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


def _is_id_like(col_name: str) -> bool:
    """ID-столбцы исключаем из корреляций — корреляции по идентификаторам бессмысленны."""
    if col_name is None:
        return False
    name = str(col_name).strip().lower()
    if name in ("id", "uid", "uuid", "guid"):
        return True
    if name.endswith("_id") or name.startswith("id_"):
        return True
    # camelCase варианты: clientId, userID → после lower: clientid, userid
    if name.endswith("id") and len(name) > 2 and not name.endswith("oid"):
        # эвристика: 'paid', 'said', 'rapid' — слова с 'id' на конце, но они редки в табличных схемах;
        # допускаем, чтобы фильтр был чуть агрессивнее — корреляция по ним всё равно редко даёт смысл.
        return True
    return False


def calculate_correlation_matrix(df: pd.DataFrame) -> Dict[str, Any]:
    """Вычисляет корреляционную матрицу для числовых столбцов (без id-столбцов)."""
    numeric_df = df.select_dtypes(include=[np.number])
    # Отфильтровываем id-подобные столбцы — корреляции по идентификаторам бессмысленны.
    keep_cols = [c for c in numeric_df.columns if not _is_id_like(c)]
    numeric_df = numeric_df[keep_cols]

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


@app.get("/api/visualization/dynamics")
async def get_dynamics(
    x: str = Query(..., description="Имя столбца с датой (ось X)"),
    y: str = Query(..., description="Имя числового столбца (ось Y, агрегируется суммой)"),
    table: Optional[str] = Query(default=None),
):
    """Динамика во времени: суммирует Y по дате X.

    Возвращает {labels: [...], values: [...]}. Группировка автоматическая:
    по дню если диапазон <= ~2 лет, иначе по месяцу.
    """
    df = _select_dataset(table)
    if x not in df.columns:
        raise HTTPException(status_code=404, detail=f"Столбец '{x}' не найден")
    if y not in df.columns:
        raise HTTPException(status_code=404, detail=f"Столбец '{y}' не найден")

    try:
        x_series = pd.to_datetime(df[x], errors="coerce")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось распарсить '{x}' как дату: {e}")

    if not pd.api.types.is_numeric_dtype(df[y]):
        raise HTTPException(status_code=400, detail=f"Столбец '{y}' не числовой")

    work = pd.DataFrame({"x": x_series, "y": df[y]}).dropna(subset=["x"])
    if work.empty:
        return JSONResponse(to_py({"success": True, "data": {"labels": [], "values": [], "freq": "D"}}))

    span_days = (work["x"].max() - work["x"].min()).days or 1
    if span_days > 730:
        freq = "M"
        work["bucket"] = work["x"].dt.to_period("M").dt.to_timestamp()
    elif span_days > 90:
        freq = "W"
        work["bucket"] = work["x"].dt.to_period("W").dt.start_time
    else:
        freq = "D"
        work["bucket"] = work["x"].dt.normalize()

    grouped = work.groupby("bucket", as_index=True)["y"].sum().sort_index()
    labels = [d.strftime("%Y-%m-%d") for d in grouped.index]
    values = [float(v) if not pd.isna(v) else None for v in grouped.values]

    return JSONResponse(to_py({
        "success": True,
        "data": {
            "labels": labels,
            "values": values,
            "freq": freq,
            "x_column": x,
            "y_column": y,
        }
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


class ChatRequest(BaseModel):
    message: str


class SupportRequest(BaseModel):
    subject: str
    description: str
    email: Optional[str] = None


class ChatCreateRequest(BaseModel):
    name: Optional[str] = None


class ChatRenameRequest(BaseModel):
    name: str


def build_session_key() -> str:
    """Уникальный ключ текущей сессии (датасета или подключения к БД)."""
    if data_source_mode == "database":
        dialect = (db_meta.get("dialect") or "db")
        host = db_meta.get("host") or "local"
        port = db_meta.get("port") or ""
        user = (db_meta.get("user") or "")
        dbname = db_meta.get("dbname") or ""
        return f"db:{dialect}://{user}@{host}:{port}/{dbname}"
    if data_source_mode == "dataset":
        fname = (dataset_metadata or {}).get("filename") or "unnamed"
        return f"dataset:{fname}"
    return "unknown:none"


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
        "user": req.user,
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
    """Диалог с Настей (без хранения чатов). Совместимость со старым клиентом."""
    global nastya_agent
    if nastya_agent is None or current_dataset is None:
        raise HTTPException(status_code=400, detail="Датасет не загружен")
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Пустое сообщение")
    try:
        result = nastya_agent.chat(req.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка LLM: {e}")
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
# Многочатность: список / создание / переименование / удаление / отправка
# ---------------------------------------------------------------------------

def _ensure_loaded() -> None:
    if data_source_mode is None or current_dataset is None:
        raise HTTPException(status_code=400, detail="Источник данных не подключён")


@app.get("/api/chats")
async def list_user_chats():
    _ensure_loaded()
    session_key = build_session_key()
    return JSONResponse(to_py({
        "success": True,
        "session_key": session_key,
        "chats": chats_store.list_chats(session_key),
    }))


@app.post("/api/chats")
async def create_user_chat(req: ChatCreateRequest):
    _ensure_loaded()
    session_key = build_session_key()
    chat_meta = chats_store.create_chat(session_key, req.name)
    return JSONResponse(to_py({"success": True, "chat": chat_meta}))


@app.get("/api/chats/{chat_id}")
async def get_user_chat(chat_id: str):
    _ensure_loaded()
    session_key = build_session_key()
    chat = chats_store.get_chat(session_key, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Чат не найден")
    # Из истории отдаём только текстовые реплики user/assistant — без tool-calls.
    visible = []
    for msg in chat.get("history") or []:
        role = msg.get("role")
        if role not in ("user", "assistant"):
            continue
        content = msg.get("content")
        if not content:
            continue
        visible.append({"role": role, "content": content})
    return JSONResponse(to_py({
        "success": True,
        "chat": {
            "id": chat["id"],
            "name": chat["name"],
            "created_at": chat.get("created_at"),
            "updated_at": chat.get("updated_at"),
            "messages": visible,
        }
    }))


@app.patch("/api/chats/{chat_id}")
async def rename_user_chat(chat_id: str, req: ChatRenameRequest):
    _ensure_loaded()
    session_key = build_session_key()
    updated = chats_store.rename_chat(session_key, chat_id, req.name)
    if updated is None:
        raise HTTPException(status_code=404, detail="Чат не найден или пустое имя")
    return JSONResponse(to_py({"success": True, "chat": updated}))


@app.delete("/api/chats/{chat_id}")
async def delete_user_chat(chat_id: str):
    _ensure_loaded()
    session_key = build_session_key()
    ok = chats_store.delete_chat(session_key, chat_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Чат не найден")
    return JSONResponse({"success": True})


@app.post("/api/chats/{chat_id}/message")
async def send_message_in_chat(chat_id: str, req: ChatRequest):
    """Отправка сообщения в конкретный чат: подменяем history агента, гоняем цикл, сохраняем."""
    global nastya_agent
    _ensure_loaded()
    if nastya_agent is None:
        raise HTTPException(status_code=400, detail="Ассистент не инициализирован")
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Пустое сообщение")

    session_key = build_session_key()
    chat = chats_store.get_chat(session_key, chat_id)
    if chat is None:
        raise HTTPException(status_code=404, detail="Чат не найден")

    # Подменяем историю агента на историю выбранного чата.
    saved_history = nastya_agent.history
    nastya_agent.history = list(chat.get("history") or [])
    try:
        result = nastya_agent.chat(req.message)
        new_history = list(nastya_agent.history)
    except Exception as e:
        nastya_agent.history = saved_history
        raise HTTPException(status_code=500, detail=f"Ошибка LLM: {e}")
    finally:
        # После сохранения возвращаем агенту его собственную историю,
        # чтобы старый /api/chat не получил мусора.
        pass

    chats_store.update_chat_history(session_key, chat_id, new_history)
    nastya_agent.history = saved_history

    # Если у чата дефолтное имя «Новый чат» — переименуем по первому сообщению.
    chat_meta = next((c for c in chats_store.list_chats(session_key) if c["id"] == chat_id), None)
    if chat_meta and (chat_meta.get("name") or "").strip() in ("", "Новый чат"):
        suggested = req.message.strip().splitlines()[0][:40] or "Новый чат"
        chats_store.rename_chat(session_key, chat_id, suggested)

    return JSONResponse(to_py({"success": True, **result}))


SUPPORT_LOG_PATH = "support.log"


@app.post("/api/support")
async def submit_support(req: SupportRequest):
    """Принимает обращение в поддержку и дописывает запись в support.log."""
    subject = (req.subject or "").strip()
    description = (req.description or "").strip()
    if not subject:
        raise HTTPException(status_code=400, detail="Тема не может быть пустой")
    if not description:
        raise HTTPException(status_code=400, detail="Описание не может быть пустым")

    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "subject": subject,
        "description": description,
        "email": (req.email or "").strip() or None,
        "context": {
            "mode": data_source_mode,
            "dialect": db_meta.get("dialect"),
            "dbname": db_meta.get("dbname"),
            "table": current_table_name,
            "dataset": (dataset_metadata or {}).get("filename"),
        }
    }

    try:
        with open(SUPPORT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Не удалось сохранить обращение: {e}")

    return JSONResponse({"success": True, "message": "Обращение принято. Спасибо!"})


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Dataset Analyzer Backend...")
    print("📊 API Documentation: http://localhost:8765/docs")
    uvicorn.run(app, host="127.0.0.1", port=8765)

