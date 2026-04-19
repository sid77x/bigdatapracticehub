import io
import re
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from .models import AnalyzeRequest, ExecuteRequest
from .services.query_executor import execute_sql
from .state import DataStore
from .validators.analyzer import SUPPORTED_LANGUAGES, analyze_query
from .validators.common import normalize_language

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
SUPPORTED_UPLOAD_EXTENSIONS = (".xlsx", ".csv", ".txt")

app = FastAPI(title="Big Data Query Tutor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = DataStore()


def sanitize_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    seen: dict[str, int] = {}
    normalized = []

    for idx, original in enumerate(dataframe.columns):
        value = str(original).strip().lower()
        value = re.sub(r"[^a-z0-9_]+", "_", value)
        value = value.strip("_") or f"column_{idx + 1}"

        if value in seen:
            seen[value] += 1
            value = f"{value}_{seen[value]}"
        else:
            seen[value] = 1

        normalized.append(value)

    cleaned = dataframe.copy(deep=True)
    cleaned.columns = normalized
    return cleaned


def parse_uploaded_dataframe(filename: str, payload: bytes) -> pd.DataFrame:
    lower_name = filename.lower()

    if lower_name.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(payload))

    if lower_name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(payload))

    if lower_name.endswith(".txt"):
        try:
            # Auto-detect separators like comma, tab, or semicolon for beginner-friendly uploads.
            return pd.read_csv(io.BytesIO(payload), sep=None, engine="python")
        except Exception:  # noqa: BLE001
            return pd.read_csv(io.BytesIO(payload), sep="\t")

    raise ValueError("Unsupported file format")


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "hasData": store.has_data(),
        "supportedLanguages": SUPPORTED_LANGUAGES,
    }


@app.post("/upload")
async def upload_table_file(file: UploadFile = File(...)) -> dict[str, Any]:
    if not file.filename or not file.filename.lower().endswith(SUPPORTED_UPLOAD_EXTENSIONS):
        raise HTTPException(status_code=400, detail="Supported file types are .xlsx, .csv, and .txt.")

    payload = await file.read()
    if len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File is too large. Max size is 10MB.")

    try:
        dataframe = parse_uploaded_dataframe(file.filename, payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Failed to parse uploaded file: {exc}") from exc

    if dataframe.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows.")

    dataframe = sanitize_column_names(dataframe)
    store.set_dataframe(dataframe)

    return {
        "table_name": "data",
        "row_count": int(len(dataframe.index)),
        "schema": store.get_schema(),
    }


@app.post("/analyze")
def analyze(request: AnalyzeRequest) -> dict[str, Any]:
    schema_columns = store.get_columns()
    analysis = analyze_query(
        query=request.query,
        language=request.language,
        schema_columns=schema_columns,
    )

    return analysis


@app.post("/execute")
def execute(request: ExecuteRequest) -> dict[str, Any]:
    schema_columns = store.get_columns()
    analysis = analyze_query(
        query=request.query,
        language=request.language,
        schema_columns=schema_columns,
    )

    if not analysis["is_valid"]:
        raise HTTPException(status_code=400, detail={"message": "Query is invalid.", "analysis": analysis})

    normalized_language = normalize_language(request.language)
    dataframe = store.get_dataframe()

    if dataframe is None:
        raise HTTPException(status_code=400, detail="Upload a .xlsx, .csv, or .txt file first.")

    if normalized_language in {"sparksql", "hiveql"}:
        result = execute_sql(request.query, dataframe)
        return {
            "language": normalized_language,
            "analysis": analysis,
            "columns": result["columns"],
            "rows": result["rows"],
            "row_count": result["row_count"],
            "translated_query": None,
        }

    translated = analysis.get("translated_query")
    if not translated:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Pig script is valid but could not be translated to SQL.",
                "analysis": analysis,
            },
        )

    result = execute_sql(translated, dataframe)
    return {
        "language": normalized_language,
        "analysis": analysis,
        "columns": result["columns"],
        "rows": result["rows"],
        "row_count": result["row_count"],
        "translated_query": translated,
    }
