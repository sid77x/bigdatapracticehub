from typing import Any

from .common import normalize_language
from .pig_validator import analyze_pig_script
from .sql_validator import analyze_sql_query

SUPPORTED_LANGUAGES = ["sparksql", "hiveql", "piglatin"]


def analyze_query(query: str, language: str, schema_columns: list[str]) -> dict[str, Any]:
    normalized = normalize_language(language)

    if normalized not in SUPPORTED_LANGUAGES:
        return {
            "language": normalized,
            "is_valid": False,
            "errors": [
                {
                    "code": "UNSUPPORTED_LANGUAGE",
                    "message": f"Unsupported language: {language}",
                    "explanation": "Use one of: SparkSQL, HiveQL, Pig Latin.",
                    "token": language,
                    "start_line": 1,
                    "start_column": 1,
                    "end_line": 1,
                    "end_column": max(1, len(language)),
                }
            ],
            "suggestions": ["Choose SparkSQL, HiveQL, or Pig Latin from the dropdown."],
            "corrected_query": None,
            "translated_query": None,
        }

    if normalized in {"sparksql", "hiveql"}:
        return analyze_sql_query(query=query, schema_columns=schema_columns, dialect=normalized)

    return analyze_pig_script(script=query, schema_columns=schema_columns)
