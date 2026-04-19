import re
from typing import Any

import duckdb
import pandas as pd

from .common import (
    COMMON_SQL_MISTAKES,
    has_balanced_delimiters,
    make_error,
    split_csv_expressions,
    suggest_keyword,
)

SQL_KEYWORDS = [
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "LIMIT",
    "JOIN",
    "ON",
    "AS",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
]

AGGREGATE_PATTERN = re.compile(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", re.IGNORECASE)
TOKEN_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
COLUMN_TOKEN_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b")


def _extract_section(query: str, start_kw: str, end_keywords: list[str]) -> str:
    start_match = re.search(rf"\b{start_kw}\b", query, re.IGNORECASE)
    if not start_match:
        return ""

    start = start_match.end()
    end = len(query)

    for kw in end_keywords:
        found = re.search(rf"\b{kw}\b", query[start:], re.IGNORECASE)
        if found:
            end = min(end, start + found.start())

    return query[start:end].strip()


def _extract_identifier(expr: str) -> str | None:
    cleaned = expr.strip().strip("`").strip('"')
    if not cleaned or cleaned == "*":
        return None

    if " AS " in cleaned.upper():
        cleaned = re.split(r"\bAS\b", cleaned, flags=re.IGNORECASE)[0].strip()

    if AGGREGATE_PATTERN.search(cleaned):
        return None

    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
        return cleaned

    return None


def _collect_unknown_identifiers(section: str, schema_columns: set[str], skip_words: set[str]) -> list[str]:
    unknown = []
    for token in COLUMN_TOKEN_PATTERN.findall(section):
        upper = token.upper()
        if upper in skip_words:
            continue
        if token.lower() in schema_columns:
            continue
        if token.isdigit():
            continue
        unknown.append(token)
    return unknown


def analyze_sql_query(query: str, schema_columns: list[str], dialect: str) -> dict[str, Any]:
    errors: list[dict] = []
    suggestions: list[str] = []
    corrected_query = query

    stripped = query.strip()
    schema_set = {name.lower() for name in schema_columns}

    if not stripped:
        errors.append(
            make_error(
                code="EMPTY_QUERY",
                message="Query is empty.",
                explanation="Start with a SELECT statement to analyze your data.",
                query=query,
            )
        )
        return {
            "language": dialect,
            "is_valid": False,
            "errors": errors,
            "suggestions": suggestions,
            "corrected_query": None,
            "translated_query": None,
        }

    if not has_balanced_delimiters(query):
        errors.append(
            make_error(
                code="UNBALANCED_DELIMITERS",
                message="Query has unbalanced parentheses or quotes.",
                explanation="Check that every opening bracket and quote has a matching closing one.",
                query=query,
            )
        )

    tokens = TOKEN_PATTERN.findall(query)
    for token in tokens:
        replacement = COMMON_SQL_MISTAKES.get(token.upper())
        if replacement:
            suggestions.append(f"Did you mean {replacement} instead of {token}?")
            corrected_query = re.sub(rf"\b{token}\b", replacement, corrected_query, flags=re.IGNORECASE)
        else:
            close = suggest_keyword(token, SQL_KEYWORDS)
            if close and token.upper() != close and token.lower() not in schema_set:
                suggestions.append(f"Did you mean {close} instead of {token}?")

    if not re.search(r"\bSELECT\b", query, re.IGNORECASE):
        first_word = tokens[0] if tokens else None
        errors.append(
            make_error(
                code="MISSING_SELECT",
                message="Query should start with SELECT.",
                explanation="SparkSQL and HiveQL examples usually begin with SELECT when reading from a table.",
                query=query,
                token=first_word,
            )
        )

    has_from = bool(re.search(r"\bFROM\b", query, re.IGNORECASE))
    if re.search(r"\bSELECT\b", query, re.IGNORECASE) and not has_from:
        errors.append(
            make_error(
                code="MISSING_FROM",
                message="Missing FROM clause.",
                explanation="A SELECT query needs a FROM clause to specify a source table. Use FROM data.",
                query=query,
                token="SELECT",
            )
        )
        corrected_query = corrected_query.rstrip(";") + " FROM data;"

    table_match = re.search(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)", query, re.IGNORECASE)
    if table_match:
        table_name = table_match.group(1)
        if table_name.lower() != "data":
            suggestions.append(f"The uploaded Excel is available as table data. Try FROM data instead of {table_name}.")

    select_section = _extract_section(query, "SELECT", ["FROM"])
    where_section = _extract_section(query, "WHERE", ["GROUP", "ORDER", "LIMIT", "HAVING"])
    group_by_section = _extract_section(query, r"GROUP\s+BY", ["ORDER", "LIMIT", "HAVING"])

    select_expressions = split_csv_expressions(select_section)
    unknown_columns = []
    for expr in select_expressions:
        identifier = _extract_identifier(expr)
        if identifier and identifier.lower() not in schema_set:
            unknown_columns.append(identifier)

    skip_words = {k.upper() for k in SQL_KEYWORDS}
    skip_words.update({"AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "LIKE", "IN", "IS"})

    unknown_columns.extend(_collect_unknown_identifiers(where_section, schema_set, skip_words))

    if unknown_columns:
        deduped = list(dict.fromkeys(unknown_columns))
        for column in deduped:
            errors.append(
                make_error(
                    code="UNKNOWN_COLUMN",
                    message=f"Unknown column {column}.",
                    explanation=(
                        "This column is not present in the uploaded Excel schema. "
                        f"Available columns: {', '.join(schema_columns) if schema_columns else 'none'}"
                    ),
                    query=query,
                    token=column,
                )
            )
            if schema_columns:
                suggestions.append(
                    f"Column {column} does not exist. Available columns: {', '.join(schema_columns)}"
                )

    if group_by_section and select_expressions:
        grouped_columns = {
            item.strip().split(".")[-1].strip().strip('"').strip("`").lower()
            for item in split_csv_expressions(group_by_section)
            if item.strip()
        }

        for expr in select_expressions:
            identifier = _extract_identifier(expr)
            if not identifier:
                continue
            if identifier.lower() not in grouped_columns:
                errors.append(
                    make_error(
                        code="GROUP_BY_MISMATCH",
                        message="Incorrect GROUP BY usage.",
                        explanation=(
                            f"Column {identifier} appears in SELECT but not in GROUP BY. "
                            "Add it to GROUP BY or wrap it in an aggregate function."
                        ),
                        query=query,
                        token=identifier,
                    )
                )

    parser_df = pd.DataFrame(columns=schema_columns) if schema_columns else pd.DataFrame(columns=["placeholder"])
    try:
        conn = duckdb.connect(database=":memory:")
        if schema_columns:
            conn.register("data", parser_df)
        conn.execute(f"EXPLAIN {query}")
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        upper = message.upper()

        if "PARSER" in upper or "SYNTAX" in upper:
            errors.append(
                make_error(
                    code="SQL_SYNTAX_ERROR",
                    message="Syntax error detected.",
                    explanation=message,
                    query=query,
                )
            )
        elif "REFERENCED COLUMN" in upper and "NOT FOUND" in upper:
            col_match = re.search(r'"([A-Za-z_][A-Za-z0-9_]*)"', message)
            missing = col_match.group(1) if col_match else "unknown"
            errors.append(
                make_error(
                    code="UNKNOWN_COLUMN",
                    message=f"Unknown column {missing}.",
                    explanation=(
                        f"DuckDB could not find column {missing}. "
                        f"Available columns: {', '.join(schema_columns) if schema_columns else 'none'}"
                    ),
                    query=query,
                    token=missing,
                )
            )
        elif "TABLE" in upper and "NOT FOUND" in upper:
            errors.append(
                make_error(
                    code="UNKNOWN_TABLE",
                    message="Unknown table name.",
                    explanation="Use FROM data because your uploaded file is exposed as table data.",
                    query=query,
                    token="FROM",
                )
            )
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:  # noqa: BLE001
            pass

    deduped_errors = []
    seen = set()
    for error in errors:
        key = (error["code"], error["message"], error["token"])
        if key in seen:
            continue
        seen.add(key)
        deduped_errors.append(error)

    suggestions = list(dict.fromkeys(suggestions))

    return {
        "language": dialect,
        "is_valid": len(deduped_errors) == 0,
        "errors": deduped_errors,
        "suggestions": suggestions,
        "corrected_query": corrected_query if corrected_query.strip() != query.strip() else None,
        "translated_query": None,
    }
