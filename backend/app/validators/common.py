import re
from difflib import get_close_matches
from typing import Optional


COMMON_SQL_MISTAKES = {
    "SELCT": "SELECT",
    "FORM": "FROM",
    "GROPU": "GROUP",
    "WHRE": "WHERE",
    "ORDR": "ORDER",
    "HAVNG": "HAVING",
}

COMMON_PIG_MISTAKES = {
    "LOD": "LOAD",
    "FILTR": "FILTER",
    "GENRATE": "GENERATE",
    "FRACH": "FOREACH",
    "GRUP": "GROUP",
    "DMUP": "DUMP",
}


def find_token_position(text: str, token: str) -> tuple[int, int, int, int]:
    if not token:
        return (1, 1, 1, 1)

    idx = text.upper().find(token.upper())
    if idx < 0:
        return (1, 1, 1, 1)

    before = text[:idx]
    line = before.count("\n") + 1
    last_newline = before.rfind("\n")
    col = idx + 1 if last_newline == -1 else idx - last_newline

    end_col = col + max(len(token) - 1, 0)
    return (line, col, line, end_col)


def make_error(
    *,
    code: str,
    message: str,
    explanation: str,
    query: str,
    token: Optional[str] = None,
) -> dict:
    start_line, start_col, end_line, end_col = find_token_position(query, token or "")
    return {
        "code": code,
        "message": message,
        "explanation": explanation,
        "token": token,
        "start_line": start_line,
        "start_column": start_col,
        "end_line": end_line,
        "end_column": end_col,
    }


def split_csv_expressions(raw: str) -> list[str]:
    parts = []
    chunk = []
    depth = 0
    for ch in raw:
        if ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1

        if ch == "," and depth == 0:
            value = "".join(chunk).strip()
            if value:
                parts.append(value)
            chunk = []
            continue

        chunk.append(ch)

    tail = "".join(chunk).strip()
    if tail:
        parts.append(tail)

    return parts


def suggest_keyword(token: str, keywords: list[str]) -> Optional[str]:
    if not token:
        return None
    match = get_close_matches(token.upper(), keywords, n=1, cutoff=0.75)
    return match[0] if match else None


def has_balanced_delimiters(query: str) -> bool:
    pairs = {")": "(", "]": "[", "}": "{"}
    stack = []
    in_single = False
    in_double = False

    for ch in query:
        if ch == "'" and not in_double:
            in_single = not in_single
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            continue

        if in_single or in_double:
            continue

        if ch in "([{":
            stack.append(ch)
        elif ch in pairs:
            if not stack or stack[-1] != pairs[ch]:
                return False
            stack.pop()

    return not stack and not in_single and not in_double


def normalize_language(value: str) -> str:
    language = re.sub(r"\s+", "", value.lower())
    mapping = {
        "sparksql": "sparksql",
        "spark-sql": "sparksql",
        "spark": "sparksql",
        "hiveql": "hiveql",
        "hive": "hiveql",
        "piglatin": "piglatin",
        "pig": "piglatin",
    }
    return mapping.get(language, language)
