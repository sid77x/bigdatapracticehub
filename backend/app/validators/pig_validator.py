import re
from typing import Any

from .common import COMMON_PIG_MISTAKES, make_error, suggest_keyword

PIG_KEYWORDS = ["LOAD", "USING", "AS", "FILTER", "BY", "FOREACH", "GENERATE", "GROUP", "DUMP"]

LOAD_PATTERN = re.compile(r"^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*LOAD\s+.+$", re.IGNORECASE)
FILTER_PATTERN = re.compile(
    r"^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*FILTER\s+(?P<src>[A-Za-z_][A-Za-z0-9_]*)\s+BY\s+(?P<cond>.+)$",
    re.IGNORECASE,
)
FOREACH_PATTERN = re.compile(
    r"^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*FOREACH\s+(?P<src>[A-Za-z_][A-Za-z0-9_]*)\s+GENERATE\s+(?P<cols>.+)$",
    re.IGNORECASE,
)
GROUP_PATTERN = re.compile(
    r"^(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*GROUP\s+(?P<src>[A-Za-z_][A-Za-z0-9_]*)\s+BY\s+(?P<key>.+)$",
    re.IGNORECASE,
)
DUMP_PATTERN = re.compile(r"^DUMP\s+(?P<src>[A-Za-z_][A-Za-z0-9_]*)$", re.IGNORECASE)


def _split_statements(script: str) -> list[str]:
    parts = [part.strip() for part in script.split(";")]
    return [part for part in parts if part]


def analyze_pig_script(script: str, schema_columns: list[str]) -> dict[str, Any]:
    errors: list[dict] = []
    suggestions: list[str] = []
    translated_query = None

    statements = _split_statements(script)
    if not statements:
        errors.append(
            make_error(
                code="EMPTY_SCRIPT",
                message="Pig Latin script is empty.",
                explanation="Write at least one Pig statement such as LOAD, FILTER, FOREACH, or DUMP.",
                query=script,
            )
        )
        return {
            "language": "piglatin",
            "is_valid": False,
            "errors": errors,
            "suggestions": suggestions,
            "corrected_query": None,
            "translated_query": None,
        }

    aliases = set(["data"])
    pipeline = {"where": None, "select": "*", "group_by": None, "dump": None}

    for statement in statements:
        first_token = statement.split()[0]
        replacement = COMMON_PIG_MISTAKES.get(first_token.upper())
        if replacement:
            suggestions.append(f"Did you mean {replacement} instead of {first_token}?")
        else:
            close = suggest_keyword(first_token, PIG_KEYWORDS)
            if close and close != first_token.upper():
                suggestions.append(f"Did you mean {close} instead of {first_token}?")

        if LOAD_PATTERN.match(statement):
            load_match = LOAD_PATTERN.match(statement)
            alias = load_match.group("alias")
            aliases.add(alias)
            continue

        filter_match = FILTER_PATTERN.match(statement)
        if filter_match:
            alias = filter_match.group("alias")
            src = filter_match.group("src")
            if src not in aliases:
                errors.append(
                    make_error(
                        code="UNKNOWN_ALIAS",
                        message=f"Unknown alias {src}.",
                        explanation="You can only FILTER aliases defined earlier in the script.",
                        query=script,
                        token=src,
                    )
                )
            aliases.add(alias)
            pipeline["where"] = filter_match.group("cond")
            continue

        foreach_match = FOREACH_PATTERN.match(statement)
        if foreach_match:
            alias = foreach_match.group("alias")
            src = foreach_match.group("src")
            if src not in aliases:
                errors.append(
                    make_error(
                        code="UNKNOWN_ALIAS",
                        message=f"Unknown alias {src}.",
                        explanation="FOREACH must reference an alias that already exists.",
                        query=script,
                        token=src,
                    )
                )
            aliases.add(alias)
            pipeline["select"] = foreach_match.group("cols")
            continue

        group_match = GROUP_PATTERN.match(statement)
        if group_match:
            alias = group_match.group("alias")
            src = group_match.group("src")
            if src not in aliases:
                errors.append(
                    make_error(
                        code="UNKNOWN_ALIAS",
                        message=f"Unknown alias {src}.",
                        explanation="GROUP must reference an alias that already exists.",
                        query=script,
                        token=src,
                    )
                )
            aliases.add(alias)
            pipeline["group_by"] = group_match.group("key")
            continue

        dump_match = DUMP_PATTERN.match(statement)
        if dump_match:
            src = dump_match.group("src")
            pipeline["dump"] = src
            if src not in aliases:
                errors.append(
                    make_error(
                        code="UNKNOWN_ALIAS",
                        message=f"Unknown alias {src}.",
                        explanation="DUMP must reference an alias that already exists.",
                        query=script,
                        token=src,
                    )
                )
            continue

        errors.append(
            make_error(
                code="INVALID_PIG_STATEMENT",
                message="Unsupported or invalid Pig Latin statement.",
                explanation=(
                    "Use one of the supported patterns: LOAD, FILTER, FOREACH ... GENERATE, GROUP, DUMP."
                ),
                query=script,
                token=first_token,
            )
        )

    if schema_columns and pipeline["select"] != "*":
        cleaned = [item.strip().split("::")[-1] for item in pipeline["select"].split(",")]
        for col in cleaned:
            bare = col.split(" as ")[0].strip().strip("`").strip('"')
            if bare and bare not in schema_columns and bare != "*":
                errors.append(
                    make_error(
                        code="UNKNOWN_COLUMN",
                        message=f"Unknown column {bare}.",
                        explanation=f"Available columns: {', '.join(schema_columns)}",
                        query=script,
                        token=bare,
                    )
                )

    if not errors:
        translated_query = f"SELECT {pipeline['select']} FROM data"
        if pipeline["where"]:
            translated_query += f" WHERE {pipeline['where']}"
        if pipeline["group_by"]:
            translated_query += f" GROUP BY {pipeline['group_by']}"
        translated_query += ";"

    return {
        "language": "piglatin",
        "is_valid": len(errors) == 0,
        "errors": errors,
        "suggestions": list(dict.fromkeys(suggestions)),
        "corrected_query": None,
        "translated_query": translated_query,
    }
