from typing import Optional

from pydantic import BaseModel, Field


class AnalyzeRequest(BaseModel):
    query: str = Field(..., min_length=1)
    language: str = Field(..., min_length=1)


class ExecuteRequest(BaseModel):
    query: str = Field(..., min_length=1)
    language: str = Field(..., min_length=1)


class SyntaxErrorItem(BaseModel):
    code: str
    message: str
    explanation: str
    token: Optional[str] = None
    start_line: int = 1
    start_column: int = 1
    end_line: int = 1
    end_column: int = 1


class AnalyzeResponse(BaseModel):
    language: str
    is_valid: bool
    errors: list[SyntaxErrorItem]
    suggestions: list[str]
    corrected_query: Optional[str] = None
    translated_query: Optional[str] = None
