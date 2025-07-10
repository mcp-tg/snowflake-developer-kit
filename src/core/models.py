from pydantic import BaseModel
from typing import Optional, Union, List


class DDLResponse(BaseModel):
    """Response model for DDL operations."""
    success: bool
    message: str
    results: List[str]


class DMLResponse(BaseModel):
    """Response model for DML operations."""
    success: bool
    message: str
    results: List[str]
    rows_affected: int


class SnowflakeOperationResponse(BaseModel):
    """Response model for general Snowflake operations."""
    success: bool
    message: str
    results: Optional[Union[dict, list, str]] = None