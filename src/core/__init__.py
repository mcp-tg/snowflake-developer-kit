"""
Snowflake Core Package

Core utilities for the Snowflake MCP Server:

- Response Handlers: Consistent response parsing and formatting for all operations
- Exceptions: Comprehensive error handling with specific context for different operations
- Snowflake Utils: Simple connection utilities for database operations
- Models: Data models and validation schemas

These utilities provide the foundation for all Snowflake operations across DDL, DML,
and administrative tools.
"""

from .exceptions import *
from .response_handlers import SnowflakeResponse

__all__ = [
    "SnowflakeResponse",
    # Exceptions are imported with * and include:
    # - SnowflakeException
    # - ConnectionException
    # - DDLException
    # - DMLException
    # - OperationsException
    # - ValidationException
    # - MissingArgumentsException
]

# Utility categories
UTILITY_CATEGORIES = {
    "connection": {
        "description": "Simple per-operation database connections via snowflake_utils",
        "components": ["get_snowflake_connection"]
    },
    "response_handling": {
        "description": "Response parsing and formatting for consistent tool outputs",
        "components": ["SnowflakeResponse"] 
    },
    "error_handling": {
        "description": "Comprehensive exception hierarchy for detailed error context",
        "components": [
            "SnowflakeException",
            "DDLException", 
            "DMLException",
            "OperationsException",
            "ValidationException"
        ]
    }
}