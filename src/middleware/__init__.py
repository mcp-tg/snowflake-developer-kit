"""
Snowflake MCP Server - Middleware Package

This package contains middleware components for validation, logging, security, and connection management.
"""

from .snowflake_middleware import register_middleware

__all__ = ["register_middleware"]

# Available middleware components
AVAILABLE_MIDDLEWARE = [
    "SnowflakeValidationMiddleware",
    "SnowflakeLoggingMiddleware", 
    "SnowflakeSecurityMiddleware",
    "SnowflakeConnectionMiddleware"
]