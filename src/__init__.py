"""
Snowflake MCP Server Package

A comprehensive MCP server for Snowflake database operations, providing:
- DDL operations (database structure management)
- DML operations (data manipulation)
- Operations tools (administration and utilities)
- Middleware for validation, logging, and security
- Connection management and error handling
"""

# Core utilities and response handling
from .core import SnowflakeResponse

# Manager classes for database operations
from .helpers import DDLManager, DMLManager, OperationsManager

# Tool and resource registration functions (lazy import to avoid FastMCP dependency at package level)
def get_ddl_tools_registration():
    from .tools.ddl_tools import register_ddl_tools
    return register_ddl_tools

def get_dml_tools_registration():
    from .tools.dml_tools import register_dml_tools
    return register_dml_tools

def get_operations_tools_registration():
    from .tools.operations_tools import register_operations_tools
    return register_operations_tools


def get_middleware_registration():
    from .middleware import register_middleware
    return register_middleware

# For convenience, provide direct access when FastMCP is available
try:
    from .tools.ddl_tools import register_ddl_tools
    from .tools.dml_tools import register_dml_tools  
    from .tools.operations_tools import register_operations_tools
    from .middleware import register_middleware
    _FASTMCP_AVAILABLE = True
except ImportError:
    _FASTMCP_AVAILABLE = False
    # Provide placeholder functions when FastMCP is not available
    def register_ddl_tools(*args, **kwargs):
        raise ImportError("FastMCP is required to register DDL tools")
    def register_dml_tools(*args, **kwargs):
        raise ImportError("FastMCP is required to register DML tools")
    def register_operations_tools(*args, **kwargs):
        raise ImportError("FastMCP is required to register operations tools")
    def register_middleware(*args, **kwargs):
        raise ImportError("FastMCP is required to register middleware")

__all__ = [
    # Core utilities
    "SnowflakeResponse",
    
    # Manager classes
    "DDLManager",
    "DMLManager", 
    "OperationsManager",
    
    # Registration functions
    "register_ddl_tools",
    "register_dml_tools",
    "register_operations_tools",
    "register_middleware",
]

# Package metadata
__version__ = "1.0.0"
__author__ = "Snowflake MCP Server Team"
__description__ = "FastMCP server for comprehensive Snowflake database operations"