"""
Snowflake Tools Package

This package contains FastMCP tools for Snowflake database operations:

- DDL Tools: Database structure management (CREATE, ALTER, DROP operations)
- DML Tools: Data manipulation (INSERT, SELECT, UPDATE, DELETE, MERGE operations)  
- Operations Tools: Administrative utilities (SHOW, DESCRIBE, GRANT, REVOKE, etc.)

All tools follow a consistent pattern using manager classes for database operations
and provide comprehensive error handling and response formatting.
"""

from .ddl_tools import register_ddl_tools
from .dml_tools import register_dml_tools
from .operations_tools import register_operations_tools

__all__ = [
    "register_ddl_tools",
    "register_dml_tools", 
    "register_operations_tools",
]

# Available tool categories
AVAILABLE_TOOL_CATEGORIES = {
    "ddl": {
        "description": "Data Definition Language tools for database structure management",
        "tools": [
            "execute_ddl_statement",
            "create_database",
            "create_schema", 
            "create_table",
            "drop_database_object",
            "alter_table",
            "alter_schema",
            "alter_database"
        ]
    },
    "dml": {
        "description": "Data Manipulation Language tools for data operations",
        "tools": [
            "insert_data",
            "query_data",
            "update_data",
            "delete_data",
            "execute_dml_statement",
            "merge_data"
        ]
    },
    "operations": {
        "description": "Administrative and utility tools for Snowflake operations",
        "tools": [
            "execute_sql_query",
            "show_database_objects",
            "describe_database_object", 
            "set_context",
            "alter_warehouse",
            "grant_privileges",
            "revoke_privileges"
        ]
    }
}