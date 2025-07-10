"""
DDL Tools for Snowflake MCP Server

This module provides FastMCP tools for Data Definition Language (DDL) operations,
allowing database structure management through the MCP tool interface.

Tools handle operations like creating, altering, and dropping database objects.
These tools directly use the connection manager for SQL execution.
"""

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from typing import Dict, List, Optional
from ..core.exceptions import SnowflakeException, DDLException, ValidationException
from ..core.response_handlers import SnowflakeResponse
from ..helpers.ddl_manager import DDLManager
import json
import os


def register_ddl_tools(mcp: FastMCP):
    """
    Register DDL operations as FastMCP tools.
    
    Args:
        mcp: FastMCP server instance
    """
    
    # Initialize response handler for consistent DDL response formatting
    response_handler = SnowflakeResponse()
    
    def get_snowflake_credentials():
        """Get Snowflake credentials from environment variables."""
        account_identifier = os.getenv("SNOWFLAKE_ACCOUNT")
        username = os.getenv("SNOWFLAKE_USER") 
        pat = os.getenv("SNOWFLAKE_PAT") or os.getenv("SNOWFLAKE_PASSWORD")
        
        if not account_identifier or not username or not pat:
            raise ValidationException("Missing required Snowflake credentials: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PAT/SNOWFLAKE_PASSWORD", "credentials", "environment")
        
        return account_identifier, username, pat
    
    @mcp.tool(
        name="execute_ddl_statement",
        description="Execute a custom DDL statement for database structure changes",
        tags={"database", "ddl", "custom", "structure"}
    )
    async def execute_ddl_statement(
        ddl_statement: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Execute a custom DDL statement for database structure modifications.
        
        This tool allows you to execute any Data Definition Language statement
        to create, alter, or drop database objects like databases, schemas, tables,
        views, and other structural elements.
        
        Args:
            ddl_statement: The DDL SQL statement to execute
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with execution details
            
        Example:
            ddl_statement = "CREATE TABLE my_table (id INT, name VARCHAR(100))"
        """
        try:
            # Validate DDL statement
            if not ddl_statement or not ddl_statement.strip():
                raise ValidationException("DDL statement cannot be empty", "ddl_statement", ddl_statement)
            
            await ctx.info(f"Executing DDL statement: {ddl_statement}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Execute DDL using manager
            ddl_response = ddl_manager.execute_ddl(ddl_statement)
            
            parsed_response = response_handler.parse_ddl_response(ddl_response)
            response_data = json.loads(parsed_response)
            
            await ctx.info("DDL statement executed successfully")
            return f"""DDL statement executed successfully!

            📝 Statement: {ddl_statement}
            ✅ Operation completed successfully
            📊 Results: {len(response_data['results'])} rows affected"""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except DDLException as e:
            await ctx.error(f"DDL operation failed: {e.message}")
            raise ToolError(f"DDL operation failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to DDL exceptions for better context
            ddl_error = DDLException(f"DDL execution failed: {e.message}", "EXECUTE", ddl_statement)
            await ctx.error(f"DDL execution failed: {ddl_error.message}")
            raise ToolError(f"DDL execution failed: {ddl_error.message}")
        except Exception as e:
            ddl_error = DDLException(f"Unexpected error during DDL execution: {str(e)}", "EXECUTE", ddl_statement)
            await ctx.error(f"Unexpected DDL error: {ddl_error.message}")
            raise ToolError(f"Unexpected DDL error: {ddl_error.message}")
    
    @mcp.tool(
        name="create_database",
        description="Create a new Snowflake database",
        tags={"database", "ddl", "create", "database"}
    )
    async def create_database(
        database_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Create a new Snowflake database.
        
        This tool creates a new database in your Snowflake account, ready for
        schemas, tables, and data storage. The database will be created with
        default settings and can be customized later.
        
        Args:
            database_name: Name of the database to create
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with database creation details
            
        Example:
            database_name = "MY_NEW_DATABASE"
        """
        try:
            await ctx.info(f"Creating database: {database_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Create database using manager
            ddl_response = ddl_manager.create_database(database_name)
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "CREATE_DATABASE", database_name)
                
            await ctx.info(f"Database {database_name} created successfully")
            return f"""Database '{database_name}' created successfully!

            🏗️ Database: {database_name}
            ✅ Ready for schemas and tables
            📍 Connection: {connection_name}"""
                            
        except SnowflakeException as e:
            await ctx.error(f"Database creation failed: {e.message}")
            raise ToolError(f"Database creation failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="create_schema",
        description="Create a new schema within a database",
        tags={"database", "ddl", "create", "schema"}
    )
    async def create_schema(
        database_name: str,
        schema_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Create a new schema within a specified database.
        
        This tool creates a new schema (namespace) within an existing database.
        Schemas help organize tables, views, and other objects within a database.
        
        Args:
            database_name: Name of the database to create the schema in
            schema_name: Name of the schema to create
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with schema creation details
            
        Example:
            database_name = "MY_DATABASE"
            schema_name = "ANALYTICS"
        """
        try:
            await ctx.info(f"Creating schema {database_name}.{schema_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Create schema using manager
            ddl_response = ddl_manager.create_schema(database_name, schema_name)
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "CREATE_SCHEMA", f"{database_name}.{schema_name}")
                
            await ctx.info(f"Schema {database_name}.{schema_name} created successfully")
            return f"""Schema '{database_name}.{schema_name}' created successfully!

            🏗️ Schema: {database_name}.{schema_name}
            ✅ Ready for tables and views
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Schema creation failed: {e.message}")
            raise ToolError(f"Schema creation failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="create_table",
        description="Create a new table with specified columns",
        tags={"database", "ddl", "create", "table"}
    )
    async def create_table(
        database_name: str,
        schema_name: str,
        table_name: str,
        columns: List[Dict[str, str]],
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Create a new table with specified columns.
        
        This tool creates a new table with the column specifications you provide.
        Each column should be a dictionary with 'name' and 'type' keys.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table to create
            columns: List of column specifications [{"name": "col_name", "type": "VARCHAR(100)"}]
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with table creation details
            
        Example:
            columns = [
                {"name": "id", "type": "INT PRIMARY KEY"},
                {"name": "name", "type": "VARCHAR(200)"},
                {"name": "created_at", "type": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP()"}
            ]
        """
        try:
            await ctx.info(f"Creating table {database_name}.{schema_name}.{table_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Create table using manager
            ddl_response = ddl_manager.create_table(
                database_name=database_name,
                schema_name=schema_name,
                table_name=table_name,
                columns=columns
            )
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "CREATE_TABLE", f"{database_name}.{schema_name}.{table_name}")
                
            await ctx.info(f"Table {database_name}.{schema_name}.{table_name} created successfully")
            
            # Format column info for display
            column_info = []
            for col in columns:
                column_info.append(f"  • {col['name']}: {col['type']}")
            
            return f"""Table '{database_name}.{schema_name}.{table_name}' created successfully!

            🏗️ Table: {database_name}.{schema_name}.{table_name}
            📊 Columns ({len(columns)}):
            {chr(10).join(column_info)}
            ✅ Ready for data
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Table creation failed: {e.message}")
            raise ToolError(f"Table creation failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="drop_database_object",
        description="Drop a database object (database, schema, table, etc.)",
        tags={"database", "ddl", "drop", "delete"}
    )
    async def drop_database_object(
        object_type: str,
        object_name: str,
        cascade: bool = False,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Drop (delete) a database object such as a database, schema, table, or view.
        
        This tool permanently removes database objects. Use with caution as this
        operation cannot be undone. The cascade option will also remove dependent objects.
        
        Args:
            object_type: Type of object to drop (DATABASE, SCHEMA, TABLE, VIEW, etc.)
            object_name: Fully qualified name of the object to drop
            cascade: Whether to cascade the drop to dependent objects
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with drop operation details
            
        Example:
            object_type = "TABLE"
            object_name = "MY_DATABASE.PUBLIC.OLD_TABLE"
            cascade = False
        """
        try:
            await ctx.info(f"Dropping {object_type}: {object_name}")
            
            if cascade:
                await ctx.warning("CASCADE option enabled - dependent objects will also be dropped")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Drop object using manager
            ddl_response = ddl_manager.drop_object(
                object_type=object_type,
                object_name=object_name,
                cascade=cascade
            )
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "DROP", object_name)
                
            await ctx.info(f"{object_type} {object_name} dropped successfully")
            cascade_msg = " (with CASCADE)" if cascade else ""
            return f"""{object_type} '{object_name}' dropped successfully!

            🗑️ Object: {object_name}
            🔧 Type: {object_type}
            ⚠️ Operation: DROP{cascade_msg}
            ✅ Completed successfully
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Drop operation failed: {e.message}")
            raise ToolError(f"Drop operation failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="alter_table",
        description="Alter a table's structure (add, drop, rename, or modify columns)",
        tags={"database", "ddl", "alter", "table"}
    )
    async def alter_table(
        table_name: str,
        alter_type: str,
        column_name: Optional[str] = None,
        new_name: Optional[str] = None,
        data_type: Optional[str] = None,
        default_value: Optional[str] = None,
        not_null: Optional[bool] = None,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Alter a table's structure by adding, dropping, renaming, or modifying columns.
        
        This tool allows you to modify existing tables by changing their column
        structure. You can add new columns, remove existing ones, rename columns,
        or change their data types and constraints.
        
        Args:
            table_name: Fully qualified table name (DATABASE.SCHEMA.TABLE)
            alter_type: Type of alteration (ADD, DROP, RENAME, ALTER)
            column_name: Name of the column to alter
            new_name: New name for RENAME operations
            data_type: Data type for ADD or ALTER operations
            default_value: Default value for the column
            not_null: Whether the column should be NOT NULL
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with alter operation details
            
        Example:
            # Add a new column
            alter_type = "ADD"
            column_name = "email"
            data_type = "VARCHAR(255)"
            
            # Drop a column
            alter_type = "DROP"
            column_name = "old_column"
        """
        try:
            await ctx.info(f"Altering table {table_name}: {alter_type} {column_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Alter table using manager
            ddl_response = ddl_manager.alter_table(
                table_name=table_name,
                alter_type=alter_type,
                column_name=column_name,
                new_name=new_name,
                data_type=data_type,
                default_value=default_value,
                not_null=not_null
            )
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "ALTER_TABLE", table_name)
                
            await ctx.info(f"Table {table_name} altered successfully")
            return f"""Table '{table_name}' altered successfully!

            🔧 Operation: {alter_type.upper()}
            📊 Column: {column_name}
            {f'🆕 New name: {new_name}' if new_name else ''}
            {f'🏷️ Data type: {data_type}' if data_type else ''}
            {f'📌 Default: {default_value}' if default_value else ''}
            {f'⚠️ NOT NULL: {not_null}' if not_null else ''}
            ✅ Completed successfully
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Table alteration failed: {e.message}")
            raise ToolError(f"Table alteration failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="alter_schema",
        description="Alter a schema (rename or move to different database)",
        tags={"database", "ddl", "alter", "schema"}
    )
    async def alter_schema(
        schema_name: str,
        new_name: Optional[str] = None,
        new_database: Optional[str] = None,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Alter a schema by renaming it or moving it to a different database.
        
        This tool allows you to modify existing schemas by changing their name
        or transferring them to a different database. The schema must be
        fully qualified (database.schema).
        
        Args:
            schema_name: Fully qualified schema name (DATABASE.SCHEMA)
            new_name: New name for the schema
            new_database: New database for the schema
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with alter operation details
            
        Example:
            schema_name = "MY_DATABASE.OLD_SCHEMA"
            new_name = "NEW_SCHEMA"
        """
        try:
            await ctx.info(f"Altering schema: {schema_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Alter schema using manager
            ddl_response = ddl_manager.alter_schema(
                schema_name=schema_name,
                new_name=new_name,
                new_database=new_database
            )
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "ALTER_SCHEMA", schema_name)
                
            await ctx.info(f"Schema {schema_name} altered successfully")
            return f"""Schema '{schema_name}' altered successfully!

            🔧 Original: {schema_name}
            {f'🆕 New name: {new_name}' if new_name else ''}
            {f'🏗️ New database: {new_database}' if new_database else ''}
            ✅ Completed successfully
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Schema alteration failed: {e.message}")
            raise ToolError(f"Schema alteration failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="alter_database",
        description="Alter a database (rename)",
        tags={"database", "ddl", "alter", "database"}
    )
    async def alter_database(
        database_name: str,
        new_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Alter a database by renaming it.
        
        This tool allows you to rename an existing database. All schemas,
        tables, and data within the database will be preserved but accessible
        under the new database name.
        
        Args:
            database_name: Name of the database to alter
            new_name: New name for the database
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with alter operation details
            
        Example:
            database_name = "OLD_DATABASE"
            new_name = "NEW_DATABASE"
        """
        try:
            await ctx.info(f"Renaming database {database_name} to {new_name}")
            
            # Get Snowflake credentials and create DDL manager
            account_identifier, username, pat = get_snowflake_credentials()
            ddl_manager = DDLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Alter database using manager
            ddl_response = ddl_manager.alter_database(
                database_name=database_name,
                new_name=new_name
            )
            
            if not ddl_response["success"]:
                raise DDLException(ddl_response["message"], "ALTER_DATABASE", database_name)
                
            await ctx.info(f"Database {database_name} renamed to {new_name} successfully")
            return f"""Database '{database_name}' renamed successfully!

            🔧 Original name: {database_name}
            🆕 New name: {new_name}
            ✅ All schemas and data preserved
            📍 Connection: {connection_name}"""
                
        except SnowflakeException as e:
            await ctx.error(f"Database alteration failed: {e.message}")
            raise ToolError(f"Database alteration failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")