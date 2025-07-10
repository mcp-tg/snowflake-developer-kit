"""
DML Tools for Snowflake MCP Server

This module provides FastMCP tools for Data Manipulation Language (DML) operations,
allowing data operations through the MCP tool interface.

Tools handle operations like inserting, updating, deleting, and querying data.
These tools directly use the connection manager.
"""

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from typing import Dict, List, Optional, Union
import json
from ..core.exceptions import SnowflakeException, DMLException, ValidationException
from ..core.response_handlers import SnowflakeResponse
from ..helpers.dml_manager import DMLManager
import os


def register_dml_tools(mcp: FastMCP):
    """
    Register DML operations as FastMCP tools.
    
    Args:
        mcp: FastMCP server instance
    """
    
    # Initialize response handler for consistent DML response formatting
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
        name="insert_data",
        description="Insert new data into a Snowflake table",
        tags={"database", "dml", "insert", "data"}
    )
    async def insert_data(
        table_name: str,
        data: dict,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Insert new data into a Snowflake table.
        
        This tool inserts a new record into the specified table. The data should
        be provided as a dictionary where keys are column names and values are
        the data to insert. Complex data types will be automatically converted to JSON.
        
        Args:
            table_name: Full table name (DATABASE.SCHEMA.TABLE)
            data: Dictionary containing column names and values to insert
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with insertion details
            
        Example:
            table_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            data = {"id": "CUST_001", "name": "John Doe", "email": "john@example.com"}
        """
        try:
            await ctx.info(f"Inserting data into table: {table_name}")
            
            # Validate inputs with specific exceptions
            if not table_name or not table_name.strip():
                raise ValidationException("Table name cannot be empty", "table_name", table_name)
            
            if not data or not isinstance(data, dict):
                raise ValidationException("Data must be a non-empty dictionary", "data", str(data))
            
            # Prepare data for insertion
            columns = list(data.keys())
            values = list(data.values())
            
            # Convert complex objects to JSON strings
            for i, value in enumerate(values):
                if isinstance(value, (list, dict)):
                    values[i] = json.dumps(value)
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Insert data using manager
            dml_response = dml_manager.insert_data(
                table_name=table_name,
                columns=columns,
                values=values
            )
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "INSERT", table_name)
            
            parsed_response = response_handler.parse_dml_response(dml_response)
            response_data = json.loads(parsed_response)
            
            await ctx.info(f"Data inserted successfully into {table_name}")
            return f"""Data inserted successfully into '{table_name}'!

            📊 Columns: {', '.join(columns)}
            📝 Rows affected: {response_data['rows_affected']}
            ✅ Operation completed successfully"""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except DMLException as e:
            await ctx.error(f"DML operation failed: {e.message}")
            raise ToolError(f"DML operation failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to DML exceptions for better context
            dml_error = DMLException(f"Insert operation failed: {e.message}", "INSERT", table_name)
            await ctx.error(f"Insert failed: {dml_error.message}")
            raise ToolError(f"Insert failed: {dml_error.message}")
        except Exception as e:
            dml_error = DMLException(f"Unexpected error during insert: {str(e)}", "INSERT", table_name)
            await ctx.error(f"Unexpected insert error: {dml_error.message}")
            raise ToolError(f"Unexpected insert error: {dml_error.message}")
    
    @mcp.tool(
        name="query_data",
        description="Query data from a Snowflake table with filtering options",
        tags={"database", "dml", "select", "query"}
    )
    async def query_data(
        table_name: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = 100,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Query data from a Snowflake table with filtering and column selection.
        
        This tool retrieves data from the specified table. You can specify which
        columns to return, filter conditions, and limit the number of results.
        
        Args:
            table_name: Full table name (DATABASE.SCHEMA.TABLE)
            columns: List of column names to retrieve (None for all columns)
            where_clause: SQL WHERE clause for filtering (None for all rows)
            limit: Maximum number of rows to return (default: 100)
            connection_name: Which connection to use for the operation
            
        Returns:
            Formatted results with the queried data
            
        Example:
            table_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            columns = ["name", "email"]
            where_clause = "created_at > '2024-01-01'"
            limit = 50
        """
        try:
            await ctx.info(f"Querying data from table: {table_name}")
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Select data using manager
            dml_response = dml_manager.select_data(
                table_name=table_name,
                columns=columns,
                where_clause=where_clause,
                order_by=None,  # Will add order_by to tool parameters if needed
                limit=limit,
                offset=None   # Will add offset to tool parameters if needed
            )
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "SELECT", table_name)
            
            # Convert results back to formatted results for display
            results_strings = dml_response.get("results", [])
            formatted_results = []
            for result_str in results_strings:
                try:
                    # Try to parse as JSON if it looks like structured data
                    if result_str.startswith('(') and result_str.endswith(')'):
                        # Handle tuple-like string representation
                        formatted_results.append({"data": result_str})
                    else:
                        formatted_results.append({"data": result_str})
                except:
                    formatted_results.append({"data": result_str})
            
            parsed_response = response_handler.parse_dml_response(dml_response)
            response_data = json.loads(parsed_response)
            
            await ctx.info(f"Data retrieved successfully from {table_name}")
            
            # Format the results for display
            if formatted_results:
                results_text = []
                for i, row in enumerate(formatted_results, 1):
                    results_text.append(f"Row {i}: {json.dumps(row, indent=2, default=str)}")
                
                results_display = "\n\n".join(results_text)
                    
                return f"""Data retrieved from '{table_name}':

                📊 Total rows: {len(formatted_results)}
                📋 Columns: {', '.join(columns) if columns else 'All columns'}
                🔍 Filter: {where_clause if where_clause else 'No filter'}
                📄 Limit: {limit if limit else 'No limit'}

                📈 Results:
                {results_display}"""
            else:
                return f"No data found in '{table_name}' matching the specified criteria."
                
        except SnowflakeException as e:
            await ctx.error(f"Data query failed: {e.message}")
            raise ToolError(f"Data query failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="update_data",
        description="Update existing data in a Snowflake table",
        tags={"database", "dml", "update", "modify"}
    )
    async def update_data(
        table_name: str,
        set_clause: str,
        where_clause: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Update existing data in a Snowflake table.
        
        This tool modifies existing records in the specified table. You must
        provide both the SET clause (what to update) and WHERE clause (which
        records to update) for safety.
        
        Args:
            table_name: Full table name (DATABASE.SCHEMA.TABLE)
            set_clause: SQL SET clause (e.g., "name = 'New Name', status = 'Active'")
            where_clause: SQL WHERE clause to identify records to update
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with update details
            
        Example:
            table_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            set_clause = "status = 'Premium', updated_at = CURRENT_TIMESTAMP()"
            where_clause = "id = 'CUST_001'"
        """
        try:
            await ctx.info(f"Updating data in table: {table_name}")
            await ctx.info(f"SET: {set_clause}")
            await ctx.info(f"WHERE: {where_clause}")
            
            # Parse set_clause into columns and values
            # For now, we'll execute the raw SET clause since the current API expects it
            # This maintains backward compatibility while using the manager
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Execute raw UPDATE using manager's execute_dml method
            update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            dml_response = dml_manager.execute_dml(update_sql)
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "UPDATE", table_name)
            
            rows_affected = dml_response["rows_affected"]
            
            await ctx.info(f"Data updated successfully in {table_name}")
            return f"""Data updated successfully in '{table_name}'!

            🔄 SET clause: {set_clause}
            🎯 WHERE clause: {where_clause}
            📝 Rows affected: {rows_affected}
            ✅ Operation completed successfully"""
                
        except SnowflakeException as e:
            await ctx.error(f"Data update failed: {e.message}")
            raise ToolError(f"Data update failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="delete_data",
        description="Delete data from a Snowflake table with safety checks",
        tags={"database", "dml", "delete", "remove"}
    )
    async def delete_data(
        table_name: str,
        where_clause: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Delete data from a Snowflake table with safety checks.
        
        This tool permanently removes records from the specified table. A WHERE
        clause is required to prevent accidental deletion of all data. Use with
        caution as this operation cannot be undone.
        
        Args:
            table_name: Full table name (DATABASE.SCHEMA.TABLE)
            where_clause: SQL WHERE clause to identify records to delete (REQUIRED)
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with deletion details
            
        Example:
            table_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            where_clause = "status = 'Inactive' AND last_login < '2023-01-01'"
        """
        try:
            await ctx.info(f"Deleting data from table: {table_name}")
            await ctx.warning(f"WHERE clause: {where_clause}")
            await ctx.warning("This operation will permanently delete data!")
            
            # Validate WHERE clause
            if not where_clause or where_clause.strip() == "":
                raise ValidationException("WHERE clause is required for DELETE operations to prevent accidental data loss", "where_clause", where_clause)
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Delete data using manager
            dml_response = dml_manager.delete_data(
                table_name=table_name,
                where_clause=where_clause
            )
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "DELETE", table_name)
            
            rows_affected = dml_response["rows_affected"]
            
            await ctx.info(f"Data deleted successfully from {table_name}")
            return f"""Data deleted successfully from '{table_name}'!

            🎯 WHERE clause: {where_clause}
            📝 Rows affected: {rows_affected}
            ⚠️ This operation cannot be undone
            ✅ Operation completed successfully"""
                
        except SnowflakeException as e:
            await ctx.error(f"Data deletion failed: {e.message}")
            raise ToolError(f"Data deletion failed: {e.message}")
        except Exception as e:
            await ctx.error(f"Unexpected error: {str(e)}")
            raise ToolError(f"Unexpected error: {str(e)}")
    
    @mcp.tool(
        name="execute_dml_statement",
        description="Execute a custom DML statement for data operations",
        tags={"database", "dml", "custom", "query"}
    )
    async def execute_dml_statement(
        dml_statement: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Execute a custom DML statement for data operations.
        
        This tool allows you to execute any Data Manipulation Language statement
        to insert, update, delete, or query data using custom SQL.
        
        Args:
            dml_statement: The DML SQL statement to execute
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with execution details
            
        Example:
            dml_statement = "SELECT COUNT(*) FROM my_table WHERE status = 'active'"
        """
        try:
            # Validate DML statement
            if not dml_statement or not dml_statement.strip():
                raise ValidationException("DML statement cannot be empty", "dml_statement", dml_statement)
            
            await ctx.info(f"Executing DML statement: {dml_statement}")
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Execute DML using manager
            dml_response = dml_manager.execute_dml(dml_statement)
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "EXECUTE", dml_statement)
            
            # Use response handler for consistent formatting
            parsed_response = response_handler.parse_dml_response(dml_response)
            response_data = json.loads(parsed_response)
            
            await ctx.info("DML statement executed successfully")
            return f"""DML statement executed successfully!

            📝 Statement: {dml_statement}
            ✅ Operation completed successfully
            📊 Rows affected: {response_data['rows_affected']}
            📈 Results: {len(response_data['results'])} rows returned"""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except DMLException as e:
            await ctx.error(f"DML operation failed: {e.message}")
            raise ToolError(f"DML operation failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to DML exceptions for better context
            dml_error = DMLException(f"DML execution failed: {e.message}", "EXECUTE", dml_statement)
            await ctx.error(f"DML execution failed: {dml_error.message}")
            raise ToolError(f"DML execution failed: {dml_error.message}")
        except Exception as e:
            dml_error = DMLException(f"Unexpected error during DML execution: {str(e)}", "EXECUTE", dml_statement)
            await ctx.error(f"Unexpected DML error: {dml_error.message}")
            raise ToolError(f"Unexpected DML error: {dml_error.message}")
    
    @mcp.tool(
        name="merge_data",
        description="Perform a MERGE operation to synchronize data between tables",
        tags={"database", "dml", "merge", "upsert"}
    )
    async def merge_data(
        target_table: str,
        source_table: str,
        merge_condition: str,
        match_actions: List[Dict[str, Union[str, List[str], List[Union[str, int, float, bool, None]]]]],
        not_match_actions: Optional[List[Dict[str, Union[str, List[str], List[Union[str, int, float, bool, None]]]]]] = None,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Perform a MERGE operation to synchronize data between source and target tables.
        
        This tool performs complex MERGE operations that can update existing records,
        delete matched records, and insert new records in a single atomic operation.
        Perfect for data synchronization and upsert scenarios.
        
        Args:
            target_table: Fully qualified target table name (DATABASE.SCHEMA.TABLE)
            source_table: Source table name or subquery
            merge_condition: ON clause condition for matching records
            match_actions: List of actions when records match (UPDATE/DELETE)
            not_match_actions: Optional list of actions when records don't match (INSERT)
            connection_name: Which connection to use for the operation
            
        Returns:
            Success message with merge operation details
            
        Example:
            target_table = "MY_DATABASE.PUBLIC.CUSTOMERS"
            source_table = "MY_DATABASE.STAGING.NEW_CUSTOMERS"
            merge_condition = "target.customer_id = source.customer_id"
            match_actions = [
                {
                    "action": "UPDATE",
                    "columns": ["name", "email", "updated_at"],
                    "values": ["source.name", "source.email", "CURRENT_TIMESTAMP()"]
                }
            ]
            not_match_actions = [
                {
                    "action": "INSERT", 
                    "columns": ["customer_id", "name", "email", "created_at"],
                    "values": ["source.customer_id", "source.name", "source.email", "CURRENT_TIMESTAMP()"]
                }
            ]
        """
        try:
            await ctx.info(f"Performing MERGE operation on target table: {target_table}")
            await ctx.info(f"Source: {source_table}")
            await ctx.info(f"Merge condition: {merge_condition}")
            
            # Validate inputs
            if not target_table or not target_table.strip():
                raise ValidationException("Target table name cannot be empty", "target_table", target_table)
            
            if not source_table or not source_table.strip():
                raise ValidationException("Source table name cannot be empty", "source_table", source_table)
            
            if not merge_condition or not merge_condition.strip():
                raise ValidationException("Merge condition cannot be empty", "merge_condition", merge_condition)
            
            if not match_actions or len(match_actions) == 0:
                raise ValidationException("At least one match action is required", "match_actions", str(match_actions))
            
            # Get Snowflake credentials and create DML manager
            account_identifier, username, pat = get_snowflake_credentials()
            dml_manager = DMLManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Perform MERGE using manager
            dml_response = dml_manager.merge_data(
                target_table=target_table,
                source_table=source_table,
                merge_condition=merge_condition,
                match_actions=match_actions,
                not_match_actions=not_match_actions
            )
            
            if not dml_response["success"]:
                raise DMLException(dml_response["message"], "MERGE", target_table)
            
            # Use response handler for consistent formatting
            parsed_response = response_handler.parse_dml_response(dml_response)
            response_data = json.loads(parsed_response)
            
            rows_affected = dml_response["rows_affected"]
            
            # Format action summary
            match_summary = []
            for action in match_actions:
                match_summary.append(f"  • WHEN MATCHED: {action['action']}")
            
            not_match_summary = []
            if not_match_actions:
                for action in not_match_actions:
                    not_match_summary.append(f"  • WHEN NOT MATCHED: {action['action']}")
            
            await ctx.info(f"MERGE operation completed successfully on {target_table}")
            return f"""MERGE operation completed successfully!

            🎯 Target table: {target_table}
            📥 Source: {source_table}
            🔗 Merge condition: {merge_condition}
            
            📋 Match actions:
            {chr(10).join(match_summary)}
            
            {f'''📋 Not match actions:
            {chr(10).join(not_match_summary)}''' if not_match_actions else '📋 No not-match actions specified'}
            
            📝 Rows affected: {rows_affected}
            ✅ Operation completed successfully"""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except DMLException as e:
            await ctx.error(f"MERGE operation failed: {e.message}")
            raise ToolError(f"MERGE operation failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to DML exceptions for better context
            dml_error = DMLException(f"MERGE operation failed: {e.message}", "MERGE", target_table)
            await ctx.error(f"MERGE failed: {dml_error.message}")
            raise ToolError(f"MERGE failed: {dml_error.message}")
        except Exception as e:
            dml_error = DMLException(f"Unexpected error during MERGE: {str(e)}", "MERGE", target_table)
            await ctx.error(f"Unexpected MERGE error: {dml_error.message}")
            raise ToolError(f"Unexpected MERGE error: {dml_error.message}")