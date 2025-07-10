"""
Snowflake Operations Tools for Snowflake MCP Server

This module provides FastMCP tools for general Snowflake operations and utilities,
allowing specialized database operations through the MCP tool interface.

Tools handle operations like warehouse management, object inspection, and permissions.
"""

from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError
from typing import Dict, List, Optional, Union
import json
from ..core.exceptions import SnowflakeException, OperationsException, ValidationException
from ..core.response_handlers import SnowflakeResponse
from ..helpers.operations_manager import OperationsManager
import os


def register_operations_tools(mcp: FastMCP):
    """
    Register Snowflake Operations as FastMCP tools.
    
    Args:
        mcp: FastMCP server instance
    """
    
    # Initialize response handler for consistent formatting
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
        name="test_snowflake_connection",
        description="Test the Snowflake connection and return basic account information",
        tags={"database", "test", "connection", "diagnostics"}
    )
    async def test_snowflake_connection(
        ctx: Context = None
    ) -> str:
        """
        Test the Snowflake connection and return basic information.
        
        This tool creates a real connection to Snowflake and executes a simple
        query to verify connectivity and return account details.
        
        Returns:
            Connection test results and account information
        """
        try:
            await ctx.info("Testing Snowflake connection...")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            ops_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Execute a simple test query
            test_query = "SELECT CURRENT_USER() as user, CURRENT_ACCOUNT() as account, CURRENT_REGION() as region, CURRENT_TIMESTAMP() as timestamp, CURRENT_VERSION() as version"
            result = ops_manager.execute_query(test_query)
            
            if result["success"]:
                await ctx.info("✅ Snowflake connection successful!")
                
                # Parse the results
                if result["results"]:
                    # The result is a string representation, let's format it nicely
                    return f"""🎉 Snowflake Connection Test SUCCESSFUL!
                    
                    🔗 Connection Details:
                    Account: {account_identifier}
                    User: {username}
                    
                    📊 Query Results:
                    {result['results'][0] if result['results'] else 'No detailed results available'}

                    ✅ Connection Status: ACTIVE
                    ✅ Authentication: VERIFIED
                    ✅ Query Execution: WORKING

                    Your Snowflake MCP server can successfully connect and execute queries!"""
                else:
                    return f"""✅ Snowflake Connection Test SUCCESSFUL!
                    
                    🔗 Connection Details:
                    Account: {account_identifier}
                    User: {username}
                    
                    ✅ Connection Status: ACTIVE
                    ✅ Authentication: VERIFIED

                    Your Snowflake connection is working properly!"""
            else:
                await ctx.error(f"Connection test query failed: {result['message']}")
                return f"""❌ Snowflake Connection Test FAILED!
                
🔗 Attempted Connection:
Account: {account_identifier}
User: {username}

❌ Error: {result['message']}

Please check your credentials and network connectivity."""
                
        except Exception as e:
            await ctx.error(f"Connection test failed: {str(e)}")
            return f"""❌ Snowflake Connection Test FAILED!
            
❌ Error: {str(e)}
            
Please verify:
1. SNOWFLAKE_ACCOUNT is correct
2. SNOWFLAKE_USER exists and has permissions
3. SNOWFLAKE_PAT is valid and not expired
4. Network connectivity to Snowflake"""
    
    @mcp.tool(
        name="execute_sql_query",
        description="Execute a custom SQL query on Snowflake",
        tags={"database", "operations", "query", "sql"}
    )
    async def execute_sql_query(
        query: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Execute a custom SQL query on Snowflake.
        
        This tool allows you to run any SQL query against your Snowflake database.
        It's perfect for custom analysis, data exploration, or operations that
        don't fit into the standard DDL/DML patterns.
        
        Args:
            query: SQL query to execute
            connection_name: Which connection to use for the operation
            
        Returns:
            Formatted results from the query execution
            
        Example:
            query = "SELECT COUNT(*) as total_records FROM my_table WHERE status = 'active'"
        """
        try:
            await ctx.info(f"Executing SQL query: {query[:100]}...")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Execute query using manager
            result = operations_manager.execute_query(query)
            
            if not result["success"]:
                raise OperationsException(result["message"], "EXECUTE", query)
            
            # Convert string results back to dict format for display
            formatted_results = []
            for result_str in result["results"]:
                # For now, just treat each result as a row
                formatted_results.append({"data": result_str})
            
            # Update result for consistency
            result["results"] = formatted_results
            
            # Use response handler for consistent formatting
            parsed_response = response_handler.parse_snowflake_operation_response(result)
            response_data = json.loads(parsed_response)
            
            if response_data["success"]:
                await ctx.info("SQL query executed successfully")
                
                # Format the results for display
                if response_data["results"]:
                    formatted_results = []
                    for i, row in enumerate(result["results"], 1):
                        formatted_results.append(f"Row {i}: {json.dumps(row, indent=2, default=str)}")
                    
                    results_text = "\n\n".join(formatted_results)
                    
                    return f"""SQL query executed successfully:

                    📝 Query: {query}
                    📊 Total rows: {len(result['results'])}

                    📈 Results:
                    {results_text}"""
                else:
                    return f"SQL query executed successfully (no results returned):\n\n📝 Query: {query}"
            else:
                raise ToolError(f"SQL query failed: {result['message']}")
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Operations failed: {e.message}")
            raise ToolError(f"Operations failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"SQL query failed: {e.message}", "EXECUTE", query)
            await ctx.error(f"SQL query failed: {ops_error.message}")
            raise ToolError(f"SQL query failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during SQL execution: {str(e)}", "EXECUTE", query)
            await ctx.error(f"Unexpected SQL error: {ops_error.message}")
            raise ToolError(f"Unexpected SQL error: {ops_error.message}")
    
    @mcp.tool(
        name="show_database_objects",
        description="Show Snowflake objects of a specific type",
        tags={"database", "operations", "show", "inspect"}
    )
    async def show_database_objects(
        object_type: str,
        pattern: Optional[str] = None,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Show Snowflake objects of a specific type with optional filtering.
        
        This tool lists database objects like databases, schemas, tables, views,
        warehouses, etc. You can optionally filter by name pattern.
        
        Args:
            object_type: Type of objects to show (DATABASES, SCHEMAS, TABLES, VIEWS, WAREHOUSES, etc.)
            pattern: Optional pattern to filter objects by name (e.g., 'MY_%')
            connection_name: Which connection to use for the operation
            
        Returns:
            Formatted list of matching objects
            
        Example:
            object_type = "TABLES"
            pattern = "CUSTOMER_%"
        """
        try:
            await ctx.info(f"Showing {object_type}" + (f" matching '{pattern}'" if pattern else ""))
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Show objects using manager
            result = operations_manager.show_objects(
                object_type=object_type.upper(),
                pattern=pattern
            )
            
            if not result["success"]:
                raise OperationsException(result["message"], "SHOW", object_type)
            
            # Convert string results back to dict format for display
            formatted_results = []
            for result_str in result["results"]:
                formatted_results.append({"data": result_str})
            
            # Update result for consistency
            result = {
                "success": True,
                "message": f"Found {len(formatted_results)} {object_type.lower()}",
                "results": formatted_results
            }
            
            # Use response handler for consistent formatting
            parsed_response = response_handler.parse_snowflake_operation_response(result)
            response_data = json.loads(parsed_response)
            
            if response_data["success"]:
                await ctx.info(f"Found {len(result['results'])} {object_type.lower()}")
                
                # Format the results for display
                if result["results"]:
                    formatted_results = []
                    for i, obj in enumerate(result["results"], 1):
                        formatted_results.append(f"{i}. {json.dumps(obj, indent=2, default=str)}")
                    
                    results_text = "\n\n".join(formatted_results)
                    
                    return f"""Database objects found:

                    🔍 Type: {object_type}
                    🎯 Pattern: {pattern if pattern else 'All'}
                    📊 Total found: {len(result['results'])}

                    📈 Objects:
                    {results_text}"""
                else:
                    return f"No {object_type.lower()} found" + (f" matching pattern '{pattern}'" if pattern else "")
            else:
                raise ToolError(f"Show objects failed: {result['message']}")
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Show objects failed: {e.message}")
            raise ToolError(f"Show objects failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Show objects failed: {e.message}", "SHOW", object_type)
            await ctx.error(f"Show objects failed: {ops_error.message}")
            raise ToolError(f"Show objects failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during show objects: {str(e)}", "SHOW", object_type)
            await ctx.error(f"Unexpected show error: {ops_error.message}")
            raise ToolError(f"Unexpected show error: {ops_error.message}")
    
    @mcp.tool(
        name="describe_database_object",
        description="Get detailed information about a database object",
        tags={"database", "operations", "describe", "inspect"}
    )
    async def describe_database_object(
        object_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Get detailed information about a specific database object.
        
        This tool provides comprehensive details about tables, views, and other
        database objects including columns, data types, constraints, and metadata.
        
        Args:
            object_name: Fully qualified name of the object (DATABASE.SCHEMA.OBJECT)
            connection_name: Which connection to use for the operation
            
        Returns:
            Detailed description of the object
            
        Example:
            object_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
        """
        try:
            await ctx.info(f"Describing object: {object_name}")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Describe object using manager
            result = operations_manager.describe_object(object_name)
            
            if not result["success"]:
                raise OperationsException(result["message"], "DESCRIBE", object_name)
            
            # Convert string results back to dict format for display
            formatted_results = []
            for result_str in result["results"]:
                formatted_results.append({"data": result_str})
            
            # Update result for consistency
            result = {
                "success": True,
                "message": f"Object description retrieved for {object_name}",
                "results": formatted_results
            }
            
            if result["success"]:
                await ctx.info(f"Object description retrieved for {object_name}")
                
                # Format the results for display
                if result["results"]:
                    formatted_results = []
                    for i, detail in enumerate(result["results"], 1):
                        formatted_results.append(f"{i}. {json.dumps(detail, indent=2, default=str)}")
                    
                    results_text = "\n\n".join(formatted_results)
                    
                    return f"""Object description:

                    🎯 Object: {object_name}
                    📊 Details found: {len(result['results'])}

                    📈 Description:
                    {results_text}"""
                else:
                    return f"No description available for {object_name}"
            else:
                raise ToolError(f"Describe object failed: {result['message']}")
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Describe object failed: {e.message}")
            raise ToolError(f"Describe object failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Describe object failed: {e.message}", "DESCRIBE", object_name)
            await ctx.error(f"Describe failed: {ops_error.message}")
            raise ToolError(f"Describe failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during describe: {str(e)}", "DESCRIBE", object_name)
            await ctx.error(f"Unexpected describe error: {ops_error.message}")
            raise ToolError(f"Unexpected describe error: {ops_error.message}")
    
    @mcp.tool(
        name="set_context",
        description="Set the current database context (database, schema, warehouse, role)",
        tags={"database", "operations", "context", "use"}
    )
    async def set_context(
        context_type: str,
        context_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Set the current database context for subsequent operations.
        
        This tool changes the active database, schema, warehouse, or role for
        your session. This affects where subsequent operations will be executed.
        
        Args:
            context_type: Type of context to set (DATABASE, SCHEMA, WAREHOUSE, ROLE)
            context_name: Name of the context to use
            connection_name: Which connection to use for the operation
            
        Returns:
            Confirmation of context change
            
        Example:
            context_type = "DATABASE"
            context_name = "MY_DATABASE"
        """
        try:
            await ctx.info(f"Setting {context_type} context to: {context_name}")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Set context using manager
            result = operations_manager.use_context(
                context_type=context_type.upper(),
                context_name=context_name
            )
            
            if not result["success"]:
                raise OperationsException(result["message"], "USE", f"{context_type} {context_name}")
            
            if result["success"]:
                await ctx.info(f"Context set successfully to {context_type} {context_name}")
                return f"""Context updated successfully!

                🎯 Context Type: {context_type}
                📍 Context Name: {context_name}
                ✅ Status: Active

                All subsequent operations will use this {context_type.lower()} context."""
            else:
                raise ToolError(f"Set context failed: {result['message']}")
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Set context failed: {e.message}")
            raise ToolError(f"Set context failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Set context failed: {e.message}", "USE", f"{context_type} {context_name}")
            await ctx.error(f"Set context failed: {ops_error.message}")
            raise ToolError(f"Set context failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during context change: {str(e)}", "USE", f"{context_type} {context_name}")
            await ctx.error(f"Unexpected context error: {ops_error.message}")
            raise ToolError(f"Unexpected context error: {ops_error.message}")
    
    @mcp.tool(
        name="alter_warehouse",
        description="Modify warehouse settings for performance optimization",
        tags={"database", "operations", "warehouse", "performance"}
    )
    async def alter_warehouse(
        warehouse_name: str,
        size: Optional[str] = None,
        auto_suspend: Optional[int] = None,
        auto_resume: Optional[bool] = None,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Modify warehouse settings for performance and cost optimization.
        
        This tool allows you to adjust warehouse parameters like size, auto-suspend
        timing, and auto-resume behavior to optimize for your workload patterns.
        
        Args:
            warehouse_name: Name of the warehouse to modify
            size: Warehouse size (XSMALL, SMALL, MEDIUM, LARGE, XLARGE, etc.)
            auto_suspend: Seconds of inactivity before auto-suspending (60-604800)
            auto_resume: Whether to auto-resume when queries are submitted
            connection_name: Which connection to use for the operation
            
        Returns:
            Confirmation of warehouse changes
            
        Example:
            warehouse_name = "COMPUTE_WH"
            size = "MEDIUM"
            auto_suspend = 300
            auto_resume = True
        """
        try:
            await ctx.info(f"Altering warehouse: {warehouse_name}")
            
            # Validate that at least one parameter is provided
            if not any([size, auto_suspend is not None, auto_resume is not None]):
                raise ValidationException("No warehouse parameters specified for alteration", "parameters", "all_none")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Alter warehouse using manager
            result = operations_manager.alter_warehouse(
                warehouse_name=warehouse_name,
                size=size,
                auto_suspend=auto_suspend,
                auto_resume=auto_resume
            )
            
            if not result["success"]:
                raise OperationsException(result["message"], "ALTER_WAREHOUSE", warehouse_name)
            
            if result["success"]:
                await ctx.info(f"Warehouse {warehouse_name} altered successfully")
                
                # Format the changes made
                changes = []
                if size:
                    changes.append(f"Size: {size}")
                if auto_suspend is not None:
                    changes.append(f"Auto-suspend: {auto_suspend} seconds")
                if auto_resume is not None:
                    changes.append(f"Auto-resume: {'Enabled' if auto_resume else 'Disabled'}")
                
                changes_text = "\n".join([f"  • {change}" for change in changes])
                
                return f"""Warehouse '{warehouse_name}' altered successfully!

                🏭 Warehouse: {warehouse_name}
                🔧 Changes applied:
                {changes_text}

                💡 The warehouse settings have been updated and will take effect immediately."""
            else:
                raise ToolError(f"Alter warehouse failed: {result['message']}")
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Alter warehouse failed: {e.message}")
            raise ToolError(f"Alter warehouse failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Alter warehouse failed: {e.message}", "ALTER_WAREHOUSE", warehouse_name)
            await ctx.error(f"Alter warehouse failed: {ops_error.message}")
            raise ToolError(f"Alter warehouse failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during warehouse alteration: {str(e)}", "ALTER_WAREHOUSE", warehouse_name)
            await ctx.error(f"Unexpected warehouse error: {ops_error.message}")
            raise ToolError(f"Unexpected warehouse error: {ops_error.message}")
    
    @mcp.tool(
        name="grant_privileges",
        description="Grant privileges to a role or user",
        tags={"database", "operations", "security", "privileges"}
    )
    async def grant_privileges(
        privileges: Union[str, List[str]],
        on_type: str,
        on_name: str,
        to_type: str,
        to_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Grant privileges to a role or user on database objects.
        
        This tool grants specific privileges to roles or users, enabling access
        control and security management for your Snowflake environment.
        
        Args:
            privileges: Single privilege or list of privileges to grant (SELECT, INSERT, UPDATE, etc.)
            on_type: Type of object to grant privileges on (DATABASE, SCHEMA, TABLE, VIEW, etc.)
            on_name: Name of the object to grant privileges on
            to_type: Type of grantee (ROLE, USER)
            to_name: Name of the role or user to grant privileges to
            connection_name: Which connection to use for the operation
            
        Returns:
            Confirmation of privilege grant
            
        Example:
            privileges = ["SELECT", "INSERT"]
            on_type = "TABLE"
            on_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            to_type = "ROLE"
            to_name = "ANALYST_ROLE"
        """
        try:
            await ctx.info(f"Granting privileges {privileges} on {on_type} {on_name} to {to_type} {to_name}")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Grant privileges using manager
            result = operations_manager.grant_privilege(
                privileges=privileges,
                on_type=on_type,
                on_name=on_name,
                to_type=to_type,
                to_name=to_name
            )
            
            if not result["success"]:
                raise OperationsException(result["message"], "GRANT", f"{privileges} on {on_name}")
            
            # Format privileges for display
            if isinstance(privileges, list):
                privileges_str = ", ".join(privileges)
            else:
                privileges_str = privileges
            
            await ctx.info(f"Privileges granted successfully")
            return f"""Privileges granted successfully!

            🔐 Privileges: {privileges_str}
            🎯 Object: {on_type} {on_name}
            👤 Grantee: {to_type} {to_name}
            ✅ Status: Active

            The specified privileges have been granted and are now in effect."""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Grant privileges failed: {e.message}")
            raise ToolError(f"Grant privileges failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Grant privileges failed: {e.message}", "GRANT", f"{privileges} on {on_name}")
            await ctx.error(f"Grant failed: {ops_error.message}")
            raise ToolError(f"Grant failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during privilege grant: {str(e)}", "GRANT", f"{privileges} on {on_name}")
            await ctx.error(f"Unexpected grant error: {ops_error.message}")
            raise ToolError(f"Unexpected grant error: {ops_error.message}")
    
    @mcp.tool(
        name="revoke_privileges",
        description="Revoke privileges from a role or user",
        tags={"database", "operations", "security", "privileges"}
    )
    async def revoke_privileges(
        privileges: Union[str, List[str]],
        on_type: str,
        on_name: str,
        from_type: str,
        from_name: str,
        connection_name: str = "default",
        ctx: Context = None
    ) -> str:
        """
        Revoke privileges from a role or user on database objects.
        
        This tool removes specific privileges from roles or users, helping
        maintain security and access control in your Snowflake environment.
        
        Args:
            privileges: Single privilege or list of privileges to revoke (SELECT, INSERT, UPDATE, etc.)
            on_type: Type of object to revoke privileges from (DATABASE, SCHEMA, TABLE, VIEW, etc.)
            on_name: Name of the object to revoke privileges from
            from_type: Type of grantee (ROLE, USER)
            from_name: Name of the role or user to revoke privileges from
            connection_name: Which connection to use for the operation
            
        Returns:
            Confirmation of privilege revocation
            
        Example:
            privileges = ["INSERT", "UPDATE"]
            on_type = "TABLE"
            on_name = "MY_DATABASE.PUBLIC.CUSTOMERS"
            from_type = "ROLE"
            from_name = "TEMP_ROLE"
        """
        try:
            await ctx.info(f"Revoking privileges {privileges} on {on_type} {on_name} from {from_type} {from_name}")
            
            # Get Snowflake credentials and create operations manager
            account_identifier, username, pat = get_snowflake_credentials()
            operations_manager = OperationsManager(
                account_identifier=account_identifier,
                username=username,
                password=pat
            )
            
            # Revoke privileges using manager
            result = operations_manager.revoke_privilege(
                privileges=privileges,
                on_type=on_type,
                on_name=on_name,
                from_type=from_type,
                from_name=from_name
            )
            
            if not result["success"]:
                raise OperationsException(result["message"], "REVOKE", f"{privileges} on {on_name}")
            
            # Format privileges for display
            if isinstance(privileges, list):
                privileges_str = ", ".join(privileges)
            else:
                privileges_str = privileges
            
            await ctx.info(f"Privileges revoked successfully")
            return f"""Privileges revoked successfully!

            🔐 Privileges: {privileges_str}
            🎯 Object: {on_type} {on_name}
            👤 From: {from_type} {from_name}
            ✅ Status: Revoked

            The specified privileges have been removed and are no longer in effect."""
                
        except ValidationException as e:
            await ctx.error(f"Validation error: {str(e)}")
            raise ToolError(f"Validation error: {str(e)}")
        except OperationsException as e:
            await ctx.error(f"Revoke privileges failed: {e.message}")
            raise ToolError(f"Revoke privileges failed: {e.message}")
        except SnowflakeException as e:
            # Convert generic Snowflake exceptions to Operations exceptions for better context
            ops_error = OperationsException(f"Revoke privileges failed: {e.message}", "REVOKE", f"{privileges} on {on_name}")
            await ctx.error(f"Revoke failed: {ops_error.message}")
            raise ToolError(f"Revoke failed: {ops_error.message}")
        except Exception as e:
            ops_error = OperationsException(f"Unexpected error during privilege revocation: {str(e)}", "REVOKE", f"{privileges} on {on_name}")
            await ctx.error(f"Unexpected revoke error: {ops_error.message}")
            raise ToolError(f"Unexpected revoke error: {ops_error.message}")