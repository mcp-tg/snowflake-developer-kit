#!/usr/bin/env python3
"""
Snowflake Developer MCP Server

A FastMCP server that provides comprehensive Snowflake database operations
and data management tools for developers.

This server provides:
- DDL operations (database structure management)
- DML operations (data manipulation)
- Snowflake-specific operations (warehouses, grants, etc.)
- Connection management with health monitoring
- Response parsing and error handling

Factory Analogy:
This server acts as a complete Snowflake factory management system,
providing all the tools developers need to build, manage, and query
Snowflake databases.
"""

import logging
from fastmcp import FastMCP

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not available, skip loading .env file
    pass
from src.tools.ddl_tools import register_ddl_tools
from src.tools.dml_tools import register_dml_tools
from src.tools.operations_tools import register_operations_tools
from src.middleware.snowflake_middleware import register_middleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Initialize and run the Snowflake MCP Server.
    """
    logger.info("Starting Snowflake Developer MCP Server...")
    
    # Create the FastMCP server
    logger.info("Creating Snowflake Developer MCP server...")
    mcp = FastMCP("SnowflakeDeveloperServer")
    
    # Register middleware
    logger.info("Registering middleware...")
    register_middleware(mcp)
    
    
    # Register Snowflake tools
    logger.info("Registering Snowflake tools...")
    register_ddl_tools(mcp)
    register_dml_tools(mcp)
    register_operations_tools(mcp)
    
    # Start the server
    logger.info("Starting Snowflake Developer MCP Server on stdio...")
    logger.info("Server ready! Available capabilities:")
    logger.info("🔧 Tools: DDL, DML, and Snowflake operations")
    logger.info("🔗 Connection: Simple per-operation pattern")
    
    return mcp

if __name__ == "__main__":
    server = main()
    server.run()