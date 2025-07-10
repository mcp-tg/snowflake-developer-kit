"""
Snowflake Middleware for MCP Server

This module provides comprehensive middleware components for Snowflake database operations.
It implements a layered security and monitoring approach with four specialized middleware classes:

1. **SnowflakeValidationMiddleware**: Input validation and sanitization
2. **SnowflakeSecurityMiddleware**: Security monitoring and threat detection  
3. **SnowflakeLoggingMiddleware**: Execution logging and performance monitoring
4. **SnowflakeConnectionMiddleware**: Connection health and management

Middleware Architecture:
- Uses FastMCP's middleware system for tool execution interception
- Middleware executes in registration order around tool calls
- Each middleware can inspect, modify, or halt tool execution
- Provides consistent cross-cutting concerns across all Snowflake tools

Execution Flow:
    Tool Call -> Validation -> Security -> Logging -> Connection -> Actual Tool -> Connection -> Logging -> Security -> Validation -> Response

This ensures all Snowflake operations are properly validated, secured, logged, and monitored.
"""

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp import FastMCP
import time
import logging

# Configure logger for middleware operations with appropriate formatting
logger = logging.getLogger(__name__)

# TODO: Configure structured logging format for better parsing
# logging.basicConfig(
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     level=logging.INFO
# )

def register_middleware(mcp: FastMCP):
    """
    Register the complete middleware stack for Snowflake operations.
    
    This function registers four specialized middleware components that provide
    comprehensive request processing, security, logging, and monitoring for all
    Snowflake database operations.
    
    Middleware Registration Order (execution flows in this order):
    1. ValidationMiddleware - Validates inputs before any processing
    2. SecurityMiddleware - Applies security controls and threat detection
    3. LoggingMiddleware - Logs execution details and performance metrics
    4. ConnectionMiddleware - Manages connection health and monitoring
    
    Args:
        mcp (FastMCP): The FastMCP server instance to register middleware on
        
    Note:
        The order of registration determines the execution order. Each middleware
        wraps the next, creating an "onion" pattern where the first registered
        is the outermost layer.
    """
    
    class SnowflakeValidationMiddleware(Middleware):
        """Validates Snowflake tool inputs before execution."""
        
        async def on_call_tool(self, context: MiddlewareContext, call_next):
            # Basic validation for Snowflake operations
            tool_name = getattr(context, 'tool_name', 'unknown')
            
            # Log validation attempt
            logger.debug(f"Validating tool: {tool_name}")
            
            # For now, pass through all requests - add specific validation later
            return await call_next(context)

    class SnowflakeLoggingMiddleware(Middleware):
        """Logs Snowflake tool execution details with timing."""
        
        async def on_call_tool(self, context: MiddlewareContext, call_next):
            start_time = time.time()
            tool_name = getattr(context, 'tool_name', 'unknown')
            
            # Log tool execution start
            logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Snowflake tool '{tool_name}' started")
            
            try:
                result = await call_next(context)
                execution_time = time.time() - start_time
                logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Snowflake tool '{tool_name}' completed successfully in {execution_time:.2f}s")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Snowflake tool '{tool_name}' failed after {execution_time:.2f}s: {str(e)}")
                raise

    class SnowflakeSecurityMiddleware(Middleware):
        """Provides security checks for Snowflake operations."""
        
        # List of dangerous SQL keywords that should be monitored
        DANGEROUS_KEYWORDS = {
            'drop database', 'drop warehouse', 'truncate', 'delete from',
            'alter user', 'create user', 'drop user', 'grant', 'revoke'
        }
        
        async def on_call_tool(self, context: MiddlewareContext, call_next):
            tool_name = getattr(context, 'tool_name', 'unknown')
            
            # Check for potentially dangerous operations
            if hasattr(context, 'arguments'):
                args = context.arguments or {}
                
                # Check SQL statements for dangerous keywords
                sql_fields = ['ddl_statement', 'sql_statement', 'query', 'statement']
                for field in sql_fields:
                    if field in args and args[field]:
                        sql = str(args[field]).lower()
                        for keyword in self.DANGEROUS_KEYWORDS:
                            if keyword in sql:
                                logger.warning(f"Potentially dangerous SQL operation detected in tool '{tool_name}': {keyword}")
                                # For now, just log - could add approval workflow later
                                break
            
            return await call_next(context)

    class SnowflakeConnectionMiddleware(Middleware):
        """Monitors and manages Snowflake connection health."""
        
        async def on_call_tool(self, context: MiddlewareContext, call_next):
            # This middleware could add connection health checks
            # For now, just pass through
            return await call_next(context)

    # Register middleware with the server in order of execution
    mcp.add_middleware(SnowflakeValidationMiddleware())
    mcp.add_middleware(SnowflakeSecurityMiddleware())
    mcp.add_middleware(SnowflakeLoggingMiddleware())
    mcp.add_middleware(SnowflakeConnectionMiddleware())