"""
Simple Snowflake Connection Utilities

This module provides simple, stateless connection utilities for Snowflake operations,
following the pattern used in the reference implementation.

No connection pooling or persistence - each operation gets a fresh connection.
"""

import os
from typing import Optional
try:
    import snowflake.connector
    from snowflake.connector import SnowflakeConnection
except ImportError:
    snowflake = None
    SnowflakeConnection = None

from .exceptions import ConnectionException, MissingArgumentsException


def get_snowflake_connection(
    account_identifier: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs
) -> SnowflakeConnection: # type: ignore
    """Get a Snowflake connection using provided credentials.
    
    Creates a fresh Snowflake connection for each call. This follows the
    simple pattern used in the reference implementation where each operation
    gets its own connection.
    
    Parameters
    ----------
    account_identifier : str, optional
        Snowflake account identifier. If not provided, reads from SNOWFLAKE_ACCOUNT env var
    username : str, optional
        Snowflake username. If not provided, reads from SNOWFLAKE_USER env var
    password : str, optional
        Snowflake password or PAT. If not provided, reads from SNOWFLAKE_PAT or SNOWFLAKE_PASSWORD env var
    **kwargs
        Additional connection parameters (database, schema, warehouse, role, etc.)
        
    Returns
    -------
    SnowflakeConnection
        An authenticated Snowflake connection
        
    Raises
    ------
    ConnectionException
        If connection fails or required parameters are missing
    MissingArgumentsException
        If required credentials are not provided
    """
    if snowflake is None:
        raise ConnectionException("snowflake-connector-python is required but not installed")
    
    # Get credentials from parameters or environment variables
    account_identifier = account_identifier or os.getenv("SNOWFLAKE_ACCOUNT")
    username = username or os.getenv("SNOWFLAKE_USER")
    password = password or os.getenv("SNOWFLAKE_PAT") or os.getenv("SNOWFLAKE_PASSWORD")
    
    # Check for missing credentials
    missing = []
    if not account_identifier:
        missing.append("account_identifier (or SNOWFLAKE_ACCOUNT env var)")
    if not username:
        missing.append("username (or SNOWFLAKE_USER env var)")
    if not password:
        missing.append("password (or SNOWFLAKE_PAT/SNOWFLAKE_PASSWORD env var)")
        
    if missing:
        raise MissingArgumentsException(missing)
        
    try:
        return snowflake.connector.connect(
            account=account_identifier,
            user=username,
            password=password,
            autocommit=True,  # Enable autocommit for simplicity
            **kwargs
        )
    except Exception as e:
        raise ConnectionException(f"Failed to establish Snowflake connection: {str(e)}")


