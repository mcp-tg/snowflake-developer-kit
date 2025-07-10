from typing import Optional, List
from textwrap import dedent


class SnowflakeException(Exception):
    """Custom exception class for Snowflake database errors."""
    
    def __init__(self, tool: str, message, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)
        self.tool = tool

    def __str__(self):
        """Format error message with tool context and optional status code."""
        if self.status_code:
            return f"{self.tool} Error: {self.message} (Code: {self.status_code})"
        else:
            return f"{self.tool} Error: {self.message}"


class MissingArgumentsException(Exception):
    """Exception for missing required arguments."""
    
    def __init__(self, missing: list):
        self.missing = missing
        super().__init__(missing)

    def __str__(self):
        missing_str = "\n\t\t".join(["--" + i for i in self.missing])
        message = f"""
        -----------------------------------------------------------------------------------
        Required arguments missing:
        \t{missing_str}
        These values must be specified as command-line arguments or environment variables
        -----------------------------------------------------------------------------------"""
        return dedent(message)


class ConnectionException(SnowflakeException):
    """Exception specifically for connection-related errors."""
    
    def __init__(self, message: str, connection_name: str = "default"):
        self.connection_name = connection_name
        super().__init__("Connection Manager", message)


class DDLException(SnowflakeException):
    """Exception for DDL (Data Definition Language) operation errors."""
    
    def __init__(self, message: str, operation: str = "DDL", ddl_statement: Optional[str] = None):
        self.operation = operation
        self.ddl_statement = ddl_statement
        super().__init__("DDL Manager", message)


class DMLException(SnowflakeException):
    """Exception for DML (Data Manipulation Language) operation errors."""
    
    def __init__(self, message: str, operation: str = "DML", table_name: Optional[str] = None):
        self.operation = operation
        self.table_name = table_name
        super().__init__("DML Manager", message)


class OperationsException(SnowflakeException):
    """Exception for general Snowflake operations errors."""
    
    def __init__(self, message: str, operation: str = "Operation", object_name: Optional[str] = None):
        self.operation = operation
        self.object_name = object_name
        super().__init__("Operations Manager", message)


class ValidationException(Exception):
    """Exception for input validation errors."""
    
    def __init__(self, message: str, field_name: Optional[str] = None, provided_value: Optional[str] = None):
        self.field_name = field_name
        self.provided_value = provided_value
        super().__init__(message)
    
    def __str__(self):
        if self.field_name:
            return f"Validation Error in '{self.field_name}': {self.args[0]}"
        return f"Validation Error: {self.args[0]}"