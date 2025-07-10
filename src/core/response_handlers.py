from .models import DDLResponse, DMLResponse, SnowflakeOperationResponse

class SnowflakeResponse:
    """Response parser for Snowflake database operations."""
    
    
    def parse_ddl_response(self, response: dict) -> str:
        """Parse DDL operation response."""
        ret = DDLResponse(**response)
        return ret.model_dump_json()

    def parse_dml_response(self, response: dict) -> str:
        """Parse DML operation response."""
        ret = DMLResponse(**response)
        return ret.model_dump_json()

    def parse_snowflake_operation_response(self, response: dict) -> str:
        """Parse general Snowflake operation response."""
        ret = SnowflakeOperationResponse(**response)
        return ret.model_dump_json()

