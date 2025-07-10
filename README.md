# Snowflake Developer MCP Server 🚀

A powerful Model Context Protocol (MCP) server that provides comprehensive Snowflake database operations, Cortex AI services, and data management tools for AI assistants like Claude.

## 🌟 Features

- **🔧 DDL Operations**: Create and manage databases, schemas, tables, and other database objects
- **📊 DML Operations**: Insert, update, delete, and query data with full SQL support
- **⚙️ Snowflake Operations**: Manage warehouses, grants, roles, and show database objects
- **🔒 Secure Authentication**: Support for passwords and Programmatic Access Tokens (PAT)
- **🎯 Simple Connection Pattern**: Per-operation connections for reliability and simplicity

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- UV package manager (install from https://github.com/astral-sh/uv)
- Node.js and npm (for MCP inspector)
- Snowflake account with appropriate permissions
- Snowflake credentials (account identifier, username, password/PAT)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/mcp-tg/snowflake-developer.git
   cd snowflake-developer
   ```

2. **Set up environment**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env with your Snowflake credentials
   # Required: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PAT (or SNOWFLAKE_PASSWORD)
   ```

3. **Install UV (if not already installed)**
   ```bash
   # On macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # On Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

## 🧪 Testing with MCP Inspector

The easiest way to test your setup is using the MCP Inspector:

```bash
# Run the development inspector script
./dev-inspector.sh
```

This will:
- ✅ Create a virtual environment (if needed)
- ✅ Install all dependencies via UV
- ✅ Load your Snowflake credentials from .env
- ✅ Start the MCP Inspector web interface
- ✅ Open your browser to test tools interactively

**Note:** The script automatically handles UV package installation, so you don't need to manually install dependencies.

### First Test: Verify Connection

1. In the Inspector, go to the **Tools** tab
2. Find `test_snowflake_connection` and click **Run**
3. You should see your account details and confirmation that the connection works

## 🔌 Integration with AI Assistants

### Claude Desktop

**Option 1: Direct from GitHub (no local clone needed)**
```json
{
  "mcpServers": {
    "snowflake-developer": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/mcp-tg/snowflake-developer.git",
        "main.py"
      ],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USER": "your-username",
        "SNOWFLAKE_PAT": "your-pat-token"
      }
    }
  }
}
```

**Option 2: Local installation**
```json
{
  "mcpServers": {
    "snowflake-developer": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/snowflake-developer",
        "python",
        "main.py"
      ],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USER": "your-username",
        "SNOWFLAKE_PAT": "your-pat-token"
      }
    }
  }
}
```

**Setup Instructions:**
1. Clone the repository: `git clone https://github.com/mcp-tg/snowflake-developer.git`
2. Create the Claude Desktop config file: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
3. Add the configuration above, replacing `/path/to/snowflake-developer` with your actual path
4. Replace credential placeholders with your actual Snowflake credentials
5. Restart Claude Desktop

### Cursor

**Note**: Cursor doesn't support environment variables in MCP configuration. You'll need to use the local installation option or set environment variables globally on your system.

**Option 1: Direct from GitHub (requires global env vars)**
```json
{
  "mcpServers": {
    "snowflake-developer": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/mcp-tg/snowflake-developer.git",
        "main.py"
      ]
    }
  }
}
```
*Requires setting `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PAT` as system environment variables.*

**Option 2: Local installation (recommended for Cursor)**
```json
{
  "mcpServers": {
    "snowflake-developer": {
      "command": "uv",
      "args": ["run", "/path/to/snowflake-developer/main.py"]
    }
  }
}
```
*Use a local `.env` file in the project directory with your credentials.*


## 📚 Available Tools (22 Total)

### 🔧 DDL Tools (8 Tools)

Tools for managing database structure:

| Tool | Description | Example in Inspector | Natural Language Query |
|------|-------------|---------------------|----------------------|
| `alter_database` | Rename databases | database_name: `OLD_DB`<br>new_name: `NEW_DB` | "Rename database OLD_DB to NEW_DB" |
| `alter_schema` | Rename or move schemas | schema_name: `TEST_DB.OLD_SCHEMA`<br>new_name: `NEW_SCHEMA` | "Rename OLD_SCHEMA to NEW_SCHEMA in TEST_DB" |
| `alter_table` | Modify table structure | table_name: `TEST_DB.PUBLIC.USERS`<br>alter_type: `ADD`<br>column_name: `created_at`<br>data_type: `TIMESTAMP` | "Add a created_at timestamp column to TEST_DB.PUBLIC.USERS table" |
| `create_database` | Create a new database | database_name: `TEST_DB` | "Create a new database called TEST_DB" |
| `create_schema` | Create a schema in a database | database_name: `TEST_DB`<br>schema_name: `ANALYTICS` | "Create a schema named ANALYTICS in TEST_DB database" |
| `create_table` | Create a table with columns | database_name: `TEST_DB`<br>schema_name: `PUBLIC`<br>table_name: `USERS`<br>columns: `[{"name": "id", "type": "INT"}, {"name": "email", "type": "VARCHAR(255)"}]` | "Create a USERS table in TEST_DB.PUBLIC with id as INT and email as VARCHAR(255)" |
| `drop_database_object` | Drop any database object | object_type: `TABLE`<br>object_name: `TEST_DB.PUBLIC.OLD_TABLE` | "Drop the table TEST_DB.PUBLIC.OLD_TABLE" |
| `execute_ddl_statement` | Run custom DDL SQL | ddl_statement: `CREATE VIEW TEST_DB.PUBLIC.ACTIVE_USERS AS SELECT * FROM TEST_DB.PUBLIC.USERS WHERE status = 'active'` | "Create a view called ACTIVE_USERS that shows only active users" |

### 📊 DML Tools (6 Tools)

Tools for working with data:

| Tool | Description | Example in Inspector | Natural Language Query |
|------|-------------|---------------------|----------------------|
| `delete_data` | Delete rows from a table | table_name: `TEST_DB.PUBLIC.USERS`<br>where_clause: `status = 'deleted'` | "Delete all users with status 'deleted'" |
| `execute_dml_statement` | Run custom DML SQL | dml_statement: `UPDATE TEST_DB.PUBLIC.USERS SET last_login = CURRENT_TIMESTAMP() WHERE id = 1` | "Update the last login timestamp for user with id 1" |
| `insert_data` | Insert rows into a table | table_name: `TEST_DB.PUBLIC.USERS`<br>data: `{"id": 1, "email": "john@example.com", "name": "John Doe"}` | "Insert a new user with id 1, email john@example.com, and name John Doe into the USERS table" |
| `merge_data` | Synchronize data between tables | target_table: `TEST_DB.PUBLIC.USERS`<br>source_table: `TEST_DB.STAGING.NEW_USERS`<br>merge_condition: `target.id = source.id`<br>match_actions: `[{"action": "UPDATE", "columns": ["email", "name"], "values": ["source.email", "source.name"]}]`<br>not_match_actions: `[{"action": "INSERT", "columns": ["id", "email", "name"], "values": ["source.id", "source.email", "source.name"]}]` | "Merge new users from staging table into production users table, updating existing records and inserting new ones" |
| `query_data` | Query data from tables | table_name: `TEST_DB.PUBLIC.USERS`<br>columns: `["id", "email", "name"]`<br>where_clause: `status = 'active'`<br>limit: `10` | "Show me the first 10 active users with their id, email, and name" |
| `update_data` | Update existing rows | table_name: `TEST_DB.PUBLIC.USERS`<br>data: `{"status": "inactive"}`<br>where_clause: `last_login < '2023-01-01'` | "Set status to inactive for all users who haven't logged in since January 2023" |

### ⚙️ Snowflake Operations Tools (8 Tools)

Tools for Snowflake-specific operations:

| Tool | Description | Example in Inspector | Natural Language Query |
|------|-------------|---------------------|----------------------|
| `alter_warehouse` | Modify warehouse settings | warehouse_name: `COMPUTE_WH`<br>warehouse_size: `MEDIUM`<br>auto_suspend: `300` | "Change COMPUTE_WH to MEDIUM size and auto-suspend after 5 minutes" |
| `describe_database_object` | Get object details | object_name: `TEST_DB.PUBLIC.USERS` | "Describe the structure of TEST_DB.PUBLIC.USERS table" |
| `execute_sql_query` | Run any SQL query | query: `SELECT CURRENT_USER(), CURRENT_WAREHOUSE()` | "Show me my current user and warehouse" |
| `grant_privileges` | Grant permissions | privileges: `["SELECT", "INSERT"]`<br>on_type: `TABLE`<br>on_name: `TEST_DB.PUBLIC.USERS`<br>to_type: `ROLE`<br>to_name: `ANALYST_ROLE` | "Grant SELECT and INSERT on TEST_DB.PUBLIC.USERS table to ANALYST_ROLE" |
| `revoke_privileges` | Revoke permissions | privileges: `["SELECT"]`<br>on_type: `TABLE`<br>on_name: `TEST_DB.PUBLIC.USERS`<br>from_type: `ROLE`<br>from_name: `ANALYST_ROLE` | "Revoke SELECT on TEST_DB.PUBLIC.USERS table from ANALYST_ROLE" |
| `set_context` | Set database/schema/warehouse/role | context_type: `DATABASE`<br>context_name: `TEST_DB` | "Use TEST_DB as the current database" |
| `show_database_objects` | List database objects | object_type: `DATABASES` | "Show me all databases" |
| `test_snowflake_connection` | Test connection to Snowflake | (no parameters) | "Test my Snowflake connection" |


## 🏗️ Architecture

The server uses a simple per-operation connection pattern:
- Each tool/resource call creates a fresh Snowflake connection
- Connections are automatically closed after each operation
- No connection pooling or persistence required
- Credentials are read from environment variables

## 🛡️ Security Best Practices

1. **Use Programmatic Access Tokens (PAT)** instead of passwords when possible
2. **Never commit `.env` files** to version control
3. **Use least-privilege roles** for your Snowflake user
4. **Rotate credentials regularly**
5. **Consider using external secret management** for production

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Troubleshooting

### Connection Issues
- Verify your account identifier format
- Check that your user has appropriate permissions
- Ensure your PAT token hasn't expired
- Test network connectivity to Snowflake

### Tool Errors
- Check the error message in the Inspector console
- Verify required parameters are provided
- Ensure database objects exist before referencing them
- Check SQL syntax for custom statements

## 🚀 FastMCP Framework

This MCP server is built using **FastMCP**, a modern Python framework that simplifies building Model Context Protocol servers. FastMCP provides:

### Why FastMCP?

- **🎯 Simple API**: Decorator-based tool and resource registration
- **⚡ High Performance**: Async/await support with efficient message handling
- **🔧 Type Safety**: Full TypeScript-style type hints and validation
- **📝 Auto Documentation**: Automatic tool/resource documentation generation
- **🛡️ Error Handling**: Built-in exception handling and response formatting
- **🔌 MCP Compliance**: Full compatibility with MCP protocol specification

### FastMCP vs Traditional MCP

```python
# Traditional MCP server setup
class MyMCPServer:
    def __init__(self):
        self.tools = {}
    
    def register_tool(self, name, handler, schema):
        # Manual registration and validation
        pass

# FastMCP - Clean and Simple
from fastmcp import FastMCP

mcp = FastMCP("MyServer")

@mcp.tool()
def my_tool(param: str) -> str:
    """Tool with automatic type validation and documentation."""
    return f"Result: {param}"

@mcp.resource("my://resource/{id}")
async def my_resource(id: str, ctx: Context) -> dict:
    """Resource with built-in async support and context."""
    return {"data": f"Resource {id}"}
```

### Key FastMCP Features Used

1. **Decorator Registration**: Tools are registered using simple decorators
2. **Type Validation**: Automatic parameter validation using Python type hints  
3. **Context Management**: Built-in context for progress reporting and logging
4. **Resource Patterns**: URI template matching for dynamic resource endpoints
5. **Error Handling**: Automatic exception catching and standardized error responses

### FastMCP Installation

```bash
# Install FastMCP
pip install fastmcp

# Or with UV (recommended)
uv add fastmcp
```

### Learning FastMCP

- **Official Docs**: [FastMCP Documentation](https://github.com/jlowin/fastmcp)
- **Examples**: Browse FastMCP example servers in the repository
- **TypeScript MCP SDK**: [MCP TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk)

## 📚 Additional Resources

### Snowflake Resources
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- [Programmatic Access Tokens](https://docs.snowflake.com/en/user-guide/security-access-tokens)

### MCP Protocol & Tools
- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [MCP TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [MCP Inspector Tool](https://github.com/modelcontextprotocol/inspector)

### Development Tools
- [UV Package Manager](https://github.com/astral-sh/uv)
- [FastMCP Framework](https://github.com/jlowin/fastmcp)
- [Claude Desktop Configuration](https://claude.ai/docs)
- [Cursor IDE Integration](https://cursor.sh/)