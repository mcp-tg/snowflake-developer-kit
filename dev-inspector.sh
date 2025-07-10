#!/bin/bash

# Snowflake Developer MCP Server Inspector Development Script

echo "🔍 Starting Snowflake Developer MCP Server with Inspector"

# Check if .env file exists and create example if needed
if [ ! -f .env ]; then
    echo "📝 Creating .env file template..."
    cat > .env << 'EOF'
# Snowflake Connection Configuration
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PAT=your-programmatic-access-token
# Alternative: SNOWFLAKE_PASSWORD=your-password

# Optional: Additional connection parameters
# SNOWFLAKE_DATABASE=your-default-database
# SNOWFLAKE_SCHEMA=your-default-schema
# SNOWFLAKE_WAREHOUSE=your-default-warehouse
# SNOWFLAKE_ROLE=your-default-role
EOF
    echo "⚠️  Please update .env with your Snowflake credentials before running the server"
    echo "💡 You need:"
    echo "   - SNOWFLAKE_ACCOUNT: Your account identifier"
    echo "   - SNOWFLAKE_USER: Your username"
    echo "   - SNOWFLAKE_PAT: Your programmatic access token (or SNOWFLAKE_PASSWORD)"
fi

# Check if UV is installed
if ! command -v uv &> /dev/null; then
    echo "📦 UV package manager not found. Installing UV..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo "✅ UV installed successfully"
    echo "💡 You may need to restart your terminal or run: source ~/.bashrc"
fi

# Check if virtual environment exists
if [ ! -d .venv ]; then
    echo "🔧 Creating virtual environment with UV..."
    uv venv
fi

# Install/sync dependencies using UV
echo "📦 Installing/updating dependencies with UV..."
uv sync

# Check if Node.js and npm are available
if ! command -v npm &> /dev/null; then
    echo "❌ npm is required for MCP Inspector"
    echo "💡 Please install Node.js from https://nodejs.org/"
    exit 1
fi

# Verify Snowflake credentials are set
if [ -f .env ]; then
    echo "📋 Loading environment variables from .env..."
    set -a  # automatically export all variables
    source .env
    set +a  # stop automatically exporting
    
    if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || ([ -z "$SNOWFLAKE_PAT" ] && [ -z "$SNOWFLAKE_PASSWORD" ]); then
        echo "⚠️  Warning: Snowflake credentials not fully configured in .env"
        echo "   The server will start but tools may fail without proper credentials"
        echo "   Required variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PAT"
    else
        echo "✅ Snowflake credentials found in .env"
        echo "   Account: $SNOWFLAKE_ACCOUNT"
        echo "   User: $SNOWFLAKE_USER"
    fi
else
    echo "⚠️  No .env file found. Creating template..."
fi

echo ""
echo "🚀 Starting Snowflake Developer MCP Inspector..."
echo "💡 This will open a web interface to test your Snowflake Developer MCP server"
echo "💡 Available capabilities:"
echo "   🔧 DDL Tools: Create databases, schemas, tables"
echo "   📊 DML Tools: Insert, update, delete, select data"
echo "   ⚙️  Operations: Show objects, grants, warehouse management"
echo "   🧠 Cortex AI: Analyst and Search resources (if configured)"
echo ""
echo "💡 If you see a port conflict, the inspector will try the next available port"
echo ""

# Run the MCP Inspector pointing to our main script with UV
# Set DANGEROUSLY_OMIT_AUTH=true to disable session token authentication for local development
DANGEROUSLY_OMIT_AUTH=true npx @modelcontextprotocol/inspector uv run main.py