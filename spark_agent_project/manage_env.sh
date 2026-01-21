#!/bin/bash

# =================================================================
# Spark Agent Environment Manager
# =================================================================
# This script handles the setup of a standalone Python 3.10+ environment
# using 'uv'. It ensures NO CONFLICT with the system's existing
# Python 3.6/3.8 or Spark configurations.
# =================================================================

set -e  # Exit immediately if a command exits with a non-zero status

# Configuration
TARGET_PYTHON="3.11"
VENV_DIR=".venv"
REQUIREMENTS="requirements.txt"
MAIN_SCRIPT="agent_client.py"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[SETUP]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 1. Check/Install uv
ensure_uv_installed() {
    if ! command -v uv &> /dev/null; then
        # Check standard cargo bin location
        if [ -f "$HOME/.cargo/bin/uv" ]; then
            export PATH="$HOME/.cargo/bin:$PATH"
        else
            log "uv not found. Installing uv (requires internet)..."
            curl -LsSf https://astral.sh/uv/install.sh | sh
            export PATH="$HOME/.cargo/bin:$PATH"
        fi
    else
        log "uv is already installed: $(uv --version)"
    fi
}

# 2. Create Virtual Environment
setup_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        log "Creating isolated Python $TARGET_PYTHON environment..."
        # uv will automatically download the python standalone build if needed
        # This stores the python binary in ~/.uv/python, NOT system folders
        uv venv --python "$TARGET_PYTHON" "$VENV_DIR"
        success "Virtual environment created at ./$VENV_DIR"
    else
        log "Virtual environment already exists."
    fi
}

# 3. Install Dependencies
install_deps() {
    if [ -f "$REQUIREMENTS" ]; then
        log "Installing dependencies from $REQUIREMENTS..."
        # Use the pip inside the venv
        uv pip install -r "$REQUIREMENTS"
        success "Dependencies installed."
    else
        warn "$REQUIREMENTS not found! Creating a basic one..."
        echo "mcp[cli]" > "$REQUIREMENTS"
        echo "langgraph" >> "$REQUIREMENTS"
        echo "langchain" >> "$REQUIREMENTS"
        echo "python-dotenv" >> "$REQUIREMENTS"
        uv pip install -r "$REQUIREMENTS"
    fi
}

# 4. Run the Agent
run_agent() {
    log "Starting Agent Client..."
    echo "---------------------------------------------------"
    
    # Check if .env exists
    if [ ! -f ".env" ]; then
        warn "No .env file found. Copying .env.template..."
        if [ -f ".env.template" ]; then
            cp .env.template .env
            warn "Please edit .env to set your API_KEY, then run this script again."
            exit 0
        else
            warn "No .env.template found. Agent might fail."
        fi
    fi

    # Execute using the ISOLATED python binary
    # We do not need to 'activate' the shell, just call the binary directly
    ./$VENV_DIR/bin/python "$MAIN_SCRIPT"
}

# =================================================================
# Main Logic
# =================================================================

if [ "$1" == "start" ]; then
    ensure_uv_installed
    setup_venv
    install_deps
    run_agent
else
    echo "Usage: $0 start"
    echo ""
    echo "This will:"
    echo "  1. Install 'uv' (if missing)."
    echo "  2. Create a local Python $TARGET_PYTHON environment in ./$VENV_DIR."
    echo "  3. Install dependencies."
    echo "  4. Run the agent."
    echo "  * It will NOT affect your system Python or Spark settings."
fi
