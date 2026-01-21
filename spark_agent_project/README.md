# Spark Performance Diagnosis Agent

This project implements an AI Agent using the **Model Context Protocol (MCP)** and **LangGraph** to diagnose Apache Spark application performance issues.

## Architecture

*   **MCP Server (`mcp_server.py`)**: Provides tools to read and parse Spark Event Logs and Driver Logs. It simulates HDFS access via local files for testing but uses `subprocess` to call `hdfs` commands in production.
*   **Agent Client (`agent_client.py`)**: A LangGraph-based ReAct agent that connects to the MCP Server. It reasons about performance metrics (Skew, Spill, Duration) to provide tuning advice.

## Prerequisites

*   Python 3.10+
*   `uv` or `pip`

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment**:
    Copy `.env.template` to `.env` and set your LLM credentials.
    ```bash
    cp .env.template .env
    # Edit .env and set API_KEY, MODEL_NAME, etc.
    ```

3.  **Generate Dummy Data (Optional)**:
    If you don't have real Spark logs handy, generate test files:
    ```bash
    python create_dummy_log.py
    ```
    This creates `application_1700000000000_0001` and `application_1700000000000_0001.driver`.

## Running the Agent

Start the agent client. It will automatically launch the MCP server as a subprocess.

```bash
python agent_client.py
```

**Example Conversation**:

> **User**: Analyze application_1700000000000_0001
>
> **Agent**: *Calls `get_stage_metrics`...*
> *Detects Skew Ratio > 7 in Stage 1...*
> *Calls `get_driver_logs`...*
> *Sees "Spill to disk"...*
>
> **Recommendation**: "Stage 1 suffers from severe data skew (Ratio 7.0) and disk spill (500MB). Driver logs confirm spilling. Suggest increasing executor memory or using salting for the skewed keys."

## Handling Large Logs (1T+ Data)

The `mcp_server.py` is designed for scale:
*   It reads logs line-by-line (streaming) rather than loading the full file into RAM.
*   It parses only necessary events (`SparkListenerTaskEnd`).
*   For Driver logs, it searches for keywords without storing the full content.

In a real production environment, ensure the machine running the MCP server has network access to the HDFS client configuration.
