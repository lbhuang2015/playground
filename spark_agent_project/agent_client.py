import os
import sys
import asyncio
import logging
from dotenv import load_dotenv
from langchain_community.chat_models import init_chat_model
from langchain_core.messages import SystemMessage

from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp.client.session import ClientSession

# --- Basic Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(message)s')

# --- Configuration ---
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
MODEL_NAME = os.getenv("MODEL_NAME", "llama3")
API_KEY = os.getenv("OPENAI_API_KEY")
BASE_URL = os.getenv("OPENAI_API_BASE")
EVENT_LOG_PATH = os.getenv("EVENT_LOG_PATH")
DRIVER_LOG_PATH = os.getenv("DRIVER_LOG_PATH")

# --- System Prompt ---
SYSTEM_PROMPT_TEMPLATE = """
## 1. Core Operational Principles
- **Hardware-Aware Execution**: Operating in a restricted CPU environment without admin privileges. **Prohibit** raw processing of 1.4GB logs via LLM. A "Pre-process then Diagnose" strategy is mandatory.
- **Total Transparency (Telemetry)**: During execution, the Agent MUST output real-time `[STAGE: XXX]` logs to eliminate "black-box" silence during long CPU-bound tasks.
- **Causal Alignment**: All diagnoses must be derived from the cross-verification of **EventLog Metrics** and **DriverLog Events**.

## 2. Workflow Execution Stages
### Stage 1: Multi-Source Data Stitching (MCP Tool)
Invoke the `generate_merged_context` MCP tool to perform a "dehydrated" stitch of the logs.
- **Inputs**: `event_log_path`, `driver_log_path`
- **Output Requirement**: A single, high-density Markdown table (< 8,000 characters) containing aligned metrics for `SkewRatio`, `Spill`, `GC Time`, and corresponding `Driver Warnings`.

### Stage 2: Expert Reasoning & Correlation
Upon receiving the stitched data, you must apply the following **"Anomaly Mapping Matrix"**:

| **Observed Patterns (Metrics + Logs)**       | **Root Cause Diagnosis**                      | **Actionable Fix (Part 3)**                                  |
| -------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------ |
| **High Skew** + **ExecutorLost/OOM**         | Data skew causing physical memory crash.      | Enable `skewJoin`, increase `memory.overhead`, implement `Salting`. |
| **High Skew** + **Disk Spill**               | Data skew forcing I/O degradation.            | Adjust `memory.fraction`, increase `executor.memory`.        |
| **Low Skew** + **Speculation/Task Resubmit** | Environmental Straggler (Node inconsistency). | Adjust `speculation.multiplier`, inspect specific Node health. |
| **Any Metric** + **GC Time > 10%**           | Heap pressure or high object churn.           | Switch to `G1GC`, reduce `executor.cores` per executor.      |

## 3. Output Report Specification
The response must be structured into exactly three parts:

### Part 1: Performance Metrics Table
List all Stages with $SkewRatio > 3$ or those containing `ERROR/WARN` logs.
```
| StageID | SkewRatio | Duration | Input/Output | Spill(M/D) | Driver Annotations |
```

### Part 2: Core Root Cause Analysis
- Provide a bulleted synthesis explaining the "Why."
- **Example**: "Stage 12 is the primary bottleneck due to a massive SkewRatio (45x). The DriverLog confirms OOM errors on Executor 4, suggesting a skewed join key during the shuffle phase."

### Part 3: Actionable Optimization Recommendations
Provide "surgical" advice for each problematic Stage:
- **Primary Action**: [The most impactful fix]
- **Spark Config Changes**:
  - `spark.sql.adaptive.skewJoin.enabled`: `true`
  - `spark.executor.memoryOverhead`: `2G` (Reason: match with OOM alerts)
- **Code Strategy**: [Specific coding advice, e.g., "Add salt to the Join Key 'user_id'"]

---
**STITCHED LOG DATA TO ANALYZE:**
{merged_context}
"""

async def run_agent():
    # 1. Initialize LLM
    try:
        model_kwargs = {"api_key": API_KEY, "base_url": BASE_URL, "temperature": 0.1}
        model_kwargs = {k: v for k, v in model_kwargs.items() if v}
        model = init_chat_model(MODEL_NAME, model_provider=LLM_PROVIDER, **model_kwargs)
    except Exception as e:
        logging.error(f"Error initializing model: {e}")
        sys.exit(1)

    # 2. Connect to MCP Server
    server_params = StdioServerParameters(command=sys.executable, args=["mcp_server.py"], env=os.environ)
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            logging.info(f"Agent ready! (Model: {MODEL_NAME})")
            logging.info("---")

            if not EVENT_LOG_PATH or not DRIVER_LOG_PATH:
                logging.error("ERROR: EVENT_LOG_PATH and DRIVER_LOG_PATH must be set in your .env file.")
                return

            # --- STAGE 1: PYTHON PRE-PROCESSING ---
            merged_context = await session.call_tool(
                'generate_merged_context', 
                {'event_log_path': EVENT_LOG_PATH, 'driver_log_path': DRIVER_LOG_PATH}
            )

            if "ERROR:" in merged_context or "INFO:" in merged_context:
                logging.info(merged_context)
                logging.info("\n---")
                return
            
            # --- STAGE 2: CONTEXT PREPARATION ---
            logging.info(f"\n[STAGE: CONTEXT READY] Metrics aligned. Analyzing {len(merged_context.splitlines()) - 2} outlier stages.")
            final_prompt = SYSTEM_PROMPT_TEMPLATE.format(merged_context=merged_context)
            
            # --- STAGE 3: LLM INFERENCE ---
            logging.info(f"[STAGE: INFERENCE] {MODEL_NAME} is diagnosing the bottleneck (CPU Mode)...")
            
            messages = [SystemMessage(content=final_prompt)]
            try:
                async for chunk in model.astream(messages):
                    print(chunk.content, end="", flush=True)
            except Exception as e:
                logging.error(f"\n[ERROR] Inference failed: {e}")
            
            logging.info("\n\n---")
            logging.info("Analysis complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_agent())
    except KeyboardInterrupt:
        print("\nAgent stopped by user.")