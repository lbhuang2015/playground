import os
import sys
import json
import re
import logging
from typing import Dict, Any
import numpy as np
from smart_open import open as smart_open
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP

# --- Basic Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(message)s')
mcp = FastMCP("SparkLogMerger")

# --- Path Configuration ---
EVENT_LOG_DIR = os.getenv("SPARK_EVENT_LOG_DIR", "/tmp/spark-events")
DRIVER_LOG_DIR = os.getenv("SPARK_DRIVER_LOG_DIR", "/tmp/spark-driver-logs")


# --- Helper Functions ---
def format_ms(ms: float) -> str:
    if ms < 1000: return f"{ms:.0f}ms"
    s = ms / 1000
    if s < 60: return f"{s:.1f}s"
    return f"{(s / 60):.1f}min"

def format_bytes(b: float) -> str:
    if b < 1024: return f"{b:.0f}B"
    kb = b / 1024
    if kb < 1024: return f"{kb:.1f}KB"
    mb = kb / 1024
    if mb < 1024: return f"{mb:.1f}MB"
    return f"{(mb / 1024):.1f}GB"

# --- Core Logic ---

def _index_event_log(path: str, app_id: str) -> Dict[int, Dict[str, Any]]:
    logging.info(f"[STAGE: PRE-PROCESSING] Scanning Spark EventLog via Python Streamer: {path}")
    stages: Dict[int, Dict[str, Any]] = {}

    file_size = os.path.getsize(path) if os.path.exists(path) else 0
    bytes_read = 0
    last_reported_progress = -1

    for line in smart_open(path, 'r', encoding='utf-8', errors='ignore'):
        bytes_read += len(line.encode('utf-8'))
        if file_size > 0:
            progress = int((bytes_read / file_size) * 100)
            if progress > last_reported_progress and progress % 10 == 0:
                logging.info(f"[PROGRESS] {format_bytes(bytes_read)}/{format_bytes(file_size)} processed ({progress}%)...")
                last_reported_progress = progress

        try:
            event = json.loads(line)
        except json.JSONDecodeError: continue
            
        event_type = event.get("Event")
        stage_id = event.get("Stage ID")

        if stage_id is not None and stage_id not in stages:
            stages[stage_id] = {
                "tasks_durations": [], "submissionTime": None, "completionTime": None,
                "inputBytes": 0, "outputBytes": 0, "shuffleWrite": 0, "spill_disk": 0, "spill_mem": 0,
                "driver_logs": [], "skewRatio": 1.0
            }
        
        if event_type == "SparkListenerStageSubmitted":
            stages[stage_id]["submissionTime"] = event.get("Stage Info", {}).get("Submission Time", 0)
        
        elif event_type == "SparkListenerTaskEnd":
            stages[stage_id]["tasks_durations"].append(event.get("Task Info", {}).get("Duration", 0))
            metrics = event.get("Task Metrics", {})
            if metrics:
                stages[stage_id]["inputBytes"] += metrics.get("Input Metrics", {}).get("Bytes Read", 0)
                stages[stage_id]["outputBytes"] += metrics.get("Output Metrics", {}).get("Bytes Written", 0)
                stages[stage_id]["spill_mem"] += metrics.get("Memory Bytes Spilled", 0)
                stages[stage_id]["spill_disk"] += metrics.get("Disk Bytes Spilled", 0)

        elif event_type == "SparkListenerStageCompleted":
            stages[stage_id]["completionTime"] = event.get("Stage Info", {}).get("Completion Time", 0)
    
    for stage_id, data in stages.items():
        if len(data["tasks_durations"]) > 1:
            median_duration = np.median(data["tasks_durations"])
            if median_duration > 0:
                data["skewRatio"] = np.max(data["tasks_durations"]) / median_duration
    
    logging.info(f"[DEBUG] EventLog indexing complete. Found {len(stages)} stages.")
    return stages

def _align_driver_log(path: str, stages: Dict[int, Dict[str, Any]]):
    logging.info(f"[DEBUG] Streaming DriverLog for alignment: {path}...")
    matched_events = 0
    
    ts_regex = re.compile(r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})')
    stage_id_regex = re.compile(r'Stage (\d+)')
    keywords = ["WARN", "ERROR", "FATAL", "ExecutorLost", "FetchFailed", "Spilling", "GC time", "TaskSetManager", "OOM"]

    for line in smart_open(path, 'r', encoding='utf-8', errors='ignore'):
        if not any(kw in line for kw in keywords): continue

        clean_line = line.strip().replace("`", "'").replace("|", "-")
        
        stage_match = stage_id_regex.search(clean_line)
        if stage_match:
            stage_id = int(stage_match.group(1))
            if stage_id in stages:
                stages[stage_id]["driver_logs"].append(clean_line)
                matched_events += 1
                continue

        ts_match = ts_regex.match(clean_line)
        if ts_match:
            try:
                log_ts = parse_date(ts_match.group(1)).timestamp() * 1000
                for stage_id, data in stages.items():
                    if data["submissionTime"] and data["completionTime"] and (data["submissionTime"] <= log_ts <= data["completionTime"]):
                        stages[stage_id]["driver_logs"].append(clean_line)
                        matched_events += 1
                        break
            except Exception: continue
    
    logging.info(f"[DEBUG] DriverLog alignment complete. Matched {matched_events} critical events.")
    return stages

@mcp.tool()
def generate_merged_context(app_id: str) -> str:
    """
    Performs a streaming merge of Spark logs based on an app_id to generate a 
    high-density context for LLM analysis.
    """
    event_log_path = os.path.join(EVENT_LOG_DIR, f"{app_id}.log")
    driver_log_path = os.path.join(DRIVER_LOG_DIR, f"{app_id}_driver.log")
    
    try:
        stages = _index_event_log(event_log_path, app_id)
        stages = _align_driver_log(driver_log_path, stages)
    except FileNotFoundError as e:
        logging.error(f"ERROR: Could not find log file. {e}")
        return f"ERROR: Could not find log file. Please check SPARK_EVENT_LOG_DIR and SPARK_DRIVER_LOG_DIR in your .env file and ensure the log files exist with the correct naming convention. Details: {e}"
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return f"ERROR: An unexpected error occurred during log processing: {e}"

    problem_stages = []
    for stage_id, data in stages.items():
        if data["skewRatio"] > 3.0 or data["spill_disk"] > 0 or len(data["driver_logs"]) > 0:
            problem_stages.append({
                "StageID": stage_id,
                "SkewRatio": f"{data['skewRatio']:.1f}",
                "Duration": format_ms(np.median(data['tasks_durations'])) if data['tasks_durations'] else "N/A",
                "Input/Output": f"{format_bytes(data['inputBytes'])}/{format_bytes(data['outputBytes'])}",
                "Spill(M/D)": f"{format_bytes(data['spill_mem'])}/{format_bytes(data['spill_disk'])}",
                "Driver Annotations": " ".join(data["driver_logs"])
            })

    if not problem_stages:
        return f"INFO: No significant performance issues found for {app_id} based on specified thresholds."

    output = ""
    output += "| StageID | SkewRatio | Duration | Input/Output | Spill(M/D) | Driver Annotations |\n"
    output += "|---|---|---|---|---|---|
"

    char_limit = 7500
    
    for stage in sorted(problem_stages, key=lambda x: x["StageID"]):
        row_str = f"| {stage['StageID']} | {stage['SkewRatio']} | {stage['Duration']} | {stage['Input/Output']} | {stage['Spill(M/D)']} | {stage['Driver Annotations'][:200]} |\n"
        if len(output) + len(row_str) > char_limit:
            logging.warning("[WARN] Output truncated to respect character limit (<8k). Not all stages may be shown.")
            break
        output += row_str
        
    return output

if __name__ == "__main__":
    mcp.run()