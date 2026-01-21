import os
import json
import subprocess
import numpy as np
from typing import List, Dict, Any
from mcp.server.fastmcp import FastMCP

# Initialize MCP Server
mcp = FastMCP("SparkDiagnosticServer")

def _read_hdfs_file(path: str) -> Any:
    """
    Generator that reads a file from HDFS line by line.
    
    PERFORMANCE NOTE FOR 1T+ DATA:
    For extremely large log files, we should avoid loading the entire file into RAM.
    This function uses a generator to yield lines one by one.
    
    In a real production environment with massive logs:
    1. Use `hdfs dfs -tail` if only the end of the log is relevant (though EventLogs require full scan for aggregation).
    2. If full scan is needed, streaming via stdout pipe (as done here) is memory efficient.
    3. For Driver logs, consider reading only the last N KB using `hdfs dfs -cat` with range or `tail`.
    """
    
    # Check if it's a local file (for testing) or HDFS path
    if path.startswith("hdfs://") or path.startswith("/"):
        # Try using HDFS command
        cmd = ["hdfs", "dfs", "-cat", path]
        try:
            # Popen allows streaming stdout
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Yield lines as they come
            if process.stdout:
                for line in process.stdout:
                    yield line
            
            process.stdout.close()
            return_code = process.wait()
            if return_code != 0:
                # Fallback or error handling
                pass
        except FileNotFoundError:
            # Fallback for local testing if 'hdfs' command not found
            if os.path.exists(path):
                 with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        yield line
            else:
                return []
    else:
        # Assume local relative path
        if os.path.exists(path):
             with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    yield line

@mcp.tool()
def get_stage_metrics(app_id: str) -> str:
    """
    Retrieves performance metrics for each Spark stage by parsing the event log.
    
    Args:
        app_id: The Spark Application ID (e.g., application_123456_0001).
        
    Returns:
        JSON string containing metrics per stage: 
        - duration stats (min, median, max)
        - skew ratio (max / median)
        - total shuffle read bytes
        - total disk spill bytes
    """
    # In a real scenario, construct path based on config
    # For demo, we assume the log is at /tmp/{app_id}.json or similar HDFS path
    # log_path = f"{os.environ.get('SPARK_EVENT_LOG_DIR', '/tmp/spark-events')}/{app_id}"
    log_path = f"{app_id}" # Simulating passing the full path or ID for simplicity

    stage_data = {}

    try:
        for line in _read_hdfs_file(log_path):
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            if event.get("Event") == "SparkListenerTaskEnd":
                stage_id = event["Stage ID"]
                task_info = event.get("Task Info", {})
                task_metrics = event.get("Task Metrics", {})
                
                # Duration
                duration = task_info.get("Duration", 0)
                
                # Shuffle Read
                shuffle_metrics = task_metrics.get("Shuffle Read Metrics", {})
                remote_read = shuffle_metrics.get("Remote Bytes Read", 0)
                local_read = shuffle_metrics.get("Local Bytes Read", 0)
                total_read = remote_read + local_read
                
                # Disk Spill
                memory_bytes_spilled = task_metrics.get("Memory Bytes Spilled", 0)
                disk_bytes_spilled = task_metrics.get("Disk Bytes Spilled", 0)
                
                if stage_id not in stage_data:
                    stage_data[stage_id] = {
                        "durations": [],
                        "total_shuffle_read": 0,
                        "total_disk_spill": 0
                    }
                
                stage_data[stage_id]["durations"].append(duration)
                stage_data[stage_id]["total_shuffle_read"] += total_read
                stage_data[stage_id]["total_disk_spill"] += disk_bytes_spilled

    except Exception as e:
        return f"Error reading logs for {app_id}: {str(e)}"

    # Aggregate results
    results = {}
    for stage_id, data in stage_data.items():
        durations = data["durations"]
        if not durations:
            continue
            
        _min = np.min(durations)
        _median = np.median(durations)
        _max = np.max(durations)
        
        # Avoid division by zero
        skew_ratio = (_max / _median) if _median > 0 else 1.0
        
        results[f"Stage {stage_id}"] = {
            "duration_min_ms": float(_min),
            "duration_median_ms": float(_median),
            "duration_max_ms": float(_max),
            "skew_ratio": float(round(skew_ratio, 2)),
            "total_shuffle_read_mb": float(round(data["total_shuffle_read"] / (1024 * 1024), 2)),
            "total_disk_spill_mb": float(round(data["total_disk_spill"] / (1024 * 1024), 2))
        }

    return json.dumps(results, indent=2)

@mcp.tool()
def get_driver_logs(app_id: str) -> str:
    """
    Searches Driver logs for common errors and performance warnings.
    
    Args:
        app_id: The Spark Application ID.
        
    Returns:
        A summary string of found issues or extracted log lines.
    """
    # log_path = f"{os.environ.get('SPARK_DRIVER_LOG_DIR', '/tmp/spark-driver-logs')}/{app_id}"
    log_path = f"{app_id}.driver" # Simulating convention
    
    keywords = [
        "ERROR", 
        "Exception", 
        "Spill to disk", 
        "V2ScanPartitioningAndOrdering",
        "OutOfMemoryError",
        "Timeout"
    ]
    
    found_lines = []
    
    try:
        # Optimization: In real world, might only read stderr or last N lines
        for line in _read_hdfs_file(log_path):
            for kw in keywords:
                if kw in line:
                    # Truncate line if too long to save token context
                    clean_line = line.strip()[:500]
                    found_lines.append(f"[{kw}] {clean_line}")
                    break
                    
            if len(found_lines) > 50:
                found_lines.append("... (Too many errors, truncated) ...")
                break
                
    except Exception as e:
        return f"Error reading driver logs: {str(e)}"
        
    if not found_lines:
        return "No critical errors or warnings found in Driver logs."
        
    return "\n".join(found_lines)

if __name__ == "__main__":
    # The server typically runs over stdio when used by a local client
    mcp.run()
