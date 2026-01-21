import json
import os
import random

APP_ID = "application_1700000000000_0001"
LOG_FILE = f"{APP_ID}"
DRIVER_LOG = f"{APP_ID}.driver"

def create_event_log():
    with open(LOG_FILE, "w") as f:
        # Header
        f.write(json.dumps({"Event": "SparkListenerLogStart", "Spark Version": "3.3.0"}) + "\n")
        
        # Stage 0: Good stage
        for i in range(10):
            task_end = {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 0,
                "Task Info": {
                    "Task ID": i,
                    "Duration": 5000 + random.randint(-500, 500) # ~5s
                },
                "Task Metrics": {
                    "Shuffle Read Metrics": {
                        "Remote Bytes Read": 100 * 1024 * 1024, # 100MB
                        "Local Bytes Read": 0
                    },
                    "Disk Bytes Spilled": 0,
                    "Memory Bytes Spilled": 0
                }
            }
            f.write(json.dumps(task_end) + "\n")
            
        # Stage 1: Skewed Stage + Spill
        for i in range(10):
            duration = 5000
            spill = 0
            # Task 5 is the skewed one
            if i == 5:
                duration = 35000 # 35s
                spill = 500 * 1024 * 1024 # 500MB Spill
                
            task_end = {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 1,
                "Task Info": {
                    "Task ID": 10 + i,
                    "Duration": duration
                },
                "Task Metrics": {
                    "Shuffle Read Metrics": {
                        "Remote Bytes Read": 200 * 1024 * 1024,
                        "Local Bytes Read": 0
                    },
                    "Disk Bytes Spilled": spill,
                    "Memory Bytes Spilled": spill
                }
            }
            f.write(json.dumps(task_end) + "\n")
            
    print(f"Created event log: {LOG_FILE}")

def create_driver_log():
    with open(DRIVER_LOG, "w") as f:
        f.write("24/01/21 10:00:00 INFO DagScheduler: Job 0 finished\n")
        f.write("24/01/21 10:00:05 ERROR TaskSchedulerImpl: Lost executor 1 on host-1\n")
        f.write("24/01/21 10:00:10 WARN TaskMemoryManager: Spill to disk occurred. This will slow down execution.\n")
        f.write("24/01/21 10:05:00 INFO SparkContext: Successfully stopped SparkContext\n")
    print(f"Created driver log: {DRIVER_LOG}")

if __name__ == "__main__":
    create_event_log()
    create_driver_log()
    print("\nTo test:")
    print(f"1. Run 'python create_dummy_log.py'")
    print(f"2. Run 'python agent_client.py'")
    print(f"3. Ask agent: 'Analyze application_1700000000000_0001'")
