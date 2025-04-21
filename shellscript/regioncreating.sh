#!/bin/bash

#usage run: nohup ./regioncreating.sh '202503*' > run.log 2>&1 &

DATE_PATTERN=$1
shift

ROOT_DIR=/dev/sa/reference/curvatureshockindex
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/regioncreating.log"
ERR_FILE="$SCRIPT_DIR/regioncreating.err"
MAX_PARALLEL=4

IS_PREVIEW=0
IS_FORCE=0

# 解析附加参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        --preview) IS_PREVIEW=1 ;;
        --force) IS_FORCE=1 ;;
        *) echo "[WARN] Unknown option: $1" >> "$LOG_FILE" ;;
    esac
    shift
done

# 校验参数格式
if [[ -z "$DATE_PATTERN" || ! "$DATE_PATTERN" =~ ^[0-9]{6,8}\*?$ ]]; then
    echo "[ERROR] Usage: $0 <yyyymm* | yyyymmdd> [--preview] [--force]" >> "$LOG_FILE"
    exit 1
fi

# 提取通配符前缀构造正则
pattern_only=$(echo "$DATE_PATTERN" | sed 's/\*.*//')
regex="effdate=${pattern_only}[0-9]*$"

# 打印开始时间
echo "===== New execution started at $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG_FILE"
echo "===== New execution started at $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$ERR_FILE"
LOG_START_LINE=$(wc -l < "$LOG_FILE")

# 控制并发任务数
function wait_for_slot() {
  while (( $(jobs -rp | wc -l) >= MAX_PARALLEL )); do
    sleep 1
  done
}

process_effdate() {
  local effdate_path=$1
  local effdate=$(basename "$effdate_path" | cut -d= -f2)
  local timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"

  if hdfs dfs -test -d "$effdate_path/region=Asia"; then
    if [[ "$IS_FORCE" -eq 0 ]]; then
      echo "$timestamp SKIPPED: region=Asia already exists: $effdate_path" >> "$LOG_FILE"
      return
    else
      hdfs dfs -rm -r -f "$effdate_path/region=Asia" >> "$LOG_FILE" 2>&1
    fi
  fi

  if [[ "$IS_PREVIEW" -eq 1 ]]; then
    echo "$timestamp PREVIEW: Will process $effdate_path" >> "$LOG_FILE"
    return
  fi

  hdfs dfs -mkdir -p "$effdate_path/region=Asia" >> "$LOG_FILE" 2>&1

  hdfs dfs -mkdir -p "$effdate_path/region=Asia" >> "$LOG_FILE" 2>&1

  # 移动所有非 region= 子目录的文件（即直接在 effdate 目录下的内容）
  files_to_move=$(hdfs dfs -ls "$effdate_path" 2>/dev/null | awk '{print $8}' | grep -v "/region=")
  if [[ -n "$files_to_move" ]]; then
    echo "$timestamp INFO: Moving files to $effdate_path/region=Asia/" >> "$LOG_FILE"
    hdfs dfs -mv $files_to_move "$effdate_path/region=Asia/" >> "$LOG_FILE" 2>&1
  else
    echo "$timestamp WARNING: No files to move in $effdate_path" >> "$LOG_FILE"
  fi

  if [[ $? -ne 0 ]]; then
    echo "$timestamp FAILED: $effdate_path" >> "$LOG_FILE"
    echo "$effdate_path" >> "$ERR_FILE"
  else
    echo "$timestamp DONE: $effdate_path" >> "$LOG_FILE"
  fi
}

PIDS=()

# 主任务调度
matched_paths=$(hdfs dfs -ls "$ROOT_DIR" 2>/dev/null | awk '{print $8}' | grep "$regex")

if [[ -z "$matched_paths" ]]; then
  timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
  echo "$timestamp INFO: No matching directories found under $ROOT_DIR for pattern effdate=$DATE_PATTERN" >> "$LOG_FILE"
else
  for effdate_path in $matched_paths; do
    wait_for_slot
    process_effdate "$effdate_path" &
    PIDS+=($!)
  done

  for pid in "${PIDS[@]}"; do
    wait "$pid"
  done
fi

# 汇总统计
SUCCESS_COUNT=$(tail -n +"$((LOG_START_LINE + 1))" "$LOG_FILE" | grep -c "DONE:")
FAIL_COUNT=$(tail -n +"$((LOG_START_LINE + 1))" "$LOG_FILE" | grep -c "FAILED:")

timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
echo "===== Completed filter: $DATE_PATTERN =====" >> "$LOG_FILE"
echo "$timestamp ===== Summary: total=$((SUCCESS_COUNT + FAIL_COUNT)), executed=$SUCCESS_COUNT, skipped=$FAIL_COUNT =====" >> "$LOG_FILE"
echo "$timestamp Failed effdate paths have been recorded in: $ERR_FILE" >> "$LOG_FILE"