#!/bin/bash

#usage run:  nohup ./backup.sh '202504*' > run.log 2>&1 &

export HADOOP_ROOT_LOGGER=ERROR,console

DATE_PATTERN=$1
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/fsc-backwardcapatibility.log"
ERR_FILE="$SCRIPT_DIR/fcs-backwardcapatibility.err"

# > "$LOG_FILE"
# > "$ERR_FILE"
echo "===== New execution started at $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG_FILE"
echo "===== New execution started at $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$ERR_FILE"
LOG_START_LINE=$(wc -l < "$LOG_FILE")

# 参数校验
if [[ ! "$DATE_PATTERN" =~ ^[0-9]{6}\*$ && ! "$DATE_PATTERN" =~ ^[0-9]{7}\*$ && ! "$DATE_PATTERN" =~ ^[0-9]{8}$ ]]; then
    echo "Error: Invalid date pattern. Accepted formats: yyyymm*, yyyymmd*, yyyymmdd"
    exit 1
fi

SOURCE_DIRS=(
    "/dev/sa/reference/instrument"
    "/dev/sa/reference/curvatureshock"
    "/dev/sa/reference/curvatureshockindex"
)

TARGET_DIRS=(
    "/dev/sa/reference/instrument_backup"
    "/dev/sa/reference/curvatureshock_backup"
    "/dev/sa/reference/curvatureshockindex_backup"
)

# 创建顶层 backup 目录
for tgt in "${TARGET_DIRS[@]}"; do
    hdfs dfs -mkdir -p "$tgt" 2>>"$LOG_FILE"
done

# 拷贝函数（并发执行）
copy_and_log() {
    local src_path="$1"
    local tgt_path="$2"
    local timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
    local failed=0

    # 删除目标子目录（如果存在）
    hdfs dfs -rm -r -f "$tgt_path" 2>>"$LOG_FILE"
    # 重新创建
    hdfs dfs -mkdir -p "$tgt_path" 2>>"$LOG_FILE"

    # 获取子文件列表并排除空行
    hdfs dfs -ls "$src_path" 2>>"$LOG_FILE" | awk '{print $8}' | grep -v '^$' | while read -r file; do
        if [[ -n "$file" ]]; then
            if ! hdfs dfs -cp "$file" "$tgt_path/" 2>>"$LOG_FILE"; then
                failed=1
            fi
        fi
    done

    if [[ $failed -ne 0 ]]; then
        echo "$timestamp FAILED: data in source: $src_path failed to copy into target: $tgt_path" >> "$LOG_FILE"
        echo "$src_path" >> "$ERR_FILE"
    else
        echo "$timestamp SUCCESS: data in source: $src_path have been copied into target: $tgt_path" >> "$LOG_FILE"
    fi
}

PIDS=()

# 提取实际前缀（如 202504）
pattern_only=$(echo "$DATE_PATTERN" | sed 's/\*.*//')
regex="effdate=${pattern_only}[0-9]*$"

for i in "${!SOURCE_DIRS[@]}"; do
    SRC=${SOURCE_DIRS[$i]}
    TGT=${TARGET_DIRS[$i]}

    matched_paths=$(hdfs dfs -ls "$SRC/" 2>/dev/null | awk '{print $8}' | grep -E "$regex")

    if [[ -z "$matched_paths" ]]; then
        timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"
        echo "$timestamp INFO: No matching directories found under $SRC for pattern effdate=$DATE_PATTERN" >> "$LOG_FILE"
        continue
    fi

    for src_path in $matched_paths; do
        tgt_path="$TGT/$(basename "$src_path")"
        echo "$timestamp INFO: starting copy  $src_path to $tgt_path" >> "$LOG_FILE"
        copy_and_log "$src_path" "$tgt_path" &
        PIDS+=($!)
    done
done

# 等待所有后台任务完成
for pid in "${PIDS[@]}"; do
    wait "$pid"
done

# 汇总
# 从执行开始后新增的日志行中统计
SUCCESS_COUNT=$(tail -n +"$((LOG_START_LINE + 1))" "$LOG_FILE" | grep -c "SUCCESS:")
FAIL_COUNT=$(tail -n +"$((LOG_START_LINE + 1))" "$LOG_FILE" | grep -c "FAILED:")
timestamp="[$(date '+%Y-%m-%d %H:%M:%S')]"

echo "$timestamp === Summary ===" >> "$LOG_FILE"
echo "$timestamp Copy scope: $DATE_PATTERN" >> "$LOG_FILE"
echo "$timestamp Total successful effdate paths: $SUCCESS_COUNT" >> "$LOG_FILE"
echo "$timestamp Total failed effdate paths: $FAIL_COUNT" >> "$LOG_FILE"
echo "$timestamp Failed effdate paths have been recorded in: $ERR_FILE" >> "$LOG_FILE"