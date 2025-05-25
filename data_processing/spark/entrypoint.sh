#!/bin/bash

set -e

# Function to handle graceful shutdown
cleanup() {
    echo "Received shutdown signal, cleaning up..."
    exit 0
}

# Trap signals for graceful shutdown
trap cleanup SIGTERM SIGINT

# Prints để theo dõi trong Docker logs
echo "======================================================"
echo "Starting Spark processing job at $(date)"
echo "======================================================"
echo "Current directory: $(pwd)"
echo "First argument: $1"
echo "All arguments: $@"
echo "Available environment variables:"
echo "  SPARK_MASTER_URL: ${SPARK_MASTER_URL:-not set}"
echo "  SPARK_DRIVER_MEMORY: ${SPARK_DRIVER_MEMORY:-not set}"
echo "  SPARK_EXECUTOR_MEMORY: ${SPARK_EXECUTOR_MEMORY:-not set}"
echo "======================================================"

# Nếu không có arguments, exit
if [ $# -eq 0 ]; then
    echo "❌ No arguments provided. Usage:"
    echo "  docker run spark-processor daily_processing.py --date 2025-05-25"
    echo "  docker run spark-processor jobs/extraction/extract_batdongsan.py"
    exit 1
fi

# Xác định job path
JOB_PATH=""
REMAINING_ARGS=()

# Process first argument (job script)
FIRST_ARG="$1"
shift

# Copy remaining arguments
REMAINING_ARGS=("$@")

# Determine job path
if [[ "$FIRST_ARG" == /* ]]; then
    # Absolute path
    JOB_PATH="$FIRST_ARG"
elif [[ -f "/app/pipelines/$FIRST_ARG" ]]; then
    # Pipeline job
    JOB_PATH="/app/pipelines/$FIRST_ARG"
elif [[ -f "/app/jobs/$FIRST_ARG" ]]; then
    # Regular job
    JOB_PATH="/app/jobs/$FIRST_ARG"
elif [[ -f "/app/$FIRST_ARG" ]]; then
    # Root level
    JOB_PATH="/app/$FIRST_ARG"
else
    # Default to daily processing if no valid path found
    echo "⚠️  Job file '$FIRST_ARG' not found. Using default daily_processing.py"
    JOB_PATH="/app/pipelines/daily_processing.py"
    # Prepend the original first arg back to remaining args
    REMAINING_ARGS=("$FIRST_ARG" "${REMAINING_ARGS[@]}")
fi

# Verify job file exists
if [[ ! -f "$JOB_PATH" ]]; then
    echo "❌ Job file not found: $JOB_PATH"
    echo "Available files in /app:"
    find /app -name "*.py" -type f | head -10
    exit 1
fi

echo "📄 Job script: $JOB_PATH"
echo "📝 Remaining arguments: ${REMAINING_ARGS[*]}"

# Set up Spark master
if [ -z "${SPARK_MASTER_URL}" ]; then
    SPARK_MASTER="local[*]"
    echo "🔧 Using local Spark master"
else
    SPARK_MASTER="${SPARK_MASTER_URL}"
    echo "🔧 Using Spark master: $SPARK_MASTER"
fi

# Check if --master is already in arguments
MASTER_SPECIFIED=false
for arg in "${REMAINING_ARGS[@]}"; do
    if [[ "$arg" == "--master" ]]; then
        MASTER_SPECIFIED=true
        break
    fi
done

# Build spark-submit command
SPARK_CMD=(
    "spark-submit"
    "--master" "${SPARK_MASTER}"
    "--deploy-mode" "client"
    "--conf" "spark.hadoop.fs.defaultFS=hdfs://namenode:9000"
    "--conf" "spark.sql.adaptive.enabled=true"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
    "--conf" "spark.serializer=org.apache.spark.serializer.KryoSerializer"
    "--conf" "spark.sql.legacy.timeParserPolicy=LEGACY"
)

# Add memory settings if provided
if [ ! -z "${SPARK_DRIVER_MEMORY}" ]; then
    SPARK_CMD+=("--driver-memory" "${SPARK_DRIVER_MEMORY}")
fi

if [ ! -z "${SPARK_EXECUTOR_MEMORY}" ]; then
    SPARK_CMD+=("--executor-memory" "${SPARK_EXECUTOR_MEMORY}")
fi

# Add executor cores if specified
if [ ! -z "${SPARK_EXECUTOR_CORES}" ]; then
    SPARK_CMD+=("--executor-cores" "${SPARK_EXECUTOR_CORES}")
fi

# Add total executor cores if specified
if [ ! -z "${SPARK_TOTAL_EXECUTOR_CORES}" ]; then
    SPARK_CMD+=("--total-executor-cores" "${SPARK_TOTAL_EXECUTOR_CORES}")
fi

# Add job file
SPARK_CMD+=("$JOB_PATH")

# Add remaining arguments only if master not specified by user
if [ "$MASTER_SPECIFIED" = false ]; then
    SPARK_CMD+=("${REMAINING_ARGS[@]}")
    echo "🚀 Using configured Spark master: ${SPARK_MASTER}"
else
    echo "🚀 User provided master configuration will override defaults"
    # If user specified master, pass all remaining args as-is
    exec spark-submit "$JOB_PATH" "${REMAINING_ARGS[@]}"
fi

# Print final command for debugging
echo "🔄 Final command:"
echo "   ${SPARK_CMD[*]}"
echo "======================================================"

# Execute with timeout and proper signal handling
timeout 3600 "${SPARK_CMD[@]}" &
SPARK_PID=$!

# Wait for the process with signal handling
wait $SPARK_PID
EXIT_CODE=$?

echo "======================================================"
echo "Job completed at $(date) with exit code: $EXIT_CODE"
echo "======================================================"

exit $EXIT_CODE
