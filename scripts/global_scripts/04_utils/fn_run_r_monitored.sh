#!/bin/bash
# ============================================================================
# fn_run_r_monitored.sh
# Run R script with real-time monitoring and error detection
# Log location: scripts/global_scripts/00_principles/CHANGELOG/monitoring/
# ============================================================================

# Function to run R script with monitoring
run_r_monitored() {
    local script_path="$1"
    local monitor_interval="${2:-1}"  # Default 1 second
    
    # Create log directory in CHANGELOG
    # Determine subdirectory based on script type
    local base_log_dir="scripts/global_scripts/00_principles/CHANGELOG/monitoring"
    local log_dir="$base_log_dir/general"
    
    # Categorize log by script type
    if [[ "$script_path" =~ (ETL|0IM|1ST|2TR) ]]; then
        log_dir="$base_log_dir/etl"
    elif [[ "$script_path" =~ (api|API) ]]; then
        log_dir="$base_log_dir/api"
    elif [[ "$script_path" =~ (db|database|DB) ]]; then
        log_dir="$base_log_dir/database"
    fi
    
    mkdir -p "$log_dir"
    
    # Generate log file name
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local script_name=$(basename "$script_path")
    local log_file="$log_dir/monitor_${script_name}_${timestamp}.log"
    
    echo "🚀 Starting monitored R script: $script_path"
    echo "📝 Log file: $log_file"
    echo "⏱️  Monitoring every ${monitor_interval} second(s)"
    echo "════════════════════════════════════════════════════════"
    
    # Run R script with unbuffered output and error detection
    # stdbuf -oL -eL forces line buffering for real-time output
    # 2>&1 combines stderr and stdout
    # tee saves to log while displaying
    stdbuf -oL -eL Rscript "$script_path" 2>&1 | \
    while IFS= read -r line; do
        # Save to log
        echo "$line" >> "$log_file"
        
        # Color code and detect errors in real-time
        if echo "$line" | grep -qE "✅|SUCCESS|successfully"; then
            echo -e "\033[32m$line\033[0m"  # Green
        elif echo "$line" | grep -qE "⚠️|WARNING|warning"; then
            echo -e "\033[33m$line\033[0m"  # Yellow
        elif echo "$line" | grep -qE "❌|ERROR|Failed|failed|Error|error"; then
            echo -e "\033[31m$line\033[0m"  # Red
            
            # Detect specific errors
            if echo "$line" | grep -qE "rapi_register_df|std::exception"; then
                echo -e "\033[31m🔴 DETECTED: DuckDB registration error!\033[0m"
                echo "📋 Error context: This usually means:"
                echo "   1. Data frame has unsupported column types"
                echo "   2. Column names have special characters"
                echo "   3. List columns need JSON conversion"
                echo "   4. Date/time columns need proper formatting"
            fi
        else
            echo "$line"
        fi
        
        # Sleep briefly to avoid CPU overuse
        sleep 0.1
    done
    
    # Check exit status
    local exit_status=$?
    
    echo "════════════════════════════════════════════════════════"
    echo "📊 Execution completed with exit status: $exit_status"
    echo "📝 Full log saved to: $log_file"
    
    return $exit_status
}

# If called directly with arguments
if [ "$#" -gt 0 ]; then
    run_r_monitored "$@"
fi