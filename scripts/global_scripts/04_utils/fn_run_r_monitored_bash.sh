#!/bin/bash
# ============================================================================
# fn_run_r_monitored_bash.sh
# Optimal R script monitoring for Claude Code integration
# Log location: scripts/global_scripts/00_principles/CHANGELOG/monitoring/
# ============================================================================

# Function to run R script with real-time monitoring
monitor_r() {
    local script="$1"
    local base_log_dir="scripts/global_scripts/00_principles/CHANGELOG/monitoring"
    
    # Determine log subdirectory based on script type
    local log_dir="$base_log_dir/general"
    if [[ "$script" =~ (ETL|0IM|1ST|2TR) ]]; then
        log_dir="$base_log_dir/etl"
    elif [[ "$script" =~ (api|API) ]]; then
        log_dir="$base_log_dir/api"
    elif [[ "$script" =~ (db|database|DB) ]]; then
        log_dir="$base_log_dir/database"
    fi
    
    # Create log directory
    mkdir -p "$log_dir"
    
    # Generate unique log file
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local log_file="$log_dir/$(basename "$script" .R)_${timestamp}.log"
    
    echo "🚀 Starting monitored execution: $script"
    echo "📝 Log file: $log_file"
    echo "⏱️  Real-time monitoring active"
    echo "════════════════════════════════════════"
    
    # Run with forced line buffering for real-time output
    # -oL: line buffer stdout
    # -eL: line buffer stderr
    # 2>&1: combine stderr with stdout
    # tee: save to file while displaying
    stdbuf -oL -eL Rscript "$script" 2>&1 | \
    tee "$log_file" | \
    while IFS= read -r line; do
        # Color coding for different message types
        if [[ "$line" =~ (✅|SUCCESS|successfully|Completed) ]]; then
            printf "\033[32m%s\033[0m\n" "$line"  # Green
        elif [[ "$line" =~ (⚠️|WARNING|warning|Warning) ]]; then
            printf "\033[33m%s\033[0m\n" "$line"  # Yellow
        elif [[ "$line" =~ (❌|ERROR|Error|Failed|failed|exception) ]]; then
            printf "\033[31m%s\033[0m\n" "$line"  # Red
            
            # Special error detection
            if [[ "$line" =~ rapi_register_df ]]; then
                echo "🔴 CRITICAL: DuckDB registration error detected!"
            elif [[ "$line" =~ "Conflicting lock" ]]; then
                echo "🔴 CRITICAL: DuckDB file lock detected!"
            fi
        elif [[ "$line" =~ (🔄|Loading|Starting) ]]; then
            printf "\033[36m%s\033[0m\n" "$line"  # Cyan
        else
            echo "$line"
        fi
    done
    
    local exit_code=${PIPESTATUS[0]}
    
    echo "════════════════════════════════════════"
    echo "📊 Execution completed with exit code: $exit_code"
    echo "📝 Full log saved to: $log_file"
    
    return $exit_code
}

# Execute if called directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ $# -eq 0 ]]; then
        echo "Usage: $0 <R_script_path>"
        exit 1
    fi
    monitor_r "$1"
fi