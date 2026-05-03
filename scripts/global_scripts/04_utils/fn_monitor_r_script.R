# ============================================================================
# fn_monitor_r_script.R
# Real-time R script monitoring with error detection
# Follows: MP099 (Real-time Progress Reporting)
# Log location: scripts/global_scripts/00_principles/changelog/monitoring/
# ============================================================================

#' Monitor R Script Execution in Real-time
#'
#' Runs an R script in the background and monitors its output every second,
#' catching errors and warnings as they occur.
#'
#' @param script_path Path to the R script to execute
#' @param monitor_interval Interval in seconds between output checks (default: 1)
#' @param stop_on_error Stop monitoring when error is detected (default: TRUE)
#' @param verbose Print all output to console (default: TRUE)
#' 
#' @return List with execution details and captured output
#' 
#' @examples
#' \dontrun{
#' result <- monitor_r_script("scripts/update_scripts/cbz_ETL01_0IM.R")
#' }
monitor_r_script <- function(script_path, 
                           monitor_interval = 1,
                           stop_on_error = TRUE,
                           verbose = TRUE) {
  
  require(processx)
  
  # Validate script exists
  if (!file.exists(script_path)) {
    stop("Script not found: ", script_path)
  }
  
  # Create log file in CHANGELOG monitoring directory
  # This aligns with principle documentation and system history tracking
  log_dir <- "scripts/global_scripts/00_principles/changelog/monitoring"
  
  # Determine subdirectory based on script type
  if (grepl("ETL|0IM|1ST|2TR", script_path, ignore.case = TRUE)) {
    log_dir <- file.path(log_dir, "etl")
  } else if (grepl("api|API", script_path, ignore.case = TRUE)) {
    log_dir <- file.path(log_dir, "api")
  } else if (grepl("db|database|DB", script_path, ignore.case = TRUE)) {
    log_dir <- file.path(log_dir, "database")
  } else {
    log_dir <- file.path(log_dir, "general")
  }
  
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  log_file <- file.path(log_dir, paste0(
    "monitor_",
    basename(script_path),
    "_",
    format(Sys.time(), "%Y%m%d_%H%M%S"),
    ".log"
  ))
  
  # Start R process in background
  message("🚀 Starting R script: ", script_path)
  message("📝 Log file: ", log_file)
  message("⏱️  Monitoring every ", monitor_interval, " second(s)")
  message(strrep("-", 60))
  
  # Create process
  p <- processx::process$new(
    command = "Rscript",
    args = script_path,
    stdout = "|",
    stderr = "|",
    echo_cmd = TRUE
  )
  
  # Initialize monitoring variables
  all_output <- character()
  all_errors <- character()
  error_detected <- FALSE
  warning_count <- 0
  start_time <- Sys.time()
  last_output_time <- start_time
  
  # Monitor loop
  while (p$is_alive() || p$poll_io(0)["output"] == "ready") {
    
    # Check for output
    output <- p$read_output_lines(n = 1000)
    errors <- p$read_error_lines(n = 1000)
    
    # Process standard output
    if (length(output) > 0) {
      all_output <- c(all_output, output)
      last_output_time <- Sys.time()
      
      # Write to log
      cat(output, sep = "\n", file = log_file, append = TRUE)
      cat("\n", file = log_file, append = TRUE)
      
      if (verbose) {
        for (line in output) {
          # Color code output
          if (grepl("✅|SUCCESS", line, ignore.case = TRUE)) {
            cat("\033[32m", line, "\033[0m\n", sep = "")  # Green
          } else if (grepl("⚠️|WARNING", line, ignore.case = TRUE)) {
            cat("\033[33m", line, "\033[0m\n", sep = "")  # Yellow
            warning_count <- warning_count + 1
          } else if (grepl("❌|ERROR|Failed", line, ignore.case = TRUE)) {
            cat("\033[31m", line, "\033[0m\n", sep = "")  # Red
            error_detected <- TRUE
          } else {
            cat(line, "\n")
          }
        }
      }
    }
    
    # Process error output
    if (length(errors) > 0) {
      all_errors <- c(all_errors, errors)
      last_output_time <- Sys.time()
      
      # Write to log
      cat("STDERR: ", errors, sep = "\n", file = log_file, append = TRUE)
      cat("\n", file = log_file, append = TRUE)
      
      if (verbose) {
        for (line in errors) {
          cat("\033[31mERROR: ", line, "\033[0m\n", sep = "")  # Red
        }
      }
      
      # Check for specific errors
      if (any(grepl("rapi_register_df|std::exception", errors))) {
        message("\n🔴 DETECTED: DuckDB registration error!")
        error_detected <- TRUE
      }
    }
    
    # Stop on error if requested
    if (error_detected && stop_on_error) {
      message("\n⛔ Stopping due to error detection")
      p$kill()
      break
    }
    
    # Show heartbeat if no output for a while
    if (difftime(Sys.time(), last_output_time, units = "secs") > 5) {
      if (verbose) {
        cat("💓 Still running... (", 
            round(difftime(Sys.time(), start_time, units = "secs")), 
            "s)\n", sep = "")
      }
      last_output_time <- Sys.time()
    }
    
    # Wait for next check
    Sys.sleep(monitor_interval)
  }
  
  # Get exit status
  exit_status <- p$get_exit_status()
  elapsed_time <- difftime(Sys.time(), start_time, units = "secs")
  
  # Final summary
  message(strrep("-", 60))
  message("📊 Execution Summary:")
  message("   Duration: ", round(elapsed_time, 2), " seconds")
  message("   Exit status: ", ifelse(is.null(exit_status), "N/A", exit_status))
  message("   Warnings: ", warning_count)
  message("   Errors detected: ", error_detected)
  message("   Log file: ", log_file)
  
  # Return results
  invisible(list(
    script_path = script_path,
    log_file = log_file,
    duration = elapsed_time,
    exit_status = exit_status,
    output = all_output,
    errors = all_errors,
    error_detected = error_detected,
    warning_count = warning_count,
    process = p
  ))
}

#' Quick Monitor with Default Settings
#'
#' Convenience wrapper for monitoring with 1-second intervals
#'
#' @param script_path Path to R script
#' @return Monitoring results
monitor_etl <- function(script_path) {
  monitor_r_script(
    script_path = script_path,
    monitor_interval = 1,
    stop_on_error = TRUE,
    verbose = TRUE
  )
}