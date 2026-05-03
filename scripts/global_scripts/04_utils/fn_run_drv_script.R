#' MAMBA DRV Pipeline - Wrapper Functions for {targets}
#' Principle: DEV_R016 (Evolution Over Replacement)
#'
#' Phase 1 implementation: Wrap existing DRV scripts without modification
#' This allows gradual adoption of {targets} pipeline management

#' Execute a DRV script and return log file path for targets tracking
#'
#' @param script_path Path to the DRV script (relative to DRV directory)
#' @return Path to log file (used by targets for change detection)
#' @examples
#' run_drv_script("cbz/cbz_DRV_product_line_poisson.R")
#' run_drv_script("update_all_platforms_poisson.R")
run_drv_script <- function(script_path) {
  # Create log directory if needed
  log_dir <- file.path(tempdir(), "mamba_drv_logs")
  if (!dir.exists(log_dir)) {
    dir.create(log_dir, recursive = TRUE)
  }

  # Generate timestamped log file
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  script_name <- gsub("/", "_", basename(script_path))
  log_file <- file.path(log_dir, paste0(script_name, "_", timestamp, ".log"))

  # Log start
  cat(sprintf("[%s] Starting: %s\n", Sys.time(), script_path))

  # Execute script
  result <- system2(
    command = "Rscript",
    args = script_path,
    stdout = log_file,
    stderr = log_file
  )

  # Check result
  if (result != 0) {
    # Read last 50 lines of log for error context
    if (file.exists(log_file)) {
      log_content <- readLines(log_file, warn = FALSE)
      tail_lines <- tail(log_content, 50)
      cat("\n=== Error Log (last 50 lines) ===\n")
      cat(paste(tail_lines, collapse = "\n"))
      cat("\n=================================\n")
    }
    stop(sprintf("DRV script failed: %s\nExit code: %d\nFull log: %s",
                 script_path, result, log_file))
  }

  cat(sprintf("[%s] Completed: %s\n", Sys.time(), script_path))
  cat(sprintf("Log saved: %s\n", log_file))

  # Return log file path for targets tracking
  log_file
}
