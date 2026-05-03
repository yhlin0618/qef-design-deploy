#' Cleanup Backup Files
#'
#' Manages backup files by moving old backups to an archive directory and
#' optionally deleting very old backups. Follows SO_R031 (Backup File Management)
#' principle.
#'
#' Following principles:
#' - SO_R031: Backup File Management
#' - R021: One function one file rule
#' - R069: Function file naming (fn_ prefix)
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - P076: Error handling patterns
#' - MP029: No fake data (logs real actions only)
#'
#' @param search_dir Character: Directory to search for backup files (default: MAMBA root)
#' @param days_to_keep Numeric: Days to keep backups in original location (default: 7)
#' @param archive_days Numeric: Days to keep in archive before deletion (default: 30)
#' @param archive_dir Character: Path to archive directory (default: data/backups/archive)
#' @param backup_patterns Character vector: Patterns to match backup files
#'   (default: c(".backup_", ".bak", "_backup", "~"))
#' @param dry_run Logical: If TRUE, only report what would be done without making changes (default: TRUE)
#' @param recursive Logical: Search recursively in subdirectories (default: TRUE)
#' @param exclude_dirs Character vector: Directory names to exclude from search
#' @param log_file Character: Path to log file (default: NULL, logs to console only)
#'
#' @return Data.frame: Summary of actions taken or planned
#' @export
#'
#' @examples
#' # Dry run to see what would be cleaned up
#' fn_cleanup_backups(dry_run = TRUE)
#'
#' # Actually clean up with 7 day retention
#' fn_cleanup_backups(days_to_keep = 7, dry_run = FALSE)
#'
#' # Custom search directory
#' fn_cleanup_backups(search_dir = "/path/to/project", dry_run = TRUE)
#'
fn_cleanup_backups <- function(search_dir = NULL,
                               days_to_keep = 7,
                               archive_days = 30,
                               archive_dir = NULL,
                               backup_patterns = c("\\.backup_", "\\.bak$", "_backup\\.", "~$"),
                               dry_run = TRUE,
                               recursive = TRUE,
                               exclude_dirs = c(".git", "node_modules", "venv", ".Rproj.user"),
                               log_file = NULL) {

  # Following P076: Input validation
  if (!is.numeric(days_to_keep) || days_to_keep < 0) {
    stop("days_to_keep must be a non-negative number")
  }

  if (!is.numeric(archive_days) || archive_days < 0) {
    stop("archive_days must be a non-negative number")
  }

  if (!is.logical(dry_run)) {
    stop("dry_run must be logical (TRUE/FALSE)")
  }

  # Find MAMBA root if search_dir not specified
  if (is.null(search_dir)) {
    search_dir <- find_mamba_root_for_cleanup()
    if (is.null(search_dir)) {
      stop("Could not find MAMBA project root. Please specify search_dir explicitly.")
    }
  }

  if (!dir.exists(search_dir)) {
    stop(sprintf("Search directory does not exist: %s", search_dir))
  }

  # Set default archive directory
  if (is.null(archive_dir)) {
    archive_dir <- file.path(search_dir, "data", "backups", "archive")
  }

  # Initialize log function
  log_message <- function(msg, level = "INFO") {
    timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    full_msg <- sprintf("[%s] [%s] %s", timestamp, level, msg)
    message(full_msg)

    if (!is.null(log_file)) {
      tryCatch({
        cat(full_msg, "\n", file = log_file, append = TRUE)
      }, error = function(e) {
        warning(sprintf("Could not write to log file: %s", e$message))
      })
    }
  }

  # Start cleanup process
  log_message("Starting backup cleanup process")
  log_message(sprintf("Search directory: %s", search_dir))
  log_message(sprintf("Days to keep: %d", days_to_keep))
  log_message(sprintf("Archive days: %d", archive_days))
  log_message(sprintf("Dry run: %s", dry_run))

  # Build regex pattern from backup_patterns
  pattern <- paste(backup_patterns, collapse = "|")

  # Find all files matching backup patterns
  all_files <- list.files(
    search_dir,
    pattern = pattern,
    full.names = TRUE,
    recursive = recursive,
    include.dirs = FALSE
  )

  # Filter out excluded directories
  if (length(exclude_dirs) > 0) {
    exclude_pattern <- paste(exclude_dirs, collapse = "|")
    all_files <- all_files[!grepl(exclude_pattern, all_files)]
  }

  log_message(sprintf("Found %d backup files", length(all_files)))

  if (length(all_files) == 0) {
    log_message("No backup files found. Nothing to do.")
    return(data.frame(
      file = character(),
      action = character(),
      reason = character(),
      status = character(),
      stringsAsFactors = FALSE
    ))
  }

  # Get file information
  file_info <- file.info(all_files)
  file_info$path <- rownames(file_info)
  file_info$age_days <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))
  file_info$filename <- basename(file_info$path)

  # Categorize files by age
  current_date <- Sys.Date()
  keep_cutoff <- as.numeric(days_to_keep)
  archive_cutoff <- as.numeric(archive_days)

  # Initialize results
  results <- data.frame(
    file = character(),
    action = character(),
    reason = character(),
    status = character(),
    age_days = numeric(),
    size_bytes = numeric(),
    stringsAsFactors = FALSE
  )

  # Create archive directory if needed (and not dry run)
  if (!dry_run && !dir.exists(archive_dir)) {
    log_message(sprintf("Creating archive directory: %s", archive_dir))
    dir.create(archive_dir, recursive = TRUE)
  }

  # Process each file
  for (i in seq_len(nrow(file_info))) {
    f <- file_info[i, ]
    action <- "keep"
    reason <- ""
    status <- "pending"

    if (f$age_days > archive_cutoff) {
      # Delete very old backups
      action <- "delete"
      reason <- sprintf("Age (%.1f days) exceeds archive limit (%d days)", f$age_days, archive_cutoff)

      if (!dry_run) {
        tryCatch({
          file.remove(f$path)
          status <- "completed"
          log_message(sprintf("Deleted: %s", f$path))
        }, error = function(e) {
          status <- "failed"
          log_message(sprintf("Failed to delete %s: %s", f$path, e$message), "ERROR")
        })
      } else {
        status <- "dry_run"
      }

    } else if (f$age_days > keep_cutoff) {
      # Move to archive
      action <- "archive"
      reason <- sprintf("Age (%.1f days) exceeds keep limit (%d days)", f$age_days, keep_cutoff)

      if (!dry_run) {
        archive_path <- file.path(archive_dir, f$filename)

        # Handle duplicate filenames in archive
        if (file.exists(archive_path)) {
          base_name <- tools::file_path_sans_ext(f$filename)
          ext <- tools::file_ext(f$filename)
          timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
          archive_path <- file.path(archive_dir, sprintf("%s_%s.%s", base_name, timestamp, ext))
        }

        tryCatch({
          file.copy(f$path, archive_path)
          file.remove(f$path)
          status <- "completed"
          log_message(sprintf("Archived: %s -> %s", f$path, archive_path))
        }, error = function(e) {
          status <- "failed"
          log_message(sprintf("Failed to archive %s: %s", f$path, e$message), "ERROR")
        })
      } else {
        status <- "dry_run"
      }

    } else {
      # Keep the file
      action <- "keep"
      reason <- sprintf("Age (%.1f days) within keep limit (%d days)", f$age_days, keep_cutoff)
      status <- "skipped"
    }

    results <- rbind(results, data.frame(
      file = f$path,
      action = action,
      reason = reason,
      status = status,
      age_days = round(f$age_days, 1),
      size_bytes = f$size,
      stringsAsFactors = FALSE
    ))
  }

  # Generate summary
  summary_stats <- list(
    total_files = nrow(results),
    kept = sum(results$action == "keep"),
    archived = sum(results$action == "archive"),
    deleted = sum(results$action == "delete"),
    completed = sum(results$status == "completed"),
    failed = sum(results$status == "failed"),
    total_size_bytes = sum(results$size_bytes, na.rm = TRUE)
  )

  log_message("Cleanup Summary:")
  log_message(sprintf("  Total files processed: %d", summary_stats$total_files))
  log_message(sprintf("  Files kept: %d", summary_stats$kept))
  log_message(sprintf("  Files archived: %d", summary_stats$archived))
  log_message(sprintf("  Files deleted: %d", summary_stats$deleted))

  if (!dry_run) {
    log_message(sprintf("  Actions completed: %d", summary_stats$completed))
    log_message(sprintf("  Actions failed: %d", summary_stats$failed))
  } else {
    log_message("  (Dry run - no changes made)")
  }

  log_message(sprintf("  Total size: %.2f MB", summary_stats$total_size_bytes / 1024 / 1024))

  # Add summary as attribute
  attr(results, "summary") <- summary_stats

  return(results)
}


#' Find MAMBA Root for Cleanup
#'
#' Helper function to find MAMBA project root directory.
#'
#' @return Character: Path to MAMBA root, or NULL if not found
#'
find_mamba_root_for_cleanup <- function() {
  current_dir <- getwd()
  max_depth <- 10

  for (i in seq_len(max_depth)) {
    markers <- c(
      file.path(current_dir, "scripts", "global_scripts"),
      file.path(current_dir, "app_config.yaml")
    )

    if (any(file.exists(markers)) || any(dir.exists(markers))) {
      return(current_dir)
    }

    parent_dir <- dirname(current_dir)
    if (parent_dir == current_dir) break
    current_dir <- parent_dir
  }

  # Try environment variable
  mamba_root <- Sys.getenv("MAMBA_ROOT", unset = NA)
  if (!is.na(mamba_root) && dir.exists(mamba_root)) {
    return(mamba_root)
  }

  return(NULL)
}


#' Get Backup Statistics
#'
#' Returns statistics about backup files in the project.
#'
#' @param search_dir Character: Directory to search
#' @param backup_patterns Character vector: Patterns to match backup files
#'
#' @return List: Statistics about backup files
#' @export
#'
#' @examples
#' stats <- fn_get_backup_stats()
#' print(stats)
#'
fn_get_backup_stats <- function(search_dir = NULL,
                                backup_patterns = c("\\.backup_", "\\.bak$", "_backup\\.", "~$")) {

  if (is.null(search_dir)) {
    search_dir <- find_mamba_root_for_cleanup()
    if (is.null(search_dir)) {
      stop("Could not find MAMBA project root. Please specify search_dir explicitly.")
    }
  }

  pattern <- paste(backup_patterns, collapse = "|")

  all_files <- list.files(
    search_dir,
    pattern = pattern,
    full.names = TRUE,
    recursive = TRUE,
    include.dirs = FALSE
  )

  if (length(all_files) == 0) {
    return(list(
      total_files = 0,
      total_size_bytes = 0,
      total_size_mb = 0,
      oldest_file = NA,
      newest_file = NA,
      age_distribution = NULL
    ))
  }

  file_info <- file.info(all_files)
  file_info$path <- rownames(file_info)
  file_info$age_days <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))

  # Age distribution
  age_breaks <- c(0, 1, 7, 14, 30, 60, 90, Inf)
  age_labels <- c("0-1 days", "1-7 days", "7-14 days", "14-30 days", "30-60 days", "60-90 days", "90+ days")
  age_dist <- table(cut(file_info$age_days, breaks = age_breaks, labels = age_labels, right = FALSE))

  list(
    total_files = nrow(file_info),
    total_size_bytes = sum(file_info$size, na.rm = TRUE),
    total_size_mb = round(sum(file_info$size, na.rm = TRUE) / 1024 / 1024, 2),
    oldest_file = file_info$path[which.max(file_info$age_days)],
    oldest_age_days = round(max(file_info$age_days), 1),
    newest_file = file_info$path[which.min(file_info$age_days)],
    newest_age_days = round(min(file_info$age_days), 1),
    age_distribution = as.list(age_dist)
  )
}


#' Cleanup Root Directory Backup Files
#'
#' Specifically targets backup files in the project root directory and moves them
#' to the backup archive. This is a convenience wrapper that focuses on root-level
#' cleanup in compliance with SO_R031 and SO_R035.
#'
#' Following principles:
#' - SO_R031: Backup File Management
#' - SO_R035: Temporary File and Log Management
#' - R021: One function one file rule
#' - MP047: Functional programming
#'
#' @param root_dir Character: Path to MAMBA project root (default: auto-detect)
#' @param dry_run Logical: If TRUE, only report what would be done (default: TRUE)
#' @param verbose Logical: If TRUE, print detailed messages (default: TRUE)
#'
#' @return Data.frame: Summary of actions taken or planned
#' @export
#'
#' @examples
#' # Dry run to see what would be cleaned
#' fn_cleanup_root_backups(dry_run = TRUE)
#'
#' # Actually clean up root backups
#' fn_cleanup_root_backups(dry_run = FALSE)
#'
fn_cleanup_root_backups <- function(root_dir = NULL,
                                     dry_run = TRUE,
                                     verbose = TRUE) {

  # Following P076: Input validation
  if (!is.logical(dry_run)) {
    stop("dry_run must be logical (TRUE/FALSE)")
  }

  # Find MAMBA root if not specified
  if (is.null(root_dir)) {
    root_dir <- find_mamba_root_for_cleanup()
    if (is.null(root_dir)) {
      stop("Could not find MAMBA project root. Please specify root_dir explicitly.")
    }
  }

  if (!dir.exists(root_dir)) {
    stop(sprintf("Root directory does not exist: %s", root_dir))
  }

  # Log function
  log_message <- function(msg, level = "INFO") {
    if (verbose) {
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      message(sprintf("[%s] [%s] %s", timestamp, level, msg))
    }
  }

  log_message("Starting root directory backup cleanup")
  log_message(sprintf("Root directory: %s", root_dir))
  log_message(sprintf("Dry run: %s", dry_run))

  # Define backup patterns
  backup_patterns <- c(
    "\\.backup_",      # .backup_* pattern
    "\\.bak$",         # .bak extension
    "_backup\\.",      # _backup. pattern
    "~$"               # backup files ending with ~
  )

  pattern <- paste(backup_patterns, collapse = "|")

  # Find backup files in root only (not recursive)
  root_files <- list.files(root_dir, all.files = TRUE, no.. = TRUE)
  root_files <- root_files[!file.info(file.path(root_dir, root_files))$isdir]

  # Filter to backup files only
  backup_files <- root_files[grepl(pattern, root_files)]

  log_message(sprintf("Found %d backup files in root", length(backup_files)))

  if (length(backup_files) == 0) {
    log_message("No backup files found in root. Nothing to do.")
    return(data.frame(
      file = character(),
      action = character(),
      source = character(),
      destination = character(),
      status = character(),
      stringsAsFactors = FALSE
    ))
  }

  # Set archive directory
  archive_dir <- file.path(root_dir, "data", "backups", "archive")

  # Initialize results
  results <- data.frame(
    file = character(),
    action = character(),
    source = character(),
    destination = character(),
    status = character(),
    age_days = numeric(),
    size_bytes = numeric(),
    stringsAsFactors = FALSE
  )

  # Create archive directory if needed (and not dry run)
  if (!dry_run && !dir.exists(archive_dir)) {
    log_message(sprintf("Creating archive directory: %s", archive_dir))
    dir.create(archive_dir, recursive = TRUE)
  }

  # Process each backup file
  for (file in backup_files) {
    source_path <- file.path(root_dir, file)
    dest_path <- file.path(archive_dir, file)

    # Get file info
    info <- file.info(source_path)
    age_days <- as.numeric(difftime(Sys.time(), info$mtime, units = "days"))

    # Handle duplicate filenames
    if (file.exists(dest_path)) {
      base_name <- tools::file_path_sans_ext(file)
      ext <- tools::file_ext(file)
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      new_name <- if (ext != "") {
        sprintf("%s_%s.%s", base_name, timestamp, ext)
      } else {
        sprintf("%s_%s", base_name, timestamp)
      }
      dest_path <- file.path(archive_dir, new_name)
    }

    status <- "pending"

    if (!dry_run) {
      tryCatch({
        file.copy(source_path, dest_path)
        file.remove(source_path)
        status <- "completed"
        log_message(sprintf("Archived: %s -> %s", file, basename(dest_path)))
      }, error = function(e) {
        status <<- "failed"
        log_message(sprintf("Failed to archive %s: %s", file, e$message), "ERROR")
      })
    } else {
      status <- "dry_run"
      log_message(sprintf("[DRY RUN] Would archive: %s -> %s", file, basename(dest_path)))
    }

    results <- rbind(results, data.frame(
      file = file,
      action = "archive",
      source = source_path,
      destination = dest_path,
      status = status,
      age_days = round(age_days, 1),
      size_bytes = info$size,
      stringsAsFactors = FALSE
    ))
  }

  # Generate summary
  summary_stats <- list(
    total_files = nrow(results),
    completed = sum(results$status == "completed"),
    failed = sum(results$status == "failed"),
    dry_run = sum(results$status == "dry_run"),
    total_size_bytes = sum(results$size_bytes, na.rm = TRUE)
  )

  log_message("---")
  log_message("Root Backup Cleanup Summary:")
  log_message(sprintf("  Total files: %d", summary_stats$total_files))

  if (!dry_run) {
    log_message(sprintf("  Completed: %d", summary_stats$completed))
    log_message(sprintf("  Failed: %d", summary_stats$failed))
  } else {
    log_message("  (Dry run - no changes made)")
  }

  log_message(sprintf("  Total size: %.2f MB", summary_stats$total_size_bytes / 1024 / 1024))

  attr(results, "summary") <- summary_stats

  return(results)
}


#' Full Root Directory Cleanup
#'
#' Comprehensive cleanup of the project root directory, combining backup cleanup
#' with archive of all stray files. This function wraps both fn_cleanup_root_backups
#' and fn_archive_root_files for complete root maintenance.
#'
#' Following principles:
#' - SO_R031: Backup File Management
#' - SO_R033: Test Script Location Standard
#' - SO_R034: Debug Script Management
#' - SO_R035: Temporary File and Log Management
#'
#' @param root_dir Character: Path to MAMBA project root (default: auto-detect)
#' @param dry_run Logical: If TRUE, only report what would be done (default: TRUE)
#' @param verbose Logical: If TRUE, print detailed messages (default: TRUE)
#'
#' @return List containing results from both cleanup operations
#' @export
#'
#' @examples
#' # Dry run to see full cleanup plan
#' result <- fn_full_root_cleanup(dry_run = TRUE)
#'
#' # Execute full cleanup
#' result <- fn_full_root_cleanup(dry_run = FALSE)
#'
fn_full_root_cleanup <- function(root_dir = NULL,
                                  dry_run = TRUE,
                                  verbose = TRUE) {

  # Find MAMBA root if not specified
  if (is.null(root_dir)) {
    root_dir <- find_mamba_root_for_cleanup()
    if (is.null(root_dir)) {
      stop("Could not find MAMBA project root. Please specify root_dir explicitly.")
    }
  }

  # Log function
  log_message <- function(msg, level = "INFO") {
    if (verbose) {
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      message(sprintf("[%s] [%s] %s", timestamp, level, msg))
    }
  }

  log_message("=== Starting Full Root Directory Cleanup ===")
  log_message(sprintf("Root directory: %s", root_dir))
  log_message(sprintf("Dry run: %s", dry_run))

  # Step 1: Cleanup backup files
  log_message("")
  log_message("--- Step 1: Backup Files Cleanup ---")
  backup_results <- fn_cleanup_root_backups(
    root_dir = root_dir,
    dry_run = dry_run,
    verbose = verbose
  )

  # Step 2: Archive other stray files
  # Source the archive function if not already loaded
  archive_func_path <- file.path(dirname(sys.frame(1)$ofile %||% "."),
                                  "fn_archive_root_files.R")
  if (file.exists(archive_func_path)) {
    source(archive_func_path)

    log_message("")
    log_message("--- Step 2: Stray Files Archival ---")
    archive_results <- fn_archive_root_files(
      root_dir = root_dir,
      dry_run = dry_run,
      verbose = verbose,
      create_readme = TRUE
    )
  } else {
    log_message("Archive function not found, skipping stray file archival")
    archive_results <- NULL
  }

  # Combined summary
  log_message("")
  log_message("=== Full Cleanup Summary ===")

  backup_summary <- attr(backup_results, "summary")
  log_message(sprintf("Backup files processed: %d", backup_summary$total_files))

  if (!is.null(archive_results)) {
    archive_summary <- attr(archive_results, "summary")
    log_message(sprintf("Stray files processed: %d", archive_summary$total_processed))
  }

  if (dry_run) {
    log_message("(Dry run - no changes made)")
    log_message("Run with dry_run = FALSE to execute cleanup")
  }

  list(
    backup_cleanup = backup_results,
    archive_cleanup = archive_results,
    root_dir = root_dir,
    dry_run = dry_run
  )
}


#' Null coalescing operator (if not defined)
#'
#' @noRd
if (!exists("%||%", mode = "function")) {
  `%||%` <- function(x, y) {
    if (is.null(x)) y else x
  }
}
