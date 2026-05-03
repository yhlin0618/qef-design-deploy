#' Archive Stray Files from Root Directory
#'
#' Automatically archives or moves files from the project root directory to their
#' appropriate locations based on file type and naming patterns. Enforces SO_R033,
#' SO_R034, and SO_R035 principles regarding file organization.
#'
#' Following principles:
#' - SO_R033: Test Script Location Standard
#' - SO_R034: Debug Script Management and Archival
#' - SO_R035: Temporary File and Log Management
#' - SO_R031: Backup File Management
#' - SO_R030: Operational Documentation Location
#' - R021: One function one file rule
#' - R069: Function file naming (fn_ prefix)
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - P076: Error handling patterns
#' - MP029: No fake data (logs real actions only)
#'
#' @param root_dir Character: Path to MAMBA project root (default: auto-detect)
#' @param dry_run Logical: If TRUE, only report what would be done (default: TRUE)
#' @param verbose Logical: If TRUE, print detailed messages (default: TRUE)
#' @param create_readme Logical: If TRUE, create README in archive dirs (default: TRUE)
#'
#' @return Data.frame: Summary of actions taken or planned
#'
#' @export
#'
#' @examples
#' # Dry run to see what would be archived
#' result <- fn_archive_root_files(dry_run = TRUE)
#' print(result)
#'
#' # Actually archive files
#' result <- fn_archive_root_files(dry_run = FALSE)
#'
fn_archive_root_files <- function(root_dir = NULL,
                                   dry_run = TRUE,
                                   verbose = TRUE,
                                   create_readme = TRUE) {

  # Following P076: Input validation
  if (!is.logical(dry_run)) {
    stop("dry_run must be logical (TRUE/FALSE)")
  }

  if (!is.logical(verbose)) {
    stop("verbose must be logical (TRUE/FALSE)")
  }

  # Source the check function to get shared helper
  check_script_path <- file.path(
    dirname(sys.frame(1)$ofile %||% "."),
    "fn_check_root_cleanliness.R"
  )

  # Find MAMBA root if not specified
  if (is.null(root_dir)) {
    root_dir <- find_mamba_root_for_archive()
    if (is.null(root_dir)) {
      stop("Could not find MAMBA project root. Please specify root_dir explicitly.")
    }
  }

  if (!dir.exists(root_dir)) {
    stop(sprintf("Root directory does not exist: %s", root_dir))
  }

  # Define archive rules (file pattern -> destination)
  archive_rules <- list(
    # Debug scripts (SO_R034)
    list(
      name = "debug_scripts",
      pattern = "^debug_.*\\.R$",
      destination = file.path(root_dir, "scripts/global_scripts/00_principles/ISSUE_TRACKER/archive/debugging",
                             format(Sys.Date(), "%Y%m%d_root_cleanup")),
      action = "archive",
      principle = "SO_R034"
    ),
    # Test scripts (SO_R033)
    list(
      name = "test_scripts",
      pattern = "^test_.*\\.R$",
      destination = file.path(root_dir, "scripts/global_scripts/98_test"),
      action = "move",
      principle = "SO_R033"
    ),
    # Check scripts (SO_R033)
    list(
      name = "check_scripts",
      pattern = "^check_.*\\.R$",
      destination = file.path(root_dir, "scripts/global_scripts/98_test"),
      action = "move",
      principle = "SO_R033"
    ),
    # Validate scripts (SO_R033)
    list(
      name = "validate_scripts",
      pattern = "^validate_.*\\.R$",
      destination = file.path(root_dir, "scripts/global_scripts/98_test"),
      action = "move",
      principle = "SO_R033"
    ),
    # Verify scripts (SO_R033)
    list(
      name = "verify_scripts",
      pattern = "^verify_.*\\.R$",
      destination = file.path(root_dir, "scripts/global_scripts/98_test"),
      action = "move",
      principle = "SO_R033"
    ),
    # Execute scripts
    list(
      name = "execute_scripts",
      pattern = "^execute_.*\\.R$",
      destination = file.path(root_dir, "scripts/update_scripts"),
      action = "move",
      principle = "SO_R025"
    ),
    # Log files (SO_R035)
    list(
      name = "log_files",
      pattern = "\\.log$",
      destination = file.path(root_dir, "logs/archive", format(Sys.Date(), "%Y-%m")),
      action = "archive",
      principle = "SO_R035"
    ),
    # Temporary RDS files (SO_R035)
    list(
      name = "temp_rds",
      pattern = "\\.rds$",
      destination = file.path(root_dir, "data/temp/archive"),
      action = "archive",
      principle = "SO_R035"
    ),
    # Backup files - .backup_* pattern (SO_R031)
    list(
      name = "backup_files",
      pattern = "\\.backup_",
      destination = file.path(root_dir, "data/backups/archive"),
      action = "archive",
      principle = "SO_R031"
    ),
    # Backup files - .bak pattern (SO_R031)
    list(
      name = "bak_files",
      pattern = "\\.bak$",
      destination = file.path(root_dir, "data/backups/archive"),
      action = "archive",
      principle = "SO_R031"
    ),
    # Report documentation (SO_R030)
    list(
      name = "report_docs",
      pattern = "^.*REPORT.*\\.md$",
      destination = file.path(root_dir, "docs/reports"),
      action = "move",
      principle = "SO_R030"
    ),
    # Debug documentation (SO_R030)
    list(
      name = "debug_docs",
      pattern = "^.*DEBUG.*\\.md$",
      destination = file.path(root_dir, "docs/reports/debug"),
      action = "move",
      principle = "SO_R030"
    ),
    # Fix documentation (SO_R030)
    list(
      name = "fix_docs",
      pattern = "^.*FIX.*\\.md$",
      destination = file.path(root_dir, "docs/reports/fixes"),
      action = "move",
      principle = "SO_R030"
    ),
    # Implementation documentation (SO_R030)
    list(
      name = "implementation_docs",
      pattern = "^.*IMPLEMENTATION.*\\.md$",
      destination = file.path(root_dir, "docs/reports/implementations"),
      action = "move",
      principle = "SO_R030"
    ),
    # Summary documentation (SO_R030)
    list(
      name = "summary_docs",
      pattern = "^.*SUMMARY.*\\.md$",
      destination = file.path(root_dir, "docs/reports"),
      action = "move",
      principle = "SO_R030"
    ),
    # Guide documentation (SO_R030)
    list(
      name = "guide_docs",
      pattern = "^.*GUIDE.*\\.md$",
      destination = file.path(root_dir, "docs/guides"),
      action = "move",
      principle = "SO_R030"
    ),
    # Verification documentation (SO_R030)
    list(
      name = "verification_docs",
      pattern = "^.*VERIFICATION.*\\.md$",
      destination = file.path(root_dir, "docs/reports/verification"),
      action = "move",
      principle = "SO_R030"
    ),
    # Validation documentation (SO_R030)
    list(
      name = "validation_docs",
      pattern = "^.*VALIDATION.*\\.md$",
      destination = file.path(root_dir, "validation"),
      action = "move",
      principle = "SO_R030"
    )
  )

  # Log function
  log_message <- function(msg, level = "INFO") {
    if (verbose) {
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      message(sprintf("[%s] [%s] %s", timestamp, level, msg))
    }
  }

  log_message("Starting root directory archive process")
  log_message(sprintf("Root directory: %s", root_dir))
  log_message(sprintf("Dry run: %s", dry_run))

  # Get all files in root (not directories)
  root_files <- list.files(root_dir, all.files = TRUE, no.. = TRUE)
  root_files <- root_files[!file.info(file.path(root_dir, root_files))$isdir]

  log_message(sprintf("Found %d files in root", length(root_files)))

  # Initialize results
  results <- data.frame(
    file = character(),
    action = character(),
    rule = character(),
    principle = character(),
    source = character(),
    destination = character(),
    status = character(),
    error = character(),
    stringsAsFactors = FALSE
  )

  # Directories created for README tracking
  dirs_created <- character()

  # Process each file
  for (file in root_files) {
    file_path <- file.path(root_dir, file)

    # Find matching rule
    matched_rule <- NULL
    for (rule in archive_rules) {
      if (grepl(rule$pattern, file, ignore.case = TRUE)) {
        matched_rule <- rule
        break
      }
    }

    if (!is.null(matched_rule)) {
      dest_dir <- matched_rule$destination
      dest_path <- file.path(dest_dir, file)

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
        dest_path <- file.path(dest_dir, new_name)
      }

      status <- "pending"
      error_msg <- ""

      if (!dry_run) {
        tryCatch({
          # Create destination directory if needed
          if (!dir.exists(dest_dir)) {
            dir.create(dest_dir, recursive = TRUE)
            log_message(sprintf("Created directory: %s", dest_dir))
            dirs_created <- c(dirs_created, dest_dir)
          }

          # Move/Archive the file
          if (matched_rule$action == "archive") {
            file.copy(file_path, dest_path)
            file.remove(file_path)
            log_message(sprintf("Archived: %s -> %s", file, basename(dest_path)))
          } else {
            file.rename(file_path, dest_path)
            log_message(sprintf("Moved: %s -> %s", file, basename(dest_path)))
          }

          status <- "completed"
        }, error = function(e) {
          status <<- "failed"
          error_msg <<- e$message
          log_message(sprintf("Failed to process %s: %s", file, e$message), "ERROR")
        })
      } else {
        status <- "dry_run"
        log_message(sprintf("[DRY RUN] Would %s: %s -> %s",
                           matched_rule$action, file, dest_path))
      }

      results <- rbind(results, data.frame(
        file = file,
        action = matched_rule$action,
        rule = matched_rule$name,
        principle = matched_rule$principle,
        source = file_path,
        destination = dest_path,
        status = status,
        error = error_msg,
        stringsAsFactors = FALSE
      ))
    }
  }

  # Create README files in archive directories if requested
  if (create_readme && !dry_run && length(dirs_created) > 0) {
    for (dir in unique(dirs_created)) {
      readme_path <- file.path(dir, "README.md")
      if (!file.exists(readme_path)) {
        readme_content <- sprintf(
"# Archive Directory

## Purpose
This directory contains files archived from the project root on %s.

## Contents
Files archived here were detected as violating MAMBA project organization principles:
- SO_R033: Test Script Location Standard
- SO_R034: Debug Script Management and Archival
- SO_R035: Temporary File and Log Management

## Files Archived
%s

## Retention Policy
Review and delete files older than 30 days if no longer needed.

## Created
%s by fn_archive_root_files()
",
          Sys.Date(),
          paste("-", list.files(dir, pattern = "^(?!README).*", perl = TRUE), collapse = "\n"),
          Sys.time()
        )
        writeLines(readme_content, readme_path)
        log_message(sprintf("Created README: %s", readme_path))
      }
    }
  }

  # Generate summary
  summary_stats <- list(
    total_processed = nrow(results),
    archived = sum(results$action == "archive"),
    moved = sum(results$action == "move"),
    completed = sum(results$status == "completed"),
    failed = sum(results$status == "failed"),
    dry_run = sum(results$status == "dry_run")
  )

  log_message("---")
  log_message("Archive Summary:")
  log_message(sprintf("  Total files processed: %d", summary_stats$total_processed))
  log_message(sprintf("  Files to archive: %d", summary_stats$archived))
  log_message(sprintf("  Files to move: %d", summary_stats$moved))

  if (!dry_run) {
    log_message(sprintf("  Actions completed: %d", summary_stats$completed))
    log_message(sprintf("  Actions failed: %d", summary_stats$failed))
  } else {
    log_message("  (Dry run - no changes made)")
  }

  attr(results, "summary") <- summary_stats
  attr(results, "root_dir") <- root_dir
  attr(results, "dry_run") <- dry_run

  return(results)
}


#' Find MAMBA Root for Archive
#'
#' Helper function to find MAMBA project root directory.
#'
#' @return Character: Path to MAMBA root, or NULL if not found
#'
find_mamba_root_for_archive <- function() {
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


#' Quick Archive Root Files
#'
#' Convenience wrapper for fn_archive_root_files with common defaults.
#'
#' @param execute Logical: If TRUE, execute archiving; if FALSE, dry run (default: FALSE)
#'
#' @return Data.frame: Summary of actions
#' @export
#'
#' @examples
#' # See what would be archived
#' fn_quick_archive_root()
#'
#' # Actually archive
#' fn_quick_archive_root(execute = TRUE)
#'
fn_quick_archive_root <- function(execute = FALSE) {
  fn_archive_root_files(
    dry_run = !execute,
    verbose = TRUE,
    create_readme = TRUE
  )
}


#' Null coalescing operator
#'
#' @noRd
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
