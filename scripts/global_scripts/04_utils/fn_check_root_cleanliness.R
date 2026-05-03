#' Check Root Directory Cleanliness
#'
#' Validates that the MAMBA project root directory contains only allowed files
#' and reports any violations. This function enforces SO_R033, SO_R034, and SO_R035
#' principles regarding file organization.
#'
#' Following principles:
#' - SO_R033: Test Script Location Standard
#' - SO_R034: Debug Script Management and Archival
#' - SO_R035: Temporary File and Log Management
#' - R021: One function one file rule
#' - R069: Function file naming (fn_ prefix)
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - P076: Error handling patterns
#'
#' @param root_dir Character: Path to MAMBA project root (default: auto-detect)
#' @param verbose Logical: If TRUE, print detailed messages (default: FALSE)
#'
#' @return List containing:
#'   - is_clean: Logical indicating if root is clean
#'   - violations: Data.frame of files that should not be in root
#'   - allowed_files: Character vector of allowed file patterns
#'   - summary: Named list with violation counts by category
#'
#' @export
#'
#' @examples
#' # Check root cleanliness
#' result <- fn_check_root_cleanliness()
#' if (!result$is_clean) {
#'   print(result$violations)
#' }
#'
#' # Verbose output
#' fn_check_root_cleanliness(verbose = TRUE)
#'
fn_check_root_cleanliness <- function(root_dir = NULL, verbose = FALSE) {

  # Following P076: Input validation
  if (!is.logical(verbose)) {
    stop("verbose must be logical (TRUE/FALSE)")
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

  # Define allowed file patterns in root (per SO_R033, SO_R034, SO_R035)
  allowed_patterns <- list(
    # Core application files
    core = c(
      "^app\\.R$",
      "^app_config\\.yaml$",
      "^manifest\\.json$",
      "^\\.Rprofile$",
      "^renv\\.lock$",
      "^\\.renvignore$",
      "^MAMBA\\.Rproj$",
      "^MAMBA\\.code-workspace$"
    ),
    # Git and version control
    git = c(
      "^\\.git$",
      "^\\.gitignore$",
      "^\\.gitattributes$",
      "^\\.gitmodules$"
    ),
    # Documentation
    docs = c(
      "^README\\.md$",
      "^CHANGELOG\\.md$",
      "^LICENSE(\\.md)?$"
    ),
    # Environment and config
    env = c(
      "^\\.env$",
      "^\\.env\\.example$",
      "^\\.env\\.template$"
    ),
    # Standard directories
    dirs = c(
      "^scripts$",
      "^data$",
      "^docs$",
      "^logs$",
      "^tests$",
      "^archive$",
      "^www$",
      "^output$",
      "^validation$",
      "^metadata$",
      "^app_data$"
    ),
    # Hidden directories and files
    hidden_dirs = c(
      "^\\.Rproj\\.user$",
      "^\\.claude$",
      "^\\.playwright-mcp$",
      "^\\.DS_Store$"
    ),
    # R workspace files (standard R development files, allowed in root)
    r_workspace = c(
      "^\\.RData$",
      "^\\.Rhistory$",
      "^\\.Renviron$"
    ),
    # Temporary allowed - cleanup status docs
    temp_allowed = c(
      "^ROOT_CLEANUP_SUMMARY_\\d{8}\\.md$",
      "^DIRECTORY_STRUCTURE_\\d{8}\\.md$"
    )
  )

  # Define violation patterns (files that should NOT be in root)
  violation_patterns <- list(
    # Test and validation scripts (SO_R033)
    test_scripts = list(
      pattern = "^(test_|validate_|verify_|check_).*\\.R$",
      category = "test_script",
      principle = "SO_R033",
      destination = "scripts/global_scripts/98_test/"
    ),
    # Debug scripts (SO_R034)
    debug_scripts = list(
      pattern = "^debug_.*\\.R$",
      category = "debug_script",
      principle = "SO_R034",
      destination = "ISSUE_TRACKER/archive/debugging/"
    ),
    # Log files (SO_R035)
    log_files = list(
      pattern = "\\.log$",
      category = "log_file",
      principle = "SO_R035",
      destination = "logs/active/"
    ),
    # Temporary RDS files (SO_R035)
    temp_rds = list(
      pattern = "^(temp_|cache_|scratch_).*\\.rds$",
      category = "temp_data",
      principle = "SO_R035",
      destination = "data/temp/"
    ),
    # Test result RDS files (SO_R033)
    test_rds = list(
      pattern = "^test_.*\\.rds$",
      category = "test_data",
      principle = "SO_R033",
      destination = "scripts/global_scripts/98_test/"
    ),
    # Backup files (SO_R031)
    backup_files = list(
      pattern = "(\\.backup|\\.bak|_backup|~)$",
      category = "backup_file",
      principle = "SO_R031",
      destination = "data/backups/archive/"
    ),
    # Report documentation files (SO_R030, SO_R032)
    report_docs = list(
      pattern = "(REPORT|DEBUG|FIX|IMPLEMENTATION|VERIFICATION|VALIDATION|SUMMARY|GUIDE).*\\.md$",
      category = "report_doc",
      principle = "SO_R030",
      destination = "docs/reports/ or CHANGELOG/"
    ),
    # Execution scripts in root
    execute_scripts = list(
      pattern = "^execute_.*\\.R$",
      category = "execute_script",
      principle = "SO_R025",
      destination = "scripts/update_scripts/"
    ),
    # Rollback logs
    rollback_logs = list(
      pattern = "^rollback_\\d{8}_\\d{6}\\.log$",
      category = "rollback_log",
      principle = "SO_R035",
      destination = "logs/archive/"
    ),
    # Phase output logs
    phase_logs = list(
      pattern = "^phase\\d+_.*\\.log$",
      category = "phase_log",
      principle = "SO_R035",
      destination = "logs/active/etl/"
    )
  )

  # Get all items in root (files and directories)
  root_items <- list.files(root_dir, all.files = TRUE, no.. = TRUE)

  if (verbose) {
    message(sprintf("Checking root directory: %s", root_dir))
    message(sprintf("Found %d items in root", length(root_items)))
  }

  # Build combined allowed pattern
  all_allowed_patterns <- unlist(allowed_patterns)

  # Identify violations
  violations <- data.frame(
    file = character(),
    category = character(),
    principle = character(),
    destination = character(),
    is_directory = logical(),
    size_bytes = numeric(),
    modified = character(),
    stringsAsFactors = FALSE
  )

  for (item in root_items) {
    item_path <- file.path(root_dir, item)

    # Check if item matches any allowed pattern
    is_allowed <- any(sapply(all_allowed_patterns, function(p) grepl(p, item)))

    if (!is_allowed) {
      # Find which violation pattern it matches
      matched_category <- "unknown"
      matched_principle <- "N/A"
      matched_destination <- "TBD"

      for (v in violation_patterns) {
        if (grepl(v$pattern, item, ignore.case = TRUE)) {
          matched_category <- v$category
          matched_principle <- v$principle
          matched_destination <- v$destination
          break
        }
      }

      # Get file info
      info <- file.info(item_path)

      violations <- rbind(violations, data.frame(
        file = item,
        category = matched_category,
        principle = matched_principle,
        destination = matched_destination,
        is_directory = info$isdir,
        size_bytes = ifelse(is.na(info$size), 0, info$size),
        modified = as.character(info$mtime),
        stringsAsFactors = FALSE
      ))
    }
  }

  # Generate summary
  summary_stats <- list(
    total_items = length(root_items),
    violations = nrow(violations),
    is_clean = nrow(violations) == 0,
    by_category = if (nrow(violations) > 0) {
      as.list(table(violations$category))
    } else {
      list()
    },
    by_principle = if (nrow(violations) > 0) {
      as.list(table(violations$principle))
    } else {
      list()
    },
    total_violation_size_mb = round(sum(violations$size_bytes, na.rm = TRUE) / 1024 / 1024, 2)
  )

  if (verbose) {
    message("---")
    if (summary_stats$is_clean) {
      message("Root directory is CLEAN - no violations found")
    } else {
      message(sprintf("Found %d violations:", summary_stats$violations))
      for (cat in names(summary_stats$by_category)) {
        message(sprintf("  - %s: %d files", cat, summary_stats$by_category[[cat]]))
      }
      message("")
      message("Violations:")
      for (i in seq_len(min(10, nrow(violations)))) {
        v <- violations[i, ]
        message(sprintf("  [%s] %s -> %s", v$principle, v$file, v$destination))
      }
      if (nrow(violations) > 10) {
        message(sprintf("  ... and %d more", nrow(violations) - 10))
      }
    }
  }

  # Return result
  list(
    is_clean = summary_stats$is_clean,
    violations = violations,
    allowed_patterns = all_allowed_patterns,
    summary = summary_stats,
    root_dir = root_dir
  )
}


#' Get Root Violation Summary
#'
#' Returns a formatted summary of root directory violations suitable for display.
#'
#' @param check_result Result from fn_check_root_cleanliness()
#'
#' @return Character string with formatted summary
#' @export
#'
fn_format_root_check_summary <- function(check_result) {
  if (check_result$is_clean) {
    return("Root directory is CLEAN - no violations detected")
  }

  lines <- c(
    sprintf("ROOT DIRECTORY CLEANLINESS CHECK - %d VIOLATIONS FOUND", check_result$summary$violations),
    paste(rep("=", 60), collapse = ""),
    ""
  )

  # Group by category
  for (cat in names(check_result$summary$by_category)) {
    count <- check_result$summary$by_category[[cat]]
    cat_violations <- check_result$violations[check_result$violations$category == cat, ]

    lines <- c(lines, sprintf("## %s (%d files)", toupper(gsub("_", " ", cat)), count))

    for (i in seq_len(nrow(cat_violations))) {
      v <- cat_violations[i, ]
      lines <- c(lines, sprintf("  - %s [%s]", v$file, v$principle))
      lines <- c(lines, sprintf("    -> %s", v$destination))
    }
    lines <- c(lines, "")
  }

  lines <- c(lines,
    paste(rep("-", 60), collapse = ""),
    sprintf("Total violations: %d", check_result$summary$violations),
    sprintf("Total size: %.2f MB", check_result$summary$total_violation_size_mb)
  )

  paste(lines, collapse = "\n")
}


#' Find MAMBA Root for Cleanup
#'
#' Helper function to find MAMBA project root directory.
#' Shared between cleanup functions.
#'
#' @return Character: Path to MAMBA root, or NULL if not found
#'
find_mamba_root_for_cleanup <- function() {
  # Check if already defined in calling context
  if (exists("find_mamba_root_for_cleanup_internal", mode = "function")) {
    return(find_mamba_root_for_cleanup_internal())
  }

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
