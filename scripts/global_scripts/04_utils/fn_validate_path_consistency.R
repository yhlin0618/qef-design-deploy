#' Validate Path Consistency Across MAMBA Project
#'
#' This function validates that all path configurations are consistent
#' and follow MAMBA principles, preventing nested path creation issues.
#'
#' @return logical TRUE if all paths are valid, FALSE otherwise
#' @export
#'
#' @details
#' Implements MAMBA principles:
#' - MP028: Avoid Self-Reference
#' - DM_R041: ETL Directory Structure Rules
#'
#' Created: 2025-09-29
#' Author: MAMBA Principle Debugger

validate_path_consistency <- function() {
  violations <- list()

  # Load path configuration
  root_path_file <- file.path("scripts", "update_scripts", "root_path.R")
  if (file.exists(root_path_file)) {
    source(root_path_file)
  } else {
    stop("root_path.R not found")
  }

  # Validation 1: APP_DIR should point to project root, not subdirectory
  if (grepl("/scripts/update_scripts$", APP_DIR)) {
    violations$app_dir_wrong <- paste(
      "APP_DIR incorrectly points to update_scripts subdirectory:",
      APP_DIR,
      "\nShould point to project root (MAMBA directory)"
    )
  }

  # Validation 2: Check for nested paths
  nested_patterns <- c(
    "update_scripts/update_scripts",
    "global_scripts/global_scripts",
    "scripts/scripts"
  )

  for (pattern in nested_patterns) {
    test_path <- file.path(APP_DIR, pattern)
    if (dir.exists(test_path)) {
      violations[[paste0("nested_", pattern)]] <- paste(
        "Found nested directory structure:",
        test_path,
        "\nViolates MP028 (Avoid Self-Reference)"
      )
    }
  }

  # Validation 3: Verify standard directories exist
  required_dirs <- c(
    "scripts/update_scripts",
    "scripts/global_scripts",
    "data/local_data",
    "data/app_data"
  )

  for (req_dir in required_dirs) {
    full_path <- file.path(APP_DIR, req_dir)
    if (!dir.exists(full_path)) {
      violations[[paste0("missing_", gsub("/", "_", req_dir))]] <- paste(
        "Required directory missing:",
        full_path
      )
    }
  }

  # Validation 4: Check for incorrect path concatenation in scripts
  update_scripts_dir <- file.path(APP_DIR, "scripts", "update_scripts")
  if (dir.exists(update_scripts_dir)) {
    r_files <- list.files(update_scripts_dir, pattern = "\\.R$", full.names = TRUE)

    for (r_file in r_files) {
      content <- readLines(r_file, warn = FALSE)

      # Check for problematic patterns
      problematic_lines <- grep(
        'file\\.path\\(APP_DIR,\\s*"update_scripts"',
        content,
        value = TRUE
      )

      if (length(problematic_lines) > 0) {
        violations[[paste0("bad_path_", basename(r_file))]] <- paste(
          "File contains problematic path construction:",
          basename(r_file),
          "\nPattern: file.path(APP_DIR, \"update_scripts\")",
          "\nShould use: UPDATE_SCRIPTS_DIR or file.path(APP_DIR, \"scripts\", \"update_scripts\")"
        )
      }
    }
  }

  # Report results
  if (length(violations) == 0) {
    message("✅ Path validation passed - All paths follow MAMBA principles")
    return(TRUE)
  } else {
    message("❌ Path validation failed - Found ", length(violations), " violations:")
    for (name in names(violations)) {
      message("\n", name, ":")
      message(violations[[name]])
    }
    return(FALSE)
  }
}

# Run validation if sourced directly
if (sys.nframe() == 0) {
  validate_path_consistency()
}