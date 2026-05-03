# fn_initialize_covariate_exclusion.R
# Purpose: Initialize covariate exclusion configuration at application startup
# Following: MP111 Configuration Initialization Pattern, MP048 Universal Initialization
# Created: 2025-11-14

#' Initialize Covariate Exclusion Configuration
#'
#' Loads YAML configuration for covariate exclusion at application startup.
#' This function should be called during initialization, NOT at runtime.
#'
#' Architecture:
#' - Fail Fast: Configuration errors detected at startup, not during user interaction
#' - Single Load: Configuration loaded once, not on every function call
#' - Path Resilience: Auto-detects path from multiple standard locations
#' - Validation: Checks configuration structure before storing
#'
#' Following:
#' - MP111: Configuration Initialization Pattern (load config at startup)
#' - MP048: Universal Initialization (consistent initialization pattern)
#' - MP106: Console Output Transparency (verbose option)
#'
#' @param config_path Character string. Path to YAML configuration file.
#'   If NULL, auto-detects from standard locations.
#'   Default: NULL (auto-detect)
#' @param verbose Logical. Print diagnostic messages during initialization.
#'   Default: TRUE
#'
#' @return Invisible TRUE on success. Stores configuration in .GlobalEnv as
#'   .covariate_exclusion_config
#'
#' @details
#' Configuration is stored in .GlobalEnv with the following structure:
#' - metadata: version, last_updated, description
#' - exact_matches: list of exact variable names to exclude
#' - regex_patterns: regex patterns for matching variable names
#' - conditional_rules: data-dependent exclusion rules
#' - application_settings: app-specific configuration
#'
#' Standard locations checked (in order):
#' 1. Explicit config_path if provided
#' 2. scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml (from MAMBA root)
#' 3. global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml (from current dir)
#' 4. $MAMBA_ROOT/scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml (from env var)
#'
#' @examples
#' \dontrun{
#' # Auto-detect path and initialize
#' initialize_covariate_exclusion()
#'
#' # Explicit path
#' initialize_covariate_exclusion(
#'   config_path = "path/to/exclusion_rules.yaml"
#' )
#'
#' # Silent initialization
#' initialize_covariate_exclusion(verbose = FALSE)
#' }
#'
#' @export
initialize_covariate_exclusion <- function(
  config_path = NULL,
  verbose = TRUE
) {

  if (verbose) {
    cat("📋 Initializing covariate exclusion configuration...\n")
  }

  # --- Step 1: Check if yaml package is available ---
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required but not installed.\n",
         "Install it: install.packages('yaml')")
  }

  # --- Step 2: Auto-detect path if not provided ---
  if (is.null(config_path)) {
    # Try multiple standard locations
    possible_paths <- c(
      # From MAMBA root (most common in production)
      "scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml",

      # From current working directory (UPDATE_MODE or APP_MODE)
      "global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml",

      # From MAMBA_ROOT environment variable (if set)
      if (nzchar(Sys.getenv("MAMBA_ROOT"))) {
        file.path(
          Sys.getenv("MAMBA_ROOT"),
          "scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml"
        )
      } else NULL,

      # From GLOBAL_PARAMETER_DIR (if set by sc_Rprofile.R)
      if (exists("GLOBAL_PARAMETER_DIR", envir = .GlobalEnv)) {
        file.path(
          get("GLOBAL_PARAMETER_DIR", envir = .GlobalEnv),
          "scd_type2/exclusion_rules.yaml"
        )
      } else NULL
    )

    # Remove NULL entries
    possible_paths <- possible_paths[!sapply(possible_paths, is.null)]

    # Find first existing path
    config_path <- NULL
    for (path in possible_paths) {
      if (!is.null(path) && file.exists(path)) {
        config_path <- path
        break
      }
    }

    if (is.null(config_path)) {
      stop(
        "Cannot locate covariate exclusion configuration file.\n",
        "Tried the following paths:\n",
        paste("  -", possible_paths, collapse = "\n"), "\n\n",
        "Current working directory: ", getwd(), "\n\n",
        "Solutions:\n",
        "1. Set MAMBA_ROOT environment variable\n",
        "2. Run from MAMBA root directory\n",
        "3. Provide explicit config_path parameter\n",
        "4. Ensure sc_Rprofile.R has set GLOBAL_PARAMETER_DIR"
      )
    }
  }

  # --- Step 3: Validate file exists ---
  if (!file.exists(config_path)) {
    stop(
      "Configuration file not found: ", config_path, "\n",
      "Current working directory: ", getwd()
    )
  }

  if (verbose) {
    cat("   Using config file:", config_path, "\n")
  }

  # --- Step 4: Load configuration ---
  config <- tryCatch(
    yaml::read_yaml(config_path),
    error = function(e) {
      stop(
        "Failed to parse YAML configuration: ", config_path, "\n",
        "Error: ", e$message
      )
    }
  )

  # --- Step 5: Validate configuration structure ---
  required_sections <- c("metadata", "exact_matches", "regex_patterns",
                         "conditional_rules", "application_settings")
  missing_sections <- setdiff(required_sections, names(config))

  if (length(missing_sections) > 0) {
    stop(
      "Invalid configuration structure in: ", config_path, "\n",
      "Missing required sections: ", paste(missing_sections, collapse = ", "), "\n",
      "Expected sections: ", paste(required_sections, collapse = ", ")
    )
  }

  # Validate metadata
  if (is.null(config$metadata$version)) {
    warning("Configuration missing metadata$version")
  }
  if (is.null(config$metadata$last_updated)) {
    warning("Configuration missing metadata$last_updated")
  }

  # Validate application_settings has at least one app type
  if (length(config$application_settings) == 0) {
    warning("Configuration has no application_settings defined")
  }

  # --- Step 6: Store in global environment ---
  assign(".covariate_exclusion_config", config, envir = .GlobalEnv)

  if (verbose) {
    cat("✅ Covariate exclusion configuration loaded successfully\n")
    cat("   Version:", config$metadata$version, "\n")
    cat("   Last updated:", config$metadata$last_updated, "\n")
    cat("   Application types:",
        paste(names(config$application_settings), collapse = ", "), "\n")
  }

  invisible(TRUE)
}

# --- Auto-initialization when sourced ---
# Following MP111: Configuration should be loaded at initialization time
# This auto-initializes when the file is sourced, ensuring config is available
# before any functions are called.

if (!exists(".covariate_exclusion_config", envir = .GlobalEnv)) {
  tryCatch({
    initialize_covariate_exclusion(verbose = FALSE)
  }, error = function(e) {
    # Don't fail hard on auto-init - allow manual initialization
    warning(
      "Auto-initialization of covariate exclusion failed: ", e$message, "\n",
      "This may be expected if running from non-standard location.\n",
      "Please call initialize_covariate_exclusion() manually if needed."
    )
  })
}
