# fn_should_exclude_covariate.R
# Purpose: Check if a covariate should be excluded based on YAML configuration
# Following: MP111 Configuration Initialization Pattern, MP048 Universal Initialization
# Related: DM_R043 (Predictor Data Classification)
# Requires: fn_initialize_covariate_exclusion.R must be sourced first

#' Check if a covariate should be excluded from analysis display
#'
#' This function uses pre-loaded exclusion rules from YAML configuration to
#' determine whether a given variable should be excluded from presentation.
#'
#' IMPORTANT: Exclusion happens at PRESENTATION STAGE only, not during analysis.
#' Variables are retained during statistical modeling and only filtered before display.
#'
#' Architecture:
#' - Configuration must be initialized BEFORE calling this function
#' - Use initialize_covariate_exclusion() at startup (see MP111)
#' - This function does NOT load YAML (fail fast if not initialized)
#'
#' @param var_name Character string. Name of the variable to check.
#' @param data Optional data.frame. Required for conditional rules (data characteristics).
#' @param app_type Character string. Application type ("poisson_regression", "positioning_analysis", etc.)
#' @param verbose Logical. Print diagnostic messages (default: FALSE).
#'
#' @return Logical. TRUE if variable should be excluded, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # FIRST: Initialize configuration (at startup)
#' source("scripts/global_scripts/04_utils/fn_initialize_covariate_exclusion.R")
#' # Config auto-initialized when sourced
#'
#' # THEN: Use function
#' should_exclude_covariate("product_name")  # Returns TRUE
#' should_exclude_covariate("price")         # Returns FALSE
#'
#' # Check with data for conditional rules
#' should_exclude_covariate("high_cardinality_var", data = my_data)
#'
#' # Different application type
#' should_exclude_covariate("seasonality_factor", app_type = "time_series_analysis")
#' }
#'
#' @export
should_exclude_covariate <- function(
  var_name,
  data = NULL,
  app_type = "poisson_regression",
  verbose = FALSE
) {

  # Check if configuration is initialized (fail fast)
  if (!exists(".covariate_exclusion_config", envir = .GlobalEnv)) {
    stop(
      "Covariate exclusion configuration not initialized.\n",
      "Please ensure fn_initialize_covariate_exclusion.R is sourced at startup.\n",
      "This should be handled by sc_Rprofile.R or sc_initialization_*.R\n\n",
      "To fix:\n",
      "1. Source: source('scripts/global_scripts/04_utils/fn_initialize_covariate_exclusion.R')\n",
      "2. Or add to initialization scripts in 22_initializations/\n\n",
      "See MP111: Configuration Initialization Pattern"
    )
  }

  # Get pre-loaded configuration
  config <- get(".covariate_exclusion_config", envir = .GlobalEnv)

  # Get application-specific settings
  app_settings <- config$application_settings[[app_type]]
  if (is.null(app_settings)) {
    warning("Application type '", app_type, "' not found. Using 'poisson_regression' defaults.")
    app_settings <- config$application_settings$poisson_regression
  }

  if (verbose) {
    message("Checking variable: ", var_name)
    message("Application type: ", app_type)
    message("Settings: exact_matches=", app_settings$use_exact_matches,
            ", regex_patterns=", app_settings$use_regex_patterns,
            ", conditional_rules=", app_settings$use_conditional_rules)
  }

  # --- Step 1: Check application-specific additional excludes ---
  if (!is.null(app_settings$additional_excludes)) {
    if (var_name %in% app_settings$additional_excludes) {
      if (verbose) message("  -> EXCLUDED: Found in application-specific excludes")
      return(TRUE)
    }
  }

  # --- Step 2: Check exact matches ---
  if (app_settings$use_exact_matches) {
    all_exact_matches <- unlist(config$exact_matches, use.names = FALSE)
    if (var_name %in% all_exact_matches) {
      if (verbose) message("  -> EXCLUDED: Exact match found")
      return(TRUE)
    }
  }

  # --- Step 3: Check regex patterns ---
  if (app_settings$use_regex_patterns) {
    for (category_name in names(config$regex_patterns)) {
      category_rules <- config$regex_patterns[[category_name]]

      for (rule in category_rules) {
        pattern <- rule$pattern
        case_sensitive <- rule$case_sensitive
        if (is.null(case_sensitive)) case_sensitive <- TRUE

        matched <- if (case_sensitive) {
          grepl(pattern, var_name)
        } else {
          grepl(pattern, var_name, ignore.case = TRUE)
        }

        if (matched) {
          if (verbose) {
            message("  -> EXCLUDED: Matched regex pattern '", pattern,
                   "' in category '", category_name, "'")
            message("     Description: ", rule$description)
          }
          return(TRUE)
        }
      }
    }
  }

  # --- Step 4: Check conditional rules (if data provided) ---
  if (app_settings$use_conditional_rules && !is.null(data)) {
    if (var_name %in% names(data)) {
      var_data <- data[[var_name]]
      var_type <- class(var_data)[1]

      # Check data type rules
      for (rule in config$conditional_rules$by_data_type) {
        if (rule$type == var_type) {

          # Check minimum unique ratio
          if (!is.null(rule$min_unique_ratio)) {
            unique_ratio <- length(unique(var_data)) / length(var_data)
            if (unique_ratio >= rule$min_unique_ratio) {
              if (verbose) {
                message("  -> EXCLUDED: Unique ratio ", round(unique_ratio, 3),
                       " >= threshold ", rule$min_unique_ratio)
              }
              return(TRUE)
            }
          }

          # Check all same value
          if (!is.null(rule$all_same_value) && rule$all_same_value) {
            if (length(unique(var_data)) == 1) {
              if (verbose) message("  -> EXCLUDED: All values are the same")
              return(TRUE)
            }
          }

          # Check zero variance (for numeric)
          if (!is.null(rule$zero_variance) && rule$zero_variance) {
            if (is.numeric(var_data)) {
              variance <- var(var_data, na.rm = TRUE)
              if (!is.na(variance) && variance == 0) {
                if (verbose) message("  -> EXCLUDED: Zero variance")
                return(TRUE)
              }
            }
          }

          # Check all missing
          if (!is.null(rule$all_missing) && rule$all_missing) {
            if (all(is.na(var_data))) {
              if (verbose) message("  -> EXCLUDED: All values are NA")
              return(TRUE)
            }
          }
        }
      }

      # Check high cardinality (application-specific)
      if (!is.null(app_settings$exclude_high_cardinality) &&
          app_settings$exclude_high_cardinality) {
        if (is.character(var_data) || is.factor(var_data)) {
          n_unique <- length(unique(var_data))
          threshold <- app_settings$cardinality_threshold
          if (!is.null(threshold) && n_unique > threshold) {
            if (verbose) {
              message("  -> EXCLUDED: High cardinality (", n_unique, " > ", threshold, ")")
            }
            return(TRUE)
          }
        }
      }
    }
  }

  # --- No exclusion rules matched ---
  if (verbose) message("  -> INCLUDED: No exclusion rules matched")
  return(FALSE)
}

#' Filter a dataframe to remove excluded covariates
#'
#' Convenience wrapper to filter multiple variables at once.
#'
#' @param data Data.frame or tibble to filter
#' @param predictor_col Character string. Name of column containing predictor names (default: "predictor")
#' @param ... Additional arguments passed to should_exclude_covariate()
#'
#' @return Filtered data.frame with excluded covariates removed
#'
#' @examples
#' filtered_data <- filter_excluded_covariates(analysis_data)
#'
#' @export
filter_excluded_covariates <- function(data, predictor_col = "predictor", ...) {
  if (!predictor_col %in% names(data)) {
    stop("Column '", predictor_col, "' not found in data")
  }

  # Create exclusion mask
  exclude_mask <- sapply(data[[predictor_col]], should_exclude_covariate, ...)

  # Filter data
  filtered_data <- data[!exclude_mask, ]

  n_excluded <- sum(exclude_mask)
  n_total <- nrow(data)
  message("Filtered ", n_excluded, " of ", n_total, " covariates (",
         round(n_excluded/n_total * 100, 1), "% excluded)")

  return(filtered_data)
}
