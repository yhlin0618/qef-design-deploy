#' Filter covariates based on exclusion rules
#' 
#' @description
#' Filters covariates from a dataset based on exclusion rules defined in
#' exclusion_rules.yaml. Supports exact matches, regex patterns,
#' and conditional rules based on data characteristics.
#' 
#' @param data data.frame or tibble. The dataset containing covariates to filter.
#' @param var_names character vector. Variable names to check (default: names(data)).
#' @param config_path character. Path to the YAML configuration file 
#'        (default: "30_global_data/parameters/scd_type2/list_covariate_neglected.yaml").
#' @param app_type character. Application type for specific settings 
#'        (default: "poisson_regression").
#' @param verbose logical. Whether to print exclusion information (default: FALSE).
#' 
#' @return character vector. Names of variables that should be kept (not excluded).
#' 
#' @examples
#' \dontrun{
#' # Basic usage
#' filtered_vars <- fn_filter_covariates(my_data)
#' 
#' # With specific application type
#' filtered_vars <- fn_filter_covariates(my_data, app_type = "positioning_analysis")
#' 
#' # With verbose output
#' filtered_vars <- fn_filter_covariates(my_data, verbose = TRUE)
#' 
#' # Use in model formula
#' formula <- as.formula(paste("sales ~", paste(filtered_vars, collapse = " + ")))
#' }
#' 
#' @export
filter_covariates <- function(data = NULL, 
                                var_names = NULL, 
                                config_path = NULL,
                                app_type = "poisson_regression",
                                verbose = FALSE) {
  
  # Load required packages
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required but not installed.")
  }
  
  # Set default config path
  if (is.null(config_path)) {
    # Try to find the config file in common locations
    possible_paths <- c(
      "scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml",
      "global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml",
      "30_global_data/parameters/scd_type2/exclusion_rules.yaml",
      "../30_global_data/parameters/scd_type2/exclusion_rules.yaml",  # From 04_utils directory
      "../../30_global_data/parameters/scd_type2/exclusion_rules.yaml",  # From sub-subdirectories
      file.path(Sys.getenv("APP_ROOT", "."), "scripts/global_scripts/30_global_data/parameters/scd_type2/exclusion_rules.yaml")
    )
    
    config_path <- NULL
    for (path in possible_paths) {
      if (file.exists(path)) {
        config_path <- path
        break
      }
    }
    
    if (is.null(config_path)) {
      warning("Could not find exclusion_rules.yaml. No variables will be excluded.")
      return(var_names %||% names(data))
    }
  }
  
  # Load configuration
  tryCatch({
    config <- yaml::read_yaml(config_path)
  }, error = function(e) {
    warning(paste("Error loading config file:", e$message))
    return(var_names %||% names(data))
  })
  
  # Get variable names if not provided
  if (is.null(var_names)) {
    if (is.null(data)) {
      stop("Either 'data' or 'var_names' must be provided.")
    }
    var_names <- names(data)
  }
  
  # Get application-specific settings
  app_settings <- config$application_settings[[app_type]]
  if (is.null(app_settings)) {
    if (verbose) message(paste("No settings found for app_type:", app_type, ". Using poisson_regression defaults."))
    app_settings <- config$application_settings$poisson_regression
  }
  
  # Initialize excluded variables tracking
  excluded_vars <- character()
  exclusion_reasons <- list()
  
  # Helper function to check if a variable should be excluded
  should_exclude <- function(var_name) {
    reasons <- character()
    
    # Check additional excludes for this application
    if (var_name %in% app_settings$additional_excludes) {
      reasons <- c(reasons, paste("Application-specific exclude for", app_type))
    }
    
    # 1. Check exact matches if enabled
    if (app_settings$use_exact_matches) {
      for (category in names(config$exact_matches)) {
        if (var_name %in% config$exact_matches[[category]]) {
          reasons <- c(reasons, paste("Exact match in", category))
        }
      }
    }
    
    # 2. Check regex patterns if enabled
    if (app_settings$use_regex_patterns) {
      for (category in names(config$regex_patterns)) {
        for (rule in config$regex_patterns[[category]]) {
          pattern <- rule$pattern
          case_sensitive <- rule$case_sensitive %||% TRUE
          
          matched <- if (!case_sensitive) {
            grepl(pattern, var_name, ignore.case = TRUE)
          } else {
            grepl(pattern, var_name)
          }
          
          if (matched) {
            reasons <- c(reasons, paste("Regex match:", rule$description))
          }
        }
      }
    }
    
    # 3. Check conditional rules if enabled and data provided
    if (app_settings$use_conditional_rules && !is.null(data) && var_name %in% names(data)) {
      var_data <- data[[var_name]]
      var_type <- class(var_data)[1]
      
      for (rule in config$conditional_rules$by_data_type) {
        if (rule$type == var_type) {
          # Check various conditions
          if (!is.null(rule$min_unique_ratio)) {
            unique_ratio <- length(unique(var_data)) / length(var_data)
            if (unique_ratio >= rule$min_unique_ratio) {
              reasons <- c(reasons, paste("Conditional:", rule$description))
            }
          }
          
          if (!is.null(rule$max_unique_values)) {
            if (length(unique(var_data)) <= rule$max_unique_values) {
              reasons <- c(reasons, paste("Conditional:", rule$description))
            }
          }
          
          if (!is.null(rule$all_same_value) && rule$all_same_value) {
            if (length(unique(var_data[!is.na(var_data)])) <= 1) {
              reasons <- c(reasons, paste("Conditional:", rule$description))
            }
          }
          
          if (!is.null(rule$all_missing) && rule$all_missing) {
            if (all(is.na(var_data))) {
              reasons <- c(reasons, paste("Conditional:", rule$description))
            }
          }
          
          if (!is.null(rule$zero_variance) && rule$zero_variance && var_type == "numeric") {
            if (var(var_data, na.rm = TRUE) == 0 || is.na(var(var_data, na.rm = TRUE))) {
              reasons <- c(reasons, paste("Conditional:", rule$description))
            }
          }
        }
      }
      
      # Check high cardinality for factors
      if (app_settings$exclude_high_cardinality %||% FALSE) {
        if (var_type %in% c("factor", "character")) {
          n_unique <- length(unique(var_data))
          threshold <- app_settings$cardinality_threshold %||% 50
          if (n_unique > threshold) {
            reasons <- c(reasons, paste("High cardinality:", n_unique, "unique values"))
          }
        }
      }
    }
    
    if (length(reasons) > 0) {
      exclusion_reasons[[var_name]] <<- reasons
      return(TRUE)
    }
    return(FALSE)
  }
  
  # Check each variable
  for (var in var_names) {
    if (should_exclude(var)) {
      excluded_vars <- c(excluded_vars, var)
    }
  }
  
  # Print verbose output if requested
  if (verbose && length(excluded_vars) > 0) {
    message("\nExcluded covariates:")
    for (var in excluded_vars) {
      message(sprintf("  - %s: %s", var, paste(exclusion_reasons[[var]], collapse = "; ")))
    }
    message(sprintf("\nTotal excluded: %d out of %d variables", 
                   length(excluded_vars), length(var_names)))
  }
  
  # Return kept variables
  kept_vars <- setdiff(var_names, excluded_vars)
  
  if (verbose) {
    message(sprintf("Kept variables: %d", length(kept_vars)))
  }
  
  return(kept_vars)
}

#' Check if a single variable should be excluded
#' 
#' @description
#' Checks if a single variable should be excluded based on the exclusion rules.
#' This is a convenience wrapper around fn_filter_covariates for checking individual variables.
#' 
#' @param var_name character. The variable name to check.
#' @param data data.frame or NULL. Optional data for conditional rules.
#' @param config_path character. Path to the YAML configuration file.
#' @param app_type character. Application type for specific settings.
#' 
#' @return logical. TRUE if the variable should be excluded, FALSE otherwise.
#' 
#' @examples
#' \dontrun{
#' # Check if a variable should be excluded
#' should_exclude <- fn_should_exclude_covariate("is_missing_price")
#' 
#' # With data for conditional rules
#' should_exclude <- fn_should_exclude_covariate("customer_id", data = my_data)
#' }
#' 
#' @export
should_exclude_covariate <- function(var_name, 
                                       data = NULL, 
                                       config_path = NULL,
                                       app_type = "poisson_regression") {
  
  # Use filter_covariates to check
  kept_vars <- filter_covariates(
    data = data,
    var_names = var_name,
    config_path = config_path,
    app_type = app_type,
    verbose = FALSE
  )
  
  # If the variable is not in kept_vars, it should be excluded
  return(length(kept_vars) == 0)
}