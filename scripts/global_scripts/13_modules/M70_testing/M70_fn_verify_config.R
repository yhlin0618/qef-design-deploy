#' Verify application configuration
#'
#' This function verifies that the application configuration is valid and complete.
#' It checks for required fields, valid values, and proper structure.
#'
#' @param config_path Character string. Path to the application configuration file.
#' @param verbose Logical. Whether to print detailed verification information (default: FALSE).
#'
#' @return List containing verification results including any errors, warnings, and info messages.
#'
#' @examples
#' verify_results <- M70_verify_config("app_config.yaml")
#' if(verify_results$success) message("Configuration is valid!") else message("Configuration has issues.")
M70_verify_config <- function(config_path = "app_config.yaml", verbose = FALSE) {
  
  # Initialize verification results
  verify_results <- list(
    success = FALSE,
    errors = list(),
    warnings = list(),
    info = list(),
    timestamp = Sys.time()
  )
  
  # Record verification start
  verify_results$info$start_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  verify_results$info$config_path <- config_path
  
  # Verify config file exists
  if (!file.exists(config_path)) {
    verify_results$errors$config_file <- paste("Configuration file not found:", config_path)
    return(verify_results)
  }
  
  # Try to load the configuration
  tryCatch({
    if (!requireNamespace("yaml", quietly = TRUE)) {
      install.packages("yaml")
      library(yaml)
    }
    
    config <- yaml::read_yaml(config_path)
    verify_results$info$config_loaded <- TRUE
    
    # Verify config has required fields
    required_fields <- c("title", "theme", "layout", "brand")
    missing_fields <- required_fields[!required_fields %in% names(config)]
    
    if (length(missing_fields) > 0) {
      verify_results$errors$missing_fields <- paste("Configuration missing required fields:", 
                                                paste(missing_fields, collapse = ", "))
    }
    
    # Verify theme section
    if ("theme" %in% names(config)) {
      theme_required <- c("version", "bootswatch")
      theme_missing <- theme_required[!theme_required %in% names(config$theme)]
      
      if (length(theme_missing) > 0) {
        verify_results$warnings$theme <- paste("Theme section missing fields:", 
                                              paste(theme_missing, collapse = ", "))
      }
    }
    
    # Verify brand section
    if ("brand" %in% names(config)) {
      brand_required <- c("name", "language")
      brand_missing <- brand_required[!brand_required %in% names(config$brand)]
      
      if (length(brand_missing) > 0) {
        verify_results$warnings$brand <- paste("Brand section missing fields:", 
                                             paste(brand_missing, collapse = ", "))
      }
    }
    
    # Verify components section
    if ("components" %in% names(config)) {
      if (length(config$components) == 0) {
        verify_results$warnings$components <- "Components section is empty"
      }
    } else {
      verify_results$warnings$components <- "Components section is missing"
    }
    
    # If no errors, mark as success
    if (length(verify_results$errors) == 0) {
      verify_results$success <- TRUE
      verify_results$info$message <- "Configuration verification completed successfully"
    }
    
  }, error = function(e) {
    verify_results$errors$config_load <- paste("Error loading configuration:", e$message)
    verify_results$info$config_loaded <- FALSE
  })
  
  # Record verification end
  verify_results$info$end_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  
  # Generate verification report
  report <- paste0(
    "# Configuration Verification Report\n\n",
    "## Summary\n\n",
    "- **Status**: ", ifelse(verify_results$success, "VALID ✓", "INVALID ✗"), "\n",
    "- **Configuration**: ", config_path, "\n",
    "- **Timestamp**: ", verify_results$info$end_time, "\n\n"
  )
  
  if (length(verify_results$errors) > 0) {
    report <- paste0(
      report,
      "## Errors\n\n",
      paste(paste0("- ", names(verify_results$errors), ": ", verify_results$errors), collapse = "\n"),
      "\n\n"
    )
  }
  
  if (length(verify_results$warnings) > 0) {
    report <- paste0(
      report,
      "## Warnings\n\n",
      paste(paste0("- ", names(verify_results$warnings), ": ", verify_results$warnings), collapse = "\n"),
      "\n\n"
    )
  }
  
  # Print report if verbose
  if (verbose) {
    cat(report)
  }
  
  # Write verification report to file
  report_path <- file.path("modules", "M70_testing", paste0("config_verification_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".md"))
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(report, report_path)
  verify_results$info$report_path <- report_path
  
  return(verify_results)
}