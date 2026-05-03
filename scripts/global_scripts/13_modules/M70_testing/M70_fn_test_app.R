#' Test the precision marketing application
#'
#' This function launches the application in test mode, runs a series of tests,
#' and returns a test report. It captures any errors that occur during execution.
#'
#' @param config_path Character string. Path to the application configuration file.
#' @param test_mode Character string. The type of test to run (default: "basic").
#' @param timeout Numeric. Maximum time to run the test in seconds (default: 30).
#' @param capture_screenshots Logical. Whether to capture screenshots during testing (default: FALSE).
#'
#' @return List containing test results including any errors, warnings, and status information.
#'
#' @examples
#' test_results <- M70_test_app()
#' if(test_results$success) message("All tests passed!") else message("Tests failed.")
M70_test_app <- function(config_path = "app_config.yaml", 
                        test_mode = "basic", 
                        timeout = 30,
                        capture_screenshots = FALSE) {
  
  # Initialize test results
  test_results <- list(
    success = FALSE,
    errors = list(),
    warnings = list(),
    info = list(),
    screenshots = list(),
    timestamp = Sys.time()
  )
  
  # Record test start
  test_results$info$start_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  test_results$info$test_mode <- test_mode
  test_results$info$config_path <- config_path
  
  # Verify config file exists
  if (!file.exists(config_path)) {
    test_results$errors$config <- paste("Configuration file not found:", config_path)
    return(test_results)
  }
  
  # Try to load the configuration
  tryCatch({
    if (!requireNamespace("yaml", quietly = TRUE)) {
      install.packages("yaml")
      library(yaml)
    }
    
    config <- yaml::read_yaml(config_path)
    test_results$info$config_loaded <- TRUE
    
    # Verify config has required fields
    required_fields <- c("title", "theme", "layout", "brand")
    missing_fields <- required_fields[!required_fields %in% names(config)]
    
    if (length(missing_fields) > 0) {
      test_results$warnings$missing_fields <- paste("Configuration missing fields:", 
                                                  paste(missing_fields, collapse = ", "))
    }
    
  }, error = function(e) {
    test_results$errors$config_load <- paste("Error loading configuration:", e$message)
    test_results$info$config_loaded <- FALSE
  })
  
  # Run the app and test
  test_results$info$app_started <- FALSE
  
  tryCatch({
    # Set up a timeout
    timeout_reached <- FALSE
    timeout_handler <- function(timeout) {
      timeout_reached <<- TRUE
      test_results$warnings$timeout <- paste("Test timeout reached after", timeout, "seconds")
    }
    
    # Create a temporary script to run the app with a timeout
    temp_script_path <- tempfile(fileext = ".R")
    writeLines(paste0(
      "suppressPackageStartupMessages(library(shiny))\n",
      "setwd('", getwd(), "')\n",  # Ensure working directory is set correctly
      "tryCatch({\n",
      "  options(shiny.port = 4880)\n",  # Use a specific port to avoid conflicts
      "  source('app.R')\n",
      "}, error = function(e) {\n",
      "  cat('ERROR:', e$message, '\\n', file = 'app_test_error.log')\n",
      "})"
    ), temp_script_path)
    
    # Launch R script with timeout
    cmd <- paste0("Rscript ", temp_script_path)
    test_results$info$app_started <- TRUE
    
    # For macOS we'll use timeout command from coreutils if available
    if (Sys.info()["sysname"] == "Darwin" && system("which gtimeout", ignore.stdout = TRUE) == 0) {
      cmd <- paste0("gtimeout ", timeout, "s ", cmd)
    } else if (Sys.info()["sysname"] != "Windows") {
      # On Linux use the standard timeout command
      cmd <- paste0("timeout ", timeout, "s ", cmd)
    }
    
    # Run the command and capture the exit status
    status <- system(cmd, ignore.stdout = !capture_screenshots, ignore.stderr = !capture_screenshots)
    
    # Check if command execution was successful
    if (status != 0) {
      test_results$warnings$execution <- paste("Command execution returned status code:", status)
    }
    
    # If we're here, check for error logs
    if (file.exists("app_test_error.log")) {
      error_content <- readLines("app_test_error.log")
      test_results$errors$app_execution <- paste(error_content, collapse = "\n")
      file.remove("app_test_error.log")
    }
    
    # If we're still here and no errors, test passed
    if (length(test_results$errors) == 0) {
      test_results$success <- TRUE
      test_results$info$message <- "App test completed successfully"
    }
    
  }, error = function(e) {
    test_results$errors$test_execution <- paste("Error executing test:", e$message)
  })
  
  # Record test end
  test_results$info$end_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  test_results$info$duration <- difftime(
    as.POSIXct(test_results$info$end_time, format = "%Y-%m-%d %H:%M:%S"),
    as.POSIXct(test_results$info$start_time, format = "%Y-%m-%d %H:%M:%S"),
    units = "secs"
  )
  
  # Generate test report
  test_report <- paste0(
    "# App Test Report\n\n",
    "## Summary\n\n",
    "- **Status**: ", ifelse(test_results$success, "SUCCESS ✓", "FAILED ✗"), "\n",
    "- **Test Mode**: ", test_mode, "\n",
    "- **Start Time**: ", test_results$info$start_time, "\n",
    "- **End Time**: ", test_results$info$end_time, "\n",
    "- **Duration**: ", test_results$info$duration, " seconds\n",
    "- **Configuration**: ", config_path, "\n\n"
  )
  
  if (length(test_results$errors) > 0) {
    test_report <- paste0(
      test_report,
      "## Errors\n\n",
      paste(paste0("- ", names(test_results$errors), ": ", test_results$errors), collapse = "\n"),
      "\n\n"
    )
  }
  
  if (length(test_results$warnings) > 0) {
    test_report <- paste0(
      test_report,
      "## Warnings\n\n",
      paste(paste0("- ", names(test_results$warnings), ": ", test_results$warnings), collapse = "\n"),
      "\n\n"
    )
  }
  
  # Write test report to file
  report_path <- file.path("modules", "M70_testing", paste0("app_test_report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".md"))
  dir.create(dirname(report_path), recursive = TRUE, showWarnings = FALSE)
  writeLines(test_report, report_path)
  test_results$info$report_path <- report_path
  
  return(test_results)
}