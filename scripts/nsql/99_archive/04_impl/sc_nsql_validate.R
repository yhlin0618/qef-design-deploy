#!/usr/bin/env Rscript
#' NSQL Validate Script
#' 
#' Command-line wrapper for NSQL validation functionality
#' Reads NSQL statement from stdin and validates syntax and semantics
#' 
#' Usage: echo "NSQL statement" | Rscript sc_nsql_validate.R
#' 
#' @author Claude Code
#' @date 2025-07-03

# Load required libraries
suppressWarnings(suppressMessages({
  library(jsonlite)
}))

# Get script directory in a robust way
get_script_dir <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  script_arg <- cmd_args[grepl("^--file=", cmd_args)]
  if (length(script_arg) > 0) {
    script_path <- sub("^--file=", "", script_arg)
    return(dirname(script_path))
  } else {
    # Fallback to current directory
    return(getwd())
  }
}

script_dir <- get_script_dir()

# Source the main NSQL module
source(file.path(script_dir, "nsql.R"))

# Main execution
main <- function() {
  # Read input from stdin
  input_lines <- readLines(file("stdin"))
  nsql_statement <- paste(input_lines, collapse = "\n")
  
  # Validate input
  if (nchar(trimws(nsql_statement)) == 0) {
    cat("Error: No NSQL statement provided\n", file = stderr())
    cat("Usage: echo \"NSQL statement\" | Rscript sc_nsql_validate.R\n", file = stderr())
    quit(status = 1)
  }
  
  # Validate the NSQL statement
  tryCatch({
    validation_result <- nsql_validate(nsql_statement)
    
    # Output result
    cat("NSQL Validation Result:\n")
    cat("======================\n")
    cat("Input:", nsql_statement, "\n\n")
    
    if (validation_result$valid) {
      cat("✅ Valid NSQL Statement\n")
      cat("Status: PASSED\n")
    } else {
      cat("❌ Invalid NSQL Statement\n")
      cat("Status: FAILED\n\n")
      cat("Issues Found:\n")
      for (i in seq_along(validation_result$issues)) {
        issue <- validation_result$issues[[i]]
        cat(sprintf("%d. [%s] %s: %s\n", 
                   i, 
                   toupper(issue$severity), 
                   issue$type, 
                   issue$message))
      }
    }
    
    # Exit with appropriate status code
    if (!validation_result$valid) {
      quit(status = 1)
    }
    
  }, error = function(e) {
    cat("Validation Error:", e$message, "\n", file = stderr())
    quit(status = 1)
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}