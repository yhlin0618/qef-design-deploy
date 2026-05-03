#!/usr/bin/env Rscript
#' NSQL Execute Script
#' 
#' Command-line wrapper for NSQL execution functionality
#' Reads NSQL statement from stdin, translates and executes it
#' 
#' Usage: echo "NSQL statement" | Rscript sc_nsql_execute.R [target_language]
#' Target languages: sql (default), dplyr
#' 
#' @author Claude Code
#' @date 2025-07-03

# Load required libraries
suppressWarnings(suppressMessages({
  library(jsonlite)
  library(DBI)
  library(dplyr)
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

# Source database connection utilities
source(file.path(script_dir, "..", "01_db", "dbConnect.R"))

# Main execution
main <- function() {
  # Get command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  target_language <- if (length(args) > 0) args[1] else "sql"
  
  # Validate target language
  valid_targets <- c("sql", "dplyr")
  if (!target_language %in% valid_targets) {
    cat("Error: Invalid target language '", target_language, "'\n", file = stderr())
    cat("Valid targets for execution:", paste(valid_targets, collapse = ", "), "\n", file = stderr())
    quit(status = 1)
  }
  
  # Read input from stdin
  input_lines <- readLines(file("stdin"))
  nsql_statement <- paste(input_lines, collapse = "\n")
  
  # Validate input
  if (nchar(trimws(nsql_statement)) == 0) {
    cat("Error: No NSQL statement provided\n", file = stderr())
    cat("Usage: echo \"NSQL statement\" | Rscript sc_nsql_execute.R [target]\n", file = stderr())
    quit(status = 1)
  }
  
  # Execute the NSQL statement
  tryCatch({
    # First validate the statement
    validation_result <- nsql_validate(nsql_statement)
    if (!validation_result$valid) {
      cat("❌ Invalid NSQL Statement - Cannot Execute\n", file = stderr())
      for (issue in validation_result$issues) {
        cat(sprintf("[%s] %s\n", toupper(issue$severity), issue$message), file = stderr())
      }
      quit(status = 1)
    }
    
    # Translate to target language
    translated_code <- nsql_translate(nsql_statement, target = target_language)
    
    cat("NSQL Execution Result:\n")
    cat("=====================\n")
    cat("Input NSQL:", nsql_statement, "\n")
    cat("Target Language:", target_language, "\n")
    cat("Translated Code:", translated_code, "\n\n")
    
    # Execute based on target language
    if (target_language == "sql") {
      # Execute SQL
      cat("Attempting SQL execution...\n")
      tryCatch({
        conn <- dbConnect_universal()
        result <- dbGetQuery(conn, translated_code)
        dbDisconnect(conn)
        
        cat("SQL Execution Successful:\n")
        print(result)
        
      }, error = function(e) {
        cat("SQL Execution Error:", e$message, "\n", file = stderr())
        quit(status = 1)
      })
      
    } else if (target_language == "dplyr") {
      # Execute dplyr
      cat("Attempting dplyr execution...\n")
      cat("Note: dplyr execution requires data context - showing code only\n")
      cat("Executable R Code:\n")
      cat(translated_code, "\n")
    }
    
  }, error = function(e) {
    cat("Execution Error:", e$message, "\n", file = stderr())
    quit(status = 1)
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}