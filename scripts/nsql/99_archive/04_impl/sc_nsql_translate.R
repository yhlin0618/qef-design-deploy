#!/usr/bin/env Rscript
#' NSQL Translate Script
#' 
#' Command-line wrapper for NSQL translation functionality
#' Reads NSQL statement from stdin and translates to target language
#' 
#' Usage: echo "NSQL statement" | Rscript sc_nsql_translate.R [target_language]
#' Target languages: sql (default), dplyr, pandas
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
  # Get command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  target_language <- if (length(args) > 0) args[1] else "sql"
  
  # Validate target language
  valid_targets <- c("sql", "dplyr", "pandas")
  if (!target_language %in% valid_targets) {
    cat("Error: Invalid target language '", target_language, "'\n", file = stderr())
    cat("Valid targets:", paste(valid_targets, collapse = ", "), "\n", file = stderr())
    quit(status = 1)
  }
  
  # Read input from stdin
  input_lines <- readLines(file("stdin"))
  nsql_statement <- paste(input_lines, collapse = "\n")
  
  # Validate input
  if (nchar(trimws(nsql_statement)) == 0) {
    cat("Error: No NSQL statement provided\n", file = stderr())
    cat("Usage: echo \"NSQL statement\" | Rscript sc_nsql_translate.R [target]\n", file = stderr())
    quit(status = 1)
  }
  
  # Translate the NSQL statement
  tryCatch({
    translated_code <- nsql_translate(nsql_statement, target = target_language)
    
    # Output result
    cat("NSQL Translation Result:\n")
    cat("=======================\n")
    cat("Input NSQL:", nsql_statement, "\n")
    cat("Target Language:", target_language, "\n\n")
    cat("Translated Code:\n")
    cat("```", target_language, "\n", sep = "")
    cat(translated_code, "\n")
    cat("```\n")
    
  }, error = function(e) {
    cat("Translation Error:", e$message, "\n", file = stderr())
    quit(status = 1)
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}