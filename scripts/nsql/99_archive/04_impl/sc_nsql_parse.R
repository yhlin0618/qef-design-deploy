#!/usr/bin/env Rscript
#' NSQL Parse Script
#' 
#' Command-line wrapper for NSQL parsing functionality
#' Reads NSQL statement from stdin and outputs parsed AST
#' 
#' Usage: echo "NSQL statement" | Rscript sc_nsql_parse.R
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
    cat("Usage: echo \"NSQL statement\" | Rscript sc_nsql_parse.R\n", file = stderr())
    quit(status = 1)
  }
  
  # Parse the NSQL statement
  tryCatch({
    result <- nsql_parse(nsql_statement)
    
    # Output result as JSON
    cat("NSQL Parse Result:\n")
    cat("=================\n")
    cat("Input:", nsql_statement, "\n\n")
    cat("Parsed AST:\n")
    cat(toJSON(result, pretty = TRUE, auto_unbox = TRUE), "\n")
    
  }, error = function(e) {
    cat("Parse Error:", e$message, "\n", file = stderr())
    quit(status = 1)
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}