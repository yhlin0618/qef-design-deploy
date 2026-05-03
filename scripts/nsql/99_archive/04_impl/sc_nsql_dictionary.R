#!/usr/bin/env Rscript
#' NSQL Dictionary Management Script
#' 
#' Command-line wrapper for NSQL dictionary management functionality
#' 
#' Usage: 
#'   # Search dictionary
#'   echo "search_term" | Rscript sc_nsql_dictionary.R search
#'   # Show all entries
#'   Rscript sc_nsql_dictionary.R show
#'   # Add entry (interactive)
#'   Rscript sc_nsql_dictionary.R add section name
#' 
#' @author Claude Code
#' @date 2025-07-03

# Load required libraries
suppressWarnings(suppressMessages({
  library(yaml)
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

# Helper function to load dictionary
load_nsql_dictionary <- function() {
  dict_path <- file.path(script_dir, "dictionary.yaml")
  if (file.exists(dict_path)) {
    return(yaml::read_yaml(dict_path))
  } else {
    stop("Dictionary file not found: ", dict_path)
  }
}

# Helper function to search dictionary
search_dictionary <- function(search_term, dictionary) {
  results <- list()
  
  # Search through all sections
  for (section_name in names(dictionary)) {
    section <- dictionary[[section_name]]
    if (is.list(section) && !is.null(names(section))) {
      for (entry_name in names(section)) {
        entry <- section[[entry_name]]
        
        # Search in entry name and description
        if (grepl(search_term, entry_name, ignore.case = TRUE) ||
            (is.list(entry) && !is.null(entry$description) && 
             grepl(search_term, entry$description, ignore.case = TRUE))) {
          
          results[[length(results) + 1]] <- list(
            section = section_name,
            name = entry_name,
            entry = entry
          )
        }
      }
    }
  }
  
  return(results)
}

# Main execution
main <- function() {
  # Get command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) == 0) {
    cat("Error: No action specified\n", file = stderr())
    cat("Usage: Rscript sc_nsql_dictionary.R [search|show|add] [args...]\n", file = stderr())
    quit(status = 1)
  }
  
  action <- args[1]
  
  # Load dictionary
  tryCatch({
    dictionary <- load_nsql_dictionary()
    
    if (action == "search") {
      # Read search term from stdin
      input_lines <- readLines(file("stdin"))
      search_term <- paste(input_lines, collapse = " ")
      
      if (nchar(trimws(search_term)) == 0) {
        cat("Error: No search term provided\n", file = stderr())
        quit(status = 1)
      }
      
      results <- search_dictionary(search_term, dictionary)
      
      cat("NSQL Dictionary Search Results:\n")
      cat("==============================\n")
      cat("Search term:", search_term, "\n")
      cat("Found", length(results), "results:\n\n")
      
      for (i in seq_along(results)) {
        result <- results[[i]]
        cat(sprintf("%d. [%s] %s\n", i, result$section, result$name))
        if (!is.null(result$entry$description)) {
          desc <- if(is.list(result$entry$description)) {
            paste(unlist(result$entry$description), collapse = " ")
          } else {
            as.character(result$entry$description)
          }
          cat("   Description:", desc, "\n")
        }
        if (!is.null(result$entry$example)) {
          ex <- if(is.list(result$entry$example)) {
            paste(unlist(result$entry$example), collapse = " ")
          } else {
            as.character(result$entry$example)
          }
          cat("   Example:", ex, "\n")
        }
        cat("\n")
      }
      
    } else if (action == "show") {
      cat("NSQL Dictionary Contents:\n")
      cat("========================\n\n")
      
      for (section_name in names(dictionary)) {
        if (section_name != "meta") {  # Skip meta section
          cat("Section:", section_name, "\n")
          cat(strrep("-", nchar(section_name) + 9), "\n")
          
          section <- dictionary[[section_name]]
          if (is.list(section) && !is.null(names(section))) {
            for (entry_name in names(section)) {
              cat("  •", entry_name)
              entry <- section[[entry_name]]
              if (is.list(entry) && !is.null(entry$description)) {
                cat(" -", entry$description)
              }
              cat("\n")
            }
          }
          cat("\n")
        }
      }
      
    } else if (action == "add") {
      cat("Interactive dictionary entry addition not yet implemented\n")
      cat("Please edit dictionary.yaml directly\n")
      
    } else {
      cat("Error: Unknown action '", action, "'\n", file = stderr())
      cat("Valid actions: search, show, add\n", file = stderr())
      quit(status = 1)
    }
    
  }, error = function(e) {
    cat("Dictionary Error:", e$message, "\n", file = stderr())
    quit(status = 1)
  })
}

# Run main function if script is executed directly
if (sys.nframe() == 0) {
  main()
}