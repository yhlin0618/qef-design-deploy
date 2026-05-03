#' @file fn_print_query.R
#' @author Claude
#' @date 2025-04-15
#' 
#' @title Print Formatted SQL Query
#' @description Display a SQL query with optional title formatting for better readability
#' @param query Character string. The SQL query to display.
#' @param title Optional character string. Title to display above the query. If NULL, a default title is used.
#' @return None. Used for its side effect of printing to the console.
#' @export
#'

print_query <- function(query, title = NULL) {
  # Format the output
  if (!is.null(title)) {
    cat("\n\n===", title, "===\n")
  } else {
    cat("\n\n=== SQL QUERY ===\n")
  }
  
  # Display the query
  cat(query)
  
  # Add trailing newlines for separation
  cat("\n\n")
  
  # Return invisibly for use in pipes
  return(invisible(query))
  
  
# R0112 Comment-Code Spacing Rule
# R0103 Dependency-Based Sourcing
}