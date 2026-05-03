#' Utility functions for database documentation
#'
#' This file contains helper functions for the database documentation module
#' that follows the Functor-Module Correspondence Principle (MP44).
#'
#' @author Claude
#' @date 2025-04-07

#' Format a table name for display in documentation
#'
#' @param table_name The raw table name from the database
#' @return A formatted table name suitable for documentation
#' @export
format_table_name <- function(table_name) {
  # Remove any system prefixes
  clean_name <- gsub("^sys_|^tmp_", "", table_name)
  
  # Convert underscore to spaces and capitalize words
  formatted_name <- gsub("_", " ", clean_name)
  formatted_name <- tools::toTitleCase(formatted_name)
  
  return(formatted_name)
}

#' Create a valid anchor link for a table in Markdown
#'
#' @param table_name The raw table name
#' @return A valid anchor link string
#' @export
create_anchor_link <- function(table_name) {
  # Convert to lowercase, replace spaces and special chars with hyphens
  anchor <- tolower(table_name)
  anchor <- gsub("[^a-z0-9]", "-", anchor)
  anchor <- gsub("-+", "-", anchor)  # Replace multiple hyphens with single hyphen
  anchor <- gsub("^-|-$", "", anchor)  # Remove leading/trailing hyphens
  
  return(paste0("#", anchor))
}

#' Generate a table of contents for database documentation
#'
#' @param tables List of table names in the database
#' @return A markdown-formatted table of contents
#' @export
generate_table_of_contents <- function(tables) {
  if (length(tables) == 0) {
    return("*No tables found in database.*")
  }
  
  toc_lines <- c(
    "## Table of Contents",
    ""
  )
  
  # Add overview link
  toc_lines <- c(toc_lines, "- [Overview](#overview)")
  
  # Add table links
  for (table_name in tables) {
    formatted_name <- format_table_name(table_name)
    anchor <- create_anchor_link(table_name)
    toc_lines <- c(toc_lines, paste0("- [", formatted_name, "](", anchor, ")"))
  }
  
  toc_lines <- c(toc_lines, "", "---", "")
  
  return(paste(toc_lines, collapse = "\n"))
}

#' Format a data sample for Markdown display with proper escaping
#'
#' @param sample_data A data frame containing sample rows
#' @param max_width Maximum width for displayed values
#' @return A markdown-formatted table
#' @export
format_sample_data <- function(sample_data, max_width = 50) {
  if (nrow(sample_data) == 0) {
    return("*No data available for this table.*")
  }
  
  # Helper to escape and truncate values
  format_value <- function(value) {
    if (is.null(value) || is.na(value)) {
      return("NULL")
    } else if (inherits(value, "Date") || inherits(value, "POSIXct")) {
      return(as.character(value))
    } else if (is.numeric(value)) {
      return(format(value, scientific = FALSE, big.mark = ","))
    } else {
      # Truncate long text values
      text <- as.character(value)
      # Escape pipes for markdown tables
      text <- gsub("\\|", "\\\\|", text)
      if (nchar(text) > max_width) {
        text <- paste0(substr(text, 1, max_width - 3), "...")
      }
      return(text)
    }
  }
  
  # Generate header row
  column_names <- names(sample_data)
  header_row <- paste0("| ", paste(column_names, collapse = " | "), " |")
  separator <- paste0("| ", paste(rep("---", length(column_names)), collapse = " | "), " |")
  
  # Generate data rows
  data_rows <- character(nrow(sample_data))
  for (i in 1:nrow(sample_data)) {
    row_values <- sapply(1:ncol(sample_data), function(j) {
      format_value(sample_data[i, j])
    })
    data_rows[i] <- paste0("| ", paste(row_values, collapse = " | "), " |")
  }
  
  # Combine all rows
  result <- c(header_row, separator, data_rows)
  return(paste(result, collapse = "\n"))
}

#' Detect primary keys in a table
#'
#' @param conn A database connection
#' @param table_name The name of the table to examine
#' @return A character vector of likely primary key columns
#' @export
detect_primary_keys <- function(conn, table_name) {
  # This is a simple heuristic approach since getting actual constraints
  # can be database-specific
  
  tryCatch({
    # Look for columns with "id" or "_id" in the name that have unique values
    query <- paste0("SELECT * FROM \"", table_name, "\" LIMIT 0")
    result <- DBI::dbGetQuery(conn, query)
    columns <- names(result)
    
    # Identify potential ID columns
    id_cols <- columns[grepl("_id$|^id$|^id_", tolower(columns))]
    
    # If no obvious ID columns, look for columns with unique values
    if (length(id_cols) == 0) {
      return(character(0))
    }
    
    # Check if these columns have unique values
    potential_keys <- character(0)
    for (col in id_cols) {
      # Check if column has unique values
      count_query <- paste0(
        "SELECT COUNT(DISTINCT \"", col, "\") AS distinct_count, ",
        "COUNT(*) AS total_count FROM \"", table_name, "\""
      )
      counts <- DBI::dbGetQuery(conn, count_query)
      
      # If distinct count equals total count, it's likely a key
      if (counts$distinct_count[1] == counts$total_count[1]) {
        potential_keys <- c(potential_keys, col)
      }
    }
    
    return(potential_keys)
  }, error = function(e) {
    message("Error detecting primary keys for table ", table_name, ": ", e$message)
    return(character(0))
  })
}

#' Create a timestamp string in the project's standard format
#'
#' @return A formatted timestamp string
#' @export
get_formatted_timestamp <- function() {
  format(Sys.time(), "%Y-%m-%d %H:%M:%S")
}