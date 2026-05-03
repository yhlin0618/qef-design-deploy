#' Data Operations Utility Functions
#'
#' This file contains utility functions for common data operations,
#' optimized for the MAMBA application's data structures.
#'
#' @author Claude
#' @date 2025-04-07

#' Combine row-aligned tables efficiently
#'
#' This function combines two row-aligned tables using dplyr::bind_cols,
#' which is significantly faster than joins for tables that follow the P71 principle.
#' 
#' @param table1 The first data frame
#' @param table2 The second data frame
#' @param key_column The key column used for alignment verification (default: "customer_id")
#' @param verify Whether to verify row alignment before binding (default: TRUE)
#' @param fallback_to_join Whether to fall back to a join operation if tables aren't aligned (default: FALSE)
#' @param handle_duplicates How to handle duplicate column names: "error", "suffix", "first_only", or "second_only" (default: "suffix")
#' @return A combined data frame with columns from both tables
#' @export
combine_aligned_tables <- function(table1, table2, 
                                  key_column = "customer_id", 
                                  verify = TRUE,
                                  fallback_to_join = FALSE,
                                  handle_duplicates = "suffix") {
  # Check for required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required but not installed")
  }
  
  # Verify row alignment if requested (implements P71)
  is_aligned <- TRUE
  
  if (verify) {
    if (!key_column %in% names(table1) || !key_column %in% names(table2)) {
      message("Key column '", key_column, "' not found in both tables")
      is_aligned <- FALSE
    } else if (!identical(table1[[key_column]], table2[[key_column]])) {
      message("Tables are not row-aligned by '", key_column, "'")
      is_aligned <- FALSE
    }
    
    # Fall back to join if not aligned and fallback is enabled
    if (!is_aligned && fallback_to_join) {
      message("Falling back to join operation instead of column binding")
      return(dplyr::left_join(table1, table2, by = key_column))
    } else if (!is_aligned) {
      stop("Tables are not row-aligned by '", key_column, "'. Set fallback_to_join=TRUE to use join operations instead.")
    }
  }
  
  # Handle duplicate columns based on the specified strategy
  duplicate_cols <- intersect(names(table1), names(table2))
  duplicate_cols <- duplicate_cols[duplicate_cols != key_column]
  
  if (length(duplicate_cols) > 0) {
    if (handle_duplicates == "error") {
      stop("Duplicate column names found: ", paste(duplicate_cols, collapse = ", "))
    } else if (handle_duplicates == "suffix") {
      # Add suffixes to duplicate columns
      names(table2)[names(table2) %in% duplicate_cols] <- 
        paste0(names(table2)[names(table2) %in% duplicate_cols], "_2")
    } else if (handle_duplicates == "first_only") {
      # Remove duplicate columns from the second table
      table2 <- table2[, !names(table2) %in% duplicate_cols]
    } else if (handle_duplicates == "second_only") {
      # Remove duplicate columns from the first table
      table1 <- table1[, !names(table1) %in% duplicate_cols]
    }
  }
  
  # Remove duplicate key column from the second table if it exists
  if (key_column %in% names(table2)) {
    table2 <- table2[, names(table2) != key_column]
  }
  
  # Use fast column binding with dplyr (faster than base cbind for data frames)
  combined_data <- dplyr::bind_cols(table1, table2)
  
  return(combined_data)
}

#' Verify row alignment between tables
#'
#' Checks if two tables are properly row-aligned according to P71 principle
#'
#' @param table1 First data frame
#' @param table2 Second data frame
#' @param key_column Column name to check for alignment
#' @return TRUE if tables are row-aligned, FALSE otherwise
#' @export
