#' Coalesce Columns with Suffixes
#'
#' Detects columns with suffixes (e.g., column_1, column_2) and coalesces them into a single column.
#' This is useful when merging tables that may have duplicate column names.
#' Part of the D03_11 step in the Positioning Analysis derivation flow.
#'
#' @param df A data frame containing columns with suffixes to coalesce.
#' @param pattern Character. Regex pattern to detect suffix (default: "(.+)_\\d+$").
#' @param keep_suffix Logical. Whether to keep the suffix in the column name (default: FALSE).
#'
#' @return A data frame with coalesced columns.
#'
#' @examples
#' \dontrun{
#' # Example data frame with duplicate columns
#' df <- data.frame(
#'   id = 1:3,
#'   name_1 = c("A", "B", NA),
#'   name_2 = c(NA, NA, "C"),
#'   value_1 = c(10, NA, 30),
#'   value_2 = c(NA, 20, NA)
#' )
#' 
#' # Coalesce columns
#' coalesce_suffix_cols(df)
#' }
#'
#' @export
coalesce_suffix_cols <- function(df, pattern = "(.+)_\\d+$", keep_suffix = FALSE) {
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("tidyr", quietly = TRUE)) library(tidyr)
  if (!requireNamespace("stringr", quietly = TRUE)) library(stringr)
  
  # Get column names
  all_cols <- colnames(df)
  
  # Extract base names and identify columns with suffixes
  base_names <- stringr::str_match(all_cols, pattern)[, 2]
  
  # Also check for columns that appear both with and without suffix
  # For example: "卓越工藝" and "卓越工藝_1"
  cols_without_suffix <- all_cols[!grepl("_\\d+$", all_cols)]
  potential_duplicates <- character()
  
  for (col in cols_without_suffix) {
    # Check if there are columns with this name + suffix
    if (any(grepl(paste0("^", stringr::str_escape(col), "_\\d+$"), all_cols))) {
      potential_duplicates <- c(potential_duplicates, col)
    }
  }
  
  # Combine both types of duplicates
  to_coalesce <- c(na.omit(base_names), potential_duplicates)
  to_coalesce <- to_coalesce[duplicated(to_coalesce) | duplicated(to_coalesce, fromLast = TRUE)]
  to_coalesce <- unique(to_coalesce)
  
  # No columns to coalesce
  if (length(to_coalesce) == 0) {
    message("No duplicate columns found to coalesce")
    return(df)
  }
  
  # Process each base name
  result_df <- df
  
  for (base_name in to_coalesce) {
    # Find all columns with this base name
    # Include both the base name itself and any suffixed versions
    base_name_escaped <- stringr::str_escape(base_name)
    matching_cols <- all_cols[all_cols == base_name | grepl(paste0("^", base_name_escaped, "_\\d+$"), all_cols)]
    
    if (length(matching_cols) <= 1) {
      next  # Skip if only one column matches
    }
    
    message("Coalescing columns: ", paste(matching_cols, collapse = ", "), 
            " into ", if (keep_suffix) matching_cols[1] else base_name)
    
    # Coalesce the columns
    coalesced_values <- result_df %>%
      dplyr::select(dplyr::all_of(matching_cols)) %>%
      tidyr::fill(dplyr::everything(), .direction = "downup") %>%
      dplyr::transmute(
        result = dplyr::coalesce(!!!rlang::syms(matching_cols))
      ) %>%
      dplyr::pull(result)
    
    # Add the coalesced column
    new_col_name <- if (keep_suffix) matching_cols[1] else base_name
    result_df[[new_col_name]] <- coalesced_values
    
    # Remove the original columns if not keeping the first one
    if (keep_suffix) {
      result_df <- result_df %>% dplyr::select(-dplyr::all_of(matching_cols[-1]))
    } else {
      result_df <- result_df %>% dplyr::select(-dplyr::all_of(matching_cols))
    }
  }
  
  # Return the result
  return(result_df)
}