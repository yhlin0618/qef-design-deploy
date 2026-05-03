#' Clean Duplicate Column Names
#'
#' Removes duplicate column names from a data frame by keeping only the first occurrence.
#' This is useful before merging tables to prevent suffix generation.
#'
#' @param df A data frame potentially containing duplicate column names.
#' @param keep_strategy Character. Strategy for handling duplicates: "first", "last", or "merge" (default: "first").
#'
#' @return A data frame with unique column names.
#'
#' @examples
#' \dontrun{
#' # Example with duplicate columns
#' df <- data.frame(
#'   id = 1:3,
#'   quality = c(4, 5, 3),
#'   quality = c(5, 4, 3),  # Duplicate name
#'   price = c(100, 200, 150),
#'   check.names = FALSE
#' )
#' 
#' # Clean duplicates
#' clean_duplicate_columns(df)
#' }
#'
#' @export
clean_duplicate_columns <- function(df, keep_strategy = "first") {
  # Get column names
  col_names <- colnames(df)
  
  # Find duplicates
  dup_cols <- col_names[duplicated(col_names)]
  
  if (length(dup_cols) == 0) {
    return(df)  # No duplicates
  }
  
  message("Found duplicate columns: ", paste(unique(dup_cols), collapse = ", "))
  
  if (keep_strategy == "first") {
    # Keep only first occurrence
    keep_indices <- !duplicated(col_names)
    result_df <- df[, keep_indices, drop = FALSE]
  } else if (keep_strategy == "last") {
    # Keep only last occurrence
    keep_indices <- !duplicated(col_names, fromLast = TRUE)
    result_df <- df[, keep_indices, drop = FALSE]
  } else if (keep_strategy == "merge") {
    # Merge duplicate columns using coalesce
    result_df <- df
    
    for (dup_name in unique(dup_cols)) {
      # Find all indices of this duplicate
      indices <- which(col_names == dup_name)
      
      # Coalesce values
      coalesced_values <- do.call(dplyr::coalesce, df[indices])
      
      # Keep first column with coalesced values
      result_df[[indices[1]]] <- coalesced_values
      
      # Remove other duplicate columns
      if (length(indices) > 1) {
        result_df <- result_df[, -indices[-1], drop = FALSE]
        col_names <- colnames(result_df)
      }
    }
  } else {
    stop("Invalid keep_strategy. Use 'first', 'last', or 'merge'.")
  }
  
  message("Cleaned data frame has ", ncol(result_df), " columns (removed ", 
          ncol(df) - ncol(result_df), " duplicates)")
  
  return(result_df)
}