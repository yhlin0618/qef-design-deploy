#' Verify Row Alignment Between Tables
#'
#' Following P071: Row-Aligned Tables principle
verify_row_alignment <- function(table1, table2, key_column) {
  # Check if key column exists in both tables
  if (!key_column %in% names(table1) || !key_column %in% names(table2)) {
    warning("Key column '", key_column, "' not found in both tables")
    return(FALSE)
  }
  
  # Check row count match
  if (nrow(table1) != nrow(table2)) {
    warning("Tables have different row counts (", nrow(table1), " vs ", nrow(table2), ")")
    return(FALSE)
  }
  
  # Check key column values match in same order
  identical(table1[[key_column]], table2[[key_column]])
}

#' Load and combine customer data efficiently
#'
#' Loads customer profile and DNA data from database and combines them
#' using P71 row-alignment for efficiency. Implements both P71 (Row-Aligned Tables)
#' and P74 (Reactive Data Filtering) by filtering data at the database level.
#'
#' @param conn A database connection
#' @param platform_filter Optional platform ID to filter by
#' @param customer_filter Optional vector of customer_ids to filter by
#' @param value_threshold Optional minimum value threshold for customer filtering
#' @param limit Optional maximum number of rows to return
#' @param use_cache Whether to cache query results for performance (default: TRUE)
#' @return A combined data frame with customer profile and DNA data
#' @export
