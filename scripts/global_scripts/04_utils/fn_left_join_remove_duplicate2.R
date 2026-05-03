#' Enhanced Left Join with Duplicate Removal
#'
#' Performs a left join between two data frames and removes duplicate columns.
#' Designed to work with data.table objects for better performance.
#'
#' @param df1 Data.table or data.frame. The left data frame.
#' @param df2 Data.table or data.frame. The right data frame.
#' @param id String. The column name to join on. Defaults to "customer_id".
#'
#' @return data.table. The joined result with duplicated columns removed.
#'
#' @examples
#' # Join customer data with order data
#' result <- left_join_remove_duplicate2(customers, orders, id = "customer_id")
#'
left_join_remove_duplicate2 <- function(df1, df2, id = "customer_id") {
  # Ensure both dataframes are data.table
  setDT(df1)
  setDT(df2)
  
  # First take unique values from df2 by id to avoid duplicate key interference
  df2_unique <- unique(df2, by = id)
  
  # Perform left join, adding .y suffix to duplicate columns
  result <- merge(df1, df2_unique, by = id, all.x = TRUE, suffixes = c("", ".y"), allow.cartesian = TRUE)
  
  # Remove all duplicate columns ending with .y
  cols_to_remove <- grep("\\.y$", colnames(result), value = TRUE)
  result[, (cols_to_remove) := NULL]
  
  return(result)
}