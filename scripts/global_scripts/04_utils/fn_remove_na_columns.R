#' Remove Columns Containing NA Values
#'
#' Removes all columns from a data frame that contain any NA values.
#' Handles different data frame types (data.table, tibble, data.frame)
#' appropriately to maintain object class.
#'
#' Following principles:
#' - R021: One function one file rule
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function)
#' - R067: Functional encapsulation
#'
#' @param data A data frame, data.table, or tibble to clean
#' @return A data frame of the same class as input with all NA-containing
#'         columns removed. Returns empty data frame if all columns contain NA.
#' @export
#'
#' @examples
#' # Create sample data with NA values
#' df <- data.frame(
#'   a = c(1, 2, 3),
#'   b = c(NA, 2, 3),
#'   c = c(1, 2, 3)
#' )
#' remove_na_columns(df)  # Returns columns 'a' and 'c' only
#' 
#' # Works with tibbles
#' if (requireNamespace("tibble", quietly = TRUE)) {
#'   tbl <- tibble::tibble(x = 1:3, y = c(NA, 2, 3))
#'   remove_na_columns(tbl)  # Returns only column 'x'
#' }
#'
remove_na_columns <- function(data) {
  # Validate input
  if (!is.data.frame(data)) {
    stop("Input must be a data frame, data.table, or tibble")
  }
  
  # Check each column for NA values
  non_na_cols <- colSums(is.na(data)) == 0
  
  # Handle different data frame types to maintain class
  if (inherits(data, "data.table")) {
    # For data.table, use proper syntax with which() and with = FALSE
    data_clean <- data[, which(non_na_cols), with = FALSE]
  } else if (inherits(data, "tbl_df")) {
    # For tibble, preserve tibble class
    data_clean <- data[, non_na_cols, drop = FALSE]
  } else {
    # For standard data.frame
    data_clean <- data[, non_na_cols, drop = FALSE]
  }
  
  # Return cleaned data
  return(data_clean)
}




