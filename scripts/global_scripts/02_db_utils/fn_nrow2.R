#' Safe version of nrow that returns 0 for any error or invalid result
#'
#' @description
#' A defensive wrapper around nrow() that handles error cases gracefully.
#' Returns 0 for NULL, empty vectors, NA results, or errors instead of failing.
#' Special handling for tbl/tbl_sql objects to efficiently count rows without materializing the data.
#'
#' @param x An object for which the number of rows will be determined
#' @return Integer. Number of rows in x, or 0 if x is NULL, invalid, or causes an error
#'
#' @examples
#' # Standard data frame
#' nrow2(data.frame(a = 1:3, b = 4:6))  # Returns 3
#'
#' # NULL input
#' nrow2(NULL)  # Returns 0
#'
#' # Object without rows
#' nrow2(1:10)  # Returns 0
#'
#' # Error cases
#' nrow2(non_existent_object)  # Returns 0 instead of error
#'
#' # With dplyr tbl object
#' # db_conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = "app_data.duckdb")
#' # df_ref <- dplyr::tbl(db_conn, "customer_profile")
#' # nrow2(df_ref) # Uses count query without materializing data
#'
#' @export
#' @implements R116 Enhanced Data Access

# 安全版 nrow2：任何錯誤或無效結果都回傳 0
nrow2 <- function(x) {
  # Check if NULL directly
  if (is.null(x)) {
    return(0L)
  }
  
  # Special handling for dplyr tbl objects (database queries)
  # Uses count() to efficiently count rows on the database side
  if (inherits(x, "tbl") || inherits(x, "tbl_sql")) {
    count_result <- tryCatch({
      # Use dplyr's count function which optimizes to a COUNT query
      # Then pull the first value
      dplyr::count(x) %>% 
        dplyr::pull() %>%
        as.integer()
    }, error = function(e) {
      # If count fails, try collecting and counting (less efficient)
      tryCatch({
        dplyr::collect(x) %>% nrow() %>% as.integer()
      }, error = function(e2) {
        NA
      })
    })
    
    # If count_result is NA or NULL, return 0
    if (is.null(count_result) || length(count_result) == 0 || is.na(count_result)) {
      return(0L)
    }
    
    return(as.integer(count_result))
  }
  
  # For all other objects, try the standard nrow approach
  n <- tryCatch(nrow(x), error = function(e) NA)
  
  # If result is NULL, length 0, or NA, return 0
  if (is.null(n) || length(n) == 0 || is.na(n)) {
    return(0L)
  }
  
  # Otherwise return the integer count
  as.integer(n)
}