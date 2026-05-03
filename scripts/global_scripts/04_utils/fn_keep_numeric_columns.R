#' Keep Numeric Columns Function
#'
#' Retains only numeric columns in a data frame with optional exceptions
#'
#' @param data A data frame, data.table, or tibble
#' @param exceptions Character vector of column names to keep regardless of type
#' @return A data frame containing only numeric columns and exception columns
#'
#' @examples
#' df <- data.frame(a = 1:5, b = letters[1:5], c = 5:1)
#' keep_numeric_columns(df, exceptions = 'b')
keep_numeric_columns <- function(data, exceptions = NULL) {
  # 如果輸入的資料不是 data.table，將其轉換為 data.table
  if (!is.data.table(data)) {
    data <- as.data.table(data)
  }
  
  # 檢查每個欄位是否為數值型
  numeric_cols <- sapply(data, is.numeric)
  
  # 如果指定了例外欄位，將其設為 TRUE 以確保它們被保留
  if (!is.null(exceptions)) {
    exceptions <- exceptions[exceptions %in% names(data)] # 檢查例外欄位是否在資料框中
    numeric_cols[exceptions] <- TRUE
  }
  
  # 只保留數值型的欄位以及例外欄位
  data_numeric <- data[, numeric_cols, with = FALSE]
  
  # 返回處理後的數據
  return(data_numeric)
}
