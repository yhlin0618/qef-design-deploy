#' Remove Na Rows in Columns Function
#'
#' Brief description of what this function does
#'
#' @param params Description of parameters
#' @return Description of return value
#'
#' @examples
#' remove_na_rows_in_columns()
remove_na_rows_in_columns <- function(data, columns) {
  # 確保指定的欄位都存在於資料框中
  missing_columns <- setdiff(columns, names(data))
  if (length(missing_columns) > 0) {
    stop(paste("以下欄位不存在於資料框中:", paste(missing_columns, collapse = ", ")))
  }
  
  # 如果是 data.table，使用 data.table 語法處理；否則，使用 data.frame 標準語法
  if (inherits(data, "data.table")) {
    # 保留指定欄位中不含 NA 值的列
    data_clean <- data[complete.cases(data[, ..columns]), ]
  } else {
    # 若為 data.frame 或 tibble，使用標準的索引方法
    data_clean <- data[complete.cases(data[, columns, drop = FALSE]), ]
  }
  
  # 返回處理後的數據
  return(data_clean)
}
