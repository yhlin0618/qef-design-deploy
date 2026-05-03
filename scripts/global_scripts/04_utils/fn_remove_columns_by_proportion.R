#' Remove Columns Based on Proportion Function
#'
#' Brief description of what this function does
#'
#' @param params Description of parameters
#' @return Description of return value
#'
#' @examples
#' remove_columns_based_on_proportion()
library(data.table)

# 定義函數來移除基於單一數值比例閾值的列，並保護特定列不被移除
remove_columns_based_on_proportion <- function(dt, threshold, protected_cols = NULL) {
  if (!inherits(dt, "data.table")) {
    stop("Input must be a data.table")
  }
  
  # 計算每列中任何單一數值的最大比例
  max_proportions <- dt[, lapply(.SD, function(x) max(table(x) / length(x), na.rm = TRUE))]
  
  # 確保保護的列不在移除列表中
  cols_to_remove <- which((max_proportions > threshold) & !names(max_proportions) %in% protected_cols)
  
  # 移除條件滿足的列
  if (length(cols_to_remove) > 0) {
    removed_columns <- names(max_proportions)[cols_to_remove]
    dt[, (removed_columns) := NULL]
    message("Removed columns: ", paste(removed_columns, collapse = ", "))
  } else {
    message("No columns meet the criteria for removal.")
  }
  
  # 返回修改後的 data.table
  return(dt)
}
# 
# # 創建一個示例 data.table
# DT <- data.table(A = c(1, 1, 1, 1, 0), B = c(1, 1, 1, 1, 1), C = c(0, 0, 1, 0, 0))
# 
# # 設置閾值和保護列，並應用函數
# threshold_value <- 0.8
# protected_columns <- c("B")  # 即使 B 列的比例為 100% 1，也不會被移除
# new_DT <- remove_columns_based_on_proportion(DT, threshold_value, protected_columns)
# 
# # 查看處理後的 data.table
# print(new_DT)
