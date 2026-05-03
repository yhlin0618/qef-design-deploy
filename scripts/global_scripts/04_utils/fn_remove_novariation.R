#' Remove Columns with No Variation
#'
#' @title Filter Out Constant-Value Columns
#' @description Removes columns from a data frame that contain only a single unique
#' value (no variation), while preserving specified exception columns. This is useful
#' for data preprocessing to eliminate uninformative features that would not contribute
#' to analysis or modeling. The function handles NA values appropriately and can
#' preserve key identifier columns even if they have no variation.
#'
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - R050: Data.table vectorization
#' - P076: Error handling patterns
#'
#' @param data Data frame or data.table to process. Will be converted to data.table
#'        internally for efficient processing.
#' @param exceptions Character vector of column names to preserve regardless of
#'        variation. Default is c("asin") for Amazon product identifiers. Set to
#'        NULL to apply no exceptions.
#'        
#' @return Data.table with constant-value columns removed (except those in exceptions).
#'         Returns NULL with a warning if all columns would be removed.
#'         NA values are ignored when determining uniqueness.
#'         
#' @examples
#' library(data.table)
#' 
#' # Create sample data with mixed variation
#' df <- data.table(
#'   id = 1:5,                    # Has variation
#'   status = rep("active", 5),   # No variation
#'   score = c(10, 20, 30, 40, 50), # Has variation
#'   type = rep("A", 5)           # No variation
#' )
#' 
#' # Remove columns with no variation
#' cleaned <- remove_novariation(df, exceptions = NULL)
#' # Returns: data.table with 'id' and 'score' columns only
#' 
#' # Preserve identifier column even if constant
#' df2 <- data.table(
#'   asin = rep("B001", 10),
#'   price = rep(9.99, 10),
#'   rating = 1:10
#' )
#' cleaned2 <- remove_novariation(df2)  # 'asin' preserved by default
#' # Returns: data.table with 'asin' and 'rating' columns
#' 
#' # Handle NA values
#' df3 <- data.table(
#'   col1 = c(1, 1, 1, NA, 1),   # Only one unique non-NA value
#'   col2 = c(1, 2, 3, NA, 4)    # Multiple unique values
#' )
#' remove_novariation(df3, exceptions = NULL)
#' # Returns: data.table with 'col2' only
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle R050 Data.table vectorization
#' @note NA values are excluded when checking for unique values
remove_novariation <- function(data, exceptions = c("asin")) {
  # 確保資料是 data.table 格式
  if (!is.data.table(data)) {
    data <- as.data.table(data)
  }
  
  # 檢查例外欄位是否存在於資料中
  if (!is.null(exceptions)) {
    missing_exceptions <- exceptions[!exceptions %in% names(data)]
    if (length(missing_exceptions) > 0) {
      stop(paste("例外欄位不存在於資料中:", paste(missing_exceptions, collapse = ", ")))
    }
  }
  
  # 檢查每個欄位是否有變化
  non_var_cols <- sapply(data, function(x) {
    # 將 factors 轉為 characters，並處理 NA 值
    unique_values <- unique(as.character(x[!is.na(x)]))
    length(unique_values) == 1
  })
  
  # 如果有例外欄位，確保它們不被刪除
  if (!is.null(exceptions)) {
    non_var_cols[names(data) %in% exceptions] <- FALSE
  }
  
  # 移除無變化的欄位
  data_clean <- data[, !non_var_cols, with = FALSE]
  
  # 檢查清理後是否還有剩餘欄位
  if (ncol(data_clean) == 0) {
    warning("All columns have been removed due to lack of variation.")
    return(NULL)
  }
  
  # 返回清理後的資料
  return(data_clean)
}


# 使用範例
# 假設你有一個數據框名為Design.product
# Design.product2 <- remove_novariation(Design.product)


# library(data.table)

# 創建一個data.table測試集
# test_data <- data.table(
#   ID = 1:10,                         # 有變異
#   Gender = c(rep("Male", 5), rep("Female", 5)),  # 有變異
#   Age = rep(30, 10),                 # 沒有變異
#   Height = c(170, 175, 180, 165, 160, 170, 175, 180, 165, 160),  # 有變異
#   Weight = rep(70, 10),              # 沒有變異
#   Score = 1:10                       # 有變異
# )
# 
# # 檢查生成的測試集
# print(test_data)
# 
# # 測試 remove_novariation 函數
# cleaned_data <- remove_novariation(test_data)
# 
# # 檢查處理後的數據
# print(cleaned_data)
