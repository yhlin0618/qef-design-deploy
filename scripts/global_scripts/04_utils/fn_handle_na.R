#' Handle Na Function
#'
#' Brief description of what this function does
#'
#' @param params Description of parameters
#' @return Description of return value
#'
#' @examples
#' handle_na()
library(tibble)
library(dplyr)

handle_na <- function(my_tibble) {
  for (col_name in names(my_tibble)) {
    # 檢查欄位是否包含NA值
    if (any(is.na(my_tibble[[col_name]]))) {
      # 建立一個新的標記欄位，標記NA的位置
      my_tibble[[paste0(col_name, "_NA")]] <- ifelse(is.na(my_tibble[[col_name]]), 1, 0)
      
      # 判斷是否為二元變量（只含0和1）
      if (all(my_tibble[[col_name]] %in% c(0, 1, NA), na.rm = TRUE)) {
        # 對於二元變量，將NA替換為0
        my_tibble[[col_name]][is.na(my_tibble[[col_name]])] <- 0
      } else {
        # 對於非二元變量，將NA替換為平均值
        mean_val <- mean(my_tibble[[col_name]], na.rm = TRUE)
        my_tibble[[col_name]][is.na(my_tibble[[col_name]])] <- mean_val
      }
    }
  }
  return(my_tibble)
}

# 示範使用
# 創建一個包含NA值的tibble進行測試
#test_tibble <- tibble(
#  a = c(1, 2, NA, 4),
#  b = c(NA, 0, 1, 1),
#  c = c(3.5, NA, 4.5, 5)
#)

# 處理NA值
#processed_tibble <- handle_na(test_tibble)
#print(processed_tibble)
