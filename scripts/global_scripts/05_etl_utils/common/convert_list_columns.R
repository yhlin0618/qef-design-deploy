library(dplyr)

# 定義函數來轉換 list 類型的列
convert_list_columns <- function(df) {
  df %>%
    mutate(across(where(is.list), ~ {
      # 如果 list 中有空元素，將其填補為 NA
      . <- lapply(., function(x) if (length(x) == 0) NA else x)
      
      # 嘗試轉換為 double
      try_double <- tryCatch(as.double(unlist(.)), warning = function(w) NULL, error = function(e) NULL)
      if (!is.null(try_double)) {
        return(try_double)
      } else {
        # 如果無法轉換為 double，則轉換為 character
        try_character <- tryCatch(as.character(unlist(.)), warning = function(w) NULL, error = function(e) NULL)
        if (!is.null(try_character)) {
          return(try_character)
        } else {
          # 如果無法轉換為 character，則返回原始值（保留 list）
          return(.)
        }
      }
    }))
}
# 
# # 使用示例 dataframe
# df <- data.frame(
#   a = I(list(1, 2, 3)),
#   b = I(list("x", "y", "z")),
#   c = 1:3,
#   d = I(list(NULL, numeric(0), "test")),  # 包含空元素的list
#   stringsAsFactors = FALSE
# )
# 
# # 應用函數
# df_converted <- convert_list_columns(df)
# 
# print(df_converted)