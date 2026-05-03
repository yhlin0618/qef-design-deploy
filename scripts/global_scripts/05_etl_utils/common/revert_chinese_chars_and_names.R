library(openxlsx)
library(tidyverse)
#library(arrow)
library(data.table)
library(stringr)


revert_chinese_chars_and_names <- function(input_tibble) {
  # 檢查原始列名是否全部為空（在這個場景下，我們假設已經知道轉換前的狀態）
  original_col_names <- names(input_tibble)
  has_names <- !all(is.na(original_col_names))
  
  # 複製tibble以避免直接修改原始數據
  result_tibble <- input_tibble
  
  # 逆轉換列值
  result_tibble <- result_tibble %>%
    mutate(across(where(is.character), ~revert_chinese_chars(.x)))
  
  # 如果原始資料有列名，則逆轉換列名
  if (has_names) {
    reverted_names <- revert_chinese_chars(original_col_names)
    names(result_tibble) <- reverted_names
  }
  
  return(result_tibble)
}