library(openxlsx)
library(tidyverse)
#library(arrow)
library(data.table)
library(stringr)

convert_chinese_chars_and_names <- function(input_tibble) {
  # 檢查原始列名是否全部為空
  original_col_names <- names(input_tibble)
  has_names <- !all(is.na(original_col_names))
  
  # 複製tibble以避免直接修改原始數據
  result_tibble <- input_tibble
  
  # 轉換列值
  result_tibble <- result_tibble %>%
    mutate(across(where(is.character), ~convert_chinese_chars(.x)))
  
  # 如果原始資料有列名，則轉換列名
  if (has_names) {
    converted_names <- convert_chinese_chars(original_col_names)
    names(result_tibble) <- converted_names
  }
  
  return(result_tibble)
}