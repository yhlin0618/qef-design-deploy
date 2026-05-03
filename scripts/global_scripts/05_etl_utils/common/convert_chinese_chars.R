library(openxlsx)
library(tidyverse)
#library(arrow)
library(data.table)
library(stringr)

convert_chinese_chars <- function(input_strings) {
  # 保存原始輸入的屬性
  original_attributes <- attributes(input_strings)
  
  # 進行轉換（這裡是假定的轉換函數，需要替換為實際的轉換邏輯）
  converted_strings <- sapply(input_strings, function(input_string) {
    if (is.na(input_string)) {
      return(NA)
    } else {
      chars <- unlist(strsplit(input_string, ""))
      converted_chars <- sapply(chars, function(char) {
        if (grepl("[\u4e00-\u9fff]", char)) {
          paste0("CN", utf8ToInt(char))
        } else {
          char
        }
      })
      return(paste(converted_chars, collapse = ""))
    }
  })
  
  # 恢復原始輸入的屬性到轉換後的結果
  attributes(converted_strings) <- original_attributes
  
  return(converted_strings)
}
