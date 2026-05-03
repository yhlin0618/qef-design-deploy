library(openxlsx)
library(tidyverse)
#library(arrow)
library(data.table)
library(stringr)

revert_chinese_chars <- function(input_strings) {
  # 保存原始輸入的屬性
  original_attributes <- attributes(input_strings)
  
  # 進行逆轉換（這裡是假定的逆轉換函數，需要替換為實際的逆轉換邏輯）
  reverted_strings <- sapply(input_strings, function(input_string) {
    if (is.na(input_string)) {
      return(NA)
    } else{
      matches <- gregexpr("CN(\\d+)", input_string)
      codes <- regmatches(input_string, matches)[[1]]
      
      if (length(codes) == 0) return(input_string)
      
      original_chars <- sapply(codes, function(code) {
        int_code <- as.integer(sub("CN", "", code))
        return(intToUtf8(int_code))
      })
      
      for (i in seq_along(codes)) {
        input_string <- sub(codes[i], original_chars[i], input_string)
      }
      
      return(input_string)
    }
  })
  
  # 恢復原始輸入的屬性到逆轉換後的結果
  attributes(reverted_strings) <- original_attributes
  
  return(reverted_strings)
}
