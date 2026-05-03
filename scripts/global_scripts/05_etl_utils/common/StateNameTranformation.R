library(zipcodeR)
library(hash)
library(stringr)
library(data.table)
library(stringdist)

# 定義一個函數，找到最接近的 IANA 時區
find_closest_timezone <- function(timezone) {
  if (is.na(timezone)) {
    return(NA)  # 如果是 NA，直接返回 NA
  }
  
  # 計算與所有 IANA 時區的距離
  distances <- stringdist::stringdist(timezone, iana_timezones, method = "lv")
  
  # 返回距離最小的 IANA 時區
  closest_timezone <- iana_timezones[which.min(distances)]
  return(closest_timezone)
}



# ##########timezone Processing##########
# # 獲取所有的 IANA 時區名稱
# iana_timezones <- OlsonNames()
# 
# # 假設有一個資料框，裡面包含不完整或非標準的時區名稱
# zip_code_db <- data.frame(
#   timezone = c("Central", "Eastern", "Mountain", "Pacific", "America/Anchorage", "Hawaii", NA)
# )
# 
# # 定義一個函數，找到最接近的 IANA 時區
# find_closest_timezone <- function(timezone) {
#   if (is.na(timezone)) {
#     return(NA)  # 如果是 NA，直接返回 NA
#   }
#   # 計算與所有 IANA 時區的距離
#   distances <- stringdist::stringdist(timezone, iana_timezones, method = "lv")
#   
#   # 返回距離最小的 IANA 時區
#   closest_timezone <- iana_timezones[which.min(distances)]
#   return(closest_timezone)
# }
# 
# # 將函數應用到資料框的每一個時區
# zip_code_db$closest_timezone <- sapply(zip_code_db$timezone, find_closest_timezone)
# 
# # 查看轉換結果
# print(zip_code_db)
# 




reverse_zipcode_fast <- function(postal_codes, zip_code_db) {
  
  # 假设 zip_code_db 是你已有的邮政编码数据框，将其转换为 data.table
  zip_code_dt <- as.data.table(zip_code_db)
  
  # 将 "zipcode" 列设置为键
  setkey(zip_code_dt, zipcode)
  
  
  # 提取前五個數字，若提取失敗則設置為 "99999"
  extracted_codes <- str_extract(postal_codes, "\\d{5}")
  extracted_codes <- str_replace_na(extracted_codes, "99999")
  
  # 查找並返回结果
  result_dt <- zip_code_dt[J(extracted_codes), nomatch = NA]
  
  return(result_dt)
}

# convert_time_with_timezone_dt <- function(dt, time_col, timezone_col, new_col = "time_local") {
#   dt[, (new_col) := apply(.SD, 1, function(row) {
#     t <- row[[time_col]]
#     tz <- row[[timezone_col]]
#     
#     if (is.na(tz)) {
#       return(NA_character_)  # 如果時區是 NA，返回 NA
#     } else {
#       return(as.character(with_tz(t, tzone = tz)))  # 否則轉換時間
#     }
#   }), .SDcols = c(time_col, timezone_col)]  # 只選取需要的列
# }


convert_time_with_timezone <- function(time, timezone) {
  mapply(function(t, tz) {
    if (is.na(tz)) {
      return(NA)  # 如果時區是 NA，返回 NA
    } else {
      return(with_tz(t, tz))  # 否則使用 with_tz 轉換時區
    }
  }, time, timezone, SIMPLIFY = FALSE) 
}
