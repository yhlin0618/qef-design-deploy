query_sales_by_time_zip <- function(app_data) {
  # 從 app_data 數據庫中獲取 sales_by_time_zip_dta 表的數據
  sales_by_time_zip_dta <- tbl(app_data, "sales_by_time_zip_dta") %>% collect()
  
  # 若數據為空，返回空數據框
  if (nrow(sales_by_time_zip_dta) == 0) {
    message("sales_by_time_zip_dta 表中沒有數據。")
    return(tibble())
  }
  
  # 將 time 列轉換為 POSIXct 類型（如果尚未是該類型）
  if (!inherits(sales_by_time_zip_dta$time, "POSIXct")) {
    sales_by_time_zip_dta <- sales_by_time_zip_dta %>%
      mutate(time = as.POSIXct(time))
  }
  
  # 按時間排序
  sales_by_time_zip_sorted <- sales_by_time_zip_dta %>%
    arrange(time)
  
  # 獲取最近的一個月時間範圍
  latest_time <- max(sales_by_time_zip_sorted$time, na.rm = TRUE)
  one_month_ago <- latest_time - as.difftime(30, units = "days")
  
  # 過濾最近一個月的數據
  recent_sales <- sales_by_time_zip_sorted %>%
    filter(time >= one_month_ago)
  
  # 如果需要，可以在這裡進行其他處理，例如計算統計數據，生成匯總報告等
  
  # 返回處理後的數據
  message("已查詢並處理 sales_by_time_zip_dta 表中的數據。")
  return(recent_sales)
}