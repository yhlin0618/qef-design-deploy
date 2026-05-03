query_nes_trend <- function(app_data) {
  # 從 app_data 數據庫中獲取 nes_trend 表的數據
  nes_trend <- tbl(app_data, "nes_trend") %>% collect()
  
  # 若數據為空，返回空數據框
  if (nrow(nes_trend) == 0) {
    message("nes_trend 表中沒有數據。")
    return(tibble())
  }
  
  # 將 time 列轉換為 POSIXct 類型（如果尚未是該類型）
  if (!inherits(nes_trend$time, "POSIXct")) {
    nes_trend <- nes_trend %>%
      mutate(time = as.POSIXct(time))
  }
  
  # 按時間排序
  nes_trend_sorted <- nes_trend %>%
    arrange(time)
  
  # 獲取最近的一年時間範圍
  latest_time <- max(nes_trend_sorted$time, na.rm = TRUE)
  one_year_ago <- latest_time - as.difftime(365, units = "days")
  
  # 過濾最近一年的數據
  recent_nes_trend <- nes_trend_sorted %>%
    filter(time >= one_year_ago)
  
  # 計算每個月的 NES 平均值
  monthly_nes <- recent_nes_trend %>%
    mutate(month = floor_date(time, "month")) %>%
    group_by(month, source) %>%
    summarise(
      avg_nes = mean(nes, na.rm = TRUE),
      count = n(),
      .groups = "drop"
    )
  
  # 返回處理後的數據
  message("已查詢並處理 nes_trend 表中的數據。")
  return(list(
    raw = recent_nes_trend,
    monthly = monthly_nes
  ))
}