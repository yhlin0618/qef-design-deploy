update_time_range <- function(source_conn, table_name, target_conn, date_col, date_transform = as.Date) {
  library(dplyr)
  library(dbplyr)
  # 若 date_col 為字串，則轉換成 symbol
  col_sym <- if (is.character(date_col)) { sym(date_col) } else { enquo(date_col) }
  
  # 從來源表計算日期範圍
  summary_df <- tbl(source_conn, table_name) %>%
    summarise(
      start_date = min(!!col_sym, na.rm = TRUE),
      end_date   = max(!!col_sym, na.rm = TRUE)
    ) %>%
    mutate(
      start_date = as.Date(start_date),
      end_date   = as.Date(end_date),
      data_name  = table_name
    ) %>%
    collect()
  
  # 在目標資料庫中，先刪除已存在相同 data_name 的記錄（以 data_name 為主鍵）
  delete_sql <- paste0("DELETE FROM time_range WHERE data_name = '", table_name, "';")
  dbExecute(target_conn, delete_sql)
  
  # 將計算結果寫入目標資料庫中的 time_range 表（直接覆蓋）
  dbWriteTable(
    conn = target_conn,
    name = "time_range",
    value = summary_df,
    append = TRUE,  # 插入新記錄
    row.names = FALSE
  )
  
  message("Time range for ", table_name, " is updated.")
  return(summary_df)
}
