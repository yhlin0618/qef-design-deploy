process_and_save_all_products_states_and_times <- function(con_source, source, product_line_id_vec, final_db) {
  library(dplyr)
  library(data.table)
  library(dbplyr)
  library(lubridate)
  
  # 讀取來源資料表，表名格式為 "<source>_sales_zip_dta"
  sales_dt <- tbl(con_source, paste0(source, "_sales_zip_dta")) %>% 
    collect() %>% 
    as.data.table()
  
  # 設定 key 與排序
  setkey(sales_dt, state, product_line_id, time)
  setorder(sales_dt, customer_id, time)
  
  # 計算最大時間，並依此建立時間條件（無需外部輸入）
  time_end <- max(sales_dt$time, na.rm = TRUE)
  time_conditions <- list(
    m1year    = time_end %m+% years(-1),
    m1quarter = time_end %m+% months(-3),
    m1month   = time_end %m+% months(-1),
    now       = Inf  # now 表示無時間限制
  )
  
  # 取得所有 unique 的 state，加上 "ALL" 表示不進行 state 篩選
  states_vec <- c(unique(sales_dt$state), "ALL")
  
  for (state_i in states_vec) {
    # 若 state 不為 "ALL"，則篩選出該 state 資料，否則不進行篩選
    if (state_i != "ALL") {
      dt_state_filtered <- sales_dt[state == state_i]
    } else {
      dt_state_filtered <- sales_dt
    }
    
    # 遍歷每個產品線 (product_line_id_vec 已預設為字串)
    for (pl in product_line_id_vec) {
      if (pl != "000") {
        dt_filtered <- dt_state_filtered[product_line_id == pl]
      } else {
        dt_filtered <- dt_state_filtered
      }
      
      # 遍歷所有自動計算的時間條件
      for (time_label in names(time_conditions)) {
        condition <- time_conditions[[time_label]]
        
        # 當時間條件不是 "now" 時，僅保留 time 小於條件的資料
        if (time_label != "now") {
          dt_time_filtered <- dt_filtered[time < condition]
        } else {
          dt_time_filtered <- dt_filtered
        }
        
        # 呼叫 process_by_customer_time() 處理每個分組資料（此函數需事先定義）
        dta <- process_by_customer_time(dt_time_filtered) %>% as_tibble()
        
        # 定義分組表名稱，例如 "amazon_001_ALL_m1year"
        out_table_name <- paste(source, pl, state_i, time_label, sep = "_")
        
        # 將分組結果寫入 final_db 中的獨立分表（覆蓋模式）
        dbWriteTable(final_db, out_table_name, dta, overwrite = TRUE, temporary = FALSE)
        
        # 將該分組結果附加到聚合表 "sales_by_customer_date_dta"
        aggregated_data <- dta %>% 
          mutate(
            source_filter = source,
            state_filter = state_i,
            product_line_id_filter = pl,
            time_condition_filter = time_label
          )
        dbWriteTable(con_source, "sales_by_customer_date_dta", aggregated_data, append = TRUE)
        
        message(paste0("Processed data for product_line ", pl, " in state ", state_i, 
                       " with time condition ", time_label, " saved as '", out_table_name, "'."))
      }
    }
  }
}
