process_address_and_time_amazon_sales <- function(Data, SlowlyChangingData) {
  library(dplyr)
  library(dbplyr)
  library(data.table)
  
  # 從 global (SlowlyChangingData) 中讀取 ZIP 參考資料
  zip_code_db_ver2 <- tbl(SlowlyChangingData, "zip_code") %>% collect()
  
  # 讀取亞馬遜銷售資料，表名假設為 "amazon_sales_dta"
  Dta <- as.data.table(tbl(Data, "amazon_sales_dta") %>% collect())
  
  # 執行 ZIP 處理
  Dta2 <- process_zip(Dta, zip_code_db_ver2)
  
  Dta2 <- Dta2 %>% 
    filter(shipping_country_code == "US") %>%
    mutate(
      shipping_state = toupper(shipping_state),
      state = case_when(
        is.na(state) & nchar(shipping_state) == 2 ~ shipping_state,
        TRUE ~ state
      )
    ) %>% 
    filter(!is.na(state))
  
  # 執行時間處理
  Dta3 <- process_time(Dta2)
  
  # 最後將處理後的結果寫回資料庫，表名設定為 "amazon_sales_zip_dta"
  out_table <- "amazon_sales_zip_dta"
  dbWriteTable(Data, out_table, as_tibble(Dta3), overwrite = TRUE, temporary = FALSE)
  
  message(paste(out_table, "已處理並寫入資料庫。"))
  return(tbl(Data, out_table))
}