process_address_and_time_officialwebsite_sales <- function(Data, SlowlyChangingData) {
  library(dplyr)
  library(dbplyr)
  library(data.table)
  library(stringr)
  
  # 從 SlowlyChangingData 中讀取 ZIP 參考資料
  zip_code_db_ver2 <- tbl(SlowlyChangingData, "zip_code") %>% collect()
  
  # 讀取官方網站銷售資料，假設資料表名稱為 "officialwebsite_sales_dta"
  Dta <- as.data.table(tbl(Data, "officialwebsite_sales_dta") %>% collect())
  
  # 使用 process_zip() 處理 ZIP，傳入 source_i = "officialwebsite"
  Dta2 <- process_zip(Dta, zip_code_db_ver2, source_i = "officialwebsite")
  
  # 官方網站資料處理：僅保留 shipping_country 為 "US" 的資料，
  # 並根據 shipping_province 處理 state 欄位（轉大寫、若 state 為 NA 且 shipping_province 長度為 2 則使用 shipping_province）
  Dta2 <- Dta2 %>% 
    filter(shipping_country == "US") %>%
    mutate(
      shipping_province = toupper(shipping_province),
      state = case_when(
        is.na(state) & nchar(shipping_province) == 2 ~ shipping_province,
        TRUE ~ state
      )
    ) %>% 
    filter(!is.na(state))
  
  # 執行時間處理（假設 process_time() 已定義，會建立 time_Local 與 Day_time 欄位）
  Dta3 <- process_time(Dta2)
  
  # 將最終處理結果寫回資料庫，表名設定為 "officialwebsite_sales_zip_dta"
  out_table <- "officialwebsite_sales_zip_dta"
  dbWriteTable(Data, out_table, as_tibble(Dta3), overwrite = TRUE, temporary = FALSE)
  
  message(paste(out_table, "已處理並寫入資料庫。"))
  return(tbl(Data, out_table))
}