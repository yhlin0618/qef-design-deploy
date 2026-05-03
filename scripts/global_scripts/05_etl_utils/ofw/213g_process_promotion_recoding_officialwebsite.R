process_promotion_recoding_officialwebsite <- function(Data, Promotion) {
  library(dplyr)
  library(data.table)
  library(dbplyr)
  
  # 過濾 Promotion 資料：取出平台為 "官網" 的促銷資料，並根據 "折扣%" 排序
  Promotion_official <- Promotion[`平台` == "官網"]
  setorder(Promotion_official, '折扣%')
  
  # 讀取官方網站銷售資料表，假設表名為 "officialwebsite_sales_zip_dta"
  example_data_off <- tbl(Data, "officialwebsite_sales_zip_dta") %>% collect() %>% as.data.table()
  
  # 取出 sku 的第一部分（以 "-" 為分隔符）
  example_data_off[, sku_sub := tstrsplit(sku, "-", keep = 1)]
  
  # 初始化欄位：reuse、Promotion_type、discount_percent 預設為 0
  example_data_off[, `:=`(reuse = 0, Promotion_type = 0, discount_percent = 0)]
  
  for (i in 1:nrow(Promotion_official)) {
    product_list <- strsplit(Promotion_official$sku[i], ",")[[1]]
    message("Processing Official website promotion row: ", i)
    start_time <- Promotion_official$start_time[i]
    end_time   <- Promotion_official$end_time[i]
    re_val     <- Promotion_official$`是否可以重複使用`[i]
    P_t_val    <- Promotion_official$`促銷類型`[i]
    d_p        <- Promotion_official$`折扣%`[i]
    
    if (is.na(start_time) | is.na(end_time)) {
      message("Row ", i, ": start_time or end_time is NA; skipping this promotion.")
    } else {
      if (product_list[1] == "ALL") {
        example_data_off[(time >= start_time) & (time <= end_time), 
                         `:=`(reuse = re_val, Promotion_type = P_t_val, discount_percent = d_p)]
      } else {
        example_data_off[sku_sub %in% product_list & (time >= start_time) & (time <= end_time), 
                         `:=`(reuse = re_val, Promotion_type = P_t_val, discount_percent = d_p)]
      }
    }
  }
  
  # 將更新後的資料寫回資料庫（覆蓋原表）
  dbWriteTable(Data, "officialwebsite_sales_zip_dta", as_tibble(example_data_off), overwrite = TRUE, temporary = FALSE)
  message("Official website promotion recoding completed. Table 'officialwebsite_sales_zip_dta' updated.")
  
  return(as_tibble(example_data_off))
}