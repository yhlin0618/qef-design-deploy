process_promotion_recoding_amazon <- function(Data, Promotion) {
  library(dplyr)
  library(data.table)
  library(dbplyr)
  
  # 過濾 Promotion 資料：取出平台為 "Amazon" 的促銷資料，並根據 "折扣%" 排序
  Promotion_amazon <- Promotion[`平台` == "Amazon"]
  setorder(Promotion_amazon, '折扣%')
  
  # 讀取 Amazon 銷售資料表，假設表名為 "amazon_sales_zip_dta"
  example_data <- tbl(Data, "amazon_sales_zip_dta") %>% collect() %>% as.data.table()
  
  # 取出 sku 的第一部分（以 "-" 為分隔符）
  example_data[, sku_sub := tstrsplit(sku, "-", keep = 1)]
  
  # 初始化欄位：reuse、Promotion_type、discount_percent 均預設為 0
  example_data[, `:=`(reuse = 0, Promotion_type = 0, discount_percent = 0)]
  
  # 逐行處理 Promotion_amazon 中的每筆促銷資料
  for (i in 1:nrow(Promotion_amazon)) {
    product_list <- strsplit(Promotion_amazon$sku[i], ",")[[1]]
    message("Processing Amazon promotion row: ", i)
    start_time <- Promotion_amazon$start_time[i]
    end_time   <- Promotion_amazon$end_time[i]
    re_val     <- Promotion_amazon$`是否可以重複使用`[i]
    P_t_val    <- Promotion_amazon$`促銷類型`[i]
    d_p        <- Promotion_amazon$`折扣%`[i]
    
    if (product_list[1] == "ALL") {
      example_data[(time >= start_time) & (time <= end_time), 
                   `:=`(reuse = re_val, Promotion_type = P_t_val, discount_percent = d_p)]
    } else {
      example_data[sku_sub %in% product_list & (time >= start_time) & (time <= end_time), 
                   `:=`(reuse = re_val, Promotion_type = P_t_val, discount_percent = d_p)]
    }
  }
  
  # 將更新後的資料寫回資料庫（覆蓋原表）
  dbWriteTable(Data, "amazon_sales_zip_dta", as_tibble(example_data), overwrite = TRUE, temporary = FALSE)
  message("Amazon promotion recoding completed. Table 'amazon_sales_zip_dta' updated.")
  
  return(as_tibble(example_data))
}