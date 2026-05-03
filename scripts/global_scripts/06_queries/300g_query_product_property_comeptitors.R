query_product_property_comeptitors <- function(rawdata) {
  # 初始設定查詢結果物件
  asin_to_product <- NULL
  
  # 針對全域變數 vec_product_line_id_noall 中的每個 product_line_id 產生查詢
  for (product_line_id_i in vec_product_line_id_noall) {
    
    # 根據 product_line_id_i 動態組合資料表名稱，並選取指定欄位
    table_query <- tbl(rawdata, paste("product_property", product_line_id_i, sep = "_")) %>%
      select(asin, brand, product_name) %>%
      mutate(product_line_id = product_line_id_i)
    
    # 如果尚未有查詢結果，直接將本次查詢指定給 asin_to_product，
    # 否則利用 SQL 的 union_all 合併查詢結果
    if (is.null(asin_to_product)) {
      asin_to_product <- table_query
    } else {
      asin_to_product <- asin_to_product %>% union_all(table_query)
    }
  }
  
  # 收集查詢結果並回傳
  result <- asin_to_product %>% collect()
  return(result)
}