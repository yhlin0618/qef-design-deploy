generate_amazon_check_data <- function(raw_data, raw_data_folder) {
  # 載入必要套件
  require(dplyr)
  require(openxlsx)
  
  # 取得 amazon_review_dta 中所有獨特的 asin
  amazon_review_asin_list <- tbl(raw_data, "amazon_review_dta") %>% 
    select(asin) %>% 
    distinct() %>% 
    pull(asin)
  
  # 取得 amazon_competitor_sales_dta 中所有獨特的 asin
  amazon_competitor_sales_asin_list <- tbl(raw_data, "amazon_competitor_sales_dta") %>% 
    select(asin) %>% 
    distinct() %>% 
    pull(asin)
  
  # 先對 product_property_dictionary 做資料處理：
  # 1. 過濾 amazon_poisson_precision_marketing 為 1 的資料
  # 2. 利用 mutate 建立 amazon_review_data 與 amazon_competitor_data 兩個欄位，
  #    分別判斷 asin 是否存在於各自的 asin 清單中，存在為 "exist"，否則為 "missing"
  result <- tbl(raw_data, "product_property_dictionary") %>% 
    filter(amazon_poisson_precision_marketing == 1) %>% 
    mutate(amazon_review_data = if_else(asin %in% amazon_review_asin_list, "exist", "missing")) %>% 
    mutate(amazon_competitor_data = if_else(asin %in% amazon_competitor_sales_asin_list, "exist", "missing")) %>% 
    select(asin, sku, product_line_id, amazon_review_data, amazon_competitor_data) %>% 
    collect()
  
  # 過濾出 amazon_review_data 或 amazon_competitor_data 為 "missing" 的資料
  result_data <- result %>% filter(amazon_review_data == "missing" | amazon_competitor_data == "missing") %>% 
    arrange(product_line_id)
  
  # 建立一個新的 Excel workbook 並加入工作表
  wb <- createWorkbook()
  addWorksheet(wb, "amazon_data_check")
  
  # 將資料寫入工作表
  writeData(wb, "amazon_data_check", result_data)
  
  # 建立一個 highlight style，設定淺紅色背景
  highlightStyle <- createStyle(bgFill = "#FFC7CE")
  
  # 設定條件格式，將 "missing" 的儲存格標示出來
  conditionalFormatting(wb, "amazon_data_check", 
                        cols = 1:ncol(result_data), 
                        rows = 2:(nrow(result_data) + 1),
                        rule = '=="missing"',
                        style = highlightStyle)
  
  # 儲存 Excel 檔案
  saveWorkbook(wb, file.path(raw_data_folder, "amazon_data_check.xlsx"), overwrite = TRUE)
  
  # 回傳結果資料
  return(result_data)
}