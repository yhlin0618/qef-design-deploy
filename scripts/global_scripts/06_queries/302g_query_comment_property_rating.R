query_comment_property_rating <- function(raw_data, comment_property_rating, vec_product_line_id_noall) {

  # 若 make_names 不存在，可使用 base::make.names 替代；這裡假設 make_names 可用
  # library(janitor) # 若需要使用 janitor 套件中的 make_names
  
  for (product_line_id_i in vec_product_line_id_noall) {
    
    # 從原始資料庫抓取該產品線的評論資料（並移除原本的 product_line_id 欄位）
    amazon_review_dta_filtered <- tbl(raw_data, "amazon_review_dta") %>% 
      filter(product_line_id == product_line_id_i) %>% 
      select(-product_line_id) %>% 
      collect()
    
    message("處理產品線：", product_line_id_i)
    
    # 讀取該產品線在 comment_property_rating 資料庫中的 product_property_dtah 資料表，取得屬性定義
    property_dtah <- tbl(comment_property_rating, "comment_property_dtah") %>%
      filter(product_line_id == product_line_id_i) %>% 
      collect()
    
    # 取得屬性名稱（依據欄位 property_name），並轉為合法變數名稱
    new_columns <- make_names(property_dtah$property_name)
    
    # 根據 property_dtah 中的定義，在原始評論資料中動態新增欄位，預設值均為 NA_character_
    amazon_review_dta_new <- amazon_review_dta_filtered %>%
      mutate(!!!setNames(rep(list(NA_character_), length(new_columns)), new_columns))
    
    # 定義新的資料表名稱，例如 "comment_property_rating_001new"
    table_name_new <- paste0("comment_property_rating_", product_line_id_i,"_raw")
    
    # 將新增欄位後的資料寫入目標資料庫 comment_property_rating 中，overwrite = TRUE 表示若該表存在則覆蓋
    dbWriteTable(comment_property_rating, table_name_new, amazon_review_dta_new, overwrite = TRUE)
    
    message("已建立資料表 ", table_name_new)
  }
}