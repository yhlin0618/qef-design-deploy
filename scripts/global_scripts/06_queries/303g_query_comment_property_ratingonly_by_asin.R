query_comment_property_ratingonly_by_asin <- function(Data, product_line_id_noall_vec) {
  # 載入必要套件
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(mice)
  
  # 針對每個產品線進行處理
  for (product_line_id_str in product_line_id_noall_vec) {
    
    # 建立原始資料表名稱，例如 "comment_property_ratingonly_001_dta"
    orig_table_name <- paste0("comment_property_ratingonly_", product_line_id_str, "_dta")
    
    if (!dbExistsTable(Data, orig_table_name)) {
      message("資料庫中找不到表 ", orig_table_name, "，跳過產品線 ", product_line_id_str)
      next
    }
    
    # 讀取原始資料表，並依 asin 分組後對數值型欄位取平均，
    # 同時排除 likes_count 與 image_count 欄位
    Dta <- tbl(Data, orig_table_name) %>% 
      collect() %>%
      group_by(asin) %>%
      summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE))) %>%
      ungroup() %>%
      select(-any_of(c("likes_count", "image_count")))
    
    message("產品線 ", product_line_id_str, " 經 group_by(asin) 後資料筆數：", nrow(Dta))
    
    # 計算每一列的 NA 數量
    Dta_na_count <- apply(Dta, 1, function(x) sum(is.na(x)))
    max_na_count <- max(Dta_na_count)
    message("缺失值分布：")
    print(table(Dta_na_count))
    
    # 清理欄位名稱（假設 clean_column_names 與 make_names 已定義）
    temp_name <- clean_column_names(make_names(colnames(Dta)))
    # 將欄位 "品牌" 轉換成 "品牌價值"
    temp_name <- ifelse(temp_name == "品牌", "品牌價值", temp_name)
    colnames(Dta) <- temp_name
    
    # 針對缺失值數量小於最大值的列進行多重插補
    Dta_subset <- Dta[Dta_na_count < max_na_count, ] %>% as.data.frame()
    Dta_imp <- mice(Dta_subset, m = 5, method = 'pmm', maxit = 20, seed = 500) %>% complete()
    
    # 利用 rows_update 將插補後的資料更新回原資料 (僅更新 Dta_subset 部分)
    Dta_updated <- rows_update(Dta_subset, Dta_imp)
    
    # 將插補後的資料更新回原始資料中（僅更新那些列）
    Dta[Dta_na_count < max_na_count, ] <- Dta_updated
    
    # 產生處理後的表名稱，格式為 "comment_property_ratingonly_by_asin_<產品線ID>_dta"
    new_table_name <- paste0("comment_property_ratingonly_by_asin_", product_line_id_str, "_dta")
    
    # 將處理後的資料寫入新表，不覆蓋原始資料表
    dbWriteTable(Data, new_table_name, Dta, overwrite = TRUE)
    message("產品線 ", product_line_id_str, " 更新完成，處理後資料已儲存至表 ", new_table_name)
  }
}