process_and_join_positions <- function(processed_data, raw_data, app_data, vec_product_line_id_noall) {
  # 載入必要套件
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(mice)
  library(tidyr)
  
  # 針對每個產品線進行處理
  for (product_line_id in vec_product_line_id_noall) {
    
    message("處理產品線：", product_line_id)
    
    # 讀取原始表：格式 "comment_property_ratingonly_by_asin_<產品線ID>_dta"
    orig_table_name <- paste0("comment_property_ratingonly_by_asin_", product_line_id, "_dta")
    if (!dbExistsTable(processed_data, orig_table_name)) {
      message("資料庫中找不到表 ", orig_table_name, "，跳過產品線 ", product_line_id)
      next
    }
    
    Dta <- tbl(processed_data, orig_table_name) %>% collect()
    message("產品線 ", product_line_id, " 讀取資料筆數：", nrow(Dta))
    
    # 從 raw_data 讀取該產品線的 asin 列表
    asin_list <- tbl(raw_data, "product_property_dictionary") %>% 
      filter(product_line_id == product_line_id,
             amazon_poisson_precision_marketing == 1) %>% 
      select(asin) %>% 
      pull()
    message("該產品線的 asin 列表：", paste(asin_list, collapse = ", "))
    
    # 過濾資料：僅保留 asin 落在 asin_list 中的資料
    Dta <- Dta %>% filter(asin %in% asin_list)
    message("保留後資料筆數：", nrow(Dta))
    
    # 設定欄位過濾門檻：僅保留缺失值數量不超過資料列數一半的欄位
    threshold <- nrow(Dta) / 2
    Dta <- Dta[, colSums(is.na(Dta)) <= threshold]
    
    # 從 raw_data 讀取該產品線的 product_property 表（僅選擇 asin 與 brand 欄位）
    product_property <- tbl(raw_data, paste("product_property", product_line_id, sep = "_")) %>% 
      select(asin, brand)
    
    # 從 raw_data 讀取競爭對手銷售資料，依 asin 分組計算銷售總和
    cl_used_sales <- tbl(raw_data, "amazon_competitor_sales_dta") %>% 
      group_by(asin) %>% 
      summarise(sales = sum(sales, na.rm = TRUE))
    
    # 進行串接：依據 asin 依序 left join 銷售與 product_property 資料，若 brand 為 NA 則填入 "UNKNOWN"
    combined_position <- Dta %>% 
      left_join(cl_used_sales, copy = TRUE, by = join_by(asin)) %>% 
      left_join(product_property, copy = TRUE, by = join_by(asin)) %>% 
      mutate(brand = replace_na(brand, "UNKNOWN"))
    
    # 取出除 asin 與 brand 之外的其他欄位，用於計算理想值
    processed_data_s <- combined_position %>% select(-any_of(c("asin", "brand")))
    columns_to_remove <- c("rating", "sales")
    
    # 計算評分理想值：依據 rating 欄位的加權平均
    rate_ideal <- processed_data_s %>% 
      select(-any_of(columns_to_remove)) %>% 
      summarise(across(everything(), 
                       ~ sum(.x * processed_data_s$rating / sum(processed_data_s$rating, na.rm = TRUE), na.rm = TRUE)))
    
    # 計算銷售理想值：依據 sales 欄位的加權平均
    sales_ideal <- processed_data_s %>% 
      select(-any_of(columns_to_remove)) %>% 
      summarise(across(everything(), 
                       ~ sum(.x * processed_data_s$sales / sum(processed_data_s$sales, na.rm = TRUE), na.rm = TRUE)))
    
    # 計算綜合理想值：0.6 * 銷售理想值 + 0.4 * 評分理想值
    Ideal <- bind_rows(rate_ideal, sales_ideal, 0.6 * sales_ideal + 0.4 * rate_ideal)
    if (ncol(Ideal)>0){
    Ideal <- Ideal %>% mutate(brand = c("Rating", "Revenue", "Ideal"),
                              asin = c("Rating", "Revenue", "Ideal"))
    Ideal2 <- Ideal %>% filter(asin == "Ideal")
    } else{
      Ideal2 <- data.frame(brand = c("Rating", "Revenue", "Ideal"),
                           asin = c("Rating", "Revenue", "Ideal"))
    }
    # 將理想值資料附加到原始串接資料中，並調整欄位位置與新增產品線編號
    combined_position2 <- combined_position %>% 
      rows_append(Ideal2) %>% 
      relocate(asin, brand, .before = everything()) %>%  # 將 asin 與 brand 移到最前面
      relocate(rating, .after = last_col()) %>%           # 將 rating 移到最後面
      mutate(product_line_id = product_line_id)
    
    # 寫入處理後的資料到暫存表，格式為 "position_<產品線ID>_dta"
    new_table_name <- paste("position", product_line_id, "dta", sep = "_")
    dbWriteTable(processed_data, new_table_name, combined_position2, temporary = FALSE, overwrite = TRUE)
    message("產品線 ", product_line_id, " 更新完成，處理後資料已儲存至表 ", new_table_name)
  }
  
  # 多表動態 FULL JOIN 組合各產品線的處理結果
  table_names <- paste0("position_", vec_product_line_id_noall, "_dta")
  
  # 初始化第一個表
  combined_query <- tbl(processed_data, table_names[1])
  
  # 依序 FULL JOIN 其他表
  for (i in 2:length(table_names)) {
    next_table <- tbl(processed_data, table_names[i])
    common_cols <- intersect(colnames(combined_query), colnames(next_table))
    combined_query <- combined_query %>% full_join(next_table, by = common_cols)
  }
  
  # 將最終結果寫入目標資料庫 app_data 中的表 "position_dta"
  final_processed_data <- combined_query %>% 
    compute(name = "position_dta", overwrite = TRUE) %>% 
    collect()
  
  dbWriteTable(app_data, "position_dta", final_processed_data, overwrite = TRUE, temporary = FALSE)
  message("最終結果已存入目標資料庫表 position_dta")
}
