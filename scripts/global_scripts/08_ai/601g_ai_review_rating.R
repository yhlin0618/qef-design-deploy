ai_review_ratings <- function(raw_data,
                              comment_property_rating_temp,
                              vec_product_line_id_noall,
                              max_nreviews_per_asin,
                              rating_module,
                              gpt_key,
                              openai) {
  # 顯示 openai 套件版本與 rating_module 說明
  message("openai 套件版本：", openai$`__version__`)
  cat(rating_module$rate_comment$`__doc__`)
  
  # 檢查 API 金鑰
  if (gpt_key == "") stop("未取得 GPT API 金鑰，請檢查環境變數或輸入參數。")
  
  # 依產品線進行處理
  for (product_line_id_i in vec_product_line_id_noall) {
    message("=== 處理產品線：", product_line_id_i, " ===")
    
    # 定義資料表名稱：舊資料表與更新後資料表
    old_table_name <- paste0("comment_property_rating_", product_line_id_i)
    # 修改後的評分表後綴為 _rated
    new_table_name <- paste0("comment_property_rating_", product_line_id_i, "_rated")
    
    # 檢查暫存資料庫中是否已存在更新用的資料表
    if (dbExistsTable(comment_property_rating_temp, new_table_name)) {
      message("發現表 ", new_table_name, "，將從此表開始繼續評分。")
      comment_property_rating_dta <- tbl(comment_property_rating_temp, new_table_name) %>% collect()
    } else {
      message("未發現表 ", new_table_name, "，將使用原始表 ", old_table_name, "。")
      comment_property_rating_dta <- tbl(comment_property_rating_temp, old_table_name) %>% collect()
    }
    message("讀取資料筆數：", nrow(comment_property_rating_dta))
    
    # 從 raw_data 讀取該產品線的 asin 列表，並進行過濾
    asin_list <- tbl(raw_data, "product_property_dictionary") %>% 
      filter(product_line_id == product_line_id_i,
             amazon_poisson_precision_marketing == 1) %>% 
      select(asin) %>% pull()
    message("該產品線的 asin 列表：", paste(asin_list, collapse = ", "))
    
    # 對符合 asin 條件的資料進行隨機抽樣（每個 asin 取 max_nreviews_per_asin 筆）
    set.seed(5566)
    comment_property_rating_dta2 <- comment_property_rating_dta %>%
      filter(asin %in% asin_list) %>% 
      group_by(asin) %>% 
      slice_tail(n = max_nreviews_per_asin) %>% 
      ungroup()
    message("抽樣後資料筆數：", nrow(comment_property_rating_dta2))
    
    # 從暫存資料庫中讀取屬性定義檔資料 (假設表名為 "comment_property_dtah")
    comment_property <- tbl(comment_property_rating_temp, "comment_property_dtah") %>% 
      filter(product_line_id == product_line_id_i,include=="yes") %>% collect()
    message("屬性定義檔資料筆數：", nrow(comment_property))
    
    # 逐筆處理抽樣資料，每處理完一個 row 就更新一次資料庫
    for (indexc in seq_len(nrow(comment_property_rating_dta2))) {
      # 複製出一筆待更新資料 (tibble 格式)
      rowc <- comment_property_rating_dta2[indexc, ]
      message("開始處理資料索引：", indexc, " (asin: ", rowc$asin, ", title: ", rowc$title, ")")
      
      # 針對屬性定義檔中每個屬性進行評分
      for (i in seq_len(nrow(comment_property))) {
        propertyname <- comment_property$property_name[i]  # 屬性名稱
        propertytype <- comment_property$type[i]
        
        # 若該筆資料中此屬性為 NA 且該 ASIN 符合條件，則進行評分
        if (is.na(rowc[[propertyname]]) && rowc$asin %in% asin_list) {
          message("產品線 ", product_line_id_i, ": 開始評分屬性 '", propertyname, "' (資料索引：", indexc, ")")
          print("python API")
          S <- Sys.time()
         
          res <- rating_module$rate_comment(
            title = rowc$title,    # 假設評論標題欄位為 title
            body = rowc$body,      # 假設評論內容欄位為 body
            propertyname = propertyname,
            propertytype = propertytype,
            gpt_key = gpt_key,
            model = "gpt-5.2"  # 統一使用 ai_prompts.yaml 中的標準模型
          )
          print(Sys.time()-S)
          message("評分結果：", res)
          
          # 更新該筆資料的評分結果
          rowc[[propertyname]] <- res
          
          # 為避免超過 API 速率限制，加入短暫延遲
          Sys.sleep(0.001)
        }
      }
      
      # 以 c("asin", "title", "body", "time") 為 key 更新 comment_property_rating_dta 中對應的資料
      comment_property_rating_dta <- dplyr::rows_update(comment_property_rating_dta,
                                                rowc,
                                                by = c("asin", "title", "body", "time"))
      message("更新後該筆資料內容：")
      message(paste(capture.output(print(rowc)), collapse = "\n"))
      
      # 每處理完一個 row，寫入一次更新結果到資料庫中的 new_table_name (此處後綴已為 _rated)
      print(" start backup to duckdb inner loop")
      S <- Sys.time()
      write_table_safe(comment_property_rating_temp, new_table_name, comment_property_rating_dta)
      message("已儲存更新到資料庫表 ", new_table_name, " (asin: ", rowc$asin, ", title: ", rowc$title, ")")
      print(Sys.time()-S)
    }
    
    # 完成本產品線所有評分後，再次寫入（選擇性）
    print(" start backup to duckdb in outer loop")
    S <- Sys.time()
    write_table_safe(comment_property_rating_temp, new_table_name, comment_property_rating_dta)
    print(Sys.time()-S)
    
    message("產品線 ", product_line_id_i, " 評分完成，最終結果已儲存到表 ", new_table_name)
  } # end for each product_line_id_i
}
