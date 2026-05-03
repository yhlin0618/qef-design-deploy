process_amazon_review <- function(main_folder, raw_data) {
  
  # 取得符合檔名模式的所有 Excel 檔案（排除暫存檔），包含子資料夾
  file_names <- list.files(path = main_folder, 
                           pattern = "^[^~].*\\.xlsx$", 
                           full.names = TRUE, 
                           recursive = TRUE)
  
  n <- length(file_names)
  i <- 0
  
  # 逐一處理每一個檔案
  for (file_name in file_names) {
    # 讀取 Excel 檔案，抑制訊息
    new_data <- suppressMessages(read_excel(file_name))
    
    # 從檔案名稱中擷取出 10 碼 ASIN (符合 [A-Z0-9]{10})
    file.asin <- str_extract(file_name, "[A-Z0-9]{10}")
    print(file.asin)
    
    # 從該檔案所在子資料夾的名稱中擷取出三位數的 product_line_id
    product_line_id <- regmatches(basename(dirname(file_name)), 
                                  gregexpr("\\b\\d{3}\\b", basename(dirname(file_name))))[[1]]
    
    # 過濾 new_data 中 ASIN 欄位符合 file.asin 的資料，並新增 product_line_id 欄位
    new_data2 <- new_data %>% 
      filter(ASIN == file.asin) %>% 
      mutate(product_line_id = product_line_id)
    
    # 將 new_data2 寫入暫存表 temp_table（temporary table）
    dbWriteTable(raw_data, "temp_table", new_data2, temporary = TRUE, overwrite = TRUE)
    
    # Step 1: 創建 staging 表（結構與 amazon_review_dta 相同，但暫無資料）
    dbExecute(raw_data, "
      DROP TABLE IF EXISTS staging_amazon_review_dta;
      CREATE TEMP TABLE staging_amazon_review_dta AS 
      SELECT * FROM amazon_review_dta WHERE FALSE;
    ")
    
    # Step 2: 將 temp_table 的資料插入 staging 表
    dbExecute(raw_data, "
      INSERT INTO staging_amazon_review_dta
      SELECT * FROM temp_table;
    ")
    
    # Step 3: 將 staging 表中不重複的資料插入主表 amazon_review_dta
    dbExecute(raw_data, "
      INSERT INTO amazon_review_dta
      SELECT *
      FROM staging_amazon_review_dta
      WHERE asin IS NOT NULL
        AND body IS NOT NULL
        AND title IS NOT NULL
        AND time IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM amazon_review_dta AS ar
          WHERE ar.asin = staging_amazon_review_dta.asin
            AND ar.body = staging_amazon_review_dta.body
            AND ar.title = staging_amazon_review_dta.title
            AND ar.time = staging_amazon_review_dta.time
        );
    ")
    
    i <- i + 1
    message(paste(i, "/", n, "has been processed and created."))
  }
  
  # 檢查 amazon_review_dta 表的結構與資料
  print(tbl(raw_data, "amazon_review_dta"))
  message("Columns in amazon_review_dta:")
  print(colnames(tbl(raw_data, "amazon_review_dta")))
  
  unique_product_line_ids <- tbl(raw_data, "amazon_review_dta") %>% 
    select(product_line_id) %>% 
    pull() %>% 
    unique()
  message("Unique product_line_id values:")
  print(unique_product_line_ids)
  
  # 建立 amazon_review_dta 表的索引
  dbExecute(raw_data, "
    CREATE INDEX s_amazon_review_dta_product_line_id ON amazon_review_dta (product_line_id);
  ")
}