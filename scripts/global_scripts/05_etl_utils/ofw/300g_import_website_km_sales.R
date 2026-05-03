import_officialwebsite_km_sales <- function(main_folder, raw_data) {
  # 載入必要套件
  library(glue)
  library(dplyr)
  
  # 取得符合檔名模式的所有 CSV 檔案完整路徑
  file_names <- list.files(path = main_folder, pattern = "^orders.*\\.csv$", full.names = TRUE)
  
  # 初始累積資料為空 data.frame
  existing_data <- data.frame()
  
  # 逐一讀取各檔案
  for (file_name in file_names) {
    tryCatch({
      # 讀取 CSV 檔案 (預設所有欄位類型都是文字)
      new_data <- read_csv(file_name, show_col_types = FALSE,
                           col_types = cols(.default = col_character()))  
      
      # 檢查 new_data 是否有 Name 欄位
      if (!"Name" %in% colnames(new_data)) {
        message("檔案 ", file_name, " 中沒有找到 'Name' 欄位，跳過該檔案")
        next
      }
      
      # 若已有累積資料，檢查新檔案中的 Name 是否與現有資料重複
      if (nrow(existing_data) > 0) {
        dup_names <- intersect(new_data$Name, existing_data$Name)
        if (length(dup_names) > 0) {
          dup_names_str <- paste(dup_names, collapse = ", ")
          dup_names_str <- substr(dup_names_str, 1, 1000)  # 只保留前 1000 個字元
          message("檔案 ", file_name, " 中重複的 Name：", dup_names_str)
          new_data <- new_data %>% filter(!(Name %in% dup_names))
        }
      }
      
      # 若篩選後仍有資料，將新資料合併至 existing_data
      if (nrow(new_data) > 0) {
        existing_data <- bind_rows(existing_data, new_data)
        message("處理成功：", file_name)
        message("目前累積筆數 nrow = ", nrow(existing_data))
      } else {
        message("檔案 ", file_name, " 中所有 'Name' 均已存在，故跳過匯入")
      }
    }, error = function(e) {
      message("處理檔案發生錯誤：", file_name)
      message("錯誤訊息：", e$message)
    })
  }
  
  existing_data2<- existing_data %>% 
    parse_datetime_columns(c("Paid at", "Fulfilled at", "Created at", "Cancelled at"))  %>%
    remove_illegal_utf8 %>% 
    as_tibble() 
  
  
  # 所有檔案處理完成後，將累積資料寫出成 pq 檔案
  output_file <- file.path(Data_folder,"scd_type1","officialwebsite_sales_dta.pq")
  write_parquet(existing_data2, output_file)
  message("所有檔案處理完成，合併結果存檔至：", output_file)
  
  copy_sql <- sprintf("COPY officialwebsite_sales_dta FROM '%s' WITH (FORMAT parquet)", output_file)
  dbExecute(raw_data, copy_sql)
  
  # 印出資料表結構以檢查
  print(dbGetQuery(raw_data, paste0("PRAGMA table_info('", "officialwebsite_sales_dta", "')")))
}