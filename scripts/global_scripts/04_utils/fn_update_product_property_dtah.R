update_product_property_dtah <- function(googlesheet_con,database) {
  
  # 取得此 Google Sheet 中所有工作表的名稱
  all_sheet_names <- sheet_names(googlesheet_con)
  
  # 依序讀取每個工作表，並進行資料處理
  all_product_property_dictionary <- map_dfr(all_sheet_names, function(sheet_nm) {
    # 讀取該工作表的資料
    sheet_data <- read_sheet(googlesheet_con, sheet = sheet_nm)
    
    # 根據欄位型態需求轉換資料，並利用工作表名稱的前三個字元作為 product_line_id
    sheet_data_processed <- sheet_data %>% 
      mutate(
        property_id    = as.integer(property_id),
        property_name  = make_names(as.character(property_name)),
        frequency      = as.integer(frequency),
        definition     = as.character(definition),
        example        = as.character(example),
        type           = as.character(type),
        product_line_id = substr(sheet_nm, 1, 3)
      )
    
    return(sheet_data_processed)
  })
  
  # 將合併後的資料寫入資料庫中，存放至 comment_property_rating 資料庫的 "comment_property_dtah" 資料集
  dbWriteTable(database, "comment_property_dtah", all_product_property_dictionary, overwrite = TRUE)
  
  message("資料已成功寫入資料庫的 comment_property_dtah 資料集")
}

# 使用範例：
# 假設已建立資料庫連線 comment_property_rating，
# 並以 as_sheets_id() 建立 Google Sheet 連線 googlesheet_con
# googlesheet_con <- as_sheets_id("1Yw8EBXbSkyfEFWZ18D1bobvPVZWftK6v8hTNTFq4-zA")
# write_product_property_dictionary(googlesheet_con)