decode_ai_review_ratings <- function(raw_data,
                                     comment_property_rating_temp,
                                     processed_data,
                                     vec_product_line_id_noall) {
  # 載入必要套件
  library(DBI)
  library(dplyr)
  library(dbplyr)
  
  # 輔助函數：將評分字串解碼成 0~10 的整數，否則回傳 NA
  decode_rating <- function(x) {
    if (is.na(x)) return(NA_integer_)
    # 若包含 "NaN"（忽略大小寫），則直接回傳 NA
    if (grepl("NaN", x, ignore.case = TRUE)) return(NA_integer_)
    # 利用正則表達式匹配 "10" 或單個數字 (0~9)（以 word boundary 限定匹配）
    m <- regexpr("\\b(10|[0-9])\\b", x, perl = TRUE)
    if (m[1] == -1) {
      return(NA_integer_)
    } else {
      num_str <- regmatches(x, m)
      return(as.integer(num_str))
    }
  }
  
  # 依產品線逐一處理
  for (product_line_id_i in vec_product_line_id_noall) {
    message("=== 處理產品線：", product_line_id_i, " ===")
    
    # 定義資料表名稱（只會有合併後的表，不再有 _new）
    table_name <- paste0("comment_property_rating_", product_line_id_i)
    
    if (!dbExistsTable(comment_property_rating_temp, table_name)) {
      message("資料庫中找不到表 ", table_name, "，跳過此產品線")
      next
    }
    
    # 讀取該產品線的評分資料
    property_rating_dta <- tbl(comment_property_rating_temp, table_name) %>% collect()
    message("讀取資料筆數：", nrow(property_rating_dta))
    
    # 從 raw_data 讀取該產品線的 asin 列表（假設 raw_data 的表 "product_property_dictionary" 具欄位 product_line_id 與 asin）
    asin_list <- tbl(raw_data, "product_property_dictionary") %>% 
      filter(product_line_id == product_line_id_i) %>% 
      select(asin) %>% pull()
    message("該產品線的 asin 列表：", paste(asin_list, collapse = ", "))
    
    # 讀取屬性定義檔（假設表名為 "comment_property_dtah"）取得屬性名稱
    comment_property <- tbl(comment_property_rating_temp, "comment_property_dtah") %>% 
      filter(product_line_id == product_line_id_i) %>% collect()
    message("屬性定義檔資料筆數：", nrow(comment_property))
    
    # 對屬性定義檔中定義的每個屬性進行解析（向量化處理）
    for (prop in comment_property$property_name) {
      if (prop %in% names(property_rating_dta)) {
        original_values <- property_rating_dta[[prop]]
        # 對每個元素進行解碼：若 NA 或文字中含 "NaN" 則回傳 NA；否則抓取符合條件的數字
        decoded <- sapply(original_values, function(x) {
          # 轉為字串處理（若 x 為 NA 則 as.character() 也會是 NA）
          if (is.na(x)) return(NA_integer_)
          decode_rating(as.character(x))
        })
        # 以整數型態取代原欄位
        property_rating_dta[[prop]] <- as.integer(decoded)
        message("解析屬性 '", prop, "' 完成")
      } else {
        message("屬性 '", prop, "' 不存在於資料表中，跳過")
      }
    }
    
    # 寫入解析完成的資料到另一個資料庫 processed_data，
    # 表名命名為 "property_rating_<產品線ID>_processed"
    processed_table_name <- paste0("comment_property_ratingonly_", product_line_id_i, "_dta")
    dbWriteTable(processed_data, processed_table_name, property_rating_dta, overwrite = TRUE)
    message("產品線 ", product_line_id_i, " 解析完成，結果已儲存到資料庫表 ", processed_table_name)
  }
}
