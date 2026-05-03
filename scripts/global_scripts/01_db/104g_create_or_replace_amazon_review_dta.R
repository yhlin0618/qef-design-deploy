create_or_replace_amazon_review_dta <- function(con) {
  # 定義表格名稱
  table_name <- "amazon_review_dta"
  
  # 定義建立表格的 SQL 語句（包含先 DROP 舊表再 CREATE 新表）
  create_table_sql <- "
    DROP TABLE IF EXISTS amazon_review_dta;
    CREATE TABLE amazon_review_dta (
      asin VARCHAR,
      title VARCHAR,
      body VARCHAR,
      vp_review VARCHAR,
      vine_voice_review VARCHAR,
      model VARCHAR,
      rating UTINYINT,
      likes_count UINTEGER,
      image_count UINTEGER,
      image_url VARCHAR,
      has_video BOOLEAN,
      video_url VARCHAR,
      review_link VARCHAR,
      reviewer VARCHAR,
      avatar_url VARCHAR,
      country CHAR(2),
      reviewer_profile_url VARCHAR,
      influencer_program_link VARCHAR,
      time DATE,
      product_line_id CHAR(3) NOT NULL,
      PRIMARY KEY (asin, title, body, time) -- 設置複合主鍵
    );
  "
  
  # 若有額外的索引需求，可在此定義索引語句，否則傳入空字串向量
  indexes <- character(0)
  
  # 調用事先定義的 setup_table 函數，執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出資料表結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}