create_or_replace_nes_trend <- function(con) {
  # 定義表格名稱
  table_name <- "nes_trend"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE nes_trend (
      source_filter CHAR,
      state_filter CHAR, 
      product_line_id_filter CHAR, 
      time_condition_filter CHAR,
      nesstatus ENUM('N', 'E0', 'S1', 'S2', 'S3'),
      num_customers INT,
      nes_count INT,
      nes_prop DOUBLE,
      nes_mvalue DOUBLE,
      nes_mmean DOUBLE,
      PRIMARY KEY (source_filter, state_filter, product_line_id_filter, time_condition_filter, nesstatus)
    );
  "
  
  # 若需要額外索引，可在此定義（此例中不額外定義索引）
  indexes <- character(0)
  
  # 調用您先前定義的 setup_table 函數執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出該表格結構資訊以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}
