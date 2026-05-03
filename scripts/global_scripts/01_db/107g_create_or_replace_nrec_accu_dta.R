create_or_replace_nrec_accu_dta <- function(con) {
  # 定義表格名稱
  table_name <- "nrec_accu_dta"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE nrec_accu_dta (
      nrec_accu VARCHAR DEFAULT 'Accuracy: UNKNOWN',
      source_filter VARCHAR,
      state_filter VARCHAR,
      product_line_id_filter VARCHAR,
      time_condition_filter VARCHAR,
      PRIMARY KEY (nrec_accu, source_filter, product_line_id_filter, state_filter, time_condition_filter)
    );
  "
  
  # 本例未定義額外的索引，若需要可以在 indexes 中加入相應的索引語句
  indexes <- character(0)
  
  # 調用 setup_table 函數建立表格與索引
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出資料表結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}