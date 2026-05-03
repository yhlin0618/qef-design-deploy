create_or_replace_sales_by_time_state_dta <- function(con) {
  # 定義表格名稱
  table_name <- "sales_by_time_state_dta"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE sales_by_time_state_dta (
      source_filter VARCHAR,
      state_filter VARCHAR,
      product_line_id_filter VARCHAR,
      time_condition_filter VARCHAR,
      time_scale DATE,
      new_customers INT,
      num_customers INT,
      total DOUBLE,
      PRIMARY KEY (source_filter, product_line_id_filter, state_filter, time_condition_filter, time_scale)
    );
  "
  
  # 若需要額外索引，可在此定義，目前設定為空
  indexes <- character(0)
  
  # 調用 setup_table() 函數建立表格與索引
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出資料表結構資訊以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}
