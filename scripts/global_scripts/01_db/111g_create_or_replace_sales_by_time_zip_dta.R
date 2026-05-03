create_or_replace_sales_by_time_zip_dta <- function(con) {
  # 定義表格名稱
  table_name <- "sales_by_time_zip_dta"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE sales_by_time_zip_dta (
      source_filter VARCHAR,
      zipcode VARCHAR,
      product_line_id_filter VARCHAR,
      time_condition_filter VARCHAR,
      time_scale DATE,
      new_customers INT,
      num_customers INT,
      total DOUBLE,
      lat DOUBLE,
      lng DOUBLE,
      PRIMARY KEY (source_filter, product_line_id_filter, zipcode, time_condition_filter, time_scale)
    );
  "
  
  # 若有需要額外索引，這裡可以定義，目前設定為空
  indexes <- character(0)
  
  # 調用輔助函數 setup_table() 執行建立表格與索引操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出該表格結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}