create_or_replace_sales_by_customer_date_dta <- function(con) {
  # 定義表格名稱
  table_name <- "sales_by_customer_date_dta"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE sales_by_customer_date_dta (
      customer_id VARCHAR,
      date DATE,
      total DOUBLE,
      amount INTEGER,
      time TIMESTAMP,
      zipcode VARCHAR,
      state VARCHAR,
      lat DOUBLE,
      lng DOUBLE,
      ni INTEGER,
      times INTEGER,
      IPT DOUBLE,
      source_filter VARCHAR,
      state_filter VARCHAR,
      product_line_id_filter VARCHAR,
      time_condition_filter VARCHAR,
      PRIMARY KEY (customer_id, date, source_filter, product_line_id_filter, state_filter, time_condition_filter)
    );
  "
  
  # 定義索引語句：先刪除舊索引，再建立新索引
  indexes <- c(
    "DROP INDEX IF EXISTS idx_sales_by_customer_date_dta_date;",
    "DROP INDEX IF EXISTS idx_sales_by_customer_date_dta_source;",
    "DROP INDEX IF EXISTS idx_sales_by_customer_date_dta_product_line_id;",
    "DROP INDEX IF EXISTS idx_sales_by_customer_date_dta_state;",
    "DROP INDEX IF EXISTS idx_sales_by_customer_date_dta_time_condition;",
    "CREATE INDEX idx_sales_by_customer_date_dta_date ON sales_by_customer_date_dta (date);",
    "CREATE INDEX idx_sales_by_customer_date_dta_source ON sales_by_customer_date_dta (source_filter);",
    "CREATE INDEX idx_sales_by_customer_date_dta_product_line_id ON sales_by_customer_date_dta (product_line_id_filter);",
    "CREATE INDEX idx_sales_by_customer_date_dta_state ON sales_by_customer_date_dta (state_filter);",
    "CREATE INDEX idx_sales_by_customer_date_dta_time_condition ON sales_by_customer_date_dta (time_condition_filter);"
  )
  
  # 調用 setup_table 函數執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出資料表結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}