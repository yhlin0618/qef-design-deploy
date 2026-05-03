create_or_replace_amazon_competitor_sales_dta <- function(con) {
  # 定義表格名稱
  table_name <- "amazon_competitor_sales_dta"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE amazon_competitor_sales_dta (
      asin CHAR(10) NOT NULL,
      time DATE NOT NULL,
      product_line_id CHAR(3) NOT NULL,
      sales INTEGER NOT NULL,
      trend_line DOUBLE,
      weekly_day_moving_average DOUBLE,
      PRIMARY KEY (asin, time, product_line_id)
    );
  "
  
  # 定義索引語句
  indexes <- c(
    "CREATE INDEX idx_time ON amazon_competitor_sales_dta (time);",
    "CREATE INDEX idx_product_line_id ON amazon_competitor_sales_dta (product_line_id);"
  )
  
  # 調用之前定義好的 setup_table 函數執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  print(dbGetQuery(con, "PRAGMA table_info('amazon_competitor_sales_dta')"))
  
}