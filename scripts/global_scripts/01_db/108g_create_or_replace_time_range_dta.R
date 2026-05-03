create_or_replace_time_range_dta <- function(con) {
  # 定義表格名稱
  table_name <- "time_range"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE time_range (
      data_name VARCHAR PRIMARY KEY,
      start_date DATE NOT NULL,
      end_date DATE NOT NULL CHECK (end_date >= start_date)
    );
  "
  
  # 此表不需要額外的索引
  indexes <- character(0)
  
  # 調用已定義的 setup_table 函數執行建立表格與索引操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出表格結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}