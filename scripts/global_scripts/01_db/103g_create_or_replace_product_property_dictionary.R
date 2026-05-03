create_or_replace_product_property_dictionary <- function(con) {
  # 定義表格名稱
  table_name <- "product_property_dictionary"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
    CREATE OR REPLACE TABLE product_property_dictionary (
      brand CHAR,
      asin CHAR(10),
      sku CHAR,
      product_line_name CHAR,
      product_line_id CHAR(3),
      product_name CHAR,
      cost DOUBLE,
      profit DOUBLE,
      UNIQUE (asin)
    );
  "
  
  # 定義索引語句（若需要額外索引，可在此加入，目前設定為空）
  indexes <- character(0)
  
  # 調用先前定義好的 setup_table 函數，執行建立表格與索引的操作
  setup_table(con, table_name, create_table_sql, indexes)
  
  # 印出資料表結構以供檢查
  print(dbGetQuery(con, paste0("PRAGMA table_info('", table_name, "')")))
}