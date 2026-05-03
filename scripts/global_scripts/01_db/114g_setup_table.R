setup_table <- function(con, table_name, create_table_sql, indexes = NULL) {
  # 組合 DROP TABLE 與 CREATE TABLE 的 SQL 語句
  sql <- paste0("DROP TABLE IF EXISTS ", table_name, "; ", create_table_sql)
  
  # 執行 SQL 語句
  dbExecute(con, sql)
  
  # 如果提供了索引語句，逐一執行
  if (!is.null(indexes)) {
    for (idx_sql in indexes) {
      dbExecute(con, idx_sql)
    }
  }
}