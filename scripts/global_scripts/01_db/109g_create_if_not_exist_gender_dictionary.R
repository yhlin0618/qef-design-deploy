create_if_not_exist_name_gender_dictionary <- function(con){
  # 定義表格名稱
  table_name <- "name_gender_dictionary"
  
  # 定義建立表格的 SQL 語句
  create_table_sql <- "
      CREATE TABLE IF NOT EXISTS name_gender_dictionary (
        name VARCHAR NOT NULL PRIMARY KEY,
        predicted_gender VARCHAR NOT NULL DEFAULT 'unknown'
        CONSTRAINT gender_check CHECK (predicted_gender IN ('male', 'female', 'unknown'))
      );
    "
  
  dbExecute(con, create_table_sql)
  
  # 印出表格結構資訊
  print(dbGetQuery(con, "PRAGMA table_info('name_gender_dictionary')"))
}
