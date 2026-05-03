# 將連字符替換為底線
sanitize_identifier <- function(identifier) {
  gsub("-", "_", identifier)
}

quote_identifier <- function(identifier) {
  # 先將連字符轉成底線
  identifier <- sanitize_identifier(identifier)
  # 如果 identifier 包含非字母數字或底線的字元，就用雙引號括起來
  if (grepl("[^A-Za-z0-9_]", identifier)) {
    return(paste0("\"", identifier, "\""))
  } else {
    return(identifier)
  }
}

process_column_def <- function(def_str) {
  # 取得第一個空格前的欄位名稱
  parts <- strsplit(def_str, "\\s+", perl = TRUE)[[1]]
  col_name <- parts[1]
  # 將連字符替換為底線，然後用 quote_identifier 處理欄位名稱
  quoted_name <- quote_identifier(col_name)
  # 重新組合定義字串
  paste(c(quoted_name, parts[-1]), collapse = " ")
}

generate_column_definitions <- function(columns, types) {
  if (length(columns) != length(types)) {
    stop("The length of columns and types must be the same.")
  }
  # 將所有欄位名稱的連字符替換為底線
  columns <- sapply(columns, make_names)
  column_definitions <- paste(columns, types)
  return(column_definitions)
}

create_summary_table <- function(db_connection,
                               table_name,
                               column_definitions,
                               primary_keys=NULL, foreign_keys = NULL, indexes = NULL) {
  # 處理每一個欄位定義，檢查欄位名稱是否需要引用與替換
  column_definitions <- sapply(column_definitions, process_column_def)
  
  # 拼接列定義部分
  column_defs <- paste(column_definitions, collapse = ",\n    ")
  
  # 拼接主鍵定義部分，記得對主鍵欄位也做轉換
  primary_key_def <- if (!is.null(primary_keys)) {
    paste0(",\n    PRIMARY KEY (", paste(sapply(primary_keys, function(x) quote_identifier(x)), collapse = ", "), ")")
  } else {
    ""
  }
  
  # 拼接外鍵定義部分
  foreign_key_defs <- if (!is.null(foreign_keys)) {
    paste(
      sapply(foreign_keys, function(fk) {
        paste0("FOREIGN KEY (", quote_identifier(fk$column), 
               ") REFERENCES ", fk$reference_table, "(", quote_identifier(fk$reference_column), ")")
      }),
      collapse = ",\n    "
    )
  } else {
    ""
  }
  
  # 拼接索引定義部分（不包含在表內，而是單獨語句）
  index_defs <- if (!is.null(indexes)) {
    sapply(indexes, function(index) {
      paste0("CREATE INDEX ", index$name, " ON ", table_name, " (", paste(sapply(index$columns, quote_identifier), collapse = ", "), ");")
    })
  } else {
    NULL
  }
  
  # 動態生成 CREATE TABLE SQL 語句
  sql_query <- paste0(
    "DROP TABLE IF EXISTS ", table_name, ";\n",
    "CREATE TABLE ", table_name, " (\n",
    "    ", column_defs,
    if (foreign_key_defs != "") paste0(",\n    ", foreign_key_defs) else "",
    primary_key_def,
    "\n);"
  )
  
  # 執行 DROP TABLE 和 CREATE TABLE 語句
  dbExecute(db_connection, sql_query)
  
  # 執行 CREATE INDEX 語句（如果有索引）
  if (!is.null(index_defs)) {
    for (index_query in index_defs) {
      dbExecute(db_connection, index_query)
    }
  }
  
  return(cat(sql_query, "\n", paste(index_defs, collapse = "\n")))  # 返回生成的 SQL 以便檢查
}

create_or_replace_df_amazon_sales <- function(con, example_location) {
  # 定義表格名稱
  table_name <- "df_amazon_sales"
  example <- read_csvxlsx(example_location)
  
  type_mapping <- list(
    integer = "DOUBLE",
    numeric = "DOUBLE",
    double = "DOUBLE",
    character = "VARCHAR",
    Date = "VARCHAR",
    POSIXct = "VARCHAR",
    logical = "DOUBLE",
    integer64 = "DOUBLE"
  )
  
  r_types <- sapply(example, function(col) class(col)[1])
  sql_types <- sapply(r_types, function(r_type) {
    type_mapping[[r_type]]
  })
  
  create_summary_table(
    db_connection = con, 
    table_name = table_name, 
    column_definitions = generate_column_definitions(names(sql_types), sql_types)#, 
  #  primary_keys = c("amazon-order-id","merchant-order-id","purchase-date","sku","asin")  # 此處會自動轉為 amazon_order_id
  )
  
  # 印出表格結構資訊
  print(dbGetQuery(con, "PRAGMA table_info('df_amazon_sales')"))
}