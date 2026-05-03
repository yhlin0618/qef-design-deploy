#' dbDetachDuckdb
#'
#' 使用 DuckDB 原生 `DETACH` 指令，卸載先前掛載的資料庫。
#'
#' @param con   DBIConnection，DuckDB 連線物件。
#' @param alias Character，卸載目標之 schema 別名。
#'
#' @return 整數，`DBI::dbExecute()` 影響行數。
#'
#' @export
#' @importFrom DBI dbExecute
#' @use_package DBI
#' @use_package duckdb
#' @implements MP999 Simplified DuckDB Attach
#' @rdname dbDetachDuckdb

dbDetachDuckdb <- function(con, alias) {
  if (!inherits(con, "duckdb_connection")) {
    stop("`con` 必須為 DuckDB 連線物件")
  }
  
  # Check if this is a default database (seq == 0) to prevent detach errors
  tryCatch({
    dbs <- DBI::dbGetQuery(con, "PRAGMA database_list;")
    if (nrow(dbs) > 0) {
      default_names <- dbs$name[dbs$seq == 0]
      if (alias %in% default_names) {
        stop("Cannot detach default database '", alias, "'. DuckDB doesn't allow detaching the default database.")
      }
    }
  }, error = function(e) {
    # If we can't check, proceed with the original detach attempt
    warning("Could not verify database list: ", e$message)
  })
  
  sql <- sprintf("DETACH DATABASE %s", alias)
  DBI::dbExecute(con, sql)
}

# 向下相容別名 --------------------------------------------------------------
#' @export
db_detach_duckdb <- dbDetachDuckdb 