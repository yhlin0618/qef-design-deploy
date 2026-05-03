#' dbAttachDuckdb
#'
#' 使用 DuckDB 原生 `ATTACH` 指令，將另一顆 `.duckdb` 檔掛載至現有連線。
#'
#' @param con   DBIConnection，DuckDB 連線物件。
#' @param path  Character，待附掛之 .duckdb 完整路徑。
#' @param alias Character，掛載後 schema 別名，預設 "otherdb"。
#' @param read_only Logical，是否唯讀掛載，預設 FALSE。
#'
#' @return 整數，`DBI::dbExecute()` 影響行數。
#'
#' @export
#' @importFrom DBI dbExecute
#' @use_package DBI
#' @use_package duckdb
#' @implements MP999 Simplified DuckDB Attach
#' @rdname dbAttachDuckdb

dbAttachDuckdb <- function(con, path, alias = "otherdb", read_only = FALSE) {
  if (!inherits(con, "duckdb_connection")) {
    stop("`con` 必須為 DuckDB 連線物件")
  }
  if (!file.exists(path)) {
    stop("找不到資料庫檔案：", path)
  }
  opt <- if (isTRUE(read_only)) " (READ_ONLY)" else ""
  sql <- sprintf("ATTACH '%s' AS %s%s", normalizePath(path), alias, opt)
  DBI::dbExecute(con, sql)
}

# 向下相容別名 --------------------------------------------------------------
#' @export
db_attach_duckdb <- dbAttachDuckdb 