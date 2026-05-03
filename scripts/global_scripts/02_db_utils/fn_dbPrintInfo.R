#' dbPrintInfo
#'
#' 列印 DuckDB／DBI 連線的簡易資訊：資料表數、唯讀/可寫、額外屬性。
#'
#' @param con      DBIConnection 物件。
#' @param dataset  Character. 可選，用於顯示資料庫名稱。
#' @param verbose  Logical. 是否輸出表清單，預設 TRUE。
#'
#' @return 無；純輸出 side-effect。
#'
#' @export
#' @importFrom DBI dbListTables dbIsValid
#' @use_package DBI
#' @rdname dbPrintInfo

dbPrintInfo <- function(con, dataset = NULL, verbose = TRUE) {
  if (is.null(con) || !DBI::dbIsValid(con)) {
    message("[dbPrintInfo] 連線無效或已關閉")
    return(invisible(FALSE))
  }
  tbls <- DBI::dbListTables(con)
  rw    <- ifelse(isTRUE(attr(con, "connection_type") == "duckdb_readonly"), "唯讀", "可寫")
  name  <- ifelse(is.null(dataset), "<unnamed>", dataset)
  message(sprintf("目前連線 %s，模式：%s，資料表：%d", name, rw, length(tbls)))
  if (verbose && length(tbls) > 0) print(tbls)
  invisible(TRUE)
} 