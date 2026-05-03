write_table_safe <- function(conn, table_name, data, max_attempts = 3, delay = 1) {
  attempts <- 1
  success <- FALSE
  while (attempts <= max_attempts && !success) {
    tryCatch({
      dbWriteTable(conn, table_name, data, overwrite = TRUE)
      success <- TRUE
    }, error = function(e) {
      message("寫入 ", table_name, " 發生錯誤，嘗試次數 ", attempts, ": ", e$message)
      Sys.sleep(delay)
      attempts <<- attempts + 1
    })
  }
  if (!success) {
    stop("多次嘗試寫入 ", table_name, " 失敗。")
  }
  return(success)
}