# ------------------------------------------------------------
# 把 DuckDB 內所有 user tables 以 readr::write_*() 輸出為 UTF-8 CSV
# ------------------------------------------------------------
# @param con        DBI duckdb_connection
# @param out_dir    預設 NULL → file.path(COMPANY_DIR, "database_to_csv", <db_name>)
# @param overwrite  TRUE: 覆寫舊檔；FALSE: 跳過
# @param write_fun  實際寫檔函式，預設 readr::write_csv
# @param ...        其餘參數傳給 write_fun，例如 na = "", quote = "all"
# @return           匯出成功的檔案路徑向量（invisible）
# ------------------------------------------------------------
export_duckdb_dataframes <- function(con,
                                     out_dir   = NULL,
                                     overwrite = TRUE,
                                     write_fun = readr::write_excel_csv,
                                     ...) {
  
  stopifnot(DBI::dbIsValid(con))
  
  ## ---- 推斷預設 out_dir ----
  if (is.null(out_dir)) {
    if (!exists("COMPANY_DIR", inherits = TRUE))
      stop("請先在全域環境設定 COMPANY_DIR")
    db_path  <- DBI::dbGetInfo(con)$dbname
    db_label <- if (db_path %in% c(":memory:", "", ":memory"))
      "duckdb_memory"
    else
      tools::file_path_sans_ext(basename(db_path))
    out_dir <- file.path(COMPANY_DIR, "database_to_csv", db_label)
  }
  
  ## ---- 刪除舊資料夾並重新建立 ----
  if (overwrite && dir.exists(out_dir)) {
    message("刪除舊資料夾：", out_dir)
    unlink(out_dir, recursive = TRUE)
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  ## ---- 取得 user tables ----
  tbls <- DBI::dbListTables(con)
  if (!length(tbls)) {
    message("資料庫沒有可匯出的資料表")
    return(character())
  }
  
  exported <- character()
  
  for (tbl in tbls) {
    csv_path <- file.path(out_dir, paste0(tbl, ".csv"))
    
    if (file.exists(csv_path) && !overwrite) {
      message("跳過已存在：", basename(csv_path))
      next
    }
    
    message("→ 讀取 ", tbl, " 中…")
    df <- tryCatch({
      DBI::dbReadTable(con, tbl)                 # 直接拉回 R
    }, error = function(e) {
      warning("✘ 無法讀取 ", tbl, "：", e$message)
      return(NULL)
    })
    if (is.null(df)) next
    
    ## ---- 確保所有字串轉 UTF-8 ----
    df[] <- lapply(df, function(col) {
      if (is.character(col)) iconv(col, from = "", to = "UTF-8")
      else col
    })
    
    ## ---- 寫檔 ----
    tryCatch({
      write_fun(df, csv_path, ...)               # 預設 = write_csv()
      message("✓ 已匯出：", basename(csv_path))
      exported <- c(exported, csv_path)
    }, error = function(e) {
      warning("✘ 匯出失敗：", tbl, " —— ", e$message)
    })
  }
  
  invisible(exported)
}