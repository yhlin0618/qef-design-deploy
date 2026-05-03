#' dbConnectDuckdb
#'
#' 精簡封裝 `DBI::dbConnect(duckdb::duckdb())`，只處理錯誤訊息與唯讀旗標。
#'
#' @param db_path  Character. DuckDB 檔案路徑；可使用 ":memory:" 建立記憶體資料庫。
#' @param read_only Logical. 是否唯讀開啟，預設 FALSE。
#'
#' @return duckdb_connection 物件。
#'
#' @export
#' @importFrom DBI dbConnect dbIsValid
#' @use_package DBI
#' @use_package duckdb
#' @rdname dbConnectDuckdb

dbConnectDuckdb <- function(db_path = ":memory:", read_only = FALSE) {
  # 檢查 db_path 是否有效
  if (is.null(db_path) || is.na(db_path) || length(db_path) == 0) {
    stop("資料庫路徑無效：db_path 為 NULL、NA 或空值")
  }
  
  # 如果不是記憶體資料庫，檢查並建立目錄
  if (!is.na(db_path) && db_path != ":memory:" && !grepl("^:memory:", db_path)) {
    db_dir <- dirname(db_path)
    if (!dir.exists(db_dir) && db_dir != ".") {
      tryCatch({
        dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)
        message("建立資料庫目錄：", db_dir)
      }, error = function(e) {
        warning("無法建立資料庫目錄 ", db_dir, ": ", e$message)
      })
    }
  }
  
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = read_only)
    attr(con, "connection_type") <- ifelse(read_only, "duckdb_readonly", "duckdb_rw")
    attr(con, "connection_time") <- Sys.time()
    con
  }, error = function(e) {
    # #437: enrich lock-conflict errors with holder info (PID, command, user)
    # so operators see who holds the file instead of a bare rapi_startup message.
    # APP_MODE gate (DA-1 from verify-437): Posit Connect / Shiny APP_MODE
    # should NEVER run the multi-second lsof + ps enrichment at startup — when
    # local DuckDB fails there, dbConnectAppData() routes to Supabase anyway
    # and the enrichment would only add 2-3s of useless Dropbox FSEvents scan.
    op_mode <- tryCatch(
      {
        if (exists(".InitEnv", envir = globalenv(), inherits = FALSE)) {
          get(".InitEnv", envir = globalenv())$OPERATION_MODE
        } else {
          Sys.getenv("OPERATION_MODE", "")
        }
      },
      error = function(e2) ""
    )
    is_app_mode <- identical(op_mode, "APP_MODE")
    holder_info <- ""
    is_lock_err <- !is_app_mode && grepl(
      "Conflicting lock|set lock on file|errno.:.35",
      e$message
    )
    if (is_lock_err) {
      # Ensure check_db_locks is available. In a full autoinit session it is
      # already sourced into GlobalEnv; in minimal test/debug contexts we
      # locate the utility via GLOBAL_DIR env var or a set of known-relative
      # candidate paths (same pattern other 02_db_utils helpers use).
      if (!exists("check_db_locks", mode = "function", inherits = TRUE)) {
        util_candidates <- character()
        if (nzchar(Sys.getenv("GLOBAL_DIR", ""))) {
          util_candidates <- c(util_candidates,
            file.path(Sys.getenv("GLOBAL_DIR"), "04_utils",
                      "fn_check_db_locks.R"))
        }
        util_candidates <- c(util_candidates,
          file.path("scripts", "global_scripts", "04_utils",
                    "fn_check_db_locks.R"),
          file.path("shared", "global_scripts", "04_utils",
                    "fn_check_db_locks.R"),
          file.path("..", "global_scripts", "04_utils",
                    "fn_check_db_locks.R"),
          file.path("..", "..", "global_scripts", "04_utils",
                    "fn_check_db_locks.R"),
          file.path("..", "..", "..", "global_scripts", "04_utils",
                    "fn_check_db_locks.R"))
        util_path <- util_candidates[file.exists(util_candidates)][1]
        if (!is.na(util_path)) {
          try(source(util_path, local = FALSE), silent = TRUE)
        }
      }
      if (exists("check_db_locks", mode = "function", inherits = TRUE)) {
        holders <- tryCatch(
          check_db_locks(list(target = db_path), exclude_self = TRUE),
          error = function(e2) list()
        )
        if (length(holders) > 0) {
          lines <- vapply(holders, function(h) {
            sprintf("  - pid=%d user=%s\n    command: %s",
                    h$pid, h$user, h$command)
          }, character(1))
          holder_info <- paste0(
            "\n\n鎖持有者(DuckDB lock holders):\n",
            paste(lines, collapse = "\n"),
            "\n建議:關閉該 process(若是 dev session,在其終端 Ctrl+C 即可),",
            "\n      或改用 read_only=TRUE 開啟。不要盲目 kill -9 不屬於你的 session。"
          )
        }
      }
    }
    stop("DuckDB 連線失敗:", e$message, "\n路徑:", db_path,
         "\n模式:", ifelse(read_only, "唯讀", "可寫"),
         holder_info,
         call. = FALSE)
  })
}

# %||% fallback for R < 4.4
if (!exists("%||%", mode = "function")) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}
