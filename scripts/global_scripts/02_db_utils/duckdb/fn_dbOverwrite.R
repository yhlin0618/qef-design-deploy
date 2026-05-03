dbOverwrite <- function(old_conn, new_conn, wait_time = 10) {
  # 取得舊資料庫連線的檔案路徑
  drv_old <- tryCatch(old_conn@driver,
                      error = function(e) stop("無法從舊連線中取得 driver: ", e$message))
  old_db_path <- tryCatch(drv_old@dbdir,
                          error = function(e) stop("無法從舊連線 driver 中取得 dbdir: ", e$message))
  
  # 取得新資料庫連線的檔案路徑
  drv_new <- tryCatch(new_conn@driver,
                      error = function(e) stop("無法從新連線中取得 driver: ", e$message))
  new_db_path <- tryCatch(drv_new@dbdir,
                          error = function(e) stop("無法從新連線 driver 中取得 dbdir: ", e$message))
  
  message("舊資料庫路徑：", old_db_path)
  message("新資料庫路徑：", new_db_path)
  
  # 檢查檔案是否存在
  if (!file.exists(old_db_path)) stop("舊資料庫檔案不存在：", old_db_path)
  if (!file.exists(new_db_path)) stop("新資料庫檔案不存在：", new_db_path)
  
  # 取得檔案大小（位元組）
  old_size <- file.info(old_db_path)$size
  new_size <- file.info(new_db_path)$size
  
  message("舊資料庫大小：", old_size, " bytes")
  message("新資料庫大小：", new_size, " bytes")
  
  # 判斷覆蓋條件：僅在新檔案大於舊檔案時才進行覆蓋
  if (new_size <= old_size) {
    message("覆蓋條件不成立：新資料庫檔案大小並不大於舊資料庫檔案。")
    return(invisible(NULL))
  }
  
  # 斷開所有資料庫連線（使用已定義的 dbDisconnect_all 函數）
  dbDisconnect_all()
  message("所有資料庫連線已斷開。")
  
  # 等待一段時間以確保檔案完全釋放
  Sys.sleep(wait_time)
  message("等待了 ", wait_time, " 秒。")
  
  # 執行覆蓋：將新資料庫檔案複製到舊資料庫位置（overwrite = TRUE）
  if (file.copy(new_db_path, old_db_path, overwrite = TRUE)) {
    message("覆蓋成功：新資料庫檔案已覆蓋到舊資料庫位置。")
    # 覆蓋成功後，刪除新資料庫檔案
    if (file.remove(new_db_path)) {
      message("已刪除新資料庫檔案：", new_db_path)
    } else {
      warning("刪除新資料庫檔案失敗：", new_db_path)
    }
  } else {
    stop("覆蓋失敗：無法將新資料庫檔案複製到舊資料庫位置。")
  }
}