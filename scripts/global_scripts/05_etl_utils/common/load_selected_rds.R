# 定義函數來讀取指定資料夾中的 RDS 文件
load_selected_rds <- function(directory, file_names = NULL, all = FALSE) {
  # 如果 all = FALSE 且 file_names 為 NULL，則直接結束函數
  if (!all && is.null(file_names)) {
    message("未指定要載入的檔案名稱，且 all 設為 FALSE。未載入任何檔案。")
    return(invisible(NULL))
  }
  
  # 獲取指定資料夾中的所有 RDS 文件
  rds_files <- list.files(directory, pattern = "\\.rds$", full.names = TRUE)
  
  # 如果 all 為 FALSE，根據 file_names 篩選符合名稱的檔案
  if (!all) {
    # 使用 file_names 向量來匹配檔案名稱
    file_patterns <- paste0("^", file_names, "\\.rds$")
    rds_files <- rds_files[sapply(file_patterns, function(pat) any(grepl(pat, basename(rds_files))))]
  }
  
  # 檢查是否有符合條件的 RDS 文件
  if (length(rds_files) == 0) {
    stop("沒有找到符合條件的 RDS 文件在指定的資料夾:", directory)
  }
  
  # 遍歷符合條件的 RDS 文件並將它們讀取到環境中
  for (file in rds_files) {
    # 生成變量名基於文件名（不包含路徑和擴展名）
    var_name <- tools::file_path_sans_ext(basename(file))
    # 讀取 RDS 文件並賦值給變量
    assign(var_name, readRDS(file), envir = .GlobalEnv)
  }
  
  print(paste(length(rds_files), "RDS files loaded into the environment."))
}
