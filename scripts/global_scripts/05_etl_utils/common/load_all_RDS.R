# 定義函數來讀取指定資料夾中的所有RDS文件
load_all_rds <- function(directory) {
  # 獲取指定資料夾中的所有RDS文件
  rds_files <- list.files(directory, pattern = "\\.rds$", full.names = TRUE)
  
  # 檢查是否有RDS文件
  if (length(rds_files) == 0) {
    stop("沒有找到任何RDS文件在指定的資料夾:", directory)
  }
  
  # 遍歷所有RDS文件並將它們讀取到環境中
  for (file in rds_files) {
    # 生成一個變量名基於文件名（不包含路徑和擴展名）
    var_name <- tools::file_path_sans_ext(basename(file))
    # 讀取RDS文件並賦值給變量
    assign(var_name, readRDS(file), envir = .GlobalEnv)
  }
  
  print(paste(length(rds_files), "RDS files loaded into the environment."))
}
