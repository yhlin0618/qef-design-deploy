#!/usr/bin/env Rscript
# 通用部署腳本模板 - 互動式部署
# 此腳本可用於任何 Shiny 應用程式

# 取得腳本所在目錄（智能偵測）
get_script_dir <- function() {
  # 獲取命令行參數
  args <- commandArgs(trailingOnly = FALSE)
  
  # 尋找 --file= 參數
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    script_path <- sub("^--file=", "", file_arg[1])
    return(dirname(normalizePath(script_path)))
  }
  
  # 在 RStudio 中執行 source()
  if (exists("ofile") && !is.null(ofile)) {
    return(dirname(normalizePath(ofile)))
  }
  
  # 使用 sys.frames() 和 sys.calls() 找到 source 調用
  frames <- sys.frames()
  calls <- sys.calls()
  
  for (i in rev(seq_along(calls))) {
    call <- calls[[i]]
    if (is.call(call) && length(call) >= 1) {
      fn <- as.character(call[[1]])
      if (length(fn) > 0 && fn[1] == "source") {
        if (length(call) >= 2) {
          file_arg <- call[[2]]
          if (is.character(file_arg) && file.exists(file_arg)) {
            return(dirname(normalizePath(file_arg)))
          }
          if (i <= length(frames)) {
            file_path <- tryCatch(
              eval(file_arg, envir = frames[[i]]),
              error = function(e) NULL
            )
            if (!is.null(file_path) && is.character(file_path) && file.exists(file_path)) {
              return(dirname(normalizePath(file_path)))
            }
          }
        }
      }
    }
  }
  
  # 預設使用當前目錄
  return(getwd())
}

# 自動偵測並切換到專案目錄
find_project_root <- function(start_dir = NULL) {
  if (is.null(start_dir)) {
    start_dir <- getwd()
  }
  
  # 方法 1: RStudio 專案
  if (Sys.getenv("RSTUDIO") == "1" && requireNamespace("rstudioapi", quietly = TRUE)) {
    project <- tryCatch(
      rstudioapi::getActiveProject(),
      error = function(e) NULL
    )
    if (!is.null(project)) {
      return(project)
    }
  }
  
  # 方法 2: 使用 rprojroot
  if (requireNamespace("rprojroot", quietly = TRUE)) {
    root <- tryCatch(
      rprojroot::find_root(rprojroot::is_rstudio_project, path = start_dir),
      error = function(e) NULL
    )
    if (!is.null(root)) {
      return(root)
    }
  }
  
  # 方法 3: 尋找 .Rproj 檔案
  current_dir <- start_dir
  while (TRUE) {
    rproj_files <- list.files(current_dir, pattern = "\\.Rproj$", full.names = TRUE)
    if (length(rproj_files) > 0) {
      return(current_dir)
    }
    
    parent <- dirname(current_dir)
    if (parent == current_dir) break
    current_dir <- parent
  }
  
  # 方法 4: 尋找應用程式特徵檔案
  current_dir <- start_dir
  while (TRUE) {
    # 檢查是否有應用程式的特徵檔案
    if (file.exists(file.path(current_dir, "app.R")) || 
        file.exists(file.path(current_dir, "app_config.yaml")) ||
        (dir.exists(file.path(current_dir, "scripts")) && 
         dir.exists(file.path(current_dir, "scripts/global_scripts")))) {
      return(current_dir)
    }
    
    parent <- dirname(current_dir)
    if (parent == current_dir) break
    current_dir <- parent
  }
  
  # 使用起始目錄
  return(start_dir)
}

# 找到部署腳本的路徑
find_deployment_script <- function(project_root) {
  # 可能的部署腳本位置
  possible_paths <- c(
    "scripts/global_scripts/23_deployment/sc_deployment_config.R",
    "global_scripts/23_deployment/sc_deployment_config.R",
    "../global_scripts/23_deployment/sc_deployment_config.R",
    "../../global_scripts/23_deployment/sc_deployment_config.R"
  )
  
  for (path in possible_paths) {
    full_path <- file.path(project_root, path)
    if (file.exists(full_path)) {
      return(path)
    }
  }
  
  return(NULL)
}

# 主程式
main <- function() {
  # 取得腳本目錄
  script_dir <- get_script_dir()
  cat("📂 腳本位置:", script_dir, "\n")
  
  # 切換到專案目錄
  project_root <- find_project_root(script_dir)
  if (getwd() != project_root) {
    cat("📍 切換到專案目錄:", project_root, "\n")
    setwd(project_root)
  }
  
  # 顯示應用程式名稱
  app_name <- basename(project_root)
  cat("🚀 準備部署:", app_name, "\n\n")
  
  # 找到部署腳本
  deployment_script <- find_deployment_script(project_root)
  
  if (is.null(deployment_script)) {
    cat("❌ 錯誤：找不到部署腳本\n")
    cat("當前目錄:", getwd(), "\n")
    cat("請確認您在應用程式目錄中，且 global_scripts 已正確設定\n")
    stop("無法找到部署腳本")
  }
  
  # 執行配置驅動的一鍵部署腳本（互動式）
  source(deployment_script)
}

# 執行主程式
main() 