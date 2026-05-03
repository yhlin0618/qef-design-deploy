#!/usr/bin/env Rscript
# ============================================================================
# 更新 app.R 檔案工具
# 用途：選擇一個檔案覆蓋 app.R
# ============================================================================

# 顏色輸出函數
print_success <- function(msg) cat("✅", msg, "\n")
print_error <- function(msg) cat("❌", msg, "\n")
print_info <- function(msg) cat("ℹ️ ", msg, "\n")
print_warning <- function(msg) cat("⚠️ ", msg, "\n")

# 主函數
update_app <- function(source_file = NULL, backup = TRUE) {
  cat("\n📱 更新 app.R 工具\n")
  cat("==================\n\n")
  
  # 如果沒有指定檔案，列出可選項
  if (is.null(source_file)) {
    # 列出所有可能的 app 檔案
    app_files <- list.files(pattern = "^(full_app_.*\\.R|app.*\\.R)$", ignore.case = FALSE)
    app_files <- app_files[app_files != "app.R"]  # 排除 app.R 本身
    app_files <- sort(app_files)  # 排序
    
    if (length(app_files) == 0) {
      print_error("找不到任何可用的應用程式檔案")
      return(invisible(FALSE))
    }
    
    cat("可用的應用程式檔案：\n")
    cat("--------------------\n")
    for (i in seq_along(app_files)) {
      file_info <- file.info(app_files[i])
      size_kb <- round(file_info$size / 1024, 1)
      mod_time <- format(file_info$mtime, "%Y-%m-%d %H:%M")
      cat(sprintf("[%d] %s (%.1f KB, 修改時間: %s)\n", 
                  i, app_files[i], size_kb, mod_time))
    }
    
    # 特別標記建議的檔案
    if ("full_app_v17.R" %in% app_files) {
      cat("\n💡 建議使用 full_app_v17.R (最新版本)\n")
    }
    
    # 讓用戶選擇
    cat("\n請輸入編號選擇檔案 (輸入 0 取消): ")
    choice <- as.integer(readline())
    
    if (is.na(choice) || choice == 0) {
      print_info("取消操作")
      return(invisible(FALSE))
    }
    
    if (choice < 1 || choice > length(app_files)) {
      print_error("無效的選擇")
      return(invisible(FALSE))
    }
    
    source_file <- app_files[choice]
  }
  
  # 檢查來源檔案是否存在
  if (!file.exists(source_file)) {
    print_error(paste("找不到檔案:", source_file))
    return(invisible(FALSE))
  }
  
  # 檢查當前 app.R 狀態
  cat("\n檢查當前狀態...\n")
  if (file.exists("app.R")) {
    # 比較內容
    current_content <- readLines("app.R", warn = FALSE)
    new_content <- readLines(source_file, warn = FALSE)
    
    if (identical(current_content, new_content)) {
      print_info("app.R 已經與選定檔案相同，無需更新")
      return(invisible(TRUE))
    }
    
    # 備份現有的 app.R
    if (backup) {
      backup_name <- paste0("app.R.backup.", format(Sys.time(), "%Y%m%d_%H%M%S"))
      print_info(paste("備份現有 app.R 為:", backup_name))
      file.copy("app.R", backup_name)
    }
  }
  
  # 執行複製
  cat("\n執行更新...\n")
  print_info(paste("複製", source_file, "到 app.R"))
  
  if (file.copy(source_file, "app.R", overwrite = TRUE)) {
    print_success("app.R 已成功更新！")
    
    # 顯示檔案資訊
    file_info <- file.info("app.R")
    cat("\n新的 app.R 資訊：\n")
    cat(sprintf("  - 大小: %.1f KB\n", file_info$size / 1024))
    cat(sprintf("  - 來源: %s\n", source_file))
    
    # 提醒後續步驟
    cat("\n下一步：\n")
    cat("1. 確認 manifest.json 是最新的：rsconnect::writeManifest()\n")
    cat("2. 提交到 Git：git add app.R && git commit -m 'Update app.R'\n")
    cat("3. 推送到 GitHub：git push\n")
    cat("4. 在 Posit Connect Cloud 重新部署\n")
    
    return(invisible(TRUE))
  } else {
    print_error("複製失敗！")
    return(invisible(FALSE))
  }
}

# 快速更新函數（直接指定檔案）
quick_update <- function(version = 17) {
  source_file <- paste0("full_app_v", version, ".R")
  update_app(source_file, backup = TRUE)
}

# 如果直接執行腳本
if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) > 0) {
    # 支援命令列參數
    update_app(args[1])
  } else {
    # 互動式選擇
    update_app()
  }
} else {
  # 在 R 控制台中提供說明
  cat("使用方法：\n")
  cat("1. 互動式選擇：update_app()\n")
  cat("2. 指定檔案：update_app('full_app_v17.R')\n")
  cat("3. 快速更新到特定版本：quick_update(17)\n")
  cat("4. 不備份：update_app('full_app_v17.R', backup = FALSE)\n")
} 