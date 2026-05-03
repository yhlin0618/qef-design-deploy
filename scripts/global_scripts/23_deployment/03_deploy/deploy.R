#!/usr/bin/env Rscript
# ============================================================================
# Positioning App 部署腳本
# ============================================================================
# 用途：自動化部署 positioning_app 到 ShinyApps.io 或 Posit Connect
# 作者：AI Assistant
# 日期：2024-01-15
# ============================================================================

# 載入必要套件
library(rsconnect)

# 設定部署參數
APP_NAME <- "positioning_app"
PRIMARY_DOC <- "full_app_v17.R"
DEPLOY_DOC <- "app.R"

# 顏色輸出函數
message_info <- function(msg) {
  cat("\033[1;34m[INFO]\033[0m", msg, "\n")
}

message_success <- function(msg) {
  cat("\033[1;32m[SUCCESS]\033[0m", msg, "\n")
}

message_error <- function(msg) {
  cat("\033[1;31m[ERROR]\033[0m", msg, "\n")
}

message_warning <- function(msg) {
  cat("\033[1;33m[WARNING]\033[0m", msg, "\n")
}

# 檢查工作目錄
check_working_directory <- function() {
  message_info("檢查工作目錄...")
  
  if (!file.exists(PRIMARY_DOC)) {
    message_error(paste("找不到", PRIMARY_DOC))
    message_info("請確保在 positioning_app 目錄下執行此腳本")
    stop("工作目錄錯誤")
  }
  
  message_success("工作目錄正確")
}

# 準備部署檔案
prepare_deployment_files <- function() {
  message_info("準備部署檔案...")
  
  # 確保 app.R 是最新版本
  if (file.exists(DEPLOY_DOC)) {
    # 比較檔案內容
    if (!identical(readLines(PRIMARY_DOC), readLines(DEPLOY_DOC))) {
      message_warning("app.R 與 full_app_v17.R 不同，正在更新...")
      file.copy(PRIMARY_DOC, DEPLOY_DOC, overwrite = TRUE)
      message_success("app.R 已更新")
    } else {
      message_success("app.R 已是最新版本")
    }
  } else {
    message_info("創建 app.R...")
    file.copy(PRIMARY_DOC, DEPLOY_DOC)
    message_success("app.R 已創建")
  }
}

# 更新 manifest.json
update_manifest <- function() {
  message_info("更新 manifest.json...")
  
  tryCatch({
    rsconnect::writeManifest(appPrimaryDoc = PRIMARY_DOC)
    message_success("manifest.json 已更新")
  }, error = function(e) {
    message_error(paste("更新 manifest.json 失敗:", e$message))
    stop(e)
  })
}

# 檢查必要檔案
check_required_files <- function() {
  message_info("檢查必要檔案...")
  
  required_files <- c(
    DEPLOY_DOC,
    "manifest.json",
    "www/",
    "icons/"
  )
  
  missing_files <- c()
  
  for (file in required_files) {
    if (!file.exists(file)) {
      missing_files <- c(missing_files, file)
    }
  }
  
  if (length(missing_files) > 0) {
    message_error("缺少必要檔案：")
    for (file in missing_files) {
      cat("  -", file, "\n")
    }
    stop("請確保所有必要檔案都存在")
  }
  
  message_success("所有必要檔案都存在")
}

# 檢查 rsconnect 帳號
check_rsconnect_account <- function() {
  message_info("檢查 rsconnect 帳號...")
  
  accounts <- rsconnect::accounts()
  
  if (nrow(accounts) == 0) {
    message_error("未設置 rsconnect 帳號")
    message_info("請執行以下指令設置帳號：")
    cat('rsconnect::setAccountInfo(
  name = "kyle-lin",
  token = "YOUR_TOKEN",
  secret = "YOUR_SECRET"
)\n')
    stop("請先設置 rsconnect 帳號")
  }
  
  message_success(paste("找到", nrow(accounts), "個 rsconnect 帳號"))
  print(accounts)
}

# 部署到 ShinyApps.io
deploy_to_shinyapps <- function(force_update = TRUE) {
  message_info("開始部署到 ShinyApps.io...")
  
  # 顯示部署資訊
  message_info("部署配置：")
  cat("  - App Name:", APP_NAME, "\n")
  cat("  - Primary Doc:", PRIMARY_DOC, "\n")
  cat("  - Deploy Doc:", DEPLOY_DOC, "\n")
  
  # 執行部署
  tryCatch({
    rsconnect::deployApp(
      appName = APP_NAME,
      appTitle = APP_NAME,
      appFiles = c(
        DEPLOY_DOC,
        "manifest.json",
        "www/",
        "icons/",
        "md/",
        "documents/",
        "scripts/"
      ),
      forceUpdate = force_update,
      logLevel = "verbose"
    )
    
    message_success("部署成功！")
    
    # 顯示應用 URL
    apps <- rsconnect::applications()
    app_info <- apps[apps$name == APP_NAME, ]
    if (nrow(app_info) > 0) {
      message_info(paste("應用 URL:", app_info$url[1]))
    }
    
  }, error = function(e) {
    message_error(paste("部署失敗:", e$message))
    
    # 提供除錯建議
    message_info("除錯建議：")
    cat("1. 檢查網路連線\n")
    cat("2. 確認帳號權限\n")
    cat("3. 查看詳細錯誤：\n")
    cat('   options(rsconnect.http.verbose = TRUE)\n')
    cat('   rsconnect::showLogs(appName = "', APP_NAME, '")\n', sep = "")
    
    stop(e)
  })
}

# 顯示部署日誌
show_deployment_logs <- function() {
  message_info("顯示部署日誌...")
  
  tryCatch({
    rsconnect::showLogs(appName = APP_NAME)
  }, error = function(e) {
    message_warning("無法顯示日誌")
  })
}

# 主函數
main <- function() {
  cat("\n")
  cat("====================================\n")
  cat("   Positioning App 部署腳本\n")
  cat("====================================\n")
  cat("\n")
  
  # 執行檢查和部署步驟
  check_working_directory()
  prepare_deployment_files()
  update_manifest()
  check_required_files()
  check_rsconnect_account()
  
  # 詢問是否繼續部署
  cat("\n")
  response <- readline("是否繼續部署？(y/n): ")
  
  if (tolower(response) == "y") {
    deploy_to_shinyapps()
    show_deployment_logs()
  } else {
    message_info("部署已取消")
  }
  
  cat("\n")
  message_info("部署腳本執行完畢")
}

# 執行主函數
if (interactive()) {
  main()
} else {
  # 非互動模式下自動執行
  main()
} 