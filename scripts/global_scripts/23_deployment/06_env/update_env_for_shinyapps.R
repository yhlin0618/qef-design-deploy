#!/usr/bin/env Rscript
# ============================================================================
# 更新 .env 檔案以部署到 ShinyApps.io
# ============================================================================

# 讀取現有的 .env 檔案
if (file.exists(".env")) {
  env_lines <- readLines(".env")
  
  # 修改 DEPLOY_TARGET 為 shinyapps
  env_lines <- gsub("^DEPLOY_TARGET=.*", "DEPLOY_TARGET=shinyapps", env_lines)
  
  # 修改 SHINYAPPS_ACCOUNT 為 kyle-lin
  env_lines <- gsub("^SHINYAPPS_ACCOUNT=.*", "SHINYAPPS_ACCOUNT=kyle-lin", env_lines)
  
  # 寫回檔案
  writeLines(env_lines, ".env")
  
  cat("✅ 已更新 .env 檔案：\n")
  cat("   - DEPLOY_TARGET=shinyapps\n")
  cat("   - SHINYAPPS_ACCOUNT=kyle-lin\n")
  
} else {
  # 創建新的 .env 檔案
  env_content <- c(
    "# Positioning App 環境變數設定",
    "",
    "# 部署目標",
    "DEPLOY_TARGET=shinyapps",
    "",
    "# ShinyApps.io 設定",
    "SHINYAPPS_ACCOUNT=kyle-lin",
    "SHINYAPPS_APP_NAME=positioning_app",
    "APP_TITLE=Product Positioning Analysis"
  )
  
  writeLines(env_content, ".env")
  cat("✅ 已創建新的 .env 檔案\n")
}

cat("\n現在可以執行部署腳本了：\n")
cat("Rscript deploy_using_global_scripts.R\n") 