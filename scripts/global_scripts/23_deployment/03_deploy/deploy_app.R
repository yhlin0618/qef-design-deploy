#!/usr/bin/env Rscript
# ============================================================================
# Positioning App 部署腳本（簡化版）
# ============================================================================
# 直接從 positioning_app 目錄執行部署
# ============================================================================

cat("\n====================================\n")
cat("   Positioning App 部署\n")
cat("====================================\n\n")

# 確認當前在 positioning_app 目錄
current_dir <- basename(getwd())
if (current_dir != "positioning_app") {
  cat("⚠️  請在 positioning_app 目錄下執行此腳本\n")
  cat("當前目錄：", getwd(), "\n")
  stop("錯誤的執行目錄")
}

cat("✅ 當前目錄：", getwd(), "\n")

# 載入環境變數
if (file.exists(".env")) {
  readRenviron(".env")
  cat("✅ 已載入 .env 檔案\n")
} else {
  cat("⚠️  未找到 .env 檔案\n")
}

# 檢查部署目標
deploy_target <- Sys.getenv("DEPLOY_TARGET", "shinyapps")
cat("\n部署目標：", deploy_target, "\n")

if (deploy_target == "shinyapps") {
  account <- Sys.getenv("SHINYAPPS_ACCOUNT", "kyle-lin")
  app_name <- Sys.getenv("SHINYAPPS_APP_NAME", "positioning_app")
  cat("ShinyApps.io 帳號：", account, "\n")
  cat("應用名稱：", app_name, "\n")
}

# 執行 global_scripts 中的部署腳本
deployment_script <- "scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R"

if (!file.exists(deployment_script)) {
  cat("\n❌ 找不到部署腳本：", deployment_script, "\n")
  cat("請確認 global_scripts 已正確設置\n")
  stop("缺少部署腳本")
}

cat("\n執行部署腳本...\n")
cat(rep("-", 50), "\n", sep = "")

# Source 部署腳本
source(deployment_script) 