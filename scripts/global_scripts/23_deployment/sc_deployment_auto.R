#!/usr/bin/env Rscript
# ============================================================================
# 自動部署腳本 - Positioning App (無需互動)
# ============================================================================

cat("\n🚀 Positioning App 自動部署\n")
cat("============================\n")
cat("開始時間:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# 設定基礎路徑
if (basename(getwd()) != "positioning_app") {
  stop("❌ 請在 positioning_app 目錄下執行此腳本！")
}

DEPLOY_BASE <- "scripts/global_scripts/23_deployment"

# 自動步驟 1：更新 app.R 到最新版本
cat("📌 步驟 1：確保 app.R 是最新版本\n")
if (file.exists("full_app_v17.R")) {
  file.copy("full_app_v17.R", "app.R", overwrite = TRUE)
  cat("✅ app.R 已更新到 v17\n")
} else {
  stop("找不到 full_app_v17.R")
}

# 自動步驟 2：更新 manifest.json
cat("\n📌 步驟 2：更新依賴清單\n")
library(rsconnect)
rsconnect::writeManifest()
cat("✅ manifest.json 已更新\n")

# 自動步驟 3：檢查關鍵檔案
cat("\n📌 步驟 3：檢查關鍵檔案\n")
required_files <- c("app.R", "manifest.json", ".gitignore")
for (f in required_files) {
  if (file.exists(f)) {
    cat("✅", f, "存在\n")
  } else {
    cat("❌", f, "缺失\n")
    stop("缺少必要檔案：", f)
  }
}

# 自動步驟 4：準備 Git 提交
cat("\n📌 步驟 4：檢查 Git 狀態\n")
git_status <- system2("git", "status --porcelain", stdout = TRUE, stderr = FALSE)
if (length(git_status) > 0) {
  cat("📝 有未提交的變更：\n")
  cat(paste("  ", head(git_status, 5)), sep = "\n")
  if (length(git_status) > 5) {
    cat("  ... 還有", length(git_status) - 5, "個檔案\n")
  }
  cat("\n建議執行：\n")
  cat("  git add -A\n")
  cat("  git commit -m 'Update for deployment'\n")
  cat("  git push\n")
} else {
  cat("✅ Git 工作區乾淨\n")
}

# 顯示部署指示
cat("\n============================\n")
cat("📋 部署準備完成！\n")
cat("============================\n\n")

cat("✅ 所有檔案已準備就緒\n\n")

cat("🌐 Posit Connect Cloud 部署：\n")
cat("1. 登入: https://connect.posit.cloud\n")
cat("2. 點擊 Publish → Shiny\n")
cat("3. 填寫:\n")
cat("   Repository: kiki830621/ai_martech\n")
cat("   Application Path: l1_basic/positioning_app\n")
cat("   Primary File: app.R\n")
cat("   Branch: main\n\n")

cat("💻 或使用 ShinyApps.io：\n")
cat("執行: rsconnect::deployApp()\n")

cat("\n結束時間:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n") 
