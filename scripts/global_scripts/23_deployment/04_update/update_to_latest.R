#!/usr/bin/env Rscript
# 一鍵更新 app.R 到最新版本（full_app_v17.R）

source("update_app.R")

cat("🚀 一鍵更新 app.R 到最新版本\n")
cat("============================\n")

# 直接更新到 v17（最新版本）
result <- update_app("full_app_v17.R", backup = TRUE)

if (result) {
  cat("\n✨ 更新完成！\n")
} else {
  cat("\n❌ 更新失敗！\n")
} 