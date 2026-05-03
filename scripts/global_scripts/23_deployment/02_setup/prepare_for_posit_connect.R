#!/usr/bin/env Rscript
# ============================================================================
# 準備 Posit Connect Cloud 部署（GitHub 方式）
# ============================================================================

cat("\n====================================\n")
cat("   Posit Connect Cloud 部署準備\n")
cat("====================================\n\n")

# 步驟 1：檢查關鍵檔案
cat("步驟 1：檢查關鍵檔案\n")
cat(rep("-", 30), "\n", sep = "")

files_ok <- TRUE

# 檢查 app.R
if (file.exists("app.R")) {
  cat("✅ app.R 存在\n")
} else if (file.exists("full_app_v17.R")) {
  cat("⚠️  app.R 不存在，但找到 full_app_v17.R\n")
  cat("   正在複製...\n")
  file.copy("full_app_v17.R", "app.R", overwrite = TRUE)
  cat("✅ 已創建 app.R\n")
} else {
  cat("❌ 找不到主應用程式檔案\n")
  files_ok <- FALSE
}

# 檢查 manifest.json
if (file.exists("manifest.json")) {
  cat("✅ manifest.json 存在\n")
  # 檢查是否需要更新
  file_age <- difftime(Sys.time(), file.info("manifest.json")$mtime, units = "days")
  if (file_age > 7) {
    cat("⚠️  manifest.json 已超過 7 天，建議更新\n")
  }
} else {
  cat("⚠️  manifest.json 不存在，正在創建...\n")
  library(rsconnect)
  rsconnect::writeManifest()
  cat("✅ 已創建 manifest.json\n")
}

# 步驟 2：修正 .gitignore
cat("\n步驟 2：修正 .gitignore\n")
cat(rep("-", 30), "\n", sep = "")

if (file.exists(".gitignore")) {
  gitignore_lines <- readLines(".gitignore")
  manifest_line <- which(gitignore_lines == "manifest.json")
  
  if (length(manifest_line) > 0) {
    cat("⚠️  manifest.json 在 .gitignore 中被排除\n")
    cat("   正在修正...\n")
    
    # 註解掉 manifest.json 行
    gitignore_lines[manifest_line] <- "# manifest.json  # 需要包含在 Git 中以供 Posit Connect Cloud 使用"
    writeLines(gitignore_lines, ".gitignore")
    
    cat("✅ 已修正 .gitignore\n")
  } else {
    cat("✅ .gitignore 設定正確\n")
  }
}

# 步驟 3：檢查 Git 狀態
cat("\n步驟 3：檢查 Git 狀態\n")
cat(rep("-", 30), "\n", sep = "")

git_status <- system("git status --porcelain", intern = TRUE)
if (length(git_status) > 0) {
  cat("📝 有未提交的變更：\n")
  cat(paste("   ", git_status), sep = "\n")
} else {
  cat("✅ 所有變更已提交\n")
}

# 檢查遠端設定
remotes <- system("git remote -v", intern = TRUE)
if (length(remotes) > 0) {
  cat("\n📡 Git 遠端設定：\n")
  cat(paste("   ", remotes), sep = "\n")
} else {
  cat("\n⚠️  尚未設定 Git 遠端\n")
  cat("   請執行：git remote add origin https://github.com/YOUR_USERNAME/positioning_app.git\n")
}

# 步驟 4：提供後續步驟
cat("\n\n====================================\n")
cat("   後續步驟\n")
cat("====================================\n\n")

if (files_ok) {
  cat("1. 如果需要更新 manifest.json：\n")
  cat("   rsconnect::writeManifest()\n\n")
  
  cat("2. 提交所有變更到 Git：\n")
  cat("   git add .\n")
  cat("   git commit -m \"Prepare for Posit Connect Cloud deployment\"\n\n")
  
  cat("3. 推送到 GitHub（確保是公開 repository）：\n")
  cat("   git push origin main\n\n")
  
  cat("4. 在 Posit Connect Cloud 部署：\n")
  cat("   - 登入 https://connect.posit.cloud\n")
  cat("   - 點擊 Publish → Shiny\n")
  cat("   - 選擇您的 GitHub repository\n")
  cat("   - 選擇 app.R 作為主檔案\n")
  cat("   - 點擊 Publish\n\n")
  
  cat("詳細說明請參考：POSIT_CONNECT_CLOUD_GITHUB_DEPLOYMENT.md\n")
} else {
  cat("❌ 請先修正上述問題再繼續\n")
}

cat("\n部署準備檢查完成！\n") 