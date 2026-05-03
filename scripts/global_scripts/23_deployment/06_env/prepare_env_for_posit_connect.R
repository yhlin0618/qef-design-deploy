#!/usr/bin/env Rscript
# ============================================================================
# 準備 Posit Connect Cloud 環境變數清單
# ============================================================================
# 此腳本幫助您準備環境變數，但不會顯示敏感值
# ============================================================================

cat("\n====================================\n")
cat("   環境變數準備工具\n")
cat("====================================\n\n")

# 讀取 .env 檔案
if (!file.exists(".env")) {
  cat("❌ 找不到 .env 檔案\n")
  stop("請確認在 positioning_app 目錄下執行")
}

# 讀取環境變數
env_lines <- readLines(".env")

# 過濾出有效的環境變數行（排除註解和空行）
env_vars <- env_lines[grepl("^[A-Z_]+=", env_lines)]

# 提取變數名稱
var_names <- gsub("=.*", "", env_vars)

cat("找到以下環境變數：\n")
cat(rep("-", 50), "\n", sep = "")

# 分類顯示
pg_vars <- var_names[grepl("^PG", var_names)]
openai_vars <- var_names[grepl("^OPENAI", var_names)]
other_vars <- setdiff(var_names, c(pg_vars, openai_vars))

if (length(pg_vars) > 0) {
  cat("\n📊 PostgreSQL 相關：\n")
  for (var in pg_vars) {
    cat("  •", var, "\n")
  }
}

if (length(openai_vars) > 0) {
  cat("\n🤖 OpenAI API 相關：\n")
  for (var in openai_vars) {
    cat("  •", var, "\n")
  }
}

if (length(other_vars) > 0) {
  cat("\n🔧 其他變數：\n")
  for (var in other_vars) {
    cat("  •", var, "\n")
  }
}

# 創建 .Renviron 範本（如果需要）
cat("\n\n是否要創建 .Renviron 檔案？（用於本地測試）\n")
cat("⚠️  注意：這會包含您的實際密碼，請小心保管\n")
response <- readline("創建 .Renviron？(y/n): ")

if (tolower(response) == "y") {
  # 將 .env 格式轉換為 .Renviron 格式
  renviron_lines <- character()
  
  for (line in env_vars) {
    # 移除引號（.Renviron 不需要引號）
    line <- gsub('"', "'", line)
    renviron_lines <- c(renviron_lines, line)
  }
  
  writeLines(renviron_lines, ".Renviron")
  cat("\n✅ 已創建 .Renviron 檔案\n")
  
  # 檢查 .gitignore
  if (file.exists(".gitignore")) {
    gitignore <- readLines(".gitignore")
    if (!any(grepl("^\\.Renviron$", gitignore))) {
      cat("⚠️  .Renviron 未在 .gitignore 中，正在添加...\n")
      gitignore <- c(gitignore, ".Renviron")
      writeLines(gitignore, ".gitignore")
    }
  }
}

# 提供部署指引
cat("\n\n====================================\n")
cat("   Posit Connect Cloud 部署步驟\n")
cat("====================================\n\n")

cat("1. 確保所有變更已提交到 GitHub（但不包含 .env 或 .Renviron）\n\n")

cat("2. 在 Posit Connect Cloud 部署時：\n")
cat("   a. 選擇 GitHub repository 和 app.R\n")
cat("   b. 點擊 'Advanced settings'\n")
cat("   c. 在 'Configure variables' 下逐一添加上述環境變數\n")
cat("   d. 從您的 .env 檔案複製對應的值\n\n")

cat("3. 需要設定的環境變數（共", length(var_names), "個）：\n")
for (i in seq_along(var_names)) {
  cat(sprintf("   %2d. %s\n", i, var_names[i]))
}

cat("\n⚠️  安全提醒：\n")
cat("   • 不要將 .env 或 .Renviron 提交到 Git\n")
cat("   • 定期更新密碼和 API Key\n")
cat("   • 使用不同環境的不同憑證\n")

cat("\n詳細說明請參考：POSIT_CONNECT_ENV_VARIABLES.md\n") 