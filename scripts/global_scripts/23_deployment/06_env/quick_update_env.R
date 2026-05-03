#!/usr/bin/env Rscript
# ============================================================================
# 快速更新 .env 以使用 Posit Connect Cloud
# ============================================================================

# 讀取現有的 .env 檔案
env_lines <- readLines(".env")

# 尋找並更新 CONNECT_SERVER（取消註解）
server_line <- grep("^# CONNECT_SERVER=", env_lines)
if (length(server_line) > 0) {
  # 更新為 Posit Connect Cloud 的標準 URL
  env_lines[server_line] <- "CONNECT_SERVER=https://connect.posit.cloud"
}

# 尋找並更新 CONNECT_API_KEY（取消註解但保留提示）
api_line <- grep("^# CONNECT_API_KEY=", env_lines)
if (length(api_line) > 0) {
  env_lines[api_line] <- "CONNECT_API_KEY=YOUR_API_KEY_HERE"
}

# 寫回檔案
writeLines(env_lines, ".env")

cat("✅ 已更新 .env 檔案\n\n")
cat("⚠️  重要：請更新 CONNECT_API_KEY\n\n")

cat("請按照以下步驟獲取 API Key：\n")
cat("1. 前往 https://connect.posit.cloud\n")
cat("2. 登入您的帳戶\n")
cat("3. 點擊右上角的用戶圖標\n")
cat("4. 選擇 'API Keys'\n")
cat("5. 點擊 'New API Key'\n")
cat("6. 複製生成的 API Key\n\n")

cat("然後編輯 .env 檔案，將 YOUR_API_KEY_HERE 替換為您的實際 API Key\n\n")

cat("或執行以下命令直接設定（將 YOUR_ACTUAL_KEY 替換為您的 API Key）：\n")
cat('sed -i "" "s/CONNECT_API_KEY=YOUR_API_KEY_HERE/CONNECT_API_KEY=YOUR_ACTUAL_KEY/" .env\n') 