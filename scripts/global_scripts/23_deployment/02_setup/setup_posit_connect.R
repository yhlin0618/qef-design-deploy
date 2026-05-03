#!/usr/bin/env Rscript
# ============================================================================
# 設定 Posit Connect Cloud 部署環境
# ============================================================================

cat("\n====================================\n")
cat("   Posit Connect Cloud 部署設定\n")
cat("====================================\n\n")

# 讀取現有的 .env 檔案（如果存在）
existing_env <- character()
if (file.exists(".env")) {
  existing_env <- readLines(".env")
  cat("找到現有的 .env 檔案\n\n")
}

# 收集 Posit Connect Cloud 資訊
cat("請提供 Posit Connect Cloud 的連線資訊：\n")
cat("（您可以從 https://connect.posit.cloud 獲取這些資訊）\n\n")

# Connect Server URL
connect_server <- readline("Posit Connect Server URL (例如: https://connect.posit.cloud): ")
if (connect_server == "") {
  connect_server <- "https://connect.posit.cloud"
}

# API Key
cat("\n如何獲取 API Key：\n")
cat("1. 登入 Posit Connect Cloud\n")
cat("2. 點擊右上角的用戶圖標\n")
cat("3. 選擇 'API Keys'\n")
cat("4. 點擊 'New API Key'\n\n")
api_key <- readline("請輸入您的 API Key: ")

# 應用程式設定
app_name <- readline("\n應用程式名稱 (預設: positioning_app): ")
if (app_name == "") app_name <- "positioning_app"

app_title <- readline("應用程式標題 (預設: Product Positioning Analysis): ")
if (app_title == "") app_title <- "Product Positioning Analysis"

# 建立新的 .env 內容
new_env <- c(
  "# Positioning App 環境變數設定",
  "# Posit Connect Cloud 部署",
  paste0("# 生成時間：", Sys.time()),
  "",
  "# 部署目標",
  "DEPLOY_TARGET=connect",
  "",
  "# Posit Connect Cloud 設定",
  paste0("CONNECT_SERVER=", connect_server),
  paste0("CONNECT_API_KEY=", api_key),
  "",
  "# 應用程式設定",
  paste0("SHINYAPPS_APP_NAME=", app_name),
  paste0("APP_TITLE=", app_title),
  "",
  "# ShinyApps.io 設定（備用）",
  "SHINYAPPS_ACCOUNT=kyle-lin"
)

# 保留原有的其他設定（如資料庫連線等）
db_settings <- grep("^(PG|OPENAI)", existing_env, value = TRUE)
if (length(db_settings) > 0) {
  new_env <- c(new_env, "", "# 其他設定", db_settings)
}

# 寫入 .env 檔案
writeLines(new_env, ".env")

cat("\n✅ .env 檔案已更新！\n\n")

# 顯示設定摘要
cat("設定摘要：\n")
cat(rep("-", 50), "\n", sep = "")
cat("部署目標：Posit Connect Cloud\n")
cat("伺服器：", connect_server, "\n")
cat("應用名稱：", app_name, "\n")
cat("應用標題：", app_title, "\n")
cat(rep("-", 50), "\n\n", sep = "")

# 檢查 rsconnect 是否已配置 Posit Connect
if (requireNamespace("rsconnect", quietly = TRUE)) {
  servers <- rsconnect::servers()
  if (nrow(servers) > 0) {
    connect_configured <- any(grepl("connect", servers$url, ignore.case = TRUE))
    if (!connect_configured) {
      cat("⚠️  注意：尚未在 rsconnect 中配置 Posit Connect 伺服器\n")
      cat("您可能需要執行：\n")
      cat(sprintf('rsconnect::connectApiUser(\n  server = "%s",\n  apiKey = "%s"\n)\n', 
                  connect_server, api_key))
    }
  }
}

cat("\n下一步：\n")
cat("1. 執行部署腳本：\n")
cat("   Rscript deploy_app.R\n")
cat("   # 或\n")
cat("   source('deploy_app.R')\n\n")

cat("2. 如果是首次部署到 Posit Connect Cloud，可能需要：\n")
cat("   - 確認您的 Posit Connect Cloud 帳戶已啟用\n")
cat("   - 確認您有足夠的權限部署應用\n")
cat("   - 檢查應用程式的資源限制設定\n") 