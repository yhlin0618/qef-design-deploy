#!/usr/bin/env Rscript
# ============================================================================
# 部署環境設定助手
# ============================================================================
# 此腳本協助您設定部署所需的環境變數
# ============================================================================

cat("\n====================================\n")
cat("   Positioning App 部署環境設定\n")
cat("====================================\n\n")

# 檢查是否已有 .env 檔案
if (file.exists(".env")) {
  cat("⚠️  發現現有的 .env 檔案\n")
  response <- readline("是否要覆蓋現有設定？(y/n): ")
  if (tolower(response) != "y") {
    cat("取消設定。\n")
    quit("no")
  }
}

# 選擇部署目標
cat("\n選擇部署目標：\n")
cat("1. ShinyApps.io (預設)\n")
cat("2. Posit Connect\n")
choice <- readline("請選擇 (1 或 2): ")

if (choice == "2") {
  deploy_target <- "connect"
} else {
  deploy_target <- "shinyapps"
}

# 收集設定資訊
env_content <- paste0("# Positioning App 環境變數設定\n",
                      "# 生成時間：", Sys.time(), "\n\n")

env_content <- paste0(env_content, "# 部署目標\n")
env_content <- paste0(env_content, "DEPLOY_TARGET=", deploy_target, "\n\n")

if (deploy_target == "shinyapps") {
  cat("\n=== ShinyApps.io 設定 ===\n")
  
  # 應用名稱
  app_name <- readline("應用名稱 (預設: positioning_app): ")
  if (app_name == "") app_name <- "positioning_app"
  
  # 帳號名稱
  account <- readline("ShinyApps.io 帳號名稱 (預設: kyle-lin): ")
  if (account == "") account <- "kyle-lin"
  
  # 應用標題
  app_title <- readline("應用標題 (預設: Product Positioning Analysis): ")
  if (app_title == "") app_title <- "Product Positioning Analysis"
  
  env_content <- paste0(env_content, "# ShinyApps.io 設定\n")
  env_content <- paste0(env_content, "SHINYAPPS_APP_NAME=", app_name, "\n")
  env_content <- paste0(env_content, "SHINYAPPS_ACCOUNT=", account, "\n")
  env_content <- paste0(env_content, "APP_TITLE=", app_title, "\n\n")
  
} else {
  cat("\n=== Posit Connect 設定 ===\n")
  
  # Connect 伺服器
  server <- readline("Posit Connect 伺服器 URL: ")
  
  # API 金鑰
  api_key <- readline("Posit Connect API 金鑰: ")
  
  # 應用名稱
  app_name <- readline("應用名稱 (預設: positioning_app): ")
  if (app_name == "") app_name <- "positioning_app"
  
  # 應用標題
  app_title <- readline("應用標題 (預設: Product Positioning Analysis): ")
  if (app_title == "") app_title <- "Product Positioning Analysis"
  
  env_content <- paste0(env_content, "# Posit Connect 設定\n")
  env_content <- paste0(env_content, "CONNECT_SERVER=", server, "\n")
  env_content <- paste0(env_content, "CONNECT_API_KEY=", api_key, "\n")
  env_content <- paste0(env_content, "SHINYAPPS_APP_NAME=", app_name, "\n")
  env_content <- paste0(env_content, "APP_TITLE=", app_title, "\n\n")
}

# 詢問是否需要資料庫設定
cat("\n")
db_response <- readline("是否需要設定資料庫連線？(y/n): ")
if (tolower(db_response) == "y") {
  cat("\n=== PostgreSQL 設定 ===\n")
  
  pg_host <- readline("PostgreSQL 主機 (預設: localhost): ")
  if (pg_host == "") pg_host <- "localhost"
  
  pg_port <- readline("PostgreSQL 埠號 (預設: 5432): ")
  if (pg_port == "") pg_port <- "5432"
  
  pg_user <- readline("PostgreSQL 使用者名稱: ")
  pg_password <- readline("PostgreSQL 密碼: ")
  pg_database <- readline("PostgreSQL 資料庫名稱: ")
  
  env_content <- paste0(env_content, "# PostgreSQL 設定\n")
  env_content <- paste0(env_content, "PGHOST=", pg_host, "\n")
  env_content <- paste0(env_content, "PGPORT=", pg_port, "\n")
  env_content <- paste0(env_content, "PGUSER=", pg_user, "\n")
  env_content <- paste0(env_content, "PGPASSWORD=", pg_password, "\n")
  env_content <- paste0(env_content, "PGDATABASE=", pg_database, "\n")
  env_content <- paste0(env_content, "PGSSLMODE=require\n\n")
}

# 詢問是否需要 OpenAI API
cat("\n")
openai_response <- readline("是否需要設定 OpenAI API？(y/n): ")
if (tolower(openai_response) == "y") {
  api_key <- readline("OpenAI API 金鑰: ")
  env_content <- paste0(env_content, "# OpenAI API\n")
  env_content <- paste0(env_content, "OPENAI_API_KEY=", api_key, "\n")
}

# 寫入 .env 檔案
writeLines(env_content, ".env")
cat("\n✅ .env 檔案已創建！\n")

# 顯示下一步
cat("\n=== 下一步 ===\n")
cat("1. 檢查 .env 檔案內容：\n")
cat("   cat .env\n\n")

if (deploy_target == "shinyapps") {
  cat("2. 如果尚未設定 rsconnect 帳號，請執行：\n")
  cat("   rsconnect::setAccountInfo(\n")
  cat("     name = \"", account, "\",\n", sep = "")
  cat("     token = \"YOUR_TOKEN\",\n")
  cat("     secret = \"YOUR_SECRET\"\n")
  cat("   )\n\n")
  cat("   從 https://www.shinyapps.io/admin/#/tokens 獲取 token 和 secret\n\n")
}

cat("3. 執行部署：\n")
cat("   source(\"deploy_using_global_scripts.R\")\n")
cat("   # 或\n")
cat("   Rscript deploy_using_global_scripts.R\n\n")

cat("設定完成！\n") 