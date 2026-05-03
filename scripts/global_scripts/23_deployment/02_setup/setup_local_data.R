#!/usr/bin/env Rscript

# ============================================================================
# Positioning App - 本地資料環境設置腳本
# 
# 用途：為開發者創建必要的資料目錄結構和測試資料
# 注意：所有生成的資料都是假資料，僅供開發測試使用
# ============================================================================

# 載入必要的套件
required_packages <- c("DBI", "RSQLite", "openxlsx")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

# 1. 創建目錄結構
cat("正在創建目錄結構...\n")
dirs <- c("data/sample", "data/test", "data/user", "database", "cache")
for (dir in dirs) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  cat("✓ 創建目錄:", dir, "\n")
}

# 2. 創建 .gitkeep 檔案
gitkeep_files <- c("data/user/.gitkeep", "cache/.gitkeep")
for (file in gitkeep_files) {
  if (!file.exists(file)) {
    file.create(file)
  }
}

# 3. 創建使用者資料庫
cat("\n正在創建使用者資料庫...\n")
con <- dbConnect(SQLite(), "database/users.sqlite")
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    email TEXT,
    role TEXT DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
  )
")

# 新增測試使用者
dbExecute(con, "
  INSERT OR IGNORE INTO users (username, password, email, role) 
  VALUES ('test', 'test123', 'test@example.com', 'admin')
")
cat("✓ 創建資料庫: database/users.sqlite\n")
cat("✓ 測試帳號: username='test', password='test123'\n")
dbDisconnect(con)

# 4. 生成測試資料
cat("\n正在生成測試資料...\n")

# 生成假的評論資料
generate_fake_reviews <- function(n = 100) {
  set.seed(42)  # 確保可重現性
  
  products <- c("電動開罐器A", "電動開罐器B", "電動開罐器C")
  comments <- c(
    "非常好用，推薦！",
    "品質不錯，物超所值",
    "使用方便，但有點吵",
    "設計精美，功能完善",
    "價格偏高，但品質好",
    "普通，沒有特別之處",
    "需要改進的地方很多",
    "完全符合期待",
    "CP值很高",
    "不推薦購買"
  )
  
  data.frame(
    review_id = sprintf("R%06d", 1:n),
    product = sample(products, n, replace = TRUE),
    rating = sample(1:5, n, replace = TRUE, 
                    prob = c(0.1, 0.15, 0.25, 0.35, 0.15)),
    comment = sample(comments, n, replace = TRUE),
    reviewer = paste0("User", sample(1:50, n, replace = TRUE)),
    date = as.Date("2024-01-01") + sample(0:180, n, replace = TRUE),
    verified_purchase = sample(c(TRUE, FALSE), n, replace = TRUE, prob = c(0.8, 0.2)),
    helpful_votes = rpois(n, lambda = 5),
    total_votes = rpois(n, lambda = 8)
  )
}

# 儲存測試評論資料（Excel 格式）
fake_reviews <- generate_fake_reviews(200)
write.xlsx(fake_reviews, "data/test/fake_data.xlsx", asTable = TRUE)
cat("✓ 生成檔案: data/test/fake_data.xlsx (200 筆假評論)\n")

# 生成範例評論資料（較小的資料集）
sample_reviews <- generate_fake_reviews(50)
write.xlsx(sample_reviews, "data/sample/amazon_can_opener_reviews.xlsx", asTable = TRUE)
cat("✓ 生成檔案: data/sample/amazon_can_opener_reviews.xlsx (50 筆範例評論)\n")

# 5. 創建設定檔範本
cat("\n正在創建設定檔範本...\n")
config_template <- '# 應用程式設定
# 請複製此檔案為 config.R 並修改設定值

APP_CONFIG <- list(
  # 資料庫設定
  db_path = "database/users.sqlite",
  
  # API 設定（如需要）
  api_key = "YOUR_API_KEY_HERE",
  api_endpoint = "https://api.example.com",
  
  # 資料路徑
  sample_data = "data/sample/amazon_can_opener_reviews.xlsx",
  
  # 其他設定
  max_upload_size = 10 * 1024 * 1024  # 10MB
)
'

writeLines(config_template, "config_template.R")
cat("✓ 創建檔案: config_template.R\n")

# 6. 完成訊息
cat("\n========================================\n")
cat("✅ 本地資料環境設置完成！\n")
cat("========================================\n\n")
cat("下一步：\n")
cat("1. 複製 config_template.R 為 config.R 並修改設定\n")
cat("2. 執行 app.R 開始使用應用程式\n")
cat("3. 使用測試帳號登入：username='test', password='test123'\n")
cat("\n注意：所有生成的資料都是假資料，實際資料需另外獲取\n") 