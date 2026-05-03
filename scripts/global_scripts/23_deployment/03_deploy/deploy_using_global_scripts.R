#!/usr/bin/env Rscript
# ============================================================================
# 使用 Global Scripts 部署 positioning_app
# ============================================================================
# 這個腳本使用 global_scripts 中的標準部署工具
# ============================================================================

# 使用 rprojroot 自動尋找專案根目錄
if (!requireNamespace("rprojroot", quietly = TRUE)) {
  cat("正在安裝 rprojroot 套件...\n")
  install.packages("rprojroot")
}

library(rprojroot)

# 定義多個可能的專案根目錄標準
# 優先順序很重要：
# 1. 先找包含 l1_basic 的目錄（這是專案的特徵）
# 2. 再找 Git 根目錄
# 3. 包含 global_scripts 的目錄
root_criterion <- has_dir("l1_basic") | 
                  has_dir("global_scripts") |
                  is_git_root

# 從當前位置尋找專案根目錄
project_root <- find_root(root_criterion)
cat("✅ 找到專案根目錄：", project_root, "\n")

# 額外驗證：確保找到的是真正的專案根目錄
expected_dirs <- c("l1_basic", "l2_pro", "l4_enterprise", "global_scripts")
found_dirs <- sum(sapply(expected_dirs, function(d) dir.exists(file.path(project_root, d))))

if (found_dirs < 2) {
  # 如果找到的目錄少於2個預期目錄，嘗試從 Git 根目錄找
  cat("⚠️  找到的目錄可能不是專案根目錄，改用 Git 根目錄\n")
  project_root <- find_root(is_git_root)
  cat("使用 Git 根目錄：", project_root, "\n")
}

# 驗證找到的是正確的專案
if (!dir.exists(file.path(project_root, "l1_basic"))) {
  # 如果還是找不到，可能需要手動設定
  cat("❌ 找到的目錄不包含 l1_basic\n")
  cat("當前找到的路徑：", project_root, "\n")
  
  stop("錯誤：無法找到正確的專案根目錄")
}

# 顯示找到的目錄結構
cat("\n專案結構驗證：\n")
dirs_to_check <- c("l1_basic", "l2_pro", "l4_enterprise", "global_scripts", "docs")
for (dir in dirs_to_check) {
  if (dir.exists(file.path(project_root, dir))) {
    cat("  ✅", dir, "\n")
  }
}

# 切換到應用程式目錄
app_dir <- file.path(project_root, "l1_basic", "positioning_app")
if (!dir.exists(app_dir)) {
  stop("錯誤：找不到應用程式目錄：", app_dir)
}

setwd(app_dir)
cat("\n當前工作目錄：", getwd(), "\n\n")

# 設定環境變數來幫助 global_scripts 找到正確的專案根目錄
# 這很重要，因為 global_scripts 的部署腳本會使用 here 套件
Sys.setenv(HERE_ROOT = project_root)

# 創建一個暫時的 .here 檔案來標記專案根目錄
here_file <- file.path(project_root, ".here")
if (!file.exists(here_file)) {
  cat("創建 .here 檔案來標記專案根目錄...\n")
  file.create(here_file)
  on.exit(unlink(here_file), add = TRUE)  # 確保結束時刪除
}

# 執行 global scripts 中的部署腳本
deployment_script <- file.path(
  app_dir,
  "scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R"
)

if (!file.exists(deployment_script)) {
  stop("找不到部署腳本：", deployment_script)
}

cat("執行部署腳本：\n", deployment_script, "\n\n")

# 暫時改變工作目錄到專案根目錄（讓 here 套件能正確工作）
original_wd <- getwd()
setwd(project_root)

# Source 部署腳本
tryCatch({
  source(deployment_script)
}, finally = {
  # 恢復原始工作目錄
  setwd(original_wd)
}) 
