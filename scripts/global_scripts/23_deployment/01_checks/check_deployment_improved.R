#!/usr/bin/env Rscript
# ============================================================================
# 改進版部署檢查腳本
# ============================================================================

cat("\n📍 部署檢查 - Positioning App\n")
cat("===============================\n\n")

# 1. 確認執行位置
cat("步驟 1：檢查執行環境\n")
cat("------------------------------\n")
cat("當前工作目錄：", getwd(), "\n")

# 檢查是否在正確的目錄
if (basename(getwd()) != "positioning_app") {
  cat("⚠️  警告：請在 positioning_app 目錄下執行此腳本\n")
  cat("   請執行：cd l1_basic/positioning_app\n\n")
}

# 2. 檢查關鍵檔案
cat("\n步驟 2：檢查關鍵檔案\n")
cat("------------------------------\n")

files_to_check <- data.frame(
  file = c("app.R", "full_app_v17.R", "manifest.json", ".env", ".gitignore", "www/", "icons/"),
  description = c(
    "部署入口檔案（Posit Connect 需要）",
    "主應用程式檔案",
    "依賴清單（必須包含在 Git 中）",
    "環境變數（不應包含在 Git 中）",
    "Git 忽略規則",
    "靜態資源目錄",
    "圖標目錄"
  ),
  required = c(TRUE, FALSE, TRUE, FALSE, TRUE, TRUE, TRUE)
)

all_good <- TRUE
for (i in 1:nrow(files_to_check)) {
  exists <- file.exists(files_to_check$file[i])
  if (exists) {
    cat("✅", files_to_check$file[i], "-", files_to_check$description[i], "\n")
  } else {
    if (files_to_check$required[i]) {
      cat("❌", files_to_check$file[i], "-", files_to_check$description[i], "（必需）\n")
      all_good <- FALSE
    } else {
      cat("⚠️ ", files_to_check$file[i], "-", files_to_check$description[i], "（可選）\n")
    }
  }
}

# 3. 檢查檔案同步
cat("\n步驟 3：檢查檔案同步\n")
cat("------------------------------\n")

if (file.exists("app.R") && file.exists("full_app_v17.R")) {
  app_content <- readLines("app.R", warn = FALSE)
  full_app_content <- readLines("full_app_v17.R", warn = FALSE)
  
  if (identical(app_content, full_app_content)) {
    cat("✅ app.R 與 full_app_v17.R 內容相同\n")
  } else {
    cat("⚠️  app.R 與 full_app_v17.R 內容不同\n")
    cat("   建議執行：file.copy('full_app_v17.R', 'app.R', overwrite = TRUE)\n")
  }
}

# 4. 檢查 Git 和部署路徑
cat("\n步驟 4：確認部署路徑\n")
cat("------------------------------\n")

# 尋找 Git root
find_git_root <- function(path = ".") {
  path <- normalizePath(path, mustWork = FALSE)
  while (path != dirname(path)) {
    if (file.exists(file.path(path, ".git"))) {
      return(path)
    }
    path <- dirname(path)
  }
  return(NULL)
}

git_root <- find_git_root()
if (!is.null(git_root)) {
  rel_path <- sub(paste0("^", git_root, "/"), "", normalizePath(getwd()))
  cat("📁 Git Repository 根目錄：", git_root, "\n")
  cat("📍 Application Path：", rel_path, "\n")
  cat("✅ 在 Posit Connect Cloud 填寫：\n")
  cat("   - Repository: kiki830621/ai_martech\n")
  cat("   - Application Path:", rel_path, "\n")
  cat("   - Primary File: app.R\n")
} else {
  cat("⚠️  找不到 Git repository\n")
}

# 5. 檢查 manifest.json
cat("\n步驟 5：檢查 manifest.json\n")
cat("------------------------------\n")

if (file.exists("manifest.json")) {
  size_kb <- file.info("manifest.json")$size / 1024
  cat("✅ manifest.json 存在 (", round(size_kb, 1), "KB)\n", sep = "")
  
  # 檢查是否在 .gitignore 中
  if (file.exists(".gitignore")) {
    gitignore <- readLines(".gitignore", warn = FALSE)
    if (any(grepl("^manifest\\.json$", gitignore))) {
      cat("❌ manifest.json 被 .gitignore 排除！請移除該行\n")
      all_good <- FALSE
    } else if (any(grepl("# manifest\\.json", gitignore))) {
      cat("✅ manifest.json 已正確註解，會被包含在 Git 中\n")
    }
  }
} else {
  cat("❌ manifest.json 不存在\n")
  cat("   請執行：rsconnect::writeManifest()\n")
  all_good <- FALSE
}

# 6. 總結
cat("\n=============================\n")
if (all_good) {
  cat("✅ 所有檢查通過！可以進行部署\n")
} else {
  cat("❌ 有些問題需要修復\n")
}
cat("=============================\n") 