#!/usr/bin/env Rscript
# 檢查 Positioning App 位置

cat("=== Positioning App 位置檢查 ===\n\n")

# 1. 當前工作目錄
cat("當前目錄：\n")
cat(getwd(), "\n\n")

# 2. Git repository 根目錄
find_git_root <- function() {
  current <- getwd()
  while (current != dirname(current)) {
    if (file.exists(file.path(current, ".git"))) {
      return(current)
    }
    current <- dirname(current)
  }
  return(NULL)
}

git_root <- find_git_root()
if (!is.null(git_root)) {
  cat("Git repository 根目錄：\n")
  cat(git_root, "\n\n")
  
  # 3. 相對路徑
  rel_path <- gsub(paste0("^", git_root, "/"), "", getwd())
  cat("Application Path (用於 Posit Connect Cloud)：\n")
  cat(rel_path, "\n\n")
} else {
  cat("警告：找不到 Git repository 根目錄\n\n")
}

# 4. 檢查關鍵檔案
cat("關鍵檔案檢查：\n")
files_to_check <- c("app.R", "manifest.json", ".env", ".gitignore")
for (f in files_to_check) {
  if (file.exists(f)) {
    cat(sprintf("✓ %s 存在\n", f))
  } else {
    cat(sprintf("✗ %s 不存在\n", f))
  }
}

cat("\n=== 在 Posit Connect Cloud 填寫 ===\n")
cat("Application Path: l1_basic/positioning_app\n")
cat("Primary File: app.R\n") 