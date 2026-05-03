#!/usr/bin/env Rscript
# ============================================================================
# 通用一鍵部署腳本
# 可配置用於不同的 Shiny 應用程式
# ============================================================================

# 部署配置函數
get_app_config <- function() {
  # 根據當前目錄判斷應用程式
  current_dir <- basename(getwd())

  config <- list(
    app_name = current_dir,
    main_file = "app.R",
    version_files = NULL,
    app_path = NULL,
    github_repo = "kiki830621/ai_martech"
  )
  
  # 根據不同應用程式設定特定配置
  if (current_dir == "positioning_app") {
    config$app_name <- "Positioning App"
    config$version_files <- c("full_app_v17.R", "full_app_v16.R", "full_app_v15.R")
    config$app_path <- "l1_basic/positioning_app"
    
  } else if (current_dir == "VitalSigns") {
    config$app_name <- "VitalSigns"
    config$app_path <- "l1_basic/VitalSigns"
    
  } else if (current_dir == "InsightForge") {
    config$app_name <- "InsightForge"
    config$app_path <- "l1_basic/InsightForge"
    
  } else {
    # 嘗試自動偵測路徑
    git_root <- system2("git", "rev-parse --show-toplevel", stdout = TRUE, stderr = TRUE)
    if (length(git_root) != 1L || !nzchar(trimws(git_root[[1]])) || grepl("^fatal:", git_root[[1]])) {
      stop("無法從 Git 取得根目錄，請手動設定 app_path")
    }

    git_root <- normalizePath(trimws(git_root[[1]]))
    current_path <- normalizePath(getwd())
    if (!identical(current_path, git_root) && !startsWith(current_path, paste0(git_root, "/"))) {
      stop("目前工作目錄未位於 Git 專案內，無法推斷 app_path")
    }
    if (identical(current_path, git_root)) {
      config$app_path <- "."
    } else {
      config$app_path <- substring(current_path, nchar(git_root) + 2)
    }
  }

  if (is.null(config$app_path)) {
    stop("無法判斷部署目錄，請設定 app_path")
  }
  
  return(config)
}

# 主部署函數
deploy_app_generic <- function(interactive = TRUE) {
  config <- get_app_config()
  
  cat("\n")
  cat("🚀", config$app_name, "一鍵部署\n")
  cat("============================\n")
  cat("開始時間:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
  
  # 步驟 1：檢查環境
  cat("步驟 1：檢查部署環境\n")
  cat("---------------------------\n")
  cat("應用程式:", config$app_name, "\n")
  cat("目錄:", getwd(), "\n")
  cat("Application Path:", config$app_path, "\n\n")
  
  if (interactive) {
    cat("是否繼續？(yes/no): ")
    response <- tolower(readline())
    if (response != "yes" && response != "y") {
      cat("❌ 部署已取消\n")
      return(invisible(FALSE))
    }
  }
  
  # 步驟 2：檢查並更新主檔案
  cat("\n步驟 2：檢查主應用程式檔案\n")
  cat("---------------------------\n")
  
  if (!file.exists(config$main_file)) {
    cat("❌ 找不到", config$main_file, "\n")
    
    # 如果有版本檔案，嘗試使用最新的
    if (!is.null(config$version_files)) {
      for (vf in config$version_files) {
        if (file.exists(vf)) {
          cat("找到", vf, "，複製為", config$main_file, "\n")
          file.copy(vf, config$main_file, overwrite = TRUE)
          cat("✅", config$main_file, "已創建\n")
          break
        }
      }
    }
  } else {
    cat("✅", config$main_file, "存在\n")
    
    # 檢查是否需要更新（如果有版本檔案）
    if (!is.null(config$version_files) && interactive) {
      latest_version <- NULL
      for (vf in config$version_files) {
        if (file.exists(vf)) {
          latest_version <- vf
          break
        }
      }
      
      if (!is.null(latest_version)) {
        # 比較內容
        current_content <- readLines(config$main_file, warn = FALSE)
        latest_content <- readLines(latest_version, warn = FALSE)
        
        if (!identical(current_content, latest_content)) {
          cat("⚠️ ", config$main_file, "與", latest_version, "不同\n")
          cat("是否更新到最新版本？(yes/no): ")
          update_response <- tolower(readline())
          
          if (update_response == "yes" || update_response == "y") {
            file.copy(latest_version, config$main_file, overwrite = TRUE)
            cat("✅", config$main_file, "已更新\n")
          }
        }
      }
    }
  }
  
  # 步驟 3：更新 manifest.json
  cat("\n步驟 3：更新依賴清單\n")
  cat("---------------------------\n")
  cat("正在更新 manifest.json...\n")
  
  library(rsconnect)
  rsconnect::writeManifest()
  cat("✅ manifest.json 已更新\n")
  
  # 步驟 4：檢查必要檔案
  cat("\n步驟 4：檢查必要檔案\n")
  cat("---------------------------\n")
  
  required_files <- c(config$main_file, "manifest.json", ".gitignore")
  all_good <- TRUE
  
  for (f in required_files) {
    if (file.exists(f)) {
      cat("✅", f, "存在\n")
    } else {
      cat("❌", f, "缺失\n")
      all_good <- FALSE
    }
  }
  
  # 檢查常見目錄
  common_dirs <- c("www", "data", "scripts")
  for (d in common_dirs) {
    if (dir.exists(d)) {
      cat("📁", d, "/", "目錄存在\n")
    }
  }
  
  # 步驟 5：Git 狀態
  cat("\n步驟 5：檢查 Git 狀態\n")
  cat("---------------------------\n")
  
  git_status <- system2("git", "status --porcelain", stdout = TRUE, stderr = FALSE)
  if (length(git_status) > 0) {
    cat("📝 有未提交的變更\n")
    if (interactive) {
      cat("顯示變更？(yes/no): ")
      if (tolower(readline()) %in% c("yes", "y")) {
        cat(paste("  ", head(git_status, 10)), sep = "\n")
        if (length(git_status) > 10) {
          cat("  ... 還有", length(git_status) - 10, "個檔案\n")
        }
      }
    }
  } else {
    cat("✅ Git 工作區乾淨\n")
  }
  
  # 部署指示
  cat("\n============================\n")
  cat("📋 部署指示\n")
  cat("============================\n\n")
  
  if (all_good) {
    cat("✅ 檔案準備就緒！\n\n")
    
    cat("🌐 Posit Connect Cloud 部署：\n")
    cat("1. 提交並推送變更：\n")
    cat("   git add -A\n")
    cat("   git commit -m 'Deploy", config$app_name, "'\n")
    cat("   git push\n\n")
    
    cat("2. 登入 https://connect.posit.cloud\n\n")
    
    cat("3. 填寫部署資訊：\n")
    cat("   - Repository:", config$github_repo, "\n")
    cat("   - Application Path:", config$app_path, "\n")
    cat("   - Primary File:", config$main_file, "\n")
    cat("   - Branch: main\n\n")
    
    cat("💻 或使用 ShinyApps.io：\n")
    cat("   rsconnect::deployApp()\n")
  } else {
    cat("❌ 請先修復缺失的檔案\n")
  }
  
  cat("\n結束時間:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
  return(invisible(all_good))
}

# 如果直接執行
if (!interactive()) {
  # 檢查命令列參數
  args <- commandArgs(trailingOnly = TRUE)
  if ("--auto" %in% args) {
    deploy_app_generic(interactive = FALSE)
  } else {
    deploy_app_generic(interactive = TRUE)
  }
} 
