#!/usr/bin/env Rscript
# ============================================================================
# Positioning App 部署檢查腳本
# ============================================================================
# 用途：診斷和修復常見的部署問題
# ============================================================================

# 顏色輸出函數
print_header <- function(msg) {
  cat("\n", rep("=", 50), "\n", sep = "")
  cat(msg, "\n")
  cat(rep("=", 50), "\n", sep = "")
}

print_check <- function(msg, status = "CHECK") {
  if (status == "PASS") {
    cat("✅", msg, "\n")
  } else if (status == "FAIL") {
    cat("❌", msg, "\n")
  } else if (status == "WARN") {
    cat("⚠️ ", msg, "\n")
  } else {
    cat("🔍", msg, "\n")
  }
}

# 1. 檢查檔案系統
check_files <- function() {
  print_header("檔案系統檢查")
  
  # 必要檔案
  required_files <- list(
    "full_app_v17.R" = "主要應用程式檔案",
    "app.R" = "部署入口檔案",
    "manifest.json" = "依賴清單",
    "www/" = "靜態資源目錄",
    "icons/" = "圖標目錄"
  )
  
  for (file in names(required_files)) {
    if (file.exists(file)) {
      print_check(paste(file, "-", required_files[[file]]), "PASS")
    } else {
      print_check(paste(file, "-", required_files[[file]], "(缺失)"), "FAIL")
    }
  }
  
  # 檢查檔案大小
  if (file.exists("manifest.json")) {
    size <- file.info("manifest.json")$size / 1024
    print_check(sprintf("manifest.json 大小: %.1f KB", size), 
                if(size > 0) "PASS" else "WARN")
  }
  
  # 檢查 app.R 和 full_app_v17.R 是否同步
  if (file.exists("app.R") && file.exists("full_app_v17.R")) {
    if (identical(readLines("app.R"), readLines("full_app_v17.R"))) {
      print_check("app.R 與 full_app_v17.R 同步", "PASS")
    } else {
      print_check("app.R 與 full_app_v17.R 不同步", "WARN")
    }
  }
}

# 2. 檢查 rsconnect 配置
check_rsconnect <- function() {
  print_header("rsconnect 配置檢查")
  
  # 檢查套件
  if (requireNamespace("rsconnect", quietly = TRUE)) {
    print_check("rsconnect 套件已安裝", "PASS")
    print_check(paste("rsconnect 版本:", packageVersion("rsconnect")), "CHECK")
  } else {
    print_check("rsconnect 套件未安裝", "FAIL")
    return()
  }
  
  # 檢查帳號
  accounts <- rsconnect::accounts()
  if (nrow(accounts) > 0) {
    print_check(paste("找到", nrow(accounts), "個帳號"), "PASS")
    for (i in 1:nrow(accounts)) {
      print_check(paste("  -", accounts$name[i], "@", accounts$server[i]), "CHECK")
    }
  } else {
    print_check("未設置 rsconnect 帳號", "FAIL")
  }
  
  # 檢查部署歷史
  dcf_path <- "rsconnect/documents/full_app_v17.R/shinyapps.io/kyle-lin/positioning_app.dcf"
  if (file.exists(dcf_path)) {
    print_check("找到部署配置檔案", "PASS")
    dcf <- read.dcf(dcf_path)
    print_check(paste("  - App ID:", dcf[1, "appId"]), "CHECK")
    print_check(paste("  - URL:", dcf[1, "url"]), "CHECK")
  } else {
    print_check("未找到部署配置檔案 (首次部署)", "WARN")
  }
}

# 3. 檢查資料安全
check_data_security <- function() {
  print_header("資料安全檢查")
  
  # 檢查敏感檔案
  sensitive_patterns <- c(
    "*.csv", "*.xlsx", "*.db", "*.sqlite", 
    ".env", "config.R", "credentials.R"
  )
  
  sensitive_found <- FALSE
  for (pattern in sensitive_patterns) {
    files <- Sys.glob(pattern)
    if (length(files) > 0) {
      for (file in files) {
        # 排除 app_data 目錄
        if (!grepl("app_data/", file)) {
          print_check(paste("發現敏感檔案:", file), "WARN")
          sensitive_found <- TRUE
        }
      }
    }
  }
  
  if (!sensitive_found) {
    print_check("未發現敏感檔案", "PASS")
  }
  
  # 檢查 .gitignore
  if (file.exists(".gitignore")) {
    print_check(".gitignore 存在", "PASS")
    gitignore <- readLines(".gitignore")
    important_patterns <- c("*.csv", "*.xlsx", ".env", "rsconnect/")
    for (pattern in important_patterns) {
      if (any(grepl(pattern, gitignore, fixed = TRUE))) {
        print_check(paste("  -", pattern, "已被忽略"), "CHECK")
      }
    }
  } else {
    print_check(".gitignore 不存在", "WARN")
  }
}

# 4. 檢查依賴套件
check_dependencies <- function() {
  print_header("依賴套件檢查")
  
  if (file.exists("manifest.json")) {
    manifest <- jsonlite::fromJSON("manifest.json")
    
    if (!is.null(manifest$packages)) {
      print_check(paste("共", length(manifest$packages), "個依賴套件"), "CHECK")
      
      # 檢查主要套件
      main_packages <- c("shiny", "DBI", "dplyr", "ggplot2")
      for (pkg in main_packages) {
        pkg_info <- manifest$packages[manifest$packages$Package == pkg, ]
        if (nrow(pkg_info) > 0) {
          print_check(paste(pkg, "v", pkg_info$Version[1]), "PASS")
        }
      }
    }
  } else {
    print_check("manifest.json 不存在，無法檢查依賴", "FAIL")
  }
}

# 5. 提供修復建議
provide_fixes <- function() {
  print_header("快速修復指令")
  
  cat("\n如果 app.R 與 full_app_v17.R 不同步：\n")
  cat('file.copy("full_app_v17.R", "app.R", overwrite = TRUE)\n\n')
  
  cat("如果需要更新 manifest.json：\n")
  cat('rsconnect::writeManifest(appPrimaryDoc = "full_app_v17.R")\n\n')
  
  cat("如果需要設置 rsconnect 帳號：\n")
  cat('rsconnect::setAccountInfo(
  name = "kyle-lin",
  token = "YOUR_TOKEN",
  secret = "YOUR_SECRET"
)\n\n')
  
  cat("部署應用程式：\n")
  cat('source("deploy.R")\n')
}

# 主函數
main <- function() {
  cat("\n")
  cat("Positioning App 部署檢查\n")
  cat("========================\n")
  
  check_files()
  check_rsconnect()
  check_data_security()
  check_dependencies()
  provide_fixes()
  
  cat("\n檢查完成！\n")
}

# 執行檢查
main() 