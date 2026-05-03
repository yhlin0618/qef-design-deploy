#' @title Implementation Script for Report Integration
#' @description 自動化整合報告功能到主程式的腳本
#' @principle MP44 Functor-Module Correspondence
#' @principle R46 Source Directories Not Individual Files

# =============================================================================
# Report Integration Implementation Script
# 執行此腳本將自動為 union_production_test.R 添加報告整合功能
# =============================================================================

implement_report_integration <- function(dry_run = TRUE) {

  cat("========================================\n")
  cat("報告整合模組實施腳本\n")
  cat("========================================\n\n")

  # Check if we're in the right directory
  if (!file.exists("app.R")) {
    stop("請在 MAMBA 專案根目錄執行此腳本")
  }

  main_file <- "scripts/global_scripts/10_rshinyapp_components/unions/union_production_test.R"

  if (!file.exists(main_file)) {
    stop("找不到主程式檔案: ", main_file)
  }

  cat("✓ 找到主程式檔案\n")

  # Read the main file
  main_content <- readLines(main_file)

  # Check if already integrated
  if (any(grepl("reportIntegration", main_content))) {
    cat("⚠️  報告整合模組已經存在於主程式中\n")
    return(invisible(FALSE))
  }

  cat("✓ 確認尚未整合報告模組\n\n")

  if (dry_run) {
    cat("========================================\n")
    cat("乾跑模式 (Dry Run) - 顯示將進行的修改\n")
    cat("========================================\n\n")
  }

  # 1. Find where to add the source statement
  load_line <- which(grepl("source\\(.*passwordOnly", main_content))[1]
  if (is.na(load_line)) {
    load_line <- which(grepl("# ---- 3\\. Component", main_content))[1]
  }

  cat("1. 將在第", load_line + 2, "行加入模組載入語句:\n")
  cat("   source(\"scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/reportIntegration.R\")\n")
  cat("   source(\"scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/report_integration_addon.R\")\n\n")

  # 2. Find where to add UI menu item
  menu_line <- which(grepl("InsightForge 360.*icon.*lightbulb", main_content))[1]
  if (!is.na(menu_line)) {
    # Find the closing of InsightForge menu
    close_line <- menu_line
    paren_count <- 0
    for (i in menu_line:length(main_content)) {
      line <- main_content[i]
      paren_count <- paren_count + str_count(line, "\\(") - str_count(line, "\\)")
      if (paren_count == 0 && i > menu_line) {
        close_line <- i
        break
      }
    }

    cat("2. 將在第", close_line + 1, "行加入選單項目:\n")
    cat("   整合報告選單\n\n")
  }

  # 3. Find where to add tab item
  tab_line <- tail(which(grepl("bs4TabItem\\(tabName.*poisson", main_content)), 1)
  if (!is.na(tab_line)) {
    # Find the end of this tab item
    close_tab <- tab_line
    paren_count <- 0
    for (i in tab_line:length(main_content)) {
      line <- main_content[i]
      paren_count <- paren_count + str_count(line, "\\(") - str_count(line, "\\)")
      if (paren_count == 0 && i > tab_line) {
        close_tab <- i
        break
      }
    }

    cat("3. 將在第", close_tab + 1, "行加入頁面定義:\n")
    cat("   報告頁面 Tab\n\n")
  }

  # 4. Find where to add server code
  server_line <- which(grepl("# Initialize Poisson component servers", main_content))[1]
  if (!is.na(server_line)) {
    end_server <- server_line + 5
    cat("4. 將在第", end_server, "行加入 Server 邏輯:\n")
    cat("   AI 結果收集與報告模組初始化\n\n")
  }

  # 5. Find where to add display output
  display_line <- tail(which(grepl("output\\$poisson.*display.*renderUI2", main_content)), 1)
  if (!is.na(display_line)) {
    cat("5. 將在第", display_line + 3, "行加入顯示輸出:\n")
    cat("   報告顯示 renderUI\n\n")
  }

  if (!dry_run) {
    cat("========================================\n")
    cat("開始實施整合...\n")
    cat("========================================\n\n")

    # Create backup
    backup_file <- paste0(main_file, ".backup.", format(Sys.time(), "%Y%m%d_%H%M%S"))
    file.copy(main_file, backup_file)
    cat("✓ 已建立備份: ", basename(backup_file), "\n")

    # TODO: Implement actual file modification
    # This would require careful insertion of code at the right positions
    # For safety, we'll just create a patch file instead

    cat("\n⚠️  自動修改功能尚未完成\n")
    cat("請參考 README.md 手動整合\n")
    cat("或使用 test_report_integration.R 進行獨立測試\n")

  } else {
    cat("========================================\n")
    cat("乾跑完成\n")
    cat("========================================\n\n")
    cat("若要實際執行整合，請執行:\n")
    cat("  implement_report_integration(dry_run = FALSE)\n\n")
    cat("或參考 README.md 手動整合\n")
  }

  return(invisible(TRUE))
}

# Helper function to count occurrences
str_count <- function(string, pattern) {
  length(gregexpr(pattern, string)[[1]])
}

# =============================================================================
# 使用方式
# =============================================================================
#
# 1. 乾跑模式（預覽修改）:
#    source("scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/implement_report_integration.R")
#    implement_report_integration(dry_run = TRUE)
#
# 2. 實際執行整合:
#    implement_report_integration(dry_run = FALSE)
#
# 3. 測試報告模組:
#    source("test_report_integration.R")
#
# =============================================================================

# 執行乾跑
if (interactive()) {
  cat("\n執行乾跑模式以預覽修改...\n\n")
  implement_report_integration(dry_run = TRUE)
}