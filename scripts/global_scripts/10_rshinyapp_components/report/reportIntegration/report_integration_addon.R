#' @title Report Integration Add-on for union_production_test
#' @description 在主程式中加入報告整合功能的指引
#' @principle MP44 Functor-Module Correspondence
#' @principle R76 Module Data Connection Rule

# ============================================================================
# 整合報告功能 - 實施指引
# ============================================================================
#
# 此文件說明如何在 union_production_test.R 中整合報告生成功能
#
# 1. 在 union_production_test.R 的元件載入區（約第 60 行後）加入：
#    source("scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/reportIntegration.R")
#
# 2. 在 UI 定義中（sidebar menu 部分）加入新的選單項目：
#    bs4SidebarMenuItem("整合報告", tabName="report", icon=icon("file-alt"))
#
# 3. 在 bs4TabItems 中加入對應的 tab：
#    bs4TabItem(tabName="report",
#      fluidRow(
#        column(12,
#          bs4Card(
#            title="AI 整合分析報告",
#            status="warning",
#            width=12,
#            solidHeader=TRUE,
#            elevation=3,
#            uiOutput("report_display")
#          )
#        )
#      )
#    )
#
# 4. 在 Server 函數中建立 AI 結果收集機制
# ============================================================================

# Helper function to collect AI results from all modules
collect_ai_results <- function(
  position_strategy_res = NULL,
  poisson_comment_res = NULL,
  poisson_time_res = NULL,
  poisson_feature_res = NULL
) {

  ai_results <- list()

  # Collect position strategy AI analysis
  if (!is.null(position_strategy_res)) {
    tryCatch({
      if (!is.null(position_strategy_res$ai_analysis_result)) {
        ai_results$position_strategy <- position_strategy_res$ai_analysis_result()
      }
    }, error = function(e) {
      message("Failed to collect position strategy AI results: ", e$message)
    })
  }

  # Collect market track (poisson comment) AI analysis
  if (!is.null(poisson_comment_res)) {
    tryCatch({
      if (!is.null(poisson_comment_res$ai_insight_result)) {
        ai_results$market_track <- poisson_comment_res$ai_insight_result()
      }
    }, error = function(e) {
      message("Failed to collect market track AI results: ", e$message)
    })
  }

  # Additional modules can be added here
  # ai_results$time_analysis <- poisson_time_res$ai_result()
  # ai_results$precision_marketing <- poisson_feature_res$ai_result()

  return(ai_results)
}

# Server integration code snippet
# Add this to the server function after initializing all component servers:
#
# # Initialize report integration component
# report_comp <- reportIntegrationInitialize(
#   "report",
#   app_data_connection = app_connection,
#   ai_results = reactive({
#     collect_ai_results(
#       position_strategy_res = position_strategy_res,
#       poisson_comment_res = poisson_comment_res,
#       poisson_time_res = poisson_time_res,
#       poisson_feature_res = poisson_feature_res
#     )
#   })
# )
#
# # Render report display
# output$report_display <- renderUI2(
#   current_tab = input$sidebar_menu,
#   target_tab = "report",
#   ui_component = report_comp$ui,
#   loading_icon = "file-alt"
# )
#
# # Initialize report server
# report_res <- report_comp$server(input, output, session)

# ============================================================================
# 使用說明
# ============================================================================
#
# 1. 報告整合模組會自動收集已生成的 AI 分析結果
# 2. 使用者可以選擇要包含哪些模組的分析
# 3. 支援三種報告類型：執行摘要、完整報告、技術報告
# 4. 支援 HTML、PDF、Word 格式輸出
# 5. 包含 AI 生成的整合性洞察
#
# 注意事項：
# - 確保 OPENAI_API_KEY 環境變數已設定
# - PDF 輸出需要安裝 pagedown 套件
# - 各模組的 AI 分析結果必須是 reactive 值
# ============================================================================