#' @title Report Integration Module - Improved Version
#' @description Enhanced report generation with better error handling and debugging
#' @principle MP56 Connected Component Principle
#' @principle R091 Universal Data Access Pattern
#' @principle MP81 Explicit Parameter Specification
#' @principle MP099 Real-time progress reporting and monitoring
#' @principle MP106 Console Output Transparency

# Helper functions ------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Extract reactive value from module results - Enhanced version
#' @description Safely extracts values with better debugging output
extract_reactive_value_debug <- function(obj, field = NULL, debug_name = "unknown") {
  tryCatch({
    # MP106: Console Output Transparency
    cat(sprintf("[DEBUG] Extracting %s...\n", debug_name))

    # If field is specified, try to access it first
    if (!is.null(field) && is.list(obj)) {
      obj <- obj[[field]]
    }

    # Now extract the value based on the type
    if (is.null(obj)) {
      cat(sprintf("[DEBUG] %s is NULL\n", debug_name))
      return(NULL)
    } else if (is.function(obj)) {
      # It's a reactive or reactiveVal - call it
      result <- obj()
      # If result is still a function (nested reactive), call it again
      if (is.function(result)) {
        result <- result()
      }
      cat(sprintf("[DEBUG] %s extracted from reactive: %s\n", debug_name, class(result)[1]))
      return(result)
    } else if (is.list(obj) && "ai_analysis_result" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in ai_analysis_result field\n", debug_name))
      return(extract_reactive_value_debug(obj$ai_analysis_result, debug_name = paste0(debug_name, ".ai_analysis_result")))
    } else if (is.list(obj) && "result" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in result field\n", debug_name))
      return(extract_reactive_value_debug(obj$result, debug_name = paste0(debug_name, ".result")))
    } else if (is.list(obj) && "value" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in value field\n", debug_name))
      return(extract_reactive_value_debug(obj$value, debug_name = paste0(debug_name, ".value")))
    } else {
      # Assume it's already a value
      cat(sprintf("[DEBUG] %s is direct value: %s\n", debug_name, class(obj)[1]))
      return(obj)
    }
  }, error = function(e) {
    cat(sprintf("[ERROR] Failed to extract %s: %s\n", debug_name, e$message))
    return(NULL)
  })
}

#' Report Integration UI - Enhanced
#' @param id Module ID
#' @param translate Translation function
reportIntegrationUI <- function(id, translate = function(x) x) {
  ns <- NS(id)

  tagList(
    # Debug info panel
    conditionalPanel(
      condition = "true",  # Always show in debug mode
      style = "background: #f0f0f0; padding: 10px; margin-bottom: 10px; border-radius: 5px;",
      h5("Report Generation Status", style = "margin-top: 0;"),
      verbatimTextOutput(ns("debug_output"))
    ),

    # Progress and Preview
    uiOutput(ns("generation_progress")),

    # Report Preview
    div(
      id = ns("report_preview_section"),
      style = "display: none;",
      h4(translate("報告預覽")),
      uiOutput(ns("report_preview")),
      br(),
      downloadButton(
        ns("download_report"),
        translate("下載報告"),
        class = "btn-success"
      )
    )
  )
}

#' Report Integration Server - Enhanced with debugging
#' @param id Module ID
#' @param app_data_connection Data connection object
#' @param module_results Reactive containing analysis results from all modules
reportIntegrationServer <- function(id, app_data_connection = NULL, module_results = NULL) {
  moduleServer(id, function(input, output, session) {

    # Debug output reactive
    debug_messages <- reactiveVal("")

    add_debug <- function(msg) {
      current <- debug_messages()
      timestamp <- format(Sys.time(), "%H:%M:%S")
      new_msg <- paste0(current, "[", timestamp, "] ", msg, "\n")
      debug_messages(new_msg)
      cat(paste0("[REPORT DEBUG] ", msg, "\n"))  # MP106: Console Output Transparency
    }

    # Get OpenAI API key
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")

    # Check and load chat_api if not available
    if (!exists("chat_api")) {
      add_debug("chat_api not found, attempting to load...")
      chat_api_path <- "scripts/global_scripts/08_ai/chat_api.R"
      if (file.exists(chat_api_path)) {
        source(chat_api_path)
        add_debug("chat_api loaded successfully")
      } else {
        add_debug("WARNING: chat_api.R not found!")
      }
    }

    # Reactive values for report content
    report_content <- reactiveVal(NULL)
    report_html <- reactiveVal(NULL)

    # Debug output
    output$debug_output <- renderText({
      debug_messages()
    })

    # Generate integrated report
    observeEvent(input$generate_report, {

      add_debug("=== Starting Report Generation ===")
      add_debug(sprintf("OpenAI API Key: %s", ifelse(nzchar(gpt_key), "Available", "Missing")))
      add_debug(sprintf("chat_api function: %s", ifelse(exists("chat_api"), "Available", "Missing")))

      withProgress(message = "生成整合報告中...", value = 0, {

        incProgress(0.1, detail = "收集所有分析結果...")
        add_debug("Collecting analysis results...")

        # Automatically include all modules
        selected_modules <- c(
          "macro_kpi", "dna_dist",           # Marketing Vital-Signs
          "customer_dna",                     # TagPilot
          "position_strategy",                # BrandEdge
          "market_segment", "key_factors",    # BrandEdge
          "market_track",                     # InsightForge 360
          "time_analysis", "precision"        # InsightForge 360
        )

        incProgress(0.3, detail = "整合 AI 分析內容...")
        add_debug("Integrating AI analysis content...")

        # Debug: Check module_results structure
        if (!is.null(module_results)) {
          if (is.reactive(module_results)) {
            mod_res <- module_results()
            add_debug(sprintf("Module results type: %s", class(mod_res)[1]))
            if (is.list(mod_res)) {
              add_debug(sprintf("Module results names: %s", paste(names(mod_res), collapse = ", ")))
            }
          } else {
            add_debug("Module results is not reactive")
          }
        } else {
          add_debug("WARNING: module_results is NULL!")
        }

        # Build report structure
        report_sections <- list()

        # Report header
        report_sections$title <- "# MAMBA 整合分析報告\n\n"
        report_sections$date <- paste0("**報告日期：** ", Sys.Date(), "\n")
        report_sections$time <- paste0("**生成時間：** ", format(Sys.time(), "%H:%M:%S"), "\n\n")

        incProgress(0.5, detail = "生成報告內容...")
        add_debug("Generating report sections...")

        # Generate each section with error handling
        tryCatch({
          # Section 1: Marketing Vital Signs
          if ("macro_kpi" %in% selected_modules && !is.null(module_results)) {
            add_debug("Processing macro_kpi section...")
            if (is.reactive(module_results)) {
              mod_res <- module_results()
              kpi_data <- extract_reactive_value_debug(
                mod_res$vital_signs$micro_macro_kpi,
                "kpi_data",
                "KPI_Data"
              )

              if (!is.null(kpi_data)) {
                report_sections$macro <- paste0(
                  "## 1. 宏觀市場指標\n\n",
                  "### 關鍵績效指標\n",
                  "- ✓ KPI 數據已載入\n",
                  "- 數據涵蓋時間：", Sys.Date(), "\n\n"
                )
                add_debug("KPI section generated successfully")
              } else {
                report_sections$macro <- paste0(
                  "## 1. 宏觀市場指標\n\n",
                  "*等待 KPI 模組數據載入...*\n\n"
                )
                add_debug("KPI data not available")
              }
            }
          }

          # Section 2: Brand Positioning Strategy
          if ("position_strategy" %in% selected_modules && !is.null(module_results)) {
            add_debug("Processing position_strategy section...")
            if (is.reactive(module_results)) {
              mod_res <- module_results()

              # Try multiple paths to find the AI analysis
              ai_text <- NULL

              # Path 1: Direct position_strategy
              if (!is.null(mod_res$brandedge$position_strategy)) {
                ai_text <- extract_reactive_value_debug(
                  mod_res$brandedge$position_strategy,
                  "ai_analysis_result",
                  "Position_Strategy_AI"
                )
              }

              # Path 2: Try position module
              if (is.null(ai_text) && !is.null(mod_res$position)) {
                ai_text <- extract_reactive_value_debug(
                  mod_res$position,
                  "ai_analysis",
                  "Position_AI_Alternative"
                )
              }

              # Handle the extracted text
              if (!is.null(ai_text) && length(ai_text) > 0) {
                # Fix for vector case
                if (is.character(ai_text) && all(nzchar(ai_text))) {
                  if (length(ai_text) > 1) {
                    ai_text <- paste(ai_text, collapse = "\n")
                  }
                  report_sections$strategy <- paste0(
                    "## 2. 品牌定位策略分析\n\n",
                    ai_text, "\n\n"
                  )
                  add_debug("Position strategy AI text included")
                } else {
                  report_sections$strategy <- paste0(
                    "## 2. 品牌定位策略分析\n\n",
                    "### 策略定位分析\n",
                    "- ✓ 四象限策略分析進行中\n",
                    "- ✓ 品牌定位建議生成中\n\n"
                  )
                  add_debug("Position strategy data processing")
                }
              } else {
                report_sections$strategy <- paste0(
                  "## 2. 品牌定位策略分析\n\n",
                  "*請在品牌定位策略模組中執行分析以獲得 AI 洞察*\n\n"
                )
                add_debug("Position strategy not available")
              }
            }
          }

          # Section 3: Market Track Analysis
          if ("market_track" %in% selected_modules && !is.null(module_results)) {
            add_debug("Processing market_track section...")
            if (is.reactive(module_results)) {
              mod_res <- module_results()

              comment_text <- extract_reactive_value_debug(
                mod_res$insightforge$poisson_comment,
                NULL,
                "Market_Comment_Analysis"
              )

              if (!is.null(comment_text) && length(comment_text) > 0) {
                # Fix for vector case
                if (is.character(comment_text) && all(nzchar(comment_text))) {
                  if (length(comment_text) > 1) {
                    comment_text <- paste(comment_text, collapse = "\n")
                  }
                  report_sections$market <- paste0(
                    "## 3. 市場賽道分析\n\n",
                    comment_text, "\n\n"
                  )
                  add_debug("Market analysis text included")
                } else {
                  report_sections$market <- paste0(
                    "## 3. 市場賽道分析\n\n",
                    "### 產品賽道競爭力分析\n",
                    "- ✓ 評分與評論分析進行中\n",
                    "- ✓ 市場定位建議生成中\n\n"
                  )
                  add_debug("Market analysis data processing")
                }
              } else {
                report_sections$market <- paste0(
                  "## 3. 市場賽道分析\n\n",
                  "*請在市場洞察模組中執行分析以獲得深度見解*\n\n"
                )
                add_debug("Market analysis not available")
              }
            }
          }

        }, error = function(e) {
          add_debug(sprintf("ERROR in section generation: %s", e$message))
          report_sections$error <- paste0(
            "## ⚠️ 報告生成遇到問題\n\n",
            "部分模組數據無法正常載入，請檢查：\n",
            "1. 各分析模組是否已完成運算\n",
            "2. 資料連接是否正常\n",
            "3. API 金鑰是否正確設定\n\n"
          )
        })

        incProgress(0.7, detail = "使用 AI 生成整合見解...")
        add_debug("Generating AI integrated insights...")

        # Generate integrated insights using GPT (if available)
        if (nzchar(gpt_key) && exists("chat_api") && length(report_sections) > 3) {
          add_debug("Calling OpenAI API for integrated insights...")

          # Prepare content for AI analysis
          combined_content <- paste(unlist(report_sections), collapse = "\n")

          sys_prompt <- "你是專業的商業分析顧問，擅長整合多維度數據洞察。請用繁體中文回答。"
          user_prompt <- paste0(
            "基於以下分析報告內容，提供整合性的策略建議：\n\n",
            combined_content,
            "\n\n請提供：",
            "\n1. **整合洞察摘要**（2-3個關鍵發現）",
            "\n2. **立即行動建議**（最重要的2個行動）",
            "\n3. **潛在風險提醒**（1個主要風險）",
            "\n\n請以精簡專業的方式呈現，總字數限制在200字內。"
          )

          # Call AI for integrated insights
          tryCatch({
            ai_response <- chat_api(
              list(
                list(role = "system", content = sys_prompt),
                list(role = "user", content = user_prompt)
              ),
              gpt_key
            )

            if (!is.null(ai_response) && nzchar(ai_response)) {
              report_sections$ai_insights <- paste0(
                "## 整合策略建議\n\n",
                ai_response, "\n\n"
              )
              add_debug("AI insights generated successfully")
            }
          }, error = function(e) {
            add_debug(sprintf("AI API call failed: %s", e$message))
            report_sections$ai_insights <- paste0(
              "## 整合策略建議\n\n",
              "*AI 分析服務暫時無法使用，請稍後再試*\n\n"
            )
          })
        } else {
          if (!nzchar(gpt_key)) {
            add_debug("OpenAI API key not configured")
          }
          if (!exists("chat_api")) {
            add_debug("chat_api function not available")
          }
        }

        incProgress(0.9, detail = "格式化報告...")
        add_debug("Formatting final report...")

        # Combine all sections
        final_report <- paste(unlist(report_sections), collapse = "")

        # Add footer
        final_report <- paste0(
          final_report,
          "\n---\n",
          "*本報告由 MAMBA Enterprise Platform 自動生成*\n",
          "*生成時間：", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "*\n",
          "*Powered by AI Marketing Intelligence*"
        )

        report_content(final_report)
        add_debug("Report content saved")

        # Convert to HTML
        if (requireNamespace("markdown", quietly = TRUE)) {
          html_content <- markdown::markdownToHTML(
            text = final_report,
            fragment.only = FALSE
          )

          # Add styling
          styled_html <- paste0(
            "<html><head>",
            "<meta charset='utf-8'>",
            "<style>",
            "body { font-family: 'Microsoft YaHei', sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background: #f8f9fa; }",
            "h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }",
            "h2 { color: #34495e; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 10px; }",
            "h3 { color: #7f8c8d; }",
            "strong { color: #e74c3c; }",
            "ul { line-height: 1.8; }",
            "li { margin: 5px 0; }",
            "hr { margin: 40px 0; border: none; border-top: 2px solid #ecf0f1; }",
            "em { color: #95a5a6; }",
            "</style>",
            "</head><body>",
            html_content,
            "</body></html>"
          )

          report_html(styled_html)
          add_debug("HTML report generated")
        } else {
          report_html(paste0("<pre>", final_report, "</pre>"))
          add_debug("Markdown package not available, using plain text")
        }

        # Show preview section
        shinyjs::show("report_preview_section")

        incProgress(1.0, detail = "報告生成完成！")
        add_debug("=== Report Generation Complete ===")
      })
    })

    # Render report preview
    output$report_preview <- renderUI({
      html_content <- report_html()
      if (!is.null(html_content)) {
        tags$iframe(
          srcdoc = html_content,
          width = "100%",
          height = "600px",
          style = "border: 1px solid #ddd; border-radius: 4px; background: white;"
        )
      }
    })

    # Download handler
    output$download_report <- downloadHandler(
      filename = function() {
        paste0("MAMBA_Report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".html")
      },
      content = function(file) {
        writeLines(report_html() %||% "<html><body>No report generated</body></html>", file)
      }
    )

    # Return reactive values
    return(list(
      report_content = report_content,
      report_html = report_html,
      debug_messages = debug_messages
    ))
  })
}

# Initialize function
reportIntegrationInitialize <- function(id, app_data_connection = NULL, module_results = NULL) {
  list(
    ui = reportIntegrationUI(id),
    server = function(input, output, session) {
      reportIntegrationServer(id, app_data_connection, module_results)
    }
  )
}

#' Report Integration Component Wrapper - Enhanced
#' @description Component wrapper with improved debugging
reportIntegrationComponent <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  ns <- NS(id)

  # Create UI components - Following MP014: Company Centered Design, R72: Component ID Consistency
  ui_filter <- wellPanel(
    class = "filter-panel",
    style = "padding: 15px;",  # Standard styling matching other modules (R09: UI-Server-Defaults Triple)
    h4(translate("整合報告中心"), icon("file-alt")),  # Standard header without color override
    tags$hr(),  # Standard separator without custom color

    # Report generation button
    actionButton(
      ns("generate_report"),
      translate("生成整合報告"),
      icon = icon("magic"),
      class = "btn-primary btn-block",  # Changed to btn-primary for consistency with other modules
      width = "100%"
    ),

    tags$hr(),  # Standard separator
    p(translate("自動整合分析模組："), style = "color: #666; font-size: 12px; margin-top: 10px;"),  # Standard text color
    tags$ul(
      style = "font-size: 11px; color: #666; margin-left: -15px;",  # Standard text color
      tags$li("Marketing Vital-Signs 市場指標"),
      tags$li("TagPilot 顧客 DNA 分析"),
      tags$li("BrandEdge 品牌定位策略"),
      tags$li("InsightForge 市場賽道洞察")
    ),

    # API Status info - simplified styling
    tags$div(
      style = "margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 5px;",  # Light background matching framework
      tags$small(
        "API Status: ",
        tags$span(
          id = ns("api_status"),
          ifelse(nzchar(Sys.getenv("OPENAI_API_KEY")), "✓ Ready", "✗ Missing"),
          style = ifelse(
            nzchar(Sys.getenv("OPENAI_API_KEY")),
            "color: #28a745;",  # Standard success color
            "color: #dc3545;"   # Standard danger color
          )
        )
      )
    )
  )

  ui_display <- reportIntegrationUI(id, translate)

  # Return component structure
  list(
    ui = list(
      filter = ui_filter,
      display = ui_display
    ),
    server = function(input, output, session, module_results = NULL) {
      reportIntegrationServer(id, app_data_connection, module_results)
    }
  )
}

# Export the main functions
# Following R69: Function File Naming
# Following MP47: Functional Programming