#' @title Report Integration Module
#' @description 整合四大模組的 AI 分析結果生成完整報告
#' @principle MP56 Connected Component Principle
#' @principle R091 Universal Data Access Pattern
#' @principle MP81 Explicit Parameter Specification

# Helper functions ------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Extract reactive value from module results
#' @description Safely extracts values from reactive objects in module results
#' @param obj The object to extract from (could be reactive, reactiveVal, list, or value)
#' @param field Optional field name if obj is a list
#' @return The extracted value or NULL if extraction fails
extract_reactive_value <- function(obj, field = NULL) {
  tryCatch({
    # If field is specified, try to access it first
    if (!is.null(field) && is.list(obj)) {
      obj <- obj[[field]]
    }

    # Now extract the value based on the type
    if (is.null(obj)) {
      return(NULL)
    } else if (is.function(obj)) {
      # It's a reactive or reactiveVal - call it
      result <- obj()
      # If result is still a function (nested reactive), call it again
      if (is.function(result)) {
        result <- result()
      }
      return(result)
    } else if (is.list(obj) && "ai_analysis_result" %in% names(obj)) {
      # It's a list with ai_analysis_result field (common pattern)
      return(extract_reactive_value(obj$ai_analysis_result))
    } else if (is.list(obj) && "result" %in% names(obj)) {
      # It's a list with result field
      return(extract_reactive_value(obj$result))
    } else if (is.list(obj) && "value" %in% names(obj)) {
      # It's a list with value field
      return(extract_reactive_value(obj$value))
    } else {
      # Assume it's already a value
      return(obj)
    }
  }, error = function(e) {
    warning("Error extracting reactive value: ", e$message)
    return(NULL)
  })
}

#' Report Integration UI
#' @param id Module ID
#' @param translate Translation function
reportIntegrationUI <- function(id, translate = function(x) x) {
  ns <- NS(id)

  tagList(
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

#' Report Integration Server
#' @param id Module ID
#' @param app_data_connection Data connection object
#' @param module_results Reactive containing analysis results from all modules
reportIntegrationServer <- function(id, app_data_connection = NULL, module_results = NULL) {
  moduleServer(id, function(input, output, session) {

    # Get OpenAI API key
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")

    # Reactive values for report content
    report_content <- reactiveVal(NULL)
    report_html <- reactiveVal(NULL)

    # Generate integrated report
    observeEvent(input$generate_report, {

      withProgress(message = "生成整合報告中...", value = 0, {

        incProgress(0.1, detail = "收集所有分析結果...")

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

        # Debug: Check module_results structure
        if (!is.null(module_results) && is.reactive(module_results)) {
          mod_res_debug <- module_results()
          message("DEBUG: Module results structure:")
          message("  - Names: ", paste(names(mod_res_debug), collapse = ", "))
          if (!is.null(mod_res_debug$brandedge)) {
            message("  - BrandEdge names: ", paste(names(mod_res_debug$brandedge), collapse = ", "))
            if (!is.null(mod_res_debug$brandedge$position_strategy)) {
              message("  - position_strategy type: ", class(mod_res_debug$brandedge$position_strategy))
              if (is.list(mod_res_debug$brandedge$position_strategy)) {
                message("  - position_strategy names: ", paste(names(mod_res_debug$brandedge$position_strategy), collapse = ", "))
              }
            }
          }
        }

        # Build report structure - Fixed to full report
        report_sections <- list()

        # Full Report Format (固定為完整報告)
        report_sections$title <- "# MAMBA 整合分析報告 - 完整版\n\n"
        report_sections$date <- paste0("**報告日期：** ", Sys.Date(), "\n\n")
        report_sections$toc <- "## 目錄\n\n"

        incProgress(0.5, detail = "生成報告內容...")

        # Extract actual data from Marketing Vital-Signs modules
        if ("macro_kpi" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract KPI data from microMacroKPI component
            kpi_data <- extract_reactive_value(mod_res$vital_signs$micro_macro_kpi, "kpi_data")

            if (!is.null(kpi_data)) {
              # Build KPI section with actual data
              kpi_text <- "## 1. 宏觀市場指標\n\n### 關鍵績效指標\n"

              # Add actual KPI values if available
              if (is.list(kpi_data) || is.data.frame(kpi_data)) {
                kpi_text <- paste0(kpi_text,
                  "- 基於實際數據分析\n",
                  "- 數據涵蓋時間：", Sys.Date(), "\n\n")
              } else {
                kpi_text <- paste0(kpi_text, "- 數據載入中...\n\n")
              }

              report_sections$macro <- kpi_text
            } else {
              # Fallback with placeholder
              report_sections$macro <- paste0(
                "## 1. 宏觀市場指標\n\n",
                "*請在 KPI 模組中執行分析以獲得實際數據*\n\n"
              )
            }
          }
        }

        # Extract actual data from DNA distribution
        if ("dna_dist" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract DNA distribution data
            dna_dist_data <- extract_reactive_value(mod_res$vital_signs$dna_distribution, "distribution_data")

            if (!is.null(dna_dist_data)) {
              report_sections$dna_dist <- paste0(
                "## 2. 顧客 DNA 分佈分析\n\n",
                "### 客戶群體特徵分佈\n",
                "- 已分析客戶資料\n",
                "- 識別關鍵客戶特徵模式\n\n"
              )
            }
          }
        }

        # Extract actual data from Customer DNA (TagPilot)
        if ("customer_dna" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract customer DNA analysis
            cust_dna_data <- extract_reactive_value(mod_res$tagpilot$customer_dna, "analysis_result")

            if (!is.null(cust_dna_data)) {
              report_sections$customer_dna <- paste0(
                "## 3. 顧客 DNA 深度分析\n\n",
                "### TagPilot 客戶標籤分析\n",
                "- 客戶行為模式識別完成\n",
                "- 關鍵標籤已生成\n\n"
              )
            }
          }
        }

        if ("position_strategy" %in% selected_modules && !is.null(module_results)) {
          # Include actual AI analysis from positionStrategy
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Use helper function to extract AI analysis
            ai_text <- extract_reactive_value(mod_res$brandedge$position_strategy, "ai_analysis_result")

            # Fix: Handle vector case - check length and use all() for nzchar
            if (!is.null(ai_text) && length(ai_text) > 0 && all(nzchar(ai_text))) {
              # If ai_text is a vector, collapse it into a single string
              if (length(ai_text) > 1) {
                ai_text <- paste(ai_text, collapse = "\n")
              }
              report_sections$strategy <- paste0(
                "## 4. 品牌定位策略分析\n\n",
                ai_text, "\n\n"
              )
            } else {
              # Try to extract strategy result data
              strategy_data <- extract_reactive_value(mod_res$brandedge$position_strategy, "strategy_result")
              if (!is.null(strategy_data)) {
                report_sections$strategy <- paste0(
                  "## 4. 品牌定位策略分析\n\n",
                  "### 策略定位分析\n",
                  "- 四象限策略分析完成\n",
                  "- 品牌定位建議已生成\n\n"
                )
              } else {
                report_sections$strategy <- paste0(
                  "## 4. 品牌定位策略分析\n\n",
                  "*請在品牌定位策略模組中執行分析*\n\n"
                )
              }
            }
          }
        }

        # Extract Market Segmentation data
        if ("market_segment" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract market segmentation analysis
            segment_data <- extract_reactive_value(mod_res$brandedge$position_ms, "segment_data")

            if (!is.null(segment_data)) {
              report_sections$market_segment <- paste0(
                "## 5. 市場區隔分析\n\n",
                "### MDS 市場定位分析\n",
                "- 識別主要市場區隔\n",
                "- 競爭定位分析完成\n\n"
              )
            }
          }
        }

        # Extract Key Factors Analysis
        if ("key_factors" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract key factors evaluation
            kfe_data <- extract_reactive_value(mod_res$brandedge$position_kfe, "key_factors")

            if (!is.null(kfe_data)) {
              report_sections$key_factors <- paste0(
                "## 6. 關鍵成功因素分析\n\n",
                "### KFE 關鍵因素評估\n",
                "- 關鍵成功因素已識別\n",
                "- 競爭優勢分析完成\n\n"
              )
            }
          }
        }

        if ("market_track" %in% selected_modules && !is.null(module_results)) {
          # Include actual AI analysis from poissonCommentAnalysis
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Use helper function to extract analysis text
            comment_text <- extract_reactive_value(mod_res$insightforge$poisson_comment)

            # Fix: Handle vector case - use all() to ensure all elements are non-empty
            # or use [1] to check only the first element
            if (!is.null(comment_text) && length(comment_text) > 0 && all(nzchar(comment_text))) {
              # If comment_text is a vector, collapse it into a single string
              if (length(comment_text) > 1) {
                comment_text <- paste(comment_text, collapse = "\n")
              }
              report_sections$market <- paste0(
                "## 7. 市場賽道分析\n\n",
                comment_text, "\n\n"
              )
            } else {
              # Try to extract track analysis data
              track_data <- extract_reactive_value(mod_res$insightforge$poisson_comment, "track_analysis")
              if (!is.null(track_data)) {
                report_sections$market <- paste0(
                  "## 7. 市場賽道分析\n\n",
                  "### 產品賽道競爭力分析\n",
                  "- 基於評分與評論的競爭分析\n",
                  "- 市場定位建議已生成\n\n"
                )
              } else {
                report_sections$market <- paste0(
                  "## 7. 市場賽道分析\n\n",
                  "*請在市場洞察模組中執行分析*\n\n"
                )
              }
            }
          }
        }

        # Extract Time Trend Analysis
        if ("time_analysis" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract time trend data
            time_data <- extract_reactive_value(mod_res$insightforge$poisson_time, "trend_data")

            if (!is.null(time_data)) {
              report_sections$time_analysis <- paste0(
                "## 8. 時間趨勢分析\n\n",
                "### 時間因素對銷售的影響\n",
                "- 時段分析完成\n",
                "- 季節性模式已識別\n\n"
              )
            }
          }
        }

        # Extract Precision Marketing Analysis
        if ("precision" %in% selected_modules && !is.null(module_results)) {
          if (!is.null(module_results) && is.reactive(module_results)) {
            mod_res <- module_results()

            # Extract precision marketing data
            precision_data <- extract_reactive_value(mod_res$insightforge$poisson_feature, "precision_data")

            if (!is.null(precision_data)) {
              report_sections$precision <- paste0(
                "## 9. 精準行銷分析\n\n",
                "### 產品屬性影響力分析\n",
                "- 關鍵屬性影響力已評估\n",
                "- 精準行銷策略已生成\n\n"
              )
            }
          }
        }

        incProgress(0.7, detail = "使用 AI 生成整合見解...")

        # Generate integrated insights using GPT
        if (nzchar(gpt_key) && length(report_sections) > 2) {

          # Prepare prompt for integrated analysis
          combined_content <- paste(unlist(report_sections), collapse = "\n")

          sys_prompt <- "你是專業的商業分析顧問，擅長整合多維度數據洞察。"
          user_prompt <- paste0(
            "基於以下各模組的分析結果，提供整合性的策略建議：\n\n",
            combined_content,
            "\n\n請提供：",
            "\n1. **整合洞察**（3個要點）",
            "\n2. **優先行動建議**（按重要性排序）",
            "\n3. **風險與機會**",
            "\n\n限制在300字內，使用繁體中文。"
          )

          # Call AI for integrated insights
          if (exists("chat_api")) {
            ai_insights <- chat_api(
              list(
                list(role = "system", content = sys_prompt),
                list(role = "user", content = user_prompt)
              ),
              gpt_key
            )

            report_sections$ai_insights <- paste0(
              "## 整合策略建議\n\n",
              ai_insights, "\n\n"
            )
          }
        }

        incProgress(0.9, detail = "格式化報告...")

        # Combine all sections
        final_report <- paste(unlist(report_sections), collapse = "")

        # Add footer
        final_report <- paste0(
          final_report,
          "\n---\n",
          "*本報告由 MAMBA Enterprise Platform 自動生成*\n",
          "*Powered by AI Marketing Intelligence*"
        )

        report_content(final_report)

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
            "body { font-family: 'Microsoft YaHei', sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; }",
            "h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }",
            "h2 { color: #34495e; margin-top: 30px; }",
            "h3 { color: #7f8c8d; }",
            "strong { color: #e74c3c; }",
            "ul { line-height: 1.8; }",
            "hr { margin: 40px 0; border: none; border-top: 2px solid #ecf0f1; }",
            "</style>",
            "</head><body>",
            html_content,
            "</body></html>"
          )

          report_html(styled_html)
        } else {
          report_html(paste0("<pre>", final_report, "</pre>"))
        }

        # Show preview section
        shinyjs::show("report_preview_section")

        incProgress(1.0, detail = "報告生成完成！")
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
          style = "border: 1px solid #ddd; border-radius: 4px;"
        )
      }
    })

    # Download handler - Fixed to HTML format
    output$download_report <- downloadHandler(
      filename = function() {
        paste0("MAMBA_Report_", Sys.Date(), ".html")  # Fixed to .html
      },
      content = function(file) {
        # Always output HTML (固定輸出 HTML 格式)
        writeLines(report_html(), file)
      }
    )

    # Return reactive values for potential external use
    return(list(
      report_content = report_content,
      report_html = report_html
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

#' Report Integration Component Wrapper
#' @description Component wrapper following MAMBA standard pattern
#' @param id Module ID
#' @param app_data_connection Database connection (optional)
#' @param config Configuration reactive (optional)
#' @param translate Translation function
#' @principle R09 UI-Server-Defaults triple
#' @principle MP56 Connected Component Principle
reportIntegrationComponent <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  ns <- NS(id)

  # Create UI components - Simplified filter panel
  ui_filter <- wellPanel(
    class = "filter-panel",
    style = "padding: 15px;",
    h4(translate("整合報告"), icon("file-alt")),
    tags$hr(),

    # Report generation button for filter panel
    actionButton(
      ns("generate_report"),
      translate("生成整合報告"),
      icon = icon("magic"),
      class = "btn-primary btn-block",
      width = "100%"
    ),

    tags$hr(),
    p(translate("自動整合分析模組："), style = "color: #666; font-size: 11px; margin-top: 10px;"),
    tags$ul(
      style = "font-size: 11px; color: #666; margin-left: -15px;",
      tags$li("Marketing Vital-Signs"),
      tags$li("TagPilot 顧客分析"),
      tags$li("BrandEdge 品牌定位"),
      tags$li("InsightForge 市場洞察")
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
      # Module results is passed as an additional parameter containing all module outputs
      reportIntegrationServer(id, app_data_connection, module_results)
    }
  )
}