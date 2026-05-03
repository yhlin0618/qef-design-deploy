# =============================================================================
# customerExport.R — Customer Tag Export Component
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

customerExportComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # UX_P002: Read pre-computed RSV classification table (Tier 1)
  # Source only prepare_customer_export() for export formatting
  classification_path <- file.path("scripts", "global_scripts",
    "10_rshinyapp_components", "tagpilot", "fn_rsv_classification.R")
  if (file.exists(classification_path)) source(classification_path, local = TRUE)

  # ---- UI ----
  ui_filter <- tagList(
    selectInput(ns("filter_strategy"), translate("Marketing Strategy"),
                choices = setNames("all", translate("All")), selected = "all"),
    selectInput(ns("filter_nes"), translate("Customer Status"),
                choices = c(setNames("all", translate("All")), "N", "E0", "S1", "S2", "S3"), selected = "all"),
    selectInput(ns("filter_risk"), translate("Dormancy Risk"),
                choices = c(setNames("all", translate("All")),
                  setNames(c("High", "Mid", "Low"), sapply(c("High", "Mid", "Low"), translate))),
                selected = "all")
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_total"))),
      column(3, uiOutput(ns("kpi_tags"))),
      column(3, uiOutput(ns("kpi_avg_clv"))),
      column(3, uiOutput(ns("kpi_avg_rfm")))
    ),
    # Preview Table
    fluidRow(
      column(12, bs4Card(title = translate("Customer Tag Preview (top 1000)"),
        status = "primary", width = 12, solidHeader = TRUE,
        downloadButton(ns("download_full_csv"), translate("Download Full CSV"), class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("preview_table"))))
    ),
    # Column Description
    fluidRow(
      column(12, bs4Card(title = translate("Column Description"), status = "secondary",
        width = 12, solidHeader = TRUE, collapsed = TRUE, collapsible = TRUE,
        uiOutput(ns("column_desc"))))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # UX_P002 Tier 1: Read pre-computed RSV classification table
      classified_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"

          plt <- cfg$filters$platform_id
          df <- tbl2(app_connection, "df_rsv_classified") %>%
            dplyr::filter(platform_id == !!plt, product_line_id_filter == !!pl_id) %>%
            dplyr::collect()
          if (nrow(df) == 0) { message("[customerExport] No pre-computed data for platform='", cfg$filters$platform_id, "'"); return(NULL) }
          message("[customerExport] Loaded ", nrow(df), " pre-computed records")
          df
        }, error = function(e) {
          message("[customerExport] Data load error: ", e$message)
          NULL
        })
      })

      # Update strategy filter
      observe({
        df <- classified_data()
        if (!is.null(df)) {
          strats <- sort(unique(df$marketing_strategy))
          choices <- c(setNames("all", translate("All")), setNames(strats, sapply(strats, translate)))
          updateSelectInput(session, "filter_strategy", choices = choices, selected = "all")
        }
      })

      # Filtered data
      filtered_data <- reactive({
        df <- classified_data()
        if (is.null(df)) return(NULL)

        if (!is.null(input$filter_strategy) && input$filter_strategy != "all") {
          df <- df[df$marketing_strategy == input$filter_strategy, ]
        }
        if (!is.null(input$filter_nes) && input$filter_nes != "all") {
          df <- df[df$nes_status == input$filter_nes, ]
        }
        if (!is.null(input$filter_risk) && input$filter_risk != "all") {
          df <- df[df$r_level == input$filter_risk, ]
        }
        df
      })

      # Export-ready data
      export_data <- reactive({
        df <- filtered_data()
        if (is.null(df)) return(NULL)
        prepare_customer_export(df)
      })

      # KPIs
      output$kpi_total <- renderUI({
        df <- filtered_data()
        n <- if (is.null(df)) "-" else format(nrow(df), big.mark = ",")
        bs4ValueBox(value = n, subtitle = translate("Total Customers"),
                    icon = icon("users"), color = "primary", width = 12)
      })

      output$kpi_tags <- renderUI({
        bs4ValueBox(value = "19", subtitle = translate("Available Tags"),
                    icon = icon("tags"), color = "info", width = 12)
      })

      output$kpi_avg_clv <- renderUI({
        df <- filtered_data()
        val <- if (is.null(df)) "-" else paste0("$", format(round(mean(df$clv_value, na.rm = TRUE), 0), big.mark = ","))
        bs4ValueBox(value = val, subtitle = translate("Avg CLV"),
                    icon = icon("dollar-sign"), color = "success", width = 12)
      })

      output$kpi_avg_rfm <- renderUI({
        df <- filtered_data()
        val <- if (is.null(df)) "-" else round(mean(df$rfm_score, na.rm = TRUE), 1)
        bs4ValueBox(value = val, subtitle = translate("Avg RFM"),
                    icon = icon("chart-bar"), color = "warning", width = 12)
      })

      # Preview table (top 1000)
      output$preview_table <- DT::renderDataTable({
        edf <- export_data()
        if (is.null(edf)) return(DT::datatable(data.frame(Message = translate("Please run ETL pipeline first"))))

        preview <- head(edf, 1000)

        # Translate classification columns for display
        translate_cols <- c("cai_activity", "r_level", "s_level", "v_level",
                            "customer_type", "marketing_strategy",
                            "marketing_purpose", "marketing_recommendation")
        for (tc in translate_cols) {
          if (tc %in% names(preview)) {
            preview[[tc]] <- sapply(preview[[tc]], function(x) if (is.na(x)) NA_character_ else translate(x))
          }
        }

        # Truncate long text columns in display, full text on hover (#214)
        simple_truncate_cols <- c("marketing_purpose", "customer_type")
        simple_truncate_targets <- which(names(preview) %in% simple_truncate_cols) - 1L
        rec_col_target <- which(names(preview) == "marketing_recommendation") - 1L  # 0-indexed
        strategy_col_idx <- which(names(preview) == "marketing_strategy") - 1L
        purpose_col_idx <- which(names(preview) == "marketing_purpose") - 1L

        # Build tooltip CSS as single-line string (DT::JS joins with \n, breaking multi-line JS strings)
        tooltip_css <- ".truncated-cell { cursor: pointer; border-bottom: 1px dotted #999; } .truncated-tooltip { position: fixed; background: #333; color: #fff; padding: 8px 12px; border-radius: 4px; font-size: 13px; max-width: 350px; z-index: 99999; line-height: 1.6; pointer-events: none; white-space: pre-wrap; }"

        DT::datatable(preview,
          colnames = unname(sapply(names(preview), translate)),
          filter = "top", rownames = FALSE,
          options = list(pageLength = 15, scrollX = TRUE, dom = "lftip",
                         language = list(url = "//cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json"),
                         columnDefs = list(
                           list(
                             targets = simple_truncate_targets,
                             render = DT::JS(
                               "function(data, type, row, meta) {",
                               "  if (type === 'display' && data && data.length > 10) {",
                               "    return '<span class=\"truncated-cell\" data-full=\"' + data.replace(/\"/g, '&quot;') + '\">' + data.substr(0, 10) + '\\u2026</span>';",
                               "  }",
                               "  return data;",
                               "}"
                             )
                           ),
                           list(
                             targets = rec_col_target,
                             render = DT::JS(
                               "function(data, type, row, meta) {",
                               "  if (type === 'display' && data && data.length > 10) {",
                               paste0("    var strategy = row[", strategy_col_idx, "] || '';"),
                               paste0("    var purpose = row[", purpose_col_idx, "] || '';"),
                               "    var full = '\\u3010\\u7b56\\u7565\\u3011' + strategy + '\\n\\u3010\\u76ee\\u7684\\u3011' + purpose + '\\n\\u3010\\u5efa\\u8b70\\u3011' + data;",
                               "    return '<span class=\"truncated-cell\" data-full=\"' + full.replace(/\"/g, '&quot;') + '\">' + data.substr(0, 10) + '\\u2026</span>';",
                               "  }",
                               "  return data;",
                               "}"
                             )
                           )
                         ),
                         initComplete = DT::JS(
                           "function(settings, json) {",
                           paste0("  var css = '", tooltip_css, "';"),
                           "  if (!document.getElementById('truncated-tooltip-style')) {",
                           "    var style = document.createElement('style');",
                           "    style.id = 'truncated-tooltip-style';",
                           "    style.textContent = css;",
                           "    document.head.appendChild(style);",
                           "  }",
                           "  var tooltip = document.createElement('div');",
                           "  tooltip.className = 'truncated-tooltip';",
                           "  tooltip.style.display = 'none';",
                           "  document.body.appendChild(tooltip);",
                           "  $(settings.nTable).on('mouseenter', '.truncated-cell', function(e) {",
                           "    var full = $(this).data('full');",
                           "    if (full) {",
                           "      tooltip.textContent = full;",
                           "      tooltip.style.display = 'block';",
                           "      tooltip.style.left = (e.clientX + 10) + 'px';",
                           "      tooltip.style.top = (e.clientY - 30) + 'px';",
                           "    }",
                           "  }).on('mouseleave', '.truncated-cell', function() {",
                           "    tooltip.style.display = 'none';",
                           "  }).on('mousemove', '.truncated-cell', function(e) {",
                           "    tooltip.style.left = (e.clientX + 10) + 'px';",
                           "    tooltip.style.top = (e.clientY - 30) + 'px';",
                           "  });",
                           "}"
                         )))
      })

      # Column description
      output$column_desc <- renderUI({
        desc_keys <- c("Customer ID", "RFM Score (3-15)", "Recency (days)",
                       "Purchase Frequency", "Purchase Amount", "Activity (ni<4 = NA)",
                       "Status (N/E0/S1/S2/S3)", "Churn Probability (0-1)", "Transaction Count",
                       "Avg Purchase Interval (days, ni<=1 = NA)", "Total Spent",
                       "Predicted CLV",
                       "Risk Level (High/Mid/Low)", "Stability Level (High/Mid/Low)",
                       "Value Level (High/Mid/Low)", "RSV Customer Type (27 types)",
                       "Marketing Strategy (13 types)", "Marketing Purpose", "Marketing Recommendation")
        desc_df <- data.frame(
          Column = c("customer_id", "rfm_score", "r_value", "f_value", "m_value",
                     "cai_activity", "nes_status", "churn_prob", "transaction_count",
                     "avg_purchase_interval", "total_spent", "clv",
                     "r_level", "s_level", "v_level", "customer_type",
                     "marketing_strategy", "marketing_purpose", "marketing_recommendation"),
          Description = sapply(desc_keys, translate),
          stringsAsFactors = FALSE
        )
        tags$div(
          tags$table(class = "table table-sm table-striped",
            tags$thead(tags$tr(tags$th(translate("Column")), tags$th(translate("Description")))),
            tags$tbody(
              lapply(seq_len(nrow(desc_df)), function(i) {
                tags$tr(tags$td(tags$code(desc_df$Column[i])), tags$td(desc_df$Description[i]))
              })
            )
          )
        )
      })

      # Full CSV download (UTF-8 BOM) — #226: column names in Chinese
      output$download_full_csv <- downloadHandler(
        filename = function() paste0("customer_tags_", Sys.Date(), ".csv"),
        content = function(file) {
          edf <- export_data()
          if (!is.null(edf)) {
            # Translate column names to Chinese for customer readability
            names(edf) <- sapply(names(edf), translate)
            # Translate classification values
            translate_cols_orig <- c("cai_activity", "r_level", "s_level", "v_level",
                                     "customer_type", "marketing_strategy",
                                     "marketing_purpose", "marketing_recommendation")
            # Match by translated column names
            for (orig_name in translate_cols_orig) {
              zh_name <- translate(orig_name)
              if (zh_name %in% names(edf)) {
                edf[[zh_name]] <- sapply(edf[[zh_name]], function(x) if (is.na(x)) NA_character_ else translate(x))
              }
            }
            # Write UTF-8 BOM for Excel compatibility
            con <- file(file, "wb")
            writeBin(charToRaw("\xef\xbb\xbf"), con)
            close(con)
            # write.table (NOT write.csv) — write.csv ignores append=TRUE (DEV_R051)
            utils::write.table(edf, file, row.names = FALSE, sep = ",",
                               quote = TRUE, append = TRUE, fileEncoding = "UTF-8")
          }
        }
      )

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
