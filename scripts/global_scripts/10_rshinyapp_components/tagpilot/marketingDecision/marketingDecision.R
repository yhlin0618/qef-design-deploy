# =============================================================================
# marketingDecision.R — Marketing Decision Table Component
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

marketingDecisionComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # UX_P002: Read pre-computed RSV classification table (Tier 1)
  # No longer source fn_rsv_classification.R at runtime.

  # ---- UI ----
  ui_filter <- tagList(
    selectInput(ns("filter_strategy"), translate("Filter Strategy"),
                choices = setNames("all", translate("All")), selected = "all")
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_awaken"))),
      column(3, uiOutput(ns("kpi_new"))),
      column(3, uiOutput(ns("kpi_nurture"))),
      column(3, uiOutput(ns("kpi_vip")))
    ),
    # Strategy Distribution Chart
    fluidRow(
      column(12, bs4Card(title = translate("Strategy Distribution"),
        status = "primary", width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("strategy_bar"), height = "350px")))
    ),
    # Customer Detail Table
    fluidRow(
      column(12, bs4Card(title = translate("Marketing Decision Detail"),
        status = "primary", width = 12, solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"), class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("detail_table"))))
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
          if (nrow(df) == 0) { message("[marketingDecision] No pre-computed data for platform='", cfg$filters$platform_id, "'"); return(NULL) }
          message("[marketingDecision] Loaded ", nrow(df), " pre-computed records")
          df
        }, error = function(e) {
          message("[marketingDecision] Data load error: ", e$message)
          NULL
        })
      })

      # Update strategy filter choices
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
        sel <- input$filter_strategy
        if (!is.null(sel) && sel != "all") {
          df <- df[df$marketing_strategy == sel, ]
        }
        df
      })

      # KPIs
      output$kpi_awaken <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Awakening/Return"),
                                            icon = icon("bell"), color = "warning", width = 12))
        n <- sum(df$marketing_strategy == "Awakening / Return", na.rm = TRUE)
        bs4ValueBox(value = n, subtitle = translate("Awakening/Return"),
                    icon = icon("bell"), color = "warning", width = 12)
      })

      output$kpi_new <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("New Customer Onboarding"),
                                            icon = icon("user-plus"), color = "info", width = 12))
        n <- sum(df$marketing_strategy == "New Customer Nurturing", na.rm = TRUE)
        bs4ValueBox(value = n, subtitle = translate("New Customer Onboarding"),
                    icon = icon("user-plus"), color = "info", width = 12)
      })

      output$kpi_nurture <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Standard + Low-cost Nurturing"),
                                            icon = icon("seedling"), color = "success", width = 12))
        n <- sum(grepl("Nurturing", df$marketing_strategy), na.rm = TRUE)
        bs4ValueBox(value = n, subtitle = translate("Standard + Low-cost Nurturing"),
                    icon = icon("seedling"), color = "success", width = 12)
      })

      output$kpi_vip <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("VIP + Premium"),
                                            icon = icon("crown"), color = "danger", width = 12))
        n <- sum(grepl("VIP|Premium", df$marketing_strategy), na.rm = TRUE)
        bs4ValueBox(value = n, subtitle = translate("VIP + Premium"),
                    icon = icon("crown"), color = "danger", width = 12)
      })

      # Strategy bar chart
      output$strategy_bar <- renderPlotly({
        df <- classified_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))

        tbl <- as.data.frame(table(df$marketing_strategy), stringsAsFactors = FALSE)
        names(tbl) <- c("strategy", "count")
        tbl <- tbl[order(-tbl$count), ]
        tbl$label <- sapply(tbl$strategy, translate)

        colors <- c("#dc3545", "#fd7e14", "#ffc107", "#28a745", "#20c997",
                     "#17a2b8", "#007bff", "#6610f2", "#6f42c1", "#e83e8c",
                     "#343a40", "#6c757d", "#adb5bd")

        plotly::plot_ly(tbl, x = ~reorder(label, -count), y = ~count, type = "bar",
                        marker = list(color = colors[seq_len(nrow(tbl))])) %>%
          plotly::layout(
            xaxis = list(title = "", tickangle = -30),
            yaxis = list(title = translate("Customer Count")),
            margin = list(b = 120)
          )
      })

      # Detail table
      output$detail_table <- DT::renderDataTable({
        df <- filtered_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("Please run ETL pipeline first"))))

        show_df <- data.frame(
          ID       = df$customer_id,
          RFM      = df$rfm_score,
          NES      = sapply(as.character(df$nes_status), function(code) {
            key <- paste0("NES Status - ", code)
            translated <- translate(key)
            if (identical(translated, key)) code else translated
          }, USE.NAMES = FALSE),
          CAI      = round(df$cai_value, 2),
          Risk     = sapply(df$r_level, translate),
          Stability = sapply(df$s_level, translate),
          CLV_Level = sapply(df$v_level, translate),
          CLV      = round(df$clv_value, 0),
          Strategy = sapply(df$marketing_strategy, translate),
          Purpose  = sapply(df$marketing_purpose, translate),
          Action   = sapply(df$marketing_recommendation, translate),
          stringsAsFactors = FALSE
        )

        # Truncate long text columns with hover tooltip (#214)
        simple_truncate_targets <- c(8L, 9L)  # 0-indexed: Strategy(8), Purpose(9)
        action_col_target <- 10L              # 0-indexed: Action(10)
        strategy_col_idx <- 8L
        purpose_col_idx <- 9L

        tooltip_css <- ".truncated-cell { cursor: pointer; border-bottom: 1px dotted #999; } .truncated-tooltip { position: fixed; background: #333; color: #fff; padding: 8px 12px; border-radius: 4px; font-size: 13px; max-width: 350px; z-index: 99999; line-height: 1.6; pointer-events: none; white-space: pre-wrap; }"

        DT::datatable(show_df,
          colnames = unname(sapply(names(show_df), translate)),
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
                             targets = action_col_target,
                             render = DT::JS(
                               "function(data, type, row, meta) {",
                               "  if (type === 'display' && data && data.length > 20) {",
                               "    var clean = data.replace(/<br\\s*\\/?>/gi, ' ');",
                               paste0("    var strategy = row[", strategy_col_idx, "] || '';"),
                               paste0("    var purpose = row[", purpose_col_idx, "] || '';"),
                               "    var tipData = data.replace(/<br\\s*\\/?>/gi, '\\n');",
                               "    var full = '\\u3010\\u7b56\\u7565\\u3011' + strategy + '\\n\\u3010\\u76ee\\u7684\\u3011' + purpose + '\\n\\u3010\\u5efa\\u8b70\\u3011\\n' + tipData;",
                               "    return '<span class=\"truncated-cell\" data-full=\"' + full.replace(/\"/g, '&quot;') + '\">' + clean.substr(0, 20) + '\\u2026</span>';",
                               "  }",
                               "  return data ? data.replace(/<br\\s*\\/?>/gi, ' ') : data;",
                               "}"
                             )
                           )
                         ),
                         initComplete = DT::JS(
                           "function(settings, json) {",
                           paste0("  var css = '", tooltip_css, "';"),
                           "  if (!document.getElementById('md-truncated-tooltip-style')) {",
                           "    var style = document.createElement('style');",
                           "    style.id = 'md-truncated-tooltip-style';",
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

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("marketing_decision_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- filtered_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id, rfm_score = df$rfm_score,
              nes_status = df$nes_status, cai = round(df$cai_value, 2),
              r_level = df$r_level, s_level = df$s_level, v_level = df$v_level,
              clv = round(df$clv_value, 0), customer_type = df$customer_type,
              marketing_strategy = df$marketing_strategy,
              marketing_purpose = df$marketing_purpose,
              marketing_recommendation = df$marketing_recommendation,
              stringsAsFactors = FALSE
            )
            con <- file(file, "w", encoding = "UTF-8")
            writeChar("\ufeff", con, eos = NULL)
            close(con)
            # write.table (NOT write.csv) — write.csv ignores append=TRUE (DEV_R051)
            utils::write.table(export, file, row.names = FALSE, sep = ",",
                               quote = TRUE, append = TRUE, fileEncoding = "UTF-8")
          }
        }
      )

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
