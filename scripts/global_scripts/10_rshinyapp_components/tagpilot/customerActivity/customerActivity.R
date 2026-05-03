# =============================================================================
# customerActivity.R — Customer Activity (CAI) Analysis Component
# Migrated from L3 module_tagpilot_customer_activity.R → L4 enterprise pattern
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

customerActivityComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_avg_cai"))),
      column(3, uiOutput(ns("kpi_increasing"))),
      column(3, uiOutput(ns("kpi_stable"))),
      column(3, uiOutput(ns("kpi_decreasing")))
    ),
    # Charts Row
    fluidRow(
      column(6, bs4Card(title = translate("CAI Segment Distribution"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_cai"), height = "300px"))),
      column(6, bs4Card(title = translate("CAI Index Distribution"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_cai"), height = "300px")))
    ),
    # CAI vs Monetary Scatter
    fluidRow(
      column(12, bs4Card(title = translate("CAI vs Monetary"), status = "success",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("scatter_cai_m"), height = "350px")))
    ),
    # Marketing Recommendations
    fluidRow(
      column(12, bs4Card(title = translate("CAI Marketing Recommendations"), status = "warning",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_cai"))))
    ),
    # Detail Table
    fluidRow(
      column(12, bs4Card(title = translate("Customer Detail"), status = "primary",
        width = 12, solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"), class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("detail_table"))))
    ),
    # AI Insight Result — bottom of display (GUIDE03)
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      dna_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"
          plt <- cfg$filters$platform_id
          df <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt, product_line_id_filter == !!pl_id) %>%
            dplyr::collect()
          if (nrow(df) == 0) { message("[customerActivity] No data"); return(NULL) }
          message("[customerActivity] Loaded ", nrow(df), " records")
          df
        }, error = function(e) { message("[customerActivity] Error: ", e$message); NULL })
      })

      # KPI boxes
      output$kpi_avg_cai <- renderUI({
        df <- dna_data()
        cai_vals <- if (!is.null(df)) df$cai[!is.na(df$cai)] else numeric(0)
        val <- if (length(cai_vals) == 0) "-" else round(mean(cai_vals), 2)
        bs4ValueBox(value = val, subtitle = translate("Avg CAI"),
                    icon = icon("chart-line"), color = "primary", width = 12)
      })

      output$kpi_increasing <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Increasingly Active"), icon = icon("arrow-up"), color = "success", width = 12))
        n_valid <- sum(!is.na(df$cai_label))
        n <- sum(df$cai_label == "Increasingly Active", na.rm = TRUE)
        pct <- if (n_valid > 0) round(n / n_valid * 100, 1) else 0
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("Increasingly Active"),
                    icon = icon("arrow-up"), color = "success", width = 12)
      })

      output$kpi_stable <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Stable"), icon = icon("minus"), color = "info", width = 12))
        n_valid <- sum(!is.na(df$cai_label))
        n <- sum(df$cai_label == "Stable", na.rm = TRUE)
        pct <- if (n_valid > 0) round(n / n_valid * 100, 1) else 0
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("Stable"),
                    icon = icon("minus"), color = "info", width = 12)
      })

      output$kpi_decreasing <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Gradually Inactive"), icon = icon("arrow-down"), color = "danger", width = 12))
        n_valid <- sum(!is.na(df$cai_label))
        n <- sum(df$cai_label == "Gradually Inactive", na.rm = TRUE)
        pct <- if (n_valid > 0) round(n / n_valid * 100, 1) else 0
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("Gradually Inactive"),
                    icon = icon("arrow-down"), color = "danger", width = 12)
      })

      # Pie chart: CAI segments
      output$pie_cai <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        labels_valid <- df$cai_label[!is.na(df$cai_label)]
        if (length(labels_valid) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl <- as.data.frame(table(labels_valid), stringsAsFactors = FALSE)
        names(tbl) <- c("level", "count")
        tbl <- tbl[tbl$count > 0, ]
        if (nrow(tbl) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl$label <- sapply(as.character(tbl$level), translate)
        plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                        marker = list(colors = c("#28a745", "#17a2b8", "#dc3545")),
                        textinfo = "label+percent") %>%
          plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
      })

      # Histogram: CAI distribution — no cap needed (normalized index)
      output$hist_cai <- render_histogram(dna_data, "cai",
        xlab = translate("CAI"), color = "#17a2b8", translate_fn = translate)

      # Scatter: CAI vs Monetary
      output$scatter_cai_m <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        plot_df <- df[!is.na(df$cai) & !is.na(df$m_value), ]
        if (nrow(plot_df) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        # Sample for large datasets
        if (nrow(plot_df) > 2000) plot_df <- plot_df[sample(nrow(plot_df), 2000), ]
        plot_df$label <- sapply(plot_df$cai_label, translate)
        colors <- c("Increasingly Active" = "#28a745", "Stable" = "#17a2b8", "Gradually Inactive" = "#dc3545")
        plotly::plot_ly(plot_df, x = ~cai, y = ~m_value, color = ~cai_label,
                        colors = colors, type = "scatter", mode = "markers",
                        marker = list(size = 5, opacity = 0.6),
                        text = ~paste0("ID: ", customer_id, "\nCAI: ", round(cai, 2), "\nM: $", round(m_value, 0)),
                        hoverinfo = "text") %>%
          plotly::layout(xaxis = list(title = translate("CAI")), yaxis = list(title = translate("Purchase Amount")),
                         margin = list(t = 10, b = 50))
      })

      # Marketing Recommendations (DEV_R052: pass English labels directly)
      output$rec_cai <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(tags$p(translate("No Data")))
        rec_df <- data.frame(segment = df$cai_label, stringsAsFactors = FALSE)
        rec_df <- rec_df[!is.na(rec_df$segment), , drop = FALSE]
        if (nrow(rec_df) == 0) return(tags$p(class = "text-muted", translate("No Data")))
        generate_recommendations_html(rec_df, "CAI", translate)
      })

      # Detail table
      output$detail_table <- DT::renderDataTable({
        df <- dna_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("No Data"))))
        show_df <- data.frame(
          ID = df$customer_id,
          CAI = round(df$cai, 3),
          CAI_ECDF = round(df$cai_ecdf, 3),
          Segment = sapply(df$cai_label, translate),
          M = round(df$m_value, 0),
          F = df$f_value,
          stringsAsFactors = FALSE
        )
        DT::datatable(show_df,
          colnames = c(translate("Customer ID"), translate("CAI"), translate("CAI ECDF"),
                       translate("Activity Level"), translate("M"), translate("F")),
          filter = "top", rownames = FALSE,
          options = list(pageLength = 15, scrollX = TRUE, dom = "lftip",
                         language = list(url = "//cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json")))
      })

      # AI Insight — non-blocking via ExtendedTask (GUIDE03, TD_P004 compliant)
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "customer_analysis.activity_insights",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          cai_vals <- df$cai[!is.na(df$cai)]
          n_valid <- sum(!is.na(df$cai_label))
          list(
            total_customers = as.character(nrow(df)),
            active_pct = as.character(if (n_valid > 0) round(sum(df$cai_label == "Increasingly Active", na.rm = TRUE) / n_valid * 100, 1) else 0),
            stable_pct = as.character(if (n_valid > 0) round(sum(df$cai_label == "Stable", na.rm = TRUE) / n_valid * 100, 1) else 0),
            declining_pct = as.character(if (n_valid > 0) round(sum(df$cai_label == "Gradually Inactive", na.rm = TRUE) / n_valid * 100, 1) else 0),
            avg_cai = as.character(round(mean(cai_vals), 3)),
            cai_median = as.character(round(stats::median(cai_vals), 3)),
            avg_monetary = paste0("$", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ",")),
            avg_frequency = as.character(round(mean(df$f_value, na.rm = TRUE), 2))
          )
        },
        component_label = "customerActivity"
      )

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("customer_activity_cai_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- dna_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id,
              cai = round(df$cai, 3), cai_ecdf = round(df$cai_ecdf, 3),
              cai_label = df$cai_label, m_value = round(df$m_value, 2),
              f_value = df$f_value,
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
