# =============================================================================
# customerValue.R — Customer Value (RFM) Analysis Component
# Migrated from L3 module_tagpilot_customer_value.R → L4 enterprise pattern
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

customerValueComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_total_customers"))),
      column(3, uiOutput(ns("kpi_avg_monetary"))),
      column(3, uiOutput(ns("kpi_avg_frequency"))),
      column(3, uiOutput(ns("kpi_avg_recency")))
    ),
    # Value Tier KPI Row (Issue #235)
    fluidRow(
      column(4, uiOutput(ns("kpi_high_value"))),
      column(4, uiOutput(ns("kpi_medium_value"))),
      column(4, uiOutput(ns("kpi_low_value")))
    ),
    # Value Segmentation Charts (Issue #235)
    fluidRow(
      column(6, bs4Card(title = translate("Value Segmentation"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_value_tier"), height = "280px"))),
      column(6, bs4Card(title = translate("Value Score Distribution"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_value_score"), height = "280px")))
    ),
    # RFM Distribution Charts
    fluidRow(
      column(4, bs4Card(title = translate("R Distribution"), status = "danger",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_r"), height = "280px"))),
      column(4, bs4Card(title = translate("F Distribution"), status = "success",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_f"), height = "280px"))),
      column(4, bs4Card(title = translate("M Distribution"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_m"), height = "280px")))
    ),
    # Segment Pie Charts
    fluidRow(
      column(4, bs4Card(title = translate("R Segments"), status = "danger",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_r"), height = "280px"))),
      column(4, bs4Card(title = translate("F Segments"), status = "success",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_f"), height = "280px"))),
      column(4, bs4Card(title = translate("M Segments"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_m"), height = "280px")))
    ),
    # Marketing Recommendations
    fluidRow(
      column(4, bs4Card(title = translate("R Recommendation"), status = "danger",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_r")))),
      column(4, bs4Card(title = translate("F Recommendation"), status = "success",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_f")))),
      column(4, bs4Card(title = translate("M Recommendation"), status = "info",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_m"))))
    ),
    # Customer Detail Table
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
          if (nrow(df) == 0) { message("[customerValue] No data"); return(NULL) }
          message("[customerValue] Loaded ", nrow(df), " records")
          # Compute combined value score & tier (Issue #235)
          df$value_score <- (df$r_ecdf + df$f_ecdf + df$m_ecdf) / 3
          df$value_tier <- dplyr::case_when(
            df$value_score >= 0.67 ~ "High Value",
            df$value_score >= 0.33 ~ "Medium Value",
            TRUE                   ~ "Low Value"
          )
          df$value_tier <- factor(df$value_tier, levels = c("High Value", "Medium Value", "Low Value"))
          df
        }, error = function(e) { message("[customerValue] Error: ", e$message); NULL })
      })

      # KPI boxes
      output$kpi_total_customers <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else format(nrow(df), big.mark = ",")
        bs4ValueBox(value = val, subtitle = translate("Total Customers"),
                    icon = icon("users"), color = "primary", width = 12)
      })

      output$kpi_avg_monetary <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else paste0("$", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ","))
        bs4ValueBox(value = val, subtitle = translate("Avg Order Value"),
                    icon = icon("dollar-sign"), color = "info", width = 12)
      })

      output$kpi_avg_frequency <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else round(mean(df$f_value, na.rm = TRUE), 1)
        bs4ValueBox(value = val, subtitle = translate("Avg Frequency"),
                    icon = icon("redo"), color = "success", width = 12)
      })

      output$kpi_avg_recency <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else paste0(round(mean(df$r_value, na.rm = TRUE), 0), " ", translate("days"))
        bs4ValueBox(value = val, subtitle = translate("Avg Recency"),
                    icon = icon("clock"), color = "danger", width = 12)
      })

      # Value Tier KPI boxes (Issue #235)
      output$kpi_high_value <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("High Value Customers"),
                                            icon = icon("star"), color = "success", width = 12))
        n <- sum(df$value_tier == "High Value", na.rm = TRUE)
        pct <- round(n / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("High Value Customers"),
                    icon = icon("star"), color = "success", width = 12)
      })

      output$kpi_medium_value <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Medium Value Customers"),
                                            icon = icon("minus-circle"), color = "warning", width = 12))
        n <- sum(df$value_tier == "Medium Value", na.rm = TRUE)
        pct <- round(n / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("Medium Value Customers"),
                    icon = icon("minus-circle"), color = "warning", width = 12)
      })

      output$kpi_low_value <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Low Value Customers"),
                                            icon = icon("arrow-down"), color = "danger", width = 12))
        n <- sum(df$value_tier == "Low Value", na.rm = TRUE)
        pct <- round(n / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                    subtitle = translate("Low Value Customers"),
                    icon = icon("arrow-down"), color = "danger", width = 12)
      })

      # RFM histograms — using shared render_histogram (MP032 DRY, DP_R001)
      output$hist_r <- render_histogram(dna_data, "r_value",
        xlab = translate("Recency (days)"), color = "#dc3545", translate_fn = translate, cap_pctl = 0.99)
      output$hist_f <- render_histogram(dna_data, "f_value",
        xlab = translate("Purchase Frequency"), color = "#28a745", translate_fn = translate, cap_discrete = 50)
      output$hist_m <- render_histogram(dna_data, "m_value",
        xlab = translate("Purchase Amount"), color = "#007bff", translate_fn = translate, cap_pctl = 0.99)

      # Pie chart helper
      render_pie <- function(col, colors) {
        renderPlotly({
          df <- dna_data()
          if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
          tbl <- as.data.frame(table(df[[col]]), stringsAsFactors = FALSE)
          names(tbl) <- c("level", "count")
          tbl$label <- sapply(as.character(tbl$level), translate)
          plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                          marker = list(colors = colors),
                          textinfo = "label+percent") %>%
            plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
        })
      }

      output$pie_r <- render_pie("r_label", c("#dc3545", "#ffc107", "#28a745"))
      output$pie_f <- render_pie("f_label", c("#28a745", "#ffc107", "#dc3545"))
      output$pie_m <- render_pie("m_label", c("#007bff", "#6c757d", "#adb5bd"))

      # Value Tier charts (Issue #235)
      output$pie_value_tier <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl <- as.data.frame(table(df$value_tier), stringsAsFactors = FALSE)
        names(tbl) <- c("level", "count")
        tbl$label <- sapply(as.character(tbl$level), translate)
        plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                        marker = list(colors = c("#28a745", "#ffc107", "#dc3545")),
                        textinfo = "label+percent") %>%
          plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
      })

      output$hist_value_score <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        vals <- df$value_score[!is.na(df$value_score)]
        if (length(vals) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        plotly::plot_ly(x = vals, type = "histogram",
                        marker = list(color = "#6f42c1", line = list(color = "white", width = 1))) %>%
          plotly::layout(xaxis = list(title = translate("Value Score")),
                         yaxis = list(title = translate("Customer Count")),
                         shapes = list(
                           list(type = "line", x0 = 0.33, x1 = 0.33, y0 = 0, y1 = 1, yref = "paper",
                                line = list(color = "#ffc107", dash = "dash", width = 1.5)),
                           list(type = "line", x0 = 0.67, x1 = 0.67, y0 = 0, y1 = 1, yref = "paper",
                                line = list(color = "#28a745", dash = "dash", width = 1.5))
                         ),
                         margin = list(t = 10, b = 50))
      })

      # Marketing Recommendations per metric
      render_rec <- function(metric, label_col) {
        renderUI({
          df <- dna_data()
          if (is.null(df)) return(tags$p(translate("No Data")))
          rec_df <- data.frame(segment = df[[label_col]], stringsAsFactors = FALSE)
          generate_recommendations_html(rec_df, metric, translate)
        })
      }

      output$rec_r <- render_rec("R", "r_label")
      output$rec_f <- render_rec("F", "f_label")
      output$rec_m <- render_rec("M", "m_label")

      # Detail table
      output$detail_table <- DT::renderDataTable({
        df <- dna_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("No Data"))))
        show_df <- data.frame(
          ID = df$customer_id,
          R = round(df$r_value, 0),
          R_label = sapply(df$r_label, translate),
          F = df$f_value,
          F_label = sapply(df$f_label, translate),
          M = round(df$m_value, 0),
          M_label = sapply(df$m_label, translate),
          Value_Tier = sapply(as.character(df$value_tier), translate),
          stringsAsFactors = FALSE
        )
        DT::datatable(show_df,
          colnames = c(translate("Customer ID"), translate("R"), translate("R Level"),
                       translate("F"), translate("F Level"), translate("M"), translate("M Level"),
                       translate("Value Tier")),
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
        prompt_key = "customer_analysis.customer_dna_insights",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          r_tbl <- as.data.frame(table(df$r_label), stringsAsFactors = FALSE)
          f_tbl <- as.data.frame(table(df$f_label), stringsAsFactors = FALSE)
          m_tbl <- as.data.frame(table(df$m_label), stringsAsFactors = FALSE)

          list(
            rfm_summary = paste0(
              "Total customers: ", nrow(df), "\n",
              "Avg Recency: ", round(mean(df$r_value, na.rm = TRUE), 1), " days\n",
              "Avg Frequency: ", round(mean(df$f_value, na.rm = TRUE), 2), " times\n",
              "Avg Monetary: $", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ","), "\n",
              "Median Monetary: $", format(round(stats::median(df$m_value, na.rm = TRUE), 0), big.mark = ",")
            ),
            segment_characteristics = paste0(
              "R segments: ", paste(r_tbl$Var1, r_tbl$Freq, sep = "=", collapse = ", "), "\n",
              "F segments: ", paste(f_tbl$Var1, f_tbl$Freq, sep = "=", collapse = ", "), "\n",
              "M segments: ", paste(m_tbl$Var1, m_tbl$Freq, sep = "=", collapse = ", ")
            ),
            value_tier_summary = paste0(
              "Value segments: ",
              paste(names(table(df$value_tier)), table(df$value_tier), sep = "=", collapse = ", "),
              "\nAvg value score: ", round(mean(df$value_score, na.rm = TRUE), 3)
            ),
            value_distribution = paste0(
              "Total spend range: $", format(round(min(df$m_value, na.rm = TRUE), 0), big.mark = ","),
              " ~ $", format(round(max(df$m_value, na.rm = TRUE), 0), big.mark = ","), "\n",
              "P25: $", format(round(stats::quantile(df$m_value, 0.25, na.rm = TRUE), 0), big.mark = ","),
              ", P75: $", format(round(stats::quantile(df$m_value, 0.75, na.rm = TRUE), 0), big.mark = ","), "\n",
              "Frequency range: ", min(df$f_value, na.rm = TRUE), " ~ ", max(df$f_value, na.rm = TRUE)
            )
          )
        },
        component_label = "customerValue"
      )

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("customer_value_rfm_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- dna_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id,
              r_value = round(df$r_value, 0), r_label = df$r_label,
              f_value = df$f_value, f_label = df$f_label,
              m_value = round(df$m_value, 2), m_label = df$m_label,
              value_score = round(df$value_score, 3), value_tier = as.character(df$value_tier),
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
