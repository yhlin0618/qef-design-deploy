# =============================================================================
# customerStructure.R — Customer Structure (Base Value) Analysis Component
# Migrated from L3 module_tagpilot_customer_base_value.R → L4 enterprise pattern
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

customerStructureComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_total_customers"))),
      column(3, uiOutput(ns("kpi_avg_ipt"))),
      column(3, uiOutput(ns("kpi_avg_aov"))),
      column(3, uiOutput(ns("kpi_total_spent")))
    ),
    # Purchase cycle distribution + Monetary distribution
    fluidRow(
      column(6, bs4Card(title = translate("Purchase Cycle Distribution"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_ipt"), height = "300px"))),
      column(6, bs4Card(title = translate("Monetary Distribution"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_m"), height = "300px")))
    ),
    # PCV and CRI segment pies
    fluidRow(
      column(6, bs4Card(title = translate("Past Customer Value (PCV)"), status = "success",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_pcv"), height = "300px"))),
      column(6, bs4Card(title = translate("Customer Retention Index (CRI)"), status = "warning",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_cri"), height = "300px")))
    ),
    # Marketing Recommendations
    fluidRow(
      column(6, bs4Card(title = translate("PCV Recommendations"), status = "success",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_pcv")))),
      column(6, bs4Card(title = translate("CRI Recommendations"), status = "warning",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_cri"))))
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
          if (nrow(df) == 0) { message("[customerStructure] No data"); return(NULL) }
          message("[customerStructure] Loaded ", nrow(df), " records")
          df
        }, error = function(e) { message("[customerStructure] Error: ", e$message); NULL })
      })

      # KPI boxes
      output$kpi_total_customers <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else format(nrow(df), big.mark = ",")
        bs4ValueBox(value = val, subtitle = translate("Total Customers"),
                    icon = icon("users"), color = "primary", width = 12)
      })

      output$kpi_avg_ipt <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else paste0(round(mean(df$ipt_mean, na.rm = TRUE), 0), " ", translate("days"))
        bs4ValueBox(value = val, subtitle = translate("Avg Purchase Cycle"),
                    icon = icon("sync-alt"), color = "info", width = 12)
      })

      output$kpi_avg_aov <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else paste0("$", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ","))
        bs4ValueBox(value = val, subtitle = translate("Avg Order Value"),
                    icon = icon("shopping-cart"), color = "success", width = 12)
      })

      output$kpi_total_spent <- renderUI({
        df <- dna_data()
        val <- if (is.null(df)) "-" else paste0("$", format(round(sum(df$total_spent, na.rm = TRUE), 0), big.mark = ","))
        bs4ValueBox(value = val, subtitle = translate("Total Spent"),
                    icon = icon("coins"), color = "warning", width = 12)
      })

      # Histogram: IPT (purchase cycle) with P20/P50/P80 lines
      # Custom layout (not render_histogram) due to reference lines; DP_R001 P99 cap applied
      output$hist_ipt <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        vals <- df$ipt_mean[!is.na(df$ipt_mean) & df$ipt_mean > 0]
        if (length(vals) < 3) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        # DP_R001: cap at P99 before computing reference lines
        cap_val <- stats::quantile(vals, 0.99, na.rm = TRUE)
        vals <- pmin(vals, cap_val)
        p20 <- quantile(vals, 0.2)
        p50 <- quantile(vals, 0.5)
        p80 <- quantile(vals, 0.8)
        plotly::plot_ly(x = vals, type = "histogram",
                        marker = list(color = "#007bff", line = list(color = "white", width = 1))) %>%
          plotly::layout(
            xaxis = list(title = translate("Avg Purchase Interval (days)")),
            yaxis = list(title = translate("Customer Count")),
            shapes = list(
              list(type = "line", x0 = p20, x1 = p20, y0 = 0, y1 = 1, yref = "paper", line = list(color = "#28a745", dash = "dot")),
              list(type = "line", x0 = p50, x1 = p50, y0 = 0, y1 = 1, yref = "paper", line = list(color = "#ffc107", dash = "dot")),
              list(type = "line", x0 = p80, x1 = p80, y0 = 0, y1 = 1, yref = "paper", line = list(color = "#dc3545", dash = "dot"))
            ),
            annotations = list(
              list(x = p20, y = 1.02, yref = "paper", text = paste0("P20=", round(p20, 0), translate("days")),
                   showarrow = TRUE, arrowhead = 0, ay = -20, font = list(size = 10, color = "#28a745")),
              list(x = p50, y = 1.02, yref = "paper", text = paste0("P50=", round(p50, 0), translate("days")),
                   showarrow = TRUE, arrowhead = 0, ay = -38, font = list(size = 10, color = "#ffc107")),
              list(x = p80, y = 1.02, yref = "paper", text = paste0("P80=", round(p80, 0), translate("days")),
                   showarrow = TRUE, arrowhead = 0, ay = -56, font = list(size = 10, color = "#dc3545"))
            ),
            margin = list(t = 60, b = 50)
          )
      })

      # Histogram: M distribution — DP_R001 P99 cap (Issue #245), unified via render_histogram
      output$hist_m <- render_histogram(dna_data, "m_value",
        xlab = translate("Purchase Amount"), color = "#17a2b8", translate_fn = translate, cap_pctl = 0.99)

      # PCV segment helper — segment by PCV value using P20/P80 (Issue #246: add Medium Value)
      pcv_segmented <- reactive({
        df <- dna_data()
        if (is.null(df)) return(NULL)
        vals <- df$pcv[!is.na(df$pcv)]
        if (length(vals) < 3) return(NULL)
        p20 <- quantile(vals, 0.2)
        p80 <- quantile(vals, 0.8)
        df$pcv_segment <- ifelse(is.na(df$pcv), NA_character_,
                                  ifelse(df$pcv >= p80, "High Value",
                                         ifelse(df$pcv >= p20, "Medium Value", "Low Value")))
        df
      })

      # CRI segment helper
      cri_segmented <- reactive({
        df <- dna_data()
        if (is.null(df)) return(NULL)
        vals <- df$cri_ecdf[!is.na(df$cri_ecdf)]
        if (length(vals) < 3) return(NULL)
        df$cri_segment <- ifelse(is.na(df$cri_ecdf), NA_character_,
                                  ifelse(df$cri_ecdf >= 0.8, "High Engagement",
                                         ifelse(df$cri_ecdf >= 0.2, "Medium Engagement", "Low Engagement")))
        df
      })

      # Pie: PCV segments (DEV_R052: translate at UI)
      output$pie_pcv <- renderPlotly({
        df <- pcv_segmented()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl <- as.data.frame(table(df$pcv_segment), stringsAsFactors = FALSE)
        names(tbl) <- c("segment", "count")
        tbl <- tbl[tbl$count > 0, ]
        tbl$label <- sapply(tbl$segment, translate)
        plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                        marker = list(colors = c("#007bff", "#ffc107", "#adb5bd")),
                        textinfo = "label+percent") %>%
          plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
      })

      # Pie: CRI segments (DEV_R052: translate at UI)
      output$pie_cri <- renderPlotly({
        df <- cri_segmented()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl <- as.data.frame(table(df$cri_segment), stringsAsFactors = FALSE)
        names(tbl) <- c("segment", "count")
        tbl <- tbl[tbl$count > 0, ]
        tbl$label <- sapply(tbl$segment, translate)
        plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                        marker = list(colors = c("#28a745", "#ffc107", "#dc3545")),
                        textinfo = "label+percent") %>%
          plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
      })

      # PCV Recommendations
      output$rec_pcv <- renderUI({
        df <- pcv_segmented()
        if (is.null(df)) return(tags$p(translate("No Data")))
        rec_df <- data.frame(segment = df$pcv_segment, stringsAsFactors = FALSE)
        generate_recommendations_html(rec_df, "PCV", translate)
      })

      # CRI Recommendations
      output$rec_cri <- renderUI({
        df <- cri_segmented()
        if (is.null(df)) return(tags$p(translate("No Data")))
        rec_df <- data.frame(segment = df$cri_segment, stringsAsFactors = FALSE)
        generate_recommendations_html(rec_df, "CRI", translate)
      })

      # Detail table
      output$detail_table <- DT::renderDataTable({
        df <- dna_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("No Data"))))
        show_df <- data.frame(
          ID = df$customer_id,
          IPT = round(df$ipt_mean, 0),
          M = round(df$m_value, 0),
          F = as.integer(df$f_value),
          PCV = round(df$pcv, 2),
          CRI = round(df$cri, 3),
          stringsAsFactors = FALSE
        )
        DT::datatable(show_df,
          colnames = c(translate("Customer ID"), translate("Avg Purchase Interval"),
                       translate("M"), translate("F"), translate("PCV"), translate("CRI")),
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
        prompt_key = "customer_analysis.structure_insights",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          ipt_vals <- df$ipt_mean[!is.na(df$ipt_mean) & df$ipt_mean > 0]
          pcv_vals <- df$pcv[!is.na(df$pcv)]
          cri_vals <- df$cri[!is.na(df$cri)]

          list(
            total_customers = as.character(nrow(df)),
            purchase_cycle_summary = paste0(
              "Avg purchase interval: ", round(mean(ipt_vals), 0), " days\n",
              "Median purchase interval: ", round(stats::median(ipt_vals), 0), " days\n",
              "P20: ", round(stats::quantile(ipt_vals, 0.2), 0), " days, ",
              "P50: ", round(stats::quantile(ipt_vals, 0.5), 0), " days, ",
              "P80: ", round(stats::quantile(ipt_vals, 0.8), 0), " days"
            ),
            spending_summary = paste0(
              "Avg order value: $", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ","), "\n",
              "Total spent: $", format(round(sum(df$total_spent, na.rm = TRUE), 0), big.mark = ","), "\n",
              "Avg frequency: ", round(mean(df$f_value, na.rm = TRUE), 1), " times"
            ),
            pcv_cri_summary = paste0(
              "PCV - Avg: ", round(mean(pcv_vals), 2),
              ", Median: ", round(stats::median(pcv_vals), 2),
              ", P80: ", round(stats::quantile(pcv_vals, 0.8), 2), "\n",
              "CRI - Avg: ", round(mean(cri_vals), 3),
              ", Median: ", round(stats::median(cri_vals), 3)
            )
          )
        },
        component_label = "customerStructure"
      )

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("customer_structure_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- dna_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id,
              ipt_mean = round(df$ipt_mean, 1), m_value = round(df$m_value, 2),
              f_value = df$f_value, pcv = round(df$pcv, 2), cri = round(df$cri, 3),
              total_spent = round(df$total_spent, 0), times = df$times,
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
