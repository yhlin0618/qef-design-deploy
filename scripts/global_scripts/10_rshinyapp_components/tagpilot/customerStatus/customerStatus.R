# =============================================================================
# customerStatus.R — Customer Status (NES) Analysis Component
# Migrated from L3 module_tagpilot_customer_status.R → L4 enterprise pattern
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

customerStatusComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # DEV_R052: English canonical keys — translate() at UI boundary only
  nes_english_labels <- c("N" = "New Customer", "E0" = "Main Customer",
                           "S1" = "Risk Customer", "S2" = "Lost Customer",
                           "S3" = "Deep Sleep Customer")
  nes_colors <- c("N" = "#28a745", "E0" = "#007bff", "S1" = "#ffc107", "S2" = "#fd7e14", "S3" = "#dc3545")

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row — one per NES status
    fluidRow(
      column(2, uiOutput(ns("kpi_n"))),
      column(3, uiOutput(ns("kpi_e0"))),
      column(2, uiOutput(ns("kpi_s1"))),
      column(2, uiOutput(ns("kpi_s2"))),
      column(3, uiOutput(ns("kpi_s3")))
    ),
    # Charts Row
    fluidRow(
      column(6, bs4Card(title = translate("NES Status Distribution"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_nes"), height = "300px"))),
      column(6, bs4Card(title = translate("Churn Risk Distribution"), status = "danger",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("bar_churn_risk"), height = "300px")))
    ),
    # Days-to-churn histogram (P(alive) distribution moved to customerLifecycle per Issue #263)
    fluidRow(
      column(12, bs4Card(title = translate("Estimated Days to Churn"), status = "warning",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("hist_days_churn"), height = "300px")))
    ),
    # Marketing Recommendations
    fluidRow(
      column(12, bs4Card(title = translate("NES Marketing Recommendations"), status = "warning",
        width = 12, solidHeader = TRUE, uiOutput(ns("rec_nes"))))
    ),
    # Metric Explanations (Issue #247)
    fluidRow(
      column(12, bs4Card(title = translate("Metric Explanations"), status = "info",
        width = 12, solidHeader = FALSE, collapsible = TRUE, collapsed = TRUE,
        tags$dl(class = "row mb-0",
          tags$dt(class = "col-sm-3", "P(alive)"),
          tags$dd(class = "col-sm-9", translate("P(alive) Tooltip")),
          tags$dt(class = "col-sm-3", translate("NES Ratio")),
          tags$dd(class = "col-sm-9", translate("NES Ratio Tooltip"))
        )))
    ),
    # High-risk customer table
    fluidRow(
      column(12, bs4Card(title = translate("High Risk Customer List"), status = "danger",
        width = 12, solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"), class = "btn-sm btn-outline-danger mb-2"),
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
          if (nrow(df) == 0) { message("[customerStatus] No data"); return(NULL) }
          message("[customerStatus] Loaded ", nrow(df), " records")
          df
        }, error = function(e) { message("[customerStatus] Error: ", e$message); NULL })
      })

      # KPI helper
      render_nes_kpi <- function(status_code, label, color, icon_name) {
        renderUI({
          df <- dna_data()
          if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = label, icon = icon(icon_name), color = color, width = 12))
          n <- sum(df$nes_status == status_code, na.rm = TRUE)
          pct <- round(n / nrow(df) * 100, 1)
          bs4ValueBox(value = paste0(format(n, big.mark = ","), " (", pct, "%)"),
                      subtitle = label, icon = icon(icon_name), color = color, width = 12)
        })
      }

      output$kpi_n  <- render_nes_kpi("N",  translate(nes_english_labels["N"]),  "success", "user-plus")
      output$kpi_e0 <- render_nes_kpi("E0", translate(nes_english_labels["E0"]), "primary", "star")
      output$kpi_s1 <- render_nes_kpi("S1", translate(nes_english_labels["S1"]), "warning", "moon")
      output$kpi_s2 <- render_nes_kpi("S2", translate(nes_english_labels["S2"]), "orange",  "bed")
      output$kpi_s3 <- render_nes_kpi("S3", translate(nes_english_labels["S3"]), "danger",  "skull-crossbones")

      # Pie chart: NES distribution
      output$pie_nes <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        tbl <- as.data.frame(table(df$nes_status), stringsAsFactors = FALSE)
        names(tbl) <- c("status", "count")
        tbl <- tbl[tbl$count > 0, ]
        tbl$label <- ifelse(tbl$status %in% names(nes_english_labels),
                           sapply(nes_english_labels[tbl$status], translate), tbl$status)
        tbl$color <- ifelse(tbl$status %in% names(nes_colors), nes_colors[tbl$status], "#6c757d")
        plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                        marker = list(colors = tbl$color),
                        textinfo = "label+percent") %>%
          plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
      })

      # Bar chart: churn risk (IPT-based risk tiers, #312)
      output$bar_churn_risk <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        # #312: IPT-based risk tiers — r_value vs ipt multiples
        has_ipt <- "ipt" %in% names(df) && "r_value" %in% names(df)
        valid_idx <- if (has_ipt) !is.na(df$ipt) & df$ipt > 0 & !is.na(df$r_value) else rep(FALSE, nrow(df))
        if (sum(valid_idx) == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        r_vals <- df$r_value[valid_idx]
        ipt_vals <- df$ipt[valid_idx]

        # DEV_R052: English canonical keys for risk classification
        risk_level <- ifelse(r_vals > ipt_vals * 2.5, "High Risk",
                             ifelse(r_vals > ipt_vals * 1.5, "Medium Risk", "Low Risk"))
        tbl <- as.data.frame(table(risk_level), stringsAsFactors = FALSE)
        names(tbl) <- c("risk", "count")
        tbl$risk <- factor(tbl$risk, levels = c("Low Risk", "Medium Risk", "High Risk"))
        tbl <- tbl[order(tbl$risk), ]
        tbl$risk_label <- sapply(as.character(tbl$risk), translate)
        colors <- c("Low Risk" = "#28a745", "Medium Risk" = "#ffc107", "High Risk" = "#dc3545")
        plotly::plot_ly(tbl, x = ~risk_label, y = ~count, type = "bar",
                        marker = list(color = colors[as.character(tbl$risk)])) %>%
          plotly::layout(xaxis = list(title = "", categoryorder = "array",
                                      categoryarray = sapply(c("Low Risk", "Medium Risk", "High Risk"), translate)),
                         yaxis = list(title = translate("Customer Count")),
                         margin = list(t = 10, b = 50))
      })

      # Histogram: estimated days to churn (e0t) — DP_R001 P99 cap (unbounded continuous)
      # Pre-filter: e0t == 0 means no estimate available (not enough purchase history)
      e0t_positive <- reactive({
        df <- dna_data()
        if (is.null(df)) return(NULL)
        df[!is.na(df$e0t) & df$e0t > 0, , drop = FALSE]
      })
      output$hist_days_churn <- render_histogram(e0t_positive, "e0t",
        xlab = translate("Estimated Days"), color = "#fd7e14", translate_fn = translate, cap_pctl = 0.99)

      # Marketing Recommendations (DEV_R052: pass English codes directly)
      output$rec_nes <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(tags$p(translate("No Data")))
        rec_df <- data.frame(segment = df$nes_status, stringsAsFactors = FALSE)
        rec_df <- rec_df[!is.na(rec_df$segment), , drop = FALSE]
        if (nrow(rec_df) == 0) return(tags$p(class = "text-muted", translate("No Data")))
        generate_recommendations_html(rec_df, "NES", translate)
      })

      # Detail table (sorted by NES ratio desc — high ratio = higher churn risk; Issue #266: removed P(alive) column)
      output$detail_table <- DT::renderDataTable({
        df <- dna_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("No Data"))))
        show_df <- data.frame(
          ID = df$customer_id,
          NES = sapply(as.character(df$nes_status), function(code) {
            key <- paste0("NES Status - ", code)
            translated <- translate(key)
            if (identical(translated, key)) code else translated
          }, USE.NAMES = FALSE),
          E0T = round(df$e0t, 0),
          BTYD_Expected = round(df$btyd_expected_transactions, 2),
          NES_Ratio = round(df$nes_ratio, 3),
          stringsAsFactors = FALSE
        )
        show_df <- show_df[order(-show_df$NES_Ratio, na.last = TRUE), ]
        DT::datatable(show_df,
          colnames = c(translate("Customer ID"), translate("NES Status"),
                       translate("Estimated Days"), translate("Expected Transactions"), translate("NES Ratio")),
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
        prompt_key = "customer_analysis.status_insights",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          nes_tbl <- as.data.frame(table(df$nes_status), stringsAsFactors = FALSE)
          names(nes_tbl) <- c("status", "count")
          nes_tbl$pct <- round(nes_tbl$count / nrow(df) * 100, 1)

          # #312: IPT-based risk summary
          valid_ipt <- !is.na(df$ipt) & df$ipt > 0 & !is.na(df$r_value)

          list(
            total_customers = as.character(nrow(df)),
            nes_distribution = paste(nes_tbl$status, "=", nes_tbl$count,
                                     paste0("(", nes_tbl$pct, "%)"), collapse = ", "),
            churn_risk_summary = if (sum(valid_ipt) == 0) {
              "Risk metric: IPT-based (no valid IPT data available)"
            } else {
              paste0(
                "Risk metric: IPT-based (r_value vs ipt multiples)\n",
                "High risk (r_value > 2.5x IPT): ", sum(df$r_value[valid_ipt] > df$ipt[valid_ipt] * 2.5), " customers\n",
                "Medium risk (1.5x-2.5x IPT): ", sum(df$r_value[valid_ipt] > df$ipt[valid_ipt] * 1.5 & df$r_value[valid_ipt] <= df$ipt[valid_ipt] * 2.5), " customers\n",
                "Low risk (<= 1.5x IPT): ", sum(df$r_value[valid_ipt] <= df$ipt[valid_ipt] * 1.5), " customers\n",
                "Avg IPT: ", round(mean(df$ipt[valid_ipt], na.rm = TRUE), 1), " days"
              )
            },
            alive_probability_summary = paste0(
              "Avg P(alive): ", round(mean(df$p_alive, na.rm = TRUE), 3), "\n",
              "Median P(alive): ", round(stats::median(df$p_alive, na.rm = TRUE), 3), "\n",
              "Avg expected transactions: ", round(mean(df$btyd_expected_transactions, na.rm = TRUE), 2)
            )
          )
        },
        component_label = "customerStatus"
      )

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("customer_status_nes_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- dna_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id, nes_status = df$nes_status,
              p_alive = round(df$p_alive, 3), e0t = round(df$e0t, 0),
              btyd_expected_transactions = round(df$btyd_expected_transactions, 2),
              nes_ratio = round(df$nes_ratio, 3), nrec_prob = round(df$nrec_prob, 3),
              stringsAsFactors = FALSE
            )
            export <- export[order(export$p_alive, na.last = TRUE), ]
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
