# =============================================================================
# macroTrends.R — Macro Indicator Analysis Component (VitalSigns)
# CONSUMES: df_macro_monthly_summary (from D05), df_dna_by_customer (from D01)
# Following: UI_R001, UI_R011, UI_R026, MP064, MP029, DEV_R052, UI_R027, DP_R002
# Issue: #278, #325
# =============================================================================

macroTrendsComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    selectInput(ns("time_scale"), translate("Time Scale"),
                choices = c("Monthly" = "month", "Quarter" = "quarter", "Yearly" = "year"),
                selected = "month"),
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row 1: Revenue & Growth
    fluidRow(
      column(3, uiOutput(ns("kpi_revenue"))),
      column(3, uiOutput(ns("kpi_orders"))),
      column(3, uiOutput(ns("kpi_growth"))),
      column(3, uiOutput(ns("kpi_yoy")))
    ),
    # KPI Row 2: Customers
    fluidRow(
      column(3, uiOutput(ns("kpi_total_customers"))),
      column(3, uiOutput(ns("kpi_active_customers"))),
      column(3, uiOutput(ns("kpi_new_customer_rate"))),
      column(3, uiOutput(ns("kpi_aov")))
    ),
    # KPI Row 3: DNA Snapshot
    fluidRow(
      column(3, uiOutput(ns("kpi_avg_clv"))),
      column(3, uiOutput(ns("kpi_avg_palive"))),
      column(3, uiOutput(ns("kpi_e0_ratio"))),
      column(3, uiOutput(ns("kpi_sleep_ratio")))
    ),
    # Charts Row 1: Revenue + Orders trend
    fluidRow(
      column(6, bs4Card(title = uiOutput(ns("chart_title_revenue")), status = "primary",
        width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("revenue_trend"), height = "350px"))),
      column(6, bs4Card(title = uiOutput(ns("chart_title_orders")), status = "success",
        width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("order_trend"), height = "350px")))
    ),
    # Charts Row 2: Customer counts + AOV
    fluidRow(
      column(6, bs4Card(title = uiOutput(ns("chart_title_customers")), status = "info",
        width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("customer_trend"), height = "350px"))),
      column(6, bs4Card(title = uiOutput(ns("chart_title_aov")), status = "warning",
        width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("aov_trend"), height = "350px")))
    ),
    # Detail Table
    fluidRow(
      column(12, bs4Card(title = uiOutput(ns("table_title")), status = "primary",
        width = 12, solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"), class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("detail_table"))))
    ),
    # AI Insight Result — bottom of display (UI_R026)
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # ---- Helper: aggregate monthly data to quarter/year ----
      aggregate_time_scale <- function(df, scale) {
        if (is.null(df) || nrow(df) == 0) return(NULL)
        if (scale == "month") return(df)

        df$period <- if (scale == "quarter") {
          yr <- substr(df$year_month, 1, 4)
          mo <- as.integer(substr(df$year_month, 6, 7))
          qq <- ceiling(mo / 3)
          paste0(yr, "-Q", qq)
        } else {
          substr(df$year_month, 1, 4)
        }

        agg <- do.call(rbind, lapply(split(df, df$period), function(chunk) {
          data.frame(
            period = chunk$period[1],
            total_revenue = sum(chunk$total_revenue, na.rm = TRUE),
            order_count = sum(chunk$order_count, na.rm = TRUE),
            active_customers = sum(chunk$active_customers, na.rm = TRUE),
            new_customers = sum(chunk$new_customers, na.rm = TRUE),
            avg_order_value = if (sum(chunk$order_count, na.rm = TRUE) > 0)
              sum(chunk$total_revenue, na.rm = TRUE) / sum(chunk$order_count, na.rm = TRUE) else 0,
            stringsAsFactors = FALSE
          )
        }))
        agg <- agg[order(agg$period), ]
        rownames(agg) <- NULL

        # Compute period-over-period growth
        n <- nrow(agg)
        agg$growth_pct <- c(NA, diff(agg$total_revenue) / head(agg$total_revenue, -1) * 100)

        # YoY: compare to same period previous year
        agg$yoy_pct <- NA_real_
        if (scale == "quarter" && n >= 5) {
          for (i in 5:n) agg$yoy_pct[i] <- (agg$total_revenue[i] - agg$total_revenue[i - 4]) / agg$total_revenue[i - 4] * 100
        } else if (scale == "year" && n >= 2) {
          for (i in 2:n) agg$yoy_pct[i] <- (agg$total_revenue[i] - agg$total_revenue[i - 1]) / agg$total_revenue[i - 1] * 100
        }

        agg
      }

      # ---- Reactive: raw monthly data ----
      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2
      macro_data_raw <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          if (!DBI::dbExistsTable(app_connection, "df_macro_monthly_summary")) {
            message("[macroTrends] Table df_macro_monthly_summary not found")
            return(NULL)
          }
          plt <- cfg$filters$platform_id
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"

          df <- tbl2(app_connection, "df_macro_monthly_summary") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter == !!pl_id) %>%
            dplyr::arrange(year_month) %>%
            dplyr::collect()

          if (nrow(df) == 0) { message("[macroTrends] No data returned"); return(NULL) }
          message("[macroTrends] Loaded ", nrow(df), " months of data")
          df
        }, error = function(e) {
          message("[macroTrends] Data load error: ", e$message)
          NULL
        })
      })

      # ---- Reactive: time-scaled data ----
      macro_data <- reactive({
        df <- macro_data_raw()
        scale <- input$time_scale %||% "month"
        if (scale == "month") {
          # For monthly, reuse existing MoM/YoY columns and add period
          if (!is.null(df)) {
            df$period <- df$year_month
            df$growth_pct <- df$mom_revenue_pct
            df$yoy_pct <- df$yoy_revenue_pct
          }
          df
        } else {
          aggregate_time_scale(df, scale)
        }
      })

      # ---- Reactive: DNA snapshot (from df_dna_by_customer) ----
      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2
      dna_snapshot <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          if (!DBI::dbExistsTable(app_connection, "df_dna_by_customer")) return(NULL)
          plt <- cfg$filters$platform_id
          pline <- if (!is.null(cfg$filters$product_line_id_sliced) && cfg$filters$product_line_id_sliced != "all")
            cfg$filters$product_line_id_sliced else "all"

          tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter == !!pline) %>%
            dplyr::summarise(
              total_customers = dplyr::n(),
              avg_clv = mean(clv, na.rm = TRUE),
              avg_p_alive = mean(p_alive, na.rm = TRUE),
              e0_count = sum(dplyr::if_else(nes_status == "E0", 1L, 0L), na.rm = TRUE),
              sleep_count = sum(dplyr::if_else(nes_status %in% c("S1", "S2", "S3"), 1L, 0L), na.rm = TRUE)
            ) %>%
            dplyr::collect() %>%
            dplyr::mutate(
              e0_pct = dplyr::if_else(total_customers > 0, e0_count * 100.0 / total_customers, NA_real_),
              sleep_pct = dplyr::if_else(total_customers > 0, sleep_count * 100.0 / total_customers, NA_real_)
            ) %>%
            dplyr::select(total_customers, avg_clv, avg_p_alive, e0_pct, sleep_pct)
        }, error = function(e) {
          message("[macroTrends] DNA snapshot error: ", e$message)
          NULL
        })
      })

      # Helper: get latest period row
      latest_row <- reactive({
        df <- macro_data()
        if (is.null(df) || nrow(df) == 0) return(NULL)
        df[nrow(df), ]
      })

      # Helper: growth label by time scale
      growth_label <- reactive({
        scale <- input$time_scale %||% "month"
        switch(scale,
          "month" = translate("MoM Growth"),
          "quarter" = translate("QoQ Growth"),
          "year" = translate("YoY Growth")
        )
      })

      # Helper: time scale label for chart titles
      scale_label <- reactive({
        scale <- input$time_scale %||% "month"
        switch(scale,
          "month" = translate("Monthly"),
          "quarter" = translate("Quarter"),
          "year" = translate("Yearly")
        )
      })

      # ---- Dynamic Chart/Table Titles ----
      output$chart_title_revenue <- renderUI(tags$span(paste(scale_label(), translate("Revenue Trend"))))
      output$chart_title_orders <- renderUI(tags$span(paste(scale_label(), translate("Order Trend"))))
      output$chart_title_customers <- renderUI(tags$span(paste(scale_label(), translate("Customer Trend"))))
      output$chart_title_aov <- renderUI(tags$span(paste(scale_label(), translate("AOV Trend"))))
      output$table_title <- renderUI(tags$span(paste(scale_label(), translate("Summary Table"))))

      # ---- KPI Row 1: Revenue & Growth ----
      output$kpi_revenue <- renderUI({
        row <- latest_row()
        if (is.null(row)) return(bs4ValueBox(value = "-", subtitle = translate("Total Revenue"),
                                              icon = icon("dollar-sign"), color = "success", width = 12))
        val <- paste0("$", format(round(row$total_revenue), big.mark = ","))
        bs4ValueBox(value = val, subtitle = paste0(translate("Total Revenue"), " (", row$period, ")"),
                    icon = icon("dollar-sign"), color = "success", width = 12)
      })

      output$kpi_orders <- renderUI({
        row <- latest_row()
        if (is.null(row)) return(bs4ValueBox(value = "-", subtitle = translate("Order Count"),
                                              icon = icon("shopping-cart"), color = "primary", width = 12))
        bs4ValueBox(value = format(row$order_count, big.mark = ","),
                    subtitle = paste0(translate("Order Count"), " (", row$period, ")"),
                    icon = icon("shopping-cart"), color = "primary", width = 12)
      })

      output$kpi_growth <- renderUI({
        row <- latest_row()
        if (is.null(row) || is.na(row$growth_pct)) {
          return(bs4ValueBox(value = "-", subtitle = growth_label(),
                             icon = icon("arrow-trend-up"), color = "info", width = 12))
        }
        val <- paste0(ifelse(row$growth_pct >= 0, "+", ""), round(row$growth_pct, 1), "%")
        color <- if (row$growth_pct >= 0) "info" else "danger"
        bs4ValueBox(value = val, subtitle = growth_label(),
                    icon = icon(ifelse(row$growth_pct >= 0, "arrow-trend-up", "arrow-trend-down")),
                    color = color, width = 12)
      })

      output$kpi_yoy <- renderUI({
        row <- latest_row()
        if (is.null(row) || is.na(row$yoy_pct)) {
          return(bs4ValueBox(value = "-", subtitle = translate("YoY Growth"),
                             icon = icon("chart-line"), color = "warning", width = 12))
        }
        val <- paste0(ifelse(row$yoy_pct >= 0, "+", ""), round(row$yoy_pct, 1), "%")
        color <- if (row$yoy_pct >= 0) "warning" else "danger"
        bs4ValueBox(value = val, subtitle = translate("YoY Growth"),
                    icon = icon("chart-line"), color = color, width = 12)
      })

      # ---- KPI Row 2: Customers ----
      output$kpi_total_customers <- renderUI({
        snap <- dna_snapshot()
        if (is.null(snap) || nrow(snap) == 0) return(bs4ValueBox(value = "-", subtitle = translate("Total Customers"),
                                                                   icon = icon("users"), color = "primary", width = 12))
        bs4ValueBox(value = format(snap$total_customers, big.mark = ","),
                    subtitle = translate("Total Customers"),
                    icon = icon("users"), color = "primary", width = 12)
      })

      output$kpi_active_customers <- renderUI({
        row <- latest_row()
        if (is.null(row)) return(bs4ValueBox(value = "-", subtitle = translate("Active Customers"),
                                              icon = icon("user-check"), color = "success", width = 12))
        bs4ValueBox(value = format(row$active_customers, big.mark = ","),
                    subtitle = paste0(translate("Active Customers"), " (", row$period, ")"),
                    icon = icon("user-check"), color = "success", width = 12)
      })

      output$kpi_new_customer_rate <- renderUI({
        row <- latest_row()
        if (is.null(row) || row$active_customers == 0) {
          return(bs4ValueBox(value = "-", subtitle = translate("New Customer Rate"),
                             icon = icon("user-plus"), color = "info", width = 12))
        }
        rate <- round(row$new_customers / row$active_customers * 100, 1)
        bs4ValueBox(value = paste0(rate, "%"),
                    subtitle = paste0(translate("New Customer Rate"), " (", row$period, ")"),
                    icon = icon("user-plus"), color = "info", width = 12)
      })

      output$kpi_aov <- renderUI({
        row <- latest_row()
        if (is.null(row)) return(bs4ValueBox(value = "-", subtitle = translate("Average Order Value"),
                                              icon = icon("receipt"), color = "warning", width = 12))
        val <- paste0("$", format(round(row$avg_order_value, 2), nsmall = 2))
        bs4ValueBox(value = val, subtitle = paste0(translate("Average Order Value"), " (", row$period, ")"),
                    icon = icon("receipt"), color = "warning", width = 12)
      })

      # ---- KPI Row 3: DNA Snapshot ----
      output$kpi_avg_clv <- renderUI({
        snap <- dna_snapshot()
        if (is.null(snap) || nrow(snap) == 0 || is.na(snap$avg_clv)) {
          return(bs4ValueBox(value = "-", subtitle = translate("Avg CLV"),
                             icon = icon("gem"), color = "success", width = 12))
        }
        bs4ValueBox(value = paste0("$", format(round(snap$avg_clv), big.mark = ",")),
                    subtitle = translate("Avg CLV"),
                    icon = icon("gem"), color = "success", width = 12)
      })

      output$kpi_avg_palive <- renderUI({
        snap <- dna_snapshot()
        if (is.null(snap) || nrow(snap) == 0 || is.na(snap$avg_p_alive)) {
          return(bs4ValueBox(value = "-", subtitle = translate("Avg P(alive)"),
                             icon = icon("heartbeat"), color = "primary", width = 12))
        }
        bs4ValueBox(value = paste0(round(snap$avg_p_alive * 100, 1), "%"),
                    subtitle = translate("Avg P(alive)"),
                    icon = icon("heartbeat"), color = "primary", width = 12)
      })

      output$kpi_e0_ratio <- renderUI({
        snap <- dna_snapshot()
        if (is.null(snap) || nrow(snap) == 0 || is.na(snap$e0_pct)) {
          return(bs4ValueBox(value = "-", subtitle = translate("E0 Ratio"),
                             icon = icon("star"), color = "info", width = 12))
        }
        bs4ValueBox(value = paste0(round(snap$e0_pct, 1), "%"),
                    subtitle = translate("E0 Ratio"),
                    icon = icon("star"), color = "info", width = 12)
      })

      output$kpi_sleep_ratio <- renderUI({
        snap <- dna_snapshot()
        if (is.null(snap) || nrow(snap) == 0 || is.na(snap$sleep_pct)) {
          return(bs4ValueBox(value = "-", subtitle = translate("Sleep Customer Ratio"),
                             icon = icon("moon"), color = "danger", width = 12))
        }
        bs4ValueBox(value = paste0(round(snap$sleep_pct, 1), "%"),
                    subtitle = translate("Sleep Customer Ratio"),
                    icon = icon("moon"), color = "danger", width = 12)
      })

      # ---- Charts ----
      output$revenue_trend <- plotly::renderPlotly({
        df <- macro_data()
        if (is.null(df)) return(plotly::plotly_empty())
        plotly::plot_ly(df, x = ~period, y = ~total_revenue, type = "scatter", mode = "lines+markers",
                        line = list(color = "#3c8dbc", width = 2),
                        marker = list(color = "#3c8dbc", size = 6),
                        hovertemplate = paste0(
                          "<b>%{x}</b><br>",
                          translate("Revenue"), ": $%{y:,.0f}<br>",
                          "<extra></extra>")) %>%
          plotly::layout(
            xaxis = list(title = "", tickangle = -45),
            yaxis = list(title = translate("Revenue")),
            margin = list(b = 80)
          )
      })

      output$order_trend <- plotly::renderPlotly({
        df <- macro_data()
        if (is.null(df)) return(plotly::plotly_empty())
        plotly::plot_ly(df, x = ~period, y = ~order_count, type = "scatter", mode = "lines+markers",
                        line = list(color = "#00a65a", width = 2),
                        marker = list(color = "#00a65a", size = 6),
                        hovertemplate = paste0(
                          "<b>%{x}</b><br>",
                          translate("Order Count"), ": %{y:,.0f}<br>",
                          "<extra></extra>")) %>%
          plotly::layout(
            xaxis = list(title = "", tickangle = -45),
            yaxis = list(title = translate("Order Count")),
            margin = list(b = 80)
          )
      })

      output$customer_trend <- plotly::renderPlotly({
        df <- macro_data()
        if (is.null(df)) return(plotly::plotly_empty())
        plotly::plot_ly(df, x = ~period) %>%
          plotly::add_trace(y = ~active_customers, name = translate("Active Customers"),
                            type = "scatter", mode = "lines+markers",
                            line = list(color = "#00c0ef", width = 2),
                            marker = list(color = "#00c0ef", size = 6)) %>%
          plotly::add_trace(y = ~new_customers, name = translate("New Customers"),
                            type = "scatter", mode = "lines+markers",
                            line = list(color = "#f39c12", width = 2, dash = "dash"),
                            marker = list(color = "#f39c12", size = 6)) %>%
          plotly::layout(
            xaxis = list(title = "", tickangle = -45),
            yaxis = list(title = translate("Customer Count")),
            legend = list(orientation = "h", y = -0.2),
            margin = list(b = 80)
          )
      })

      output$aov_trend <- plotly::renderPlotly({
        df <- macro_data()
        if (is.null(df)) return(plotly::plotly_empty())
        plotly::plot_ly(df, x = ~period, y = ~avg_order_value, type = "scatter", mode = "lines+markers",
                        line = list(color = "#f39c12", width = 2),
                        marker = list(color = "#f39c12", size = 6),
                        hovertemplate = paste0(
                          "<b>%{x}</b><br>",
                          translate("Average Order Value"), ": $%{y:,.2f}<br>",
                          "<extra></extra>")) %>%
          plotly::layout(
            xaxis = list(title = "", tickangle = -45),
            yaxis = list(title = translate("Average Order Value")),
            margin = list(b = 80)
          )
      })

      # ---- Detail Table ----
      output$detail_table <- DT::renderDataTable({
        df <- macro_data()
        if (is.null(df)) return(DT::datatable(data.frame()))
        display_df <- data.frame(
          Period = df$period,
          Revenue = paste0("$", format(round(df$total_revenue), big.mark = ",")),
          Orders = format(df$order_count, big.mark = ","),
          ActiveCustomers = format(df$active_customers, big.mark = ","),
          NewCustomers = format(df$new_customers, big.mark = ","),
          AOV = paste0("$", format(round(df$avg_order_value, 2), nsmall = 2)),
          Growth = ifelse(is.na(df$growth_pct), "-",
                         paste0(ifelse(df$growth_pct >= 0, "+", ""), round(df$growth_pct, 1), "%")),
          YoY = ifelse(is.na(df$yoy_pct), "-",
                       paste0(ifelse(df$yoy_pct >= 0, "+", ""), round(df$yoy_pct, 1), "%"))
        )
        colnames(display_df) <- c(
          translate("Period"), translate("Revenue"), translate("Order Count"),
          translate("Active Customers"), translate("New Customers"),
          translate("Average Order Value"), growth_label(), translate("YoY Growth")
        )
        DT::datatable(display_df, options = list(pageLength = 12, ordering = TRUE, scrollX = TRUE),
                      rownames = FALSE)
      })

      # ---- CSV Download (DEV_R051: UTF-8 BOM) ----
      output$download_csv <- downloadHandler(
        filename = function() paste0("macro_trends_", input$time_scale %||% "month", "_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- macro_data()
          if (!is.null(df)) {
            con <- file(file, "wb")
            writeBin(charToRaw("\xef\xbb\xbf"), con)
            close(con)
            utils::write.table(df, file, row.names = FALSE, sep = ",",
                               quote = TRUE, append = TRUE, fileEncoding = "UTF-8")
          }
        }
      )

      # ---- AI Insight — non-blocking via ExtendedTask (GUIDE03, TD_P004 compliant) ----
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "vitalsigns_analysis.macro_trends_insights",
        get_template_vars = function() {
          df <- macro_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)
          n <- nrow(df)
          tail_n <- min(6, n)
          recent <- df[(n - tail_n + 1):n, ]
          snap <- dna_snapshot()
          dna_info <- if (!is.null(snap) && nrow(snap) > 0) {
            paste0("\nDNA Snapshot: Total=", snap$total_customers,
                   ", Avg CLV=$", round(snap$avg_clv),
                   ", Avg P(alive)=", round(snap$avg_p_alive, 2),
                   ", E0=", round(snap$e0_pct, 1), "%",
                   ", Sleep=", round(snap$sleep_pct, 1), "%")
          } else ""
          list(
            data_summary = paste0(
              "Macro Trends (", input$time_scale %||% "month", ", last ", tail_n, " periods):\n",
              paste(capture.output(print(recent[, c("period", "total_revenue", "order_count",
                "active_customers", "avg_order_value", "growth_pct", "yoy_pct")], row.names = FALSE)),
                collapse = "\n"),
              dna_info
            )
          )
        },
        component_label = "macroTrends"
      )

      # Return reactive results for report integration
      reactive({
        df <- macro_data()
        if (is.null(df)) return(list())
        row <- latest_row()
        snap <- dna_snapshot()
        list(
          total_revenue = row$total_revenue,
          order_count = row$order_count,
          active_customers = row$active_customers,
          growth_pct = row$growth_pct,
          yoy_pct = row$yoy_pct,
          periods_count = nrow(df),
          time_scale = input$time_scale %||% "month",
          avg_clv = if (!is.null(snap)) snap$avg_clv else NA,
          avg_p_alive = if (!is.null(snap)) snap$avg_p_alive else NA,
          e0_pct = if (!is.null(snap)) snap$e0_pct else NA,
          sleep_pct = if (!is.null(snap)) snap$sleep_pct else NA
        )
      })
    })
  }

  list(
    ui = list(filter = ui_filter, display = ui_display),
    server = server_fn
  )
}
