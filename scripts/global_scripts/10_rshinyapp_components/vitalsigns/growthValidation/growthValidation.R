# =============================================================================
# growthValidation.R — Growth Validation Component (VitalSigns) — Issue #416
# CONSUMES: df_macro_monthly_summary (from D05_01 + finalize_category)
# PRODUCES: 3 valueBoxes (brand / category / excess) + plotly time trend
# Following: UI_R001, UI_R011, UI_R026, UI_R028, MP064, MP029, DEV_R001, DEV_R052
#
# Decision 1 (locked): category = SUM revenue across platforms within same
#   product_line_id_filter (df_macro_monthly_summary already populated by
#   finalize_D05_01_category()).
# Decision 2 (locked): excess_growth = brand_pct - category_pct (差值, pp).
# Decision 3 (locked): radioButton period selector (YoY/MoM/QoQ, default YoY)
#   + 3 valueBoxes showing active period.
# =============================================================================

growthValidationComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    radioButtons(
      ns("period"),
      label = translate("Period: YoY / MoM / QoQ"),
      choices = c("YoY" = "yoy", "MoM" = "mom", "QoQ" = "qoq"),
      selected = "yoy",
      inline = TRUE
    ),
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row — 3 valueBoxes
    fluidRow(
      column(4, uiOutput(ns("kpi_brand"))),
      column(4, uiOutput(ns("kpi_category"))),
      column(4, uiOutput(ns("kpi_excess")))
    ),
    # Time-trend chart
    fluidRow(
      column(12, bs4Card(
        title = translate("Excess Growth Trend"),
        status = "primary",
        width = 12,
        solidHeader = TRUE,
        plotly::plotlyOutput(ns("excess_trend_chart"), height = "400px")
      ))
    ),
    # Detail data table
    fluidRow(
      column(12, bs4Card(
        title = translate("Growth Validation Detail"),
        status = "info",
        width = 12,
        solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"),
                       class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("detail_table"))
      ))
    ),
    # AI Insight Result — bottom
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # ---- Data reactive ----
      growth_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          if (!DBI::dbExistsTable(app_connection, "df_macro_monthly_summary")) {
            message("[growthValidation] Table df_macro_monthly_summary not found")
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

          if (nrow(df) == 0) {
            message("[growthValidation] No data returned")
            return(NULL)
          }

          # Verify required columns exist (graceful degrade if D05 finalize not run)
          required_cols <- c("year_month", "total_revenue",
                             "mom_revenue_pct", "yoy_revenue_pct", "qoq_revenue_pct",
                             "category_revenue", "category_mom_pct",
                             "category_yoy_pct", "category_qoq_pct",
                             "excess_growth_mom", "excess_growth_yoy",
                             "excess_growth_qoq")
          missing_cols <- setdiff(required_cols, names(df))
          if (length(missing_cols) > 0) {
            message("[growthValidation] Missing columns (run D05_01 + finalize): ",
                    paste(missing_cols, collapse = ", "))
            return(NULL)
          }

          message("[growthValidation] Loaded ", nrow(df), " months")
          df
        }, error = function(e) {
          message("[growthValidation] Data load error: ", e$message)
          NULL
        })
      })

      # ---- Pick column names by active period ----
      period_cols <- reactive({
        p <- input$period %||% "yoy"
        list(
          period = p,
          period_label = switch(p,
                                "yoy" = translate("YoY"),
                                "mom" = translate("MoM"),
                                "qoq" = translate("QoQ")),
          brand_col    = paste0(p, "_revenue_pct"),
          category_col = paste0("category_", p, "_pct"),
          excess_col   = paste0("excess_growth_", p)
        )
      })

      # ---- Latest month with non-NA values ----
      latest_row <- reactive({
        df <- growth_data()
        cols <- period_cols()
        if (is.null(df)) return(NULL)
        valid <- !is.na(df[[cols$brand_col]]) & !is.na(df[[cols$category_col]])
        if (!any(valid)) return(NULL)
        df[max(which(valid)), , drop = FALSE]
      })

      # ---- KPI 1: Brand growth rate ----
      output$kpi_brand <- renderUI({
        row <- latest_row()
        cols <- period_cols()
        if (is.null(row)) {
          return(bs4ValueBox(
            value = "-",
            subtitle = paste0(translate("Brand Growth Rate"), " (", cols$period_label, ")"),
            icon = icon("chart-line"),
            color = "primary",
            footer = translate("Need >= 24 months of historical data"),
            width = 12
          ))
        }
        val <- row[[cols$brand_col]]
        bs4ValueBox(
          value = sprintf("%+.1f%%", val),
          subtitle = paste0(translate("Brand Growth Rate"), " (", cols$period_label, ")"),
          icon = icon("chart-line"),
          color = "primary",
          footer = paste(translate("Latest month"), row$year_month),
          width = 12
        )
      })

      # ---- KPI 2: Category average growth rate ----
      output$kpi_category <- renderUI({
        row <- latest_row()
        cols <- period_cols()
        if (is.null(row)) {
          return(bs4ValueBox(
            value = "-",
            subtitle = paste0(translate("Category Average Growth"), " (", cols$period_label, ")"),
            icon = icon("layer-group"),
            color = "secondary",
            footer = translate("Need >= 24 months of historical data"),
            width = 12
          ))
        }
        val <- row[[cols$category_col]]
        bs4ValueBox(
          value = sprintf("%+.1f%%", val),
          subtitle = paste0(translate("Category Average Growth"), " (", cols$period_label, ")"),
          icon = icon("layer-group"),
          color = "secondary",
          footer = paste(translate("Latest month"), row$year_month),
          width = 12
        )
      })

      # ---- KPI 3: Excess growth rate (color-coded) ----
      output$kpi_excess <- renderUI({
        row <- latest_row()
        cols <- period_cols()
        if (is.null(row)) {
          return(bs4ValueBox(
            value = "-",
            subtitle = paste0(translate("Excess Growth Rate"), " (", cols$period_label, ")"),
            icon = icon("trophy"),
            color = "info",
            footer = translate("Need >= 24 months of historical data"),
            width = 12
          ))
        }
        val <- row[[cols$excess_col]]
        # 差值 (pp) coloring: green if positive, red if negative
        color_class <- if (is.na(val)) "info" else if (val >= 0) "success" else "danger"
        bs4ValueBox(
          value = sprintf("%+.1fpp", val),
          subtitle = paste0(translate("Excess Growth Rate"), " (", cols$period_label, ")"),
          icon = icon(if (is.na(val) || val >= 0) "trophy" else "exclamation-triangle"),
          color = color_class,
          footer = paste(translate("Brand minus Category"), "|", row$year_month),
          width = 12
        )
      })

      # ---- Time-trend chart: excess_growth over months ----
      output$excess_trend_chart <- plotly::renderPlotly({
        df <- growth_data()
        cols <- period_cols()
        if (is.null(df)) {
          return(plotly::plot_ly() %>%
            plotly::layout(title = translate("Need >= 24 months of historical data")))
        }
        excess_vec <- df[[cols$excess_col]]
        valid <- !is.na(excess_vec)
        if (sum(valid) < 2) {
          return(plotly::plot_ly() %>%
            plotly::layout(title = translate("Need >= 24 months of historical data")))
        }
        plot_df <- df[valid, , drop = FALSE]
        excess_v <- plot_df[[cols$excess_col]]
        # Color each point: green for positive, red for negative
        colors <- ifelse(excess_v >= 0, "#28a745", "#dc3545")

        plotly::plot_ly(
          plot_df,
          x = ~year_month,
          y = excess_v,
          type = "bar",
          marker = list(color = colors),
          text = sprintf("%+.1fpp", excess_v),
          textposition = "auto"
        ) %>%
          plotly::layout(
            title = paste(translate("Excess Growth Trend"), "(", cols$period_label, ")"),
            xaxis = list(title = translate("Month"), tickangle = -45),
            yaxis = list(title = paste0(translate("Excess Growth Rate"), " (pp)"),
                         zeroline = TRUE, zerolinecolor = "#888", zerolinewidth = 2),
            shapes = list(
              list(type = "line", x0 = 0, x1 = 1, xref = "paper",
                   y0 = 0, y1 = 0, line = list(color = "#888", dash = "dash"))
            ),
            margin = list(b = 100)
          )
      })

      # ---- Detail table ----
      output$detail_table <- DT::renderDataTable({
        df <- growth_data()
        cols <- period_cols()
        if (is.null(df)) {
          return(DT::datatable(data.frame(
            Message = translate("Need >= 24 months of historical data")
          )))
        }
        show_df <- data.frame(
          Month = df$year_month,
          Brand_Revenue = round(df$total_revenue, 0),
          Category_Revenue = round(df$category_revenue, 0),
          Brand_Pct = round(df[[cols$brand_col]], 2),
          Category_Pct = round(df[[cols$category_col]], 2),
          Excess_pp = round(df[[cols$excess_col]], 2),
          stringsAsFactors = FALSE
        )
        show_df <- show_df[order(-as.integer(gsub("-", "", show_df$Month))), ]
        names(show_df) <- c(
          translate("Month"),
          translate("Brand Revenue"),
          translate("Category Revenue"),
          paste0(translate("Brand"), " ", cols$period_label, " (%)"),
          paste0(translate("Category"), " ", cols$period_label, " (%)"),
          paste0(translate("Excess"), " (pp)")
        )
        DT::datatable(
          show_df,
          filter = "top",
          rownames = FALSE,
          options = list(
            pageLength = 12,
            scrollX = TRUE,
            dom = "lftip",
            language = list(url = "//cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json")
          )
        )
      })

      # ---- AI Insight ----
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "vitalsigns_analysis.growth_insights",
        get_template_vars = function() {
          df <- growth_data()
          cols <- period_cols()
          if (is.null(df)) return(NULL)
          row <- latest_row()
          if (is.null(row)) return(NULL)

          cfg <- comp_config()
          pl <- cfg$filters$product_line_id_sliced
          filter_context_str <- paste0(
            "Analysis scope:\n",
            "- Platform: ", cfg$filters$platform_id, "\n",
            "- Product line: ", if (!is.null(pl) && pl != "all") pl else "All", "\n",
            "- Period: ", cols$period_label
          )

          excess_vec <- df[[cols$excess_col]]
          valid <- !is.na(excess_vec)
          months_outperform <- sum(excess_vec[valid] >= 0)
          months_underperform <- sum(excess_vec[valid] < 0)

          list(
            filter_context = filter_context_str,
            latest_month = row$year_month,
            brand_pct = sprintf("%+.2f%%", row[[cols$brand_col]]),
            category_pct = sprintf("%+.2f%%", row[[cols$category_col]]),
            excess_pp = sprintf("%+.2fpp", row[[cols$excess_col]]),
            outperform_summary = sprintf(
              "Months outperforming category: %d / %d valid months",
              months_outperform, sum(valid)
            ),
            underperform_count = as.character(months_underperform)
          )
        },
        component_label = "growthValidation"
      )

      # ---- CSV download (DEV_R051: BOM + write.table for Excel) ----
      output$download_csv <- downloadHandler(
        filename = function() paste0("growth_validation_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- growth_data()
          if (!is.null(df)) {
            con <- file(file, "wb")
            writeBin(charToRaw("\xef\xbb\xbf"), con)
            close(con)
            utils::write.table(df, file, row.names = FALSE, sep = ",",
                               quote = TRUE, append = TRUE, fileEncoding = "UTF-8")
          }
        }
      )

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
