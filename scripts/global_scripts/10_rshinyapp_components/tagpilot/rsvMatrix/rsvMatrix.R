# =============================================================================
# rsvMatrix.R — RSV Life-Force Matrix Component
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001, UX_P002
# =============================================================================

rsvMatrixComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # UX_P002: Read pre-computed RSV classification table (Tier 1)
  # No longer source fn_rsv_classification.R at runtime.

  # ---- UI ----
  ui_filter <- tagList()

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(4, uiOutput(ns("kpi_high_risk"))),
      column(4, uiOutput(ns("kpi_high_stable"))),
      column(4, uiOutput(ns("kpi_high_value")))
    ),
    # Charts Row
    fluidRow(
      column(4, bs4Card(title = translate("Risk Distribution"), status = "danger",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_risk"), height = "280px"))),
      column(4, bs4Card(title = translate("Stability Distribution"), status = "success",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_stability"), height = "280px"))),
      column(4, bs4Card(title = translate("Value Distribution"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("pie_value"), height = "280px")))
    ),
    # Heatmap
    fluidRow(
      column(12, bs4Card(title = translate("R\u00d7S Heatmap (color = V)"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("heatmap_rs"), height = "350px")))
    ),
    # Customer Detail Table
    fluidRow(
      column(12, bs4Card(title = translate("Customer Detail"), status = "primary",
        width = 12, solidHeader = TRUE,
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
          if (nrow(df) == 0) { message("[rsvMatrix] No pre-computed data for platform='", cfg$filters$platform_id, "', product_line='", pl_id, "'"); return(NULL) }
          message("[rsvMatrix] Loaded ", nrow(df), " pre-computed records")
          df
        }, error = function(e) {
          message("[rsvMatrix] Data load error: ", e$message)
          NULL
        })
      })

      # Empty state helper
      empty_box <- function(title, bg) {
        renderUI(bs4ValueBox(value = "-", subtitle = title, icon = icon("question"),
                             color = bg, width = 12))
      }

      # KPI boxes
      output$kpi_high_risk <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = translate("No Data"), subtitle = translate("High Risk Customers"),
                                            icon = icon("exclamation-triangle"), color = "danger", width = 12))
        n_high <- sum(df$r_level == "High", na.rm = TRUE)
        pct <- round(n_high / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(n_high, " (", pct, "%)"),
                    subtitle = translate("High Risk Customers"),
                    icon = icon("exclamation-triangle"), color = "danger", width = 12)
      })

      output$kpi_high_stable <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = translate("No Data"), subtitle = translate("High Stability Customers"),
                                            icon = icon("shield-alt"), color = "success", width = 12))
        n_high <- sum(df$s_level == "High", na.rm = TRUE)
        pct <- round(n_high / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(n_high, " (", pct, "%)"),
                    subtitle = translate("High Stability Customers"),
                    icon = icon("shield-alt"), color = "success", width = 12)
      })

      output$kpi_high_value <- renderUI({
        df <- classified_data()
        if (is.null(df)) return(bs4ValueBox(value = translate("No Data"), subtitle = translate("High Value Customers"),
                                            icon = icon("gem"), color = "info", width = 12))
        n_high <- sum(df$v_level == "High", na.rm = TRUE)
        pct <- round(n_high / nrow(df) * 100, 1)
        bs4ValueBox(value = paste0(n_high, " (", pct, "%)"),
                    subtitle = translate("High Value Customers"),
                    icon = icon("gem"), color = "info", width = 12)
      })

      # Pie charts
      render_pie <- function(col, colors) {
        renderPlotly({
          df <- classified_data()
          if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
          tbl <- as.data.frame(table(df[[col]]), stringsAsFactors = FALSE)
          names(tbl) <- c("level", "count")
          lvl_order <- c("High", "Mid", "Low")
          tbl$level <- factor(tbl$level, levels = lvl_order)
          tbl <- tbl[order(tbl$level), ]
          tbl$label <- sapply(as.character(tbl$level), translate)
          plotly::plot_ly(tbl, labels = ~label, values = ~count, type = "pie",
                          marker = list(colors = colors),
                          textinfo = "label+percent") %>%
            plotly::layout(showlegend = TRUE, margin = list(t = 10, b = 10))
        })
      }

      output$pie_risk      <- render_pie("r_level", c("#dc3545", "#ffc107", "#28a745"))
      output$pie_stability <- render_pie("s_level", c("#28a745", "#ffc107", "#dc3545"))
      output$pie_value     <- render_pie("v_level", c("#007bff", "#6c757d", "#adb5bd"))

      # R x S Heatmap
      output$heatmap_rs <- renderPlotly({
        df <- classified_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))

        agg <- stats::aggregate(
          cbind(avg_v = clv_value, count = customer_id) ~ r_level + s_level,
          data = df, FUN = function(x) if (is.numeric(x)) mean(x, na.rm = TRUE) else length(x)
        )
        # Rebuild with proper aggregation
        agg <- stats::aggregate(clv_value ~ r_level + s_level, data = df, FUN = mean, na.rm = TRUE)
        cnt <- stats::aggregate(customer_id ~ r_level + s_level, data = df, FUN = length)
        names(agg)[3] <- "avg_clv"
        names(cnt)[3] <- "count"
        agg$count <- cnt$count[match(paste(agg$r_level, agg$s_level), paste(cnt$r_level, cnt$s_level))]

        r_lvls <- c("Low", "Mid", "High")
        s_lvls <- c("Low", "Mid", "High")
        r_labels <- sapply(r_lvls, translate)
        s_labels <- sapply(s_lvls, translate)

        mat <- matrix(NA, nrow = 3, ncol = 3, dimnames = list(r_lvls, s_lvls))
        txt <- matrix("", nrow = 3, ncol = 3, dimnames = list(r_lvls, s_lvls))
        for (i in seq_len(nrow(agg))) {
          ri <- agg$r_level[i]; si <- agg$s_level[i]
          if (ri %in% r_lvls && si %in% s_lvls) {
            mat[ri, si] <- round(agg$avg_clv[i], 0)
            txt[ri, si] <- paste0("N=", agg$count[i], "\nCLV=", round(agg$avg_clv[i], 0))
          }
        }

        plotly::plot_ly(x = s_labels, y = r_labels, z = mat, type = "heatmap",
                        text = txt, hoverinfo = "text",
                        colorscale = list(c(0, "#f0f9e8"), c(0.5, "#7bccc4"), c(1, "#084081"))) %>%
          plotly::layout(
            xaxis = list(title = translate("Stability")),
            yaxis = list(title = translate("Risk")),
            margin = list(t = 30, b = 50)
          )
      })

      # Detail table
      output$detail_table <- DT::renderDataTable({
        df <- classified_data()
        if (is.null(df)) return(DT::datatable(data.frame(Message = translate("Please run ETL pipeline first"))))

        show_df <- data.frame(
          ID         = df$customer_id,
          Risk       = sapply(df$r_level, translate),
          Stability  = sapply(df$s_level, translate),
          Value      = sapply(df$v_level, translate),
          Strategy   = sapply(df$rsv_action, translate),
          CLV        = round(df$clv_value, 0),
          RFM        = df$rfm_score,
          stringsAsFactors = FALSE
        )

        DT::datatable(show_df,
          colnames = unname(sapply(names(show_df), translate)),
          filter = "top", rownames = FALSE,
          options = list(pageLength = 15, scrollX = TRUE, dom = "lftip",
                         language = list(url = "//cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json")))
      })

      # CSV download
      output$download_csv <- downloadHandler(
        filename = function() paste0("rsv_matrix_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- classified_data()
          if (!is.null(df)) {
            export <- data.frame(
              customer_id = df$customer_id,
              r_level = df$r_level, s_level = df$s_level, v_level = df$v_level,
              rsv_key = df$rsv_key, customer_type = df$customer_type,
              rsv_action = df$rsv_action, clv = df$clv_value, rfm_score = df$rfm_score,
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
