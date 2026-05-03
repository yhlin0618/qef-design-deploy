# ============================================================
# periodComparisonServer.R  
# Period Comparison Server Component for MAMBA
# Created: 2025-09-23
# Principle: R09 (UI-Server-Defaults Triple), R92 (Universal DBI)
# ============================================================

#' Period Comparison Server Module
#' 
#' @param id Module namespace ID
#' @param db_connection Database connection object
#' @param data_source Name of the data table/view
#' @param metrics Character vector of metrics to compare
#' @param group_by Optional grouping variable
#' 
#' @description
#' Server logic for period comparison functionality.
#' Implements rolling window calculations and period-over-period analysis.
#' 
#' @export
periodComparisonServer <- function(id, db_connection, data_source, 
                                   metrics = c("revenue", "customers", "orders"),
                                   group_by = NULL) {
  
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    # Reactive: Filtered data based on date range
    filtered_data <- reactive({
      req(input$date_range)
      
      # Use tbl2 for universal data access (R92)
      data <- tbl2(db_connection, data_source) %>%
        filter(
          date >= !!input$date_range[1],
          date <= !!input$date_range[2]
        )
      
      # Apply grouping if specified
      if (!is.null(group_by)) {
        data <- data %>% group_by(!!!syms(group_by))
      }
      
      data %>% collect()
    })
    
    # Reactive: Period aggregation
    period_data <- reactive({
      req(filtered_data())
      
      data <- filtered_data()
      
      # Aggregate based on period type
      aggregated <- switch(input$period_type,
        "daily" = data %>%
          mutate(period = as.Date(date)) %>%
          group_by(period),
        
        "weekly" = data %>%
          mutate(period = floor_date(date, "week")) %>%
          group_by(period),
        
        "monthly" = data %>%
          mutate(period = floor_date(date, "month")) %>%
          group_by(period),
        
        "quarterly" = data %>%
          mutate(period = floor_date(date, "quarter")) %>%
          group_by(period),
        
        "yearly" = data %>%
          mutate(period = floor_date(date, "year")) %>%
          group_by(period)
      )
      
      # Summarize metrics
      summarized <- aggregated %>%
        summarise(
          across(all_of(metrics), list(
            total = ~sum(., na.rm = TRUE),
            avg = ~mean(., na.rm = TRUE),
            count = ~n()
          )),
          .groups = "drop"
        ) %>%
        arrange(period)
      
      summarized
    })
    
    # Reactive: Comparison calculation
    comparison_data <- eventReactive(input$apply_comparison, {
      req(period_data())
      
      data <- period_data()
      
      # Calculate based on comparison type
      if (input$comparison_type == "period_over_period") {
        # 環比: Compare with previous period
        periods_back <- 1
      } else if (input$comparison_type == "year_over_year") {
        # 同比: Compare with same period last year
        periods_back <- switch(input$period_type,
          "daily" = 365,
          "weekly" = 52,
          "monthly" = 12,
          "quarterly" = 4,
          "yearly" = 1
        )
      } else {
        # Custom comparison
        periods_back <- input$periods_back
      }
      
      # Calculate differences and rates
      result <- data
      for (metric in paste0(metrics, "_total")) {
        result <- result %>%
          mutate(
            !!paste0(metric, "_prev") := lag(.data[[metric]], n = periods_back),
            !!paste0(metric, "_diff") := .data[[metric]] - lag(.data[[metric]], n = periods_back),
            !!paste0(metric, "_rate") := ifelse(
              lag(.data[[metric]], n = periods_back) == 0,
              NA,
              (.data[[metric]] - lag(.data[[metric]], n = periods_back)) / 
                lag(.data[[metric]], n = periods_back)
            )
          )
      }
      
      result
    })
    
    # Output: Current period card
    output$current_period_card <- renderUI({
      req(comparison_data())
      
      latest <- tail(comparison_data(), 1)
      
      div(class = "info-box bg-primary",
        span(class = "info-box-icon", icon("calendar")),
        div(class = "info-box-content",
          span(class = "info-box-text", "當期數據"),
          span(class = "info-box-number", 
            format(latest$period, "%Y-%m-%d")
          ),
          div(class = "progress",
            div(class = "progress-bar", style = "width: 100%")
          ),
          span(class = "progress-description",
            paste("總營收:", 
              format(latest[[paste0(metrics[1], "_total")]], 
                big.mark = ",", scientific = FALSE)
            )
          )
        )
      )
    })
    
    # Output: Previous period card
    output$previous_period_card <- renderUI({
      req(comparison_data())
      
      latest <- tail(comparison_data(), 1)
      prev_col <- paste0(metrics[1], "_prev")
      
      if (is.na(latest[[prev_col]])) {
        return(div(class = "info-box bg-secondary",
          span(class = "info-box-icon", icon("history")),
          div(class = "info-box-content",
            span(class = "info-box-text", "上期數據"),
            span(class = "info-box-number", "N/A"),
            span(class = "progress-description", "無歷史資料")
          )
        ))
      }
      
      div(class = "info-box bg-info",
        span(class = "info-box-icon", icon("history")),
        div(class = "info-box-content",
          span(class = "info-box-text", "上期數據"),
          span(class = "info-box-number",
            format(latest[[prev_col]], big.mark = ",", scientific = FALSE)
          ),
          div(class = "progress",
            div(class = "progress-bar", style = "width: 100%")
          ),
          span(class = "progress-description",
            paste("上期營收:", 
              format(latest[[prev_col]], big.mark = ",", scientific = FALSE)
            )
          )
        )
      )
    })
    
    # Output: Change card
    output$change_card <- renderUI({
      req(comparison_data())
      
      latest <- tail(comparison_data(), 1)
      rate_col <- paste0(metrics[1], "_rate")
      diff_col <- paste0(metrics[1], "_diff")
      
      if (is.na(latest[[rate_col]])) {
        return(div(class = "info-box bg-secondary",
          span(class = "info-box-icon", icon("chart-line")),
          div(class = "info-box-content",
            span(class = "info-box-text", "變化率"),
            span(class = "info-box-number", "N/A"),
            span(class = "progress-description", "無法計算")
          )
        ))
      }
      
      change_pct <- latest[[rate_col]] * 100
      change_val <- latest[[diff_col]]
      
      # Determine color and icon based on change
      if (change_pct > 0) {
        box_class <- "bg-success"
        trend_icon <- icon("arrow-up")
        trend_class <- "trend-up"
      } else if (change_pct < 0) {
        box_class <- "bg-danger"
        trend_icon <- icon("arrow-down")
        trend_class <- "trend-down"
      } else {
        box_class <- "bg-warning"
        trend_icon <- icon("minus")
        trend_class <- "trend-neutral"
      }
      
      div(class = paste("info-box", box_class),
        span(class = "info-box-icon", trend_icon),
        div(class = "info-box-content",
          span(class = "info-box-text", "變化率"),
          span(class = paste("info-box-number", trend_class),
            sprintf("%+.1f%%", change_pct)
          ),
          div(class = "progress",
            div(class = "progress-bar", 
              style = sprintf("width: %s%%", min(abs(change_pct), 100))
            )
          ),
          span(class = "progress-description",
            paste("差異值:", 
              format(change_val, big.mark = ",", scientific = FALSE)
            )
          )
        )
      )
    })
    
    # Output: Comparison table
    output$comparison_table <- DT::renderDataTable({
      req(comparison_data())
      
      # Select relevant columns for display
      display_cols <- c("period")
      for (metric in metrics) {
        display_cols <- c(display_cols,
          paste0(metric, "_total"),
          paste0(metric, "_prev"),
          paste0(metric, "_diff"),
          paste0(metric, "_rate")
        )
      }
      
      # Filter to existing columns
      display_cols <- intersect(display_cols, names(comparison_data()))
      
      data_display <- comparison_data() %>%
        select(all_of(display_cols)) %>%
        filter(!is.na(period))
      
      # Create column names in Chinese
      col_names <- c("時間段")
      for (metric in metrics) {
        metric_zh <- switch(metric,
          "revenue" = "營收",
          "customers" = "客戶",
          "orders" = "訂單",
          metric
        )
        if (paste0(metric, "_total") %in% display_cols) {
          col_names <- c(col_names, paste0(metric_zh, "(當期)"))
        }
        if (paste0(metric, "_prev") %in% display_cols) {
          col_names <- c(col_names, paste0(metric_zh, "(上期)"))
        }
        if (paste0(metric, "_diff") %in% display_cols) {
          col_names <- c(col_names, paste0(metric_zh, "(差異)"))
        }
        if (paste0(metric, "_rate") %in% display_cols) {
          col_names <- c(col_names, paste0(metric_zh, "(變化%)"))
        }
      }
      
      # Format percentages
      rate_cols <- grep("_rate$", display_cols, value = TRUE)
      for (col in rate_cols) {
        data_display[[col]] <- sprintf("%.1f%%", data_display[[col]] * 100)
      }
      
      DT::datatable(
        data_display,
        colnames = col_names,
        options = list(
          pageLength = 10,
          scrollX = TRUE,
          language = list(
            url = "//cdn.datatables.net/plug-ins/1.10.25/i18n/Chinese-traditional.json"
          )
        ),
        rownames = FALSE
      ) %>%
      DT::formatRound(
        columns = grep("_(total|prev|diff)$", display_cols),
        digits = 0
      )
    })
    
    # Output: Trend chart
    output$trend_chart <- renderPlotly({
      req(comparison_data())
      
      # Create plotly chart
      p <- plot_ly()
      
      # Add traces for each metric
      colors <- c("#007bff", "#28a745", "#ffc107")
      for (i in seq_along(metrics)) {
        metric <- metrics[i]
        metric_col <- paste0(metric, "_total")
        
        if (metric_col %in% names(comparison_data())) {
          metric_zh <- switch(metric,
            "revenue" = "營收",
            "customers" = "客戶數",
            "orders" = "訂單數",
            metric
          )
          
          p <- p %>%
            add_trace(
              data = comparison_data(),
              x = ~period,
              y = ~get(metric_col),
              name = metric_zh,
              type = "scatter",
              mode = "lines+markers",
              line = list(color = colors[i]),
              marker = list(color = colors[i], size = 8)
            )
        }
      }
      
      # Layout configuration
      p <- p %>%
        layout(
          title = "時間段趨勢圖",
          xaxis = list(
            title = "時間",
            type = "date"
          ),
          yaxis = list(
            title = "數值",
            rangemode = "tozero"
          ),
          hovermode = "x unified",
          legend = list(
            orientation = "h",
            y = -0.2
          )
        )
      
      p
    })
    
    # Return comparison data for external use
    return(comparison_data)
  })
}