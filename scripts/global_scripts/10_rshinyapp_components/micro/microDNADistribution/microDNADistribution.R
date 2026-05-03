#LOCK FILE
#
# microDNADistribution.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP55: Computation Allocation Principle (pre-computation)
# - MP73: Interactive Visualization Preference (plotly for visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R91: Universal Data Access Pattern (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - P006: Data Visualization (visualization standards)
#

# LOCK FILE
#
# 更新紀錄：
#   • 2026-01-26: D01_07 預計算優化 - 近即時載入 (<100ms)
#     - 新增 load_precomputed_* 函數讀取 D01_07 產生的預計算表
#     - 優先級: 預計算 > SQL 聚合 > R 計算
#     - 載入時間: ~2-5秒 → <100ms
#     - 相關原則: MP055, MP064, DEV_R038
#   • 2025-05-13: 增強頻率（f_value）的長條圖顯示，確保整數順序從 1,2,3... (P006)
#     - 修改 bar_plot() 函數處理數值型類別順序
#     - 新增 frequency 直方圖顯示整數級別順序從 1 到最大值
#   • 2025-05-12: 修正 platform_id 處理，使用字串型式（如 "all", "amz"）
#     (注意：Amazon ID 已變更為 "amz"，不再使用舊的數值或字串 "2")
#   • 拔除 MP55「預先計算」邏輯 → 不再有 input$use_precomputed
#   • 空資料改用 plotly::plotly_empty()，避免連續警告
#   • 改用 tbl2 取代 universal_data_accessor (R116原則)
#   • 保留 Connected Component 架構
# -----------------------------------------------------------------------------

# helper ----------------------------------------------------------------------
#' Paste operator for string concatenation
#' @param x Character string. First string to concatenate.
#' @param y Character string. Second string to concatenate.
#' @return Character string. The concatenated result of x and y.
`%+%` <- function(x, y) paste0(x, y)

#' NULL coalescing operator
#' @param x Any value. The value to use if not NULL.
#' @param y Any value. The fallback value to use if x is NULL.
#' @return Either x or y. Returns x if it's not NULL, otherwise returns y.
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Safe row count function
#' @param x Object. The object to count rows for.
#' @return Numeric. The number of rows in x, or 0 if x is not a data frame or has no rows.
#' This function safely handles NULL values, non-data frame objects, and edge cases.
nrow2 <- function(x) {
  if (is.null(x)) return(0)
  if (!is.data.frame(x) && !is.matrix(x)) return(0)
  return(nrow(x))
}

# Filter UI -------------------------------------------------------------------
#' microDNADistributionFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the DNA distribution component.
microDNADistributionFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  wellPanel(
    style = "padding:15px;",
    h4(translate("Metric Selection")),
    actionButton(ns("m_ecdf"),  translate("Purchase Amount (M)"), class = "btn-block btn-info mb-2"),
    actionButton(ns("r_ecdf"),  translate("Recency (R)"),          class = "btn-block btn-info mb-2"),
    actionButton(ns("f_ecdf"),  translate("Frequency (F)"),        class = "btn-block btn-info mb-2"),
    actionButton(ns("ipt_ecdf"),translate("Inter‑purchase Time"),   class = "btn-block btn-info mb-2"),
    hr(), h4(translate("Visualization Type")),
    actionButton(ns("f_barplot"),  translate("Frequency Histogram"), class = "btn-block btn-secondary mb-2"),
    actionButton(ns("nes_barplot"),translate("NES Distribution"),    class = "btn-block btn-secondary mb-2"),
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' microDNADistributionDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @return shiny.tag. A Shiny UI component containing the display elements for the DNA distribution visualizations.
microDNADistributionDisplayUI <- function(id) {
  ns <- NS(id)
  tagList(
    div(class = "component-header mb-3 text-center",
        h3("Customer DNA Distribution Analysis"),
        p("Visualize key customer metrics distribution patterns")),
    div(class = "component-output",
        plotlyOutput(ns("dna_distribution_plot"), height = "450px"),
        uiOutput(ns("metric_stats")))
  )
}

# Server ----------------------------------------------------------------------
#' microDNADistributionServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data:
#'        - current_visualization: reactive value indicating the current visualization type
#'        - component_status: reactive value indicating the current status (idle, loading, ready, etc.)
#'        - df_dna_by_customer: reactive containing the fetched customer DNA data
microDNADistributionServer <- function(id, app_data_connection = NULL, config = NULL,
                                       session = getDefaultReactiveDomain()) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ 狀態 ---------------------------------------------
    #' Reactive value for tracking current visualization
    #' @description Tracks which visualization is currently active
    #' @return Character string. The ID of the current visualization button
    current_visualization <- reactiveVal("none")
    
    #' Reactive value for tracking component status
    #' @description Tracks the current status of the component (loading, ready, etc.)
    #' @return Character string. One of "idle", "loading", "ready", "computing", or "error"
    component_status      <- reactiveVal("idle")
    
    # ------------ 解析 platform_id ----------------------------------
    #' Extract platform_id from configuration
    #' 
    #' Parses the provided configuration (reactive or static) to extract platform_id.
    #' Handles both direct platform_id and nested platform_id in filters.
    #' Platform IDs are maintained as character strings for consistency with production environment.
    #' 
    #' @return Character or NULL. The extracted platform_id or NULL if not found.
    platform_id <- reactive({
      tryCatch({
        # Safely handle all config types
        if (is.null(config)) {
          # Handle null config
          return(NULL)
        } else if (is.function(config)) {
          # Only try to call a reactive function if we're in a reactive context
          if (shiny::is.reactivevalues(config) || 
              shiny::is.reactive(config) || 
              "reactive" %in% class(config)) {
            # Get config value safely (in reactive context)
            cfg <- config()
          } else {
            # Non-reactive function
            cfg <- config
          }
        } else {
          # Static config (list or other value)
          cfg <- config
        }
        
        # Extract platform_id from config if available
        if (!is.null(cfg)) {
          # Check for direct platform_id
          if (!is.null(cfg[["platform_id"]])) {
            # Ensure platform_id is a character string
            return(as.character(cfg[["platform_id"]]))
          }
          # Check for nested platform_id in filters
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
            # Ensure platform_id is a character string
            return(as.character(cfg[["filters"]][["platform_id"]]))
          }
        }
        
        # Return NULL if no platform_id found
        NULL
      }, error = function(e) {
        warning("Error extracting platform_id from config: ", e$message)
        NULL
      })
    })

    product_line_id <- reactive({
      tryCatch({
        cfg <- NULL

        if (!is.null(config)) {
          if (is.function(config)) {
            cfg <- config()
          } else if (shiny::is.reactivevalues(config) ||
              shiny::is.reactive(config) ||
              "reactive" %in% class(config)) {
            cfg <- config()
          } else {
            cfg <- config
          }
        }

        if (!is.null(cfg)) {
          # Preferred runtime filter key for sliced DRV outputs
          if (!is.null(cfg[["filters"]]) &&
              !is.null(cfg[["filters"]][["product_line_id_sliced"]]) &&
              nzchar(as.character(cfg[["filters"]][["product_line_id_sliced"]])[1])) {
            return(as.character(cfg[["filters"]][["product_line_id_sliced"]])[1])
          }

          # DM_R058 v2: fallback to product_line_id_chosen (UI selection)
          if (!is.null(cfg[["filters"]]) &&
              !is.null(cfg[["filters"]][["product_line_id_chosen"]]) &&
              nzchar(as.character(cfg[["filters"]][["product_line_id_chosen"]])[1])) {
            return(as.character(cfg[["filters"]][["product_line_id_chosen"]])[1])
          }
        }

        NULL
      }, error = function(e) {
        warning("Error extracting product_line_id from config: ", e$message)
        NULL
      })
    })
    
    active_tab <- reactive({
      cfg <- if (is.function(config)) config() else config
      cfg$active_tab %||% "dna"
    })
    
    # ------------ 資料存取 (R116) ------------------------------------
    #' Reactive data accessor for customer DNA data
    #'
    #' Retrieves customer DNA data from the database or data source using the Enhanced Data Access
    #' Pattern (R116). Applies platform filtering if specified in the configuration.
    #'
    #' PERFORMANCE OPTIMIZATION (2026-01-26):
    #' - Only select columns needed for visualization (m_value, r_value, f_value, ipt_mean, nes_status)
    #' - This reduces data transfer from ~50+ columns to only 5 columns
    #' - For 124k+ rows, this significantly reduces memory usage and load time
    #'
    #' Ensure cached data access helpers are available
    ensure_cached_access <- function() {
      if (!exists("load_dna_distribution_summary_cached", mode = "function")) {
        source("scripts/global_scripts/04_utils/fn_cached_data_access.R")
      }
    }

    #' Load raw metric values (single column) with caching
    get_metric_data <- function(metric) {
      if (!is.null(active_tab()) && active_tab() != "dna") {
        return(data.frame())
      }

      ensure_cached_access()
      plat <- platform_id()
      pl_id <- product_line_id() %||% "all"

      tryCatch({
        component_status("loading")
        if (exists("load_dna_distribution_summary_cached", mode = "function")) {
          res_data <- load_dna_distribution_summary_cached(app_data_connection, plat, metric, pl_id)
        } else {
          res <- tbl2(app_data_connection, "df_dna_by_customer")
          if (!is.null(plat) && !is.na(plat) && plat != "all") {
            res <- dplyr::filter(res, platform_id == !!plat)
          }
          if (!is.null(pl_id) && !is.na(pl_id) && pl_id != "all" &&
              "product_line_id_filter" %in% colnames(res)) {
            res <- dplyr::filter(res, product_line_id_filter == !!pl_id)
          }
          res_data <- res %>%
            dplyr::select(dplyr::all_of(c("customer_id", metric))) %>%
            collect()
        }
        component_status("ready")
        res_data
      }, error = function(e) {
        warning("Error fetching DNA data: ", e$message)
        component_status("error")
        data.frame()
      })
    }

    #' Load aggregated counts for categorical metrics (D01_07 pre-computed)
    get_category_counts <- function(field) {
      if (!is.null(active_tab()) && active_tab() != "dna") {
        return(data.frame(category = character(), count = integer()))
      }

      ensure_cached_access()
      plat <- platform_id()
      pl_id <- product_line_id() %||% "all"

      tryCatch({
        # Priority 1: Pre-computed data from D01_07
        if (exists("load_precomputed_category_counts_cached", mode = "function")) {
          load_precomputed_category_counts_cached(app_data_connection, plat, field, pl_id)
        } else if (exists("load_precomputed_category_counts", mode = "function")) {
          load_precomputed_category_counts(app_data_connection, plat, field, pl_id)
        # Priority 2: SQL-level aggregation
        } else if (exists("load_dna_category_counts_cached", mode = "function")) {
          load_dna_category_counts_cached(app_data_connection, plat, field, pl_id)
        } else {
          # Priority 3: Fallback to raw query
          res <- tbl2(app_data_connection, "df_dna_by_customer")
          if (!is.null(plat) && !is.na(plat) && plat != "all") {
            res <- dplyr::filter(res, platform_id == !!plat)
          }
          if (!is.null(pl_id) && !is.na(pl_id) && pl_id != "all" &&
              "product_line_id_filter" %in% colnames(res)) {
            res <- dplyr::filter(res, product_line_id_filter == !!pl_id)
          }
          res %>%
            dplyr::group_by(!!rlang::sym(field)) %>%
            dplyr::summarise(count = dplyr::n(), .groups = "drop") %>%
            dplyr::collect() %>%
            dplyr::rename(category = !!field)
        }
      }, error = function(e) {
        warning("Error fetching DNA category counts: ", e$message)
        data.frame(category = character(), count = integer())
      })
    }

    #' Load summary stats for a metric (D01_07 pre-computed)
    get_summary_stats <- function(metric) {
      if (!is.null(active_tab()) && active_tab() != "dna") {
        return(list(n = 0))
      }

      ensure_cached_access()
      plat <- platform_id()
      pl_id <- product_line_id() %||% "all"

      tryCatch({
        # Priority 1: Pre-computed data from D01_07
        if (exists("load_precomputed_summary_stats_cached", mode = "function")) {
          load_precomputed_summary_stats_cached(app_data_connection, plat, metric, pl_id)
        } else if (exists("load_precomputed_summary_stats", mode = "function")) {
          load_precomputed_summary_stats(app_data_connection, plat, metric, pl_id)
        # Priority 2: SQL-level computation
        } else if (exists("load_dna_summary_stats_cached", mode = "function")) {
          load_dna_summary_stats_cached(app_data_connection, plat, metric, pl_id)
        } else if (exists("load_dna_summary_stats", mode = "function")) {
          load_dna_summary_stats(app_data_connection, plat, metric, pl_id)
        } else {
          list(n = 0)
        }
      }, error = function(e) {
        warning("Error fetching DNA summary stats: ", e$message)
        list(n = 0)
      })
    }

    #' Load ECDF data (PERFORMANCE OPTIMIZED via D01_07 pre-computation)
    #'
    #' @description
    #' Reads pre-computed ECDF data from df_dna_plot_data (generated by D01_07).
    #' This provides near-instant data access (<100ms) compared to:
    #' - SQL-level computation (~500ms)
    #' - R-level computation (~2-5s for 124k rows)
    #'
    #' Falls back to SQL-level computation if pre-computed data unavailable.
    #'
    #' @param metric Character. The metric to get ECDF for.
    #' @return data.frame with columns x (values) and y (cumulative percentages).
    get_ecdf_data <- function(metric) {
      if (!is.null(active_tab()) && active_tab() != "dna") {
        return(data.frame(x = numeric(), y = numeric()))
      }

      ensure_cached_access()
      plat <- platform_id()
      pl_id <- product_line_id() %||% "all"

      tryCatch({
        component_status("loading")
        # Priority 1: Pre-computed data from D01_07 (fastest)
        if (exists("load_precomputed_ecdf_cached", mode = "function")) {
          result <- load_precomputed_ecdf_cached(app_data_connection, plat, metric, pl_id)
        } else if (exists("load_precomputed_ecdf", mode = "function")) {
          result <- load_precomputed_ecdf(app_data_connection, plat, metric, pl_id)
        # Priority 2: SQL-level computation (fallback)
        } else if (exists("load_ecdf_from_sql_cached", mode = "function")) {
          result <- load_ecdf_from_sql_cached(app_data_connection, plat, metric, 2000, pl_id)
        } else if (exists("load_ecdf_from_sql", mode = "function")) {
          result <- load_ecdf_from_sql(app_data_connection, plat, metric, 2000, pl_id)
        } else {
          # Priority 3: R-level computation (slowest fallback)
          dat <- get_metric_data(metric)
          if (is.null(dat) || nrow(dat) == 0 || !metric %in% names(dat)) {
            return(data.frame(x = numeric(), y = numeric()))
          }
          result <- compute_distribution(dat[[metric]])
          if (is.null(result)) {
            return(data.frame(x = numeric(), y = numeric()))
          }
          result <- data.frame(x = result$x, y = result$y)
        }
        component_status("ready")
        result
      }, error = function(e) {
        warning("Error fetching ECDF data: ", e$message)
        component_status("error")
        data.frame(x = numeric(), y = numeric())
      })
    }

    #' @return data.frame. A reactive data frame containing default DNA metric data.
    df_dna_by_customer <- reactive({
      get_metric_data("m_value")
    })
    
    # ------------ helper ------------------------------------------------------
    #' Compute distribution for ECDF plotting with downsampling optimization
    #'
    #' @description
    #' PERFORMANCE OPTIMIZATION (2026-01-26):
    #' When there are many unique values (e.g., monetary amounts with decimals),
    #' the ECDF plot can have thousands of points. This slows down:
    #' 1. plotly rendering in the browser
    #' 2. Data transfer to the client
    #'
    #' Solution: Downsample to max 2000 points using quantiles when needed.
    #' This preserves the visual shape of the ECDF while dramatically reducing
    #' the number of points. The visual difference is imperceptible.
    #'
    #' @param v Numeric vector. The values to compute distribution for.
    #' @param max_points Integer. Maximum number of points for the ECDF plot (default: 2000).
    #' @return data.frame with columns x (values), y (cumulative probabilities), and count (total count).
    compute_distribution <- function(v, max_points = 2000) {
      v <- v[!is.na(v)]; if (!length(v)) return(NULL)
      fn <- ecdf(v)
      x <- sort(unique(v))

      # Downsample if too many unique values
      # This preserves ECDF shape while reducing plotly rendering time
      if (length(x) > max_points) {
        # Use quantiles to get evenly distributed sample points
        x <- unique(quantile(v, probs = seq(0, 1, length.out = max_points), na.rm = TRUE))
      }

      data.frame(x = x, y = fn(x), count = length(v))
    }
    
    #' Calculate summary statistics for a numeric vector
    #' @param v Numeric vector. The values to calculate statistics for.
    #' @return List containing various statistics: n, mean, median, sd, min, max, q1, q3.
    calc_stats <- function(v) {
      v <- v[!is.na(v)]; if (!length(v)) return(list(n = 0))
      q <- quantile(v, c(.25, .75)); list(n = length(v), mean = mean(v), median = median(v), sd = sd(v),
                                          min = min(v), max = max(v), q1 = q[1], q3 = q[2])
    }
    
    #' Create a UI element showing statistics as a formatted box
    #' @param st List. Statistics list as returned by calc_stats().
    #' @param name Character string. The name of the metric being displayed.
    #' @return shiny.tag. A UI element showing the statistics in a formatted box.
    stats_box <- function(st, name) {
      if (is.null(st) || st[["n"]] == 0) return(p("No data available"))
      
      stats_list <- list(Count = st[["n"]])
      if (!is.null(st[["mean"]])) stats_list$Mean <- round(st[["mean"]], 2)
      if (!is.null(st[["median"]])) stats_list$Median <- round(st[["median"]], 2)
      if (!is.null(st[["sd"]])) stats_list$`Std Dev` <- round(st[["sd"]], 2)
      
      # Create UI elements using mapply instead of lapply to get both value and name
      stats_ui <- mapply(
        function(value, stat_name) {
          div(class = "stat-box", 
              h5(stat_name), 
              p(format(value, big.mark = ",")))
        },
        stats_list,
        names(stats_list),
        SIMPLIFY = FALSE
      )
      
      tagList(
        h4(name %+% " Summary Statistics"),
        div(class = "stats-container", stats_ui)
      )
    }
    
    #' Create a plotly ECDF plot from pre-computed ECDF data (OPTIMIZED)
    #'
    #' @description
    #' PERFORMANCE OPTIMIZATION (2026-01-26):
    #' Uses pre-computed ECDF data from SQL (via get_ecdf_data), avoiding
    #' the need to transfer and process all raw values in R.
    #'
    #' @param ecdf_data data.frame. Pre-computed ECDF with x and y columns.
    #' @param title Character string. The title for the plot.
    #' @return plotly object. A plotly visualization of the ECDF.
    ecdf_plot_optimized <- function(ecdf_data, title) {
      # Handle empty or invalid data
      if (is.null(ecdf_data) || nrow2(ecdf_data) == 0 || !all(c("x", "y") %in% names(ecdf_data))) {
        return(plotly::plotly_empty(type = "scatter") %>%
                 plotly::add_annotations(text = "No data available", showarrow = FALSE))
      }

      # Create the plot from pre-computed ECDF data
      plotly::plot_ly(ecdf_data, x = ~x, y = ~y, type = "scatter", mode = "lines",
                      line = list(color = "#1F77B4", width = 2), hoverinfo = "text",
                      text = ~paste0(title, ": ", format(x, big.mark = ","),
                                     "<br>Percentage: ", format(y * 100, digits = 2), "%")) %>%
        plotly::layout(title = list(text = title %+% " - CDF", font = list(size = 16)),
                       xaxis = list(title = title),
                       yaxis = list(title = "Cumulative %", tickformat = ".0%"))
    }

    #' Create a plotly ECDF plot for the given data (LEGACY - for backward compatibility)
    #' @param dat data.frame. The raw data containing the field to plot.
    #' @param field Character string. The field name in dat to visualize.
    #' @param title Character string. The title for the plot.
    #' @param btn Character string. The button ID that triggered this plot.
    #' @return plotly object. A plotly visualization of the ECDF.
    ecdf_plot <- function(dat, field, title, btn) {
      # Handle empty or invalid data
      if (is.null(dat) || nrow2(dat) == 0 || !field %in% names(dat)) {
        return(plotly::plotly_empty(type = "scatter") %>%
                 plotly::add_annotations(text = "No data available", showarrow = FALSE))
      }

      # Compute the distribution directly within the function
      dist_data <- compute_distribution(dat[[field]])

      # Handle empty distribution data
      if (is.null(dist_data) || nrow2(dist_data) == 0) {
        return(plotly::plotly_empty(type = "scatter") %>%
                 plotly::add_annotations(text = "No valid distribution data", showarrow = FALSE))
      }

      # Create the plot
      plotly::plot_ly(dist_data, x = ~x, y = ~y, type = "scatter", mode = "lines",
                      line = list(color = "#1F77B4", width = 2), hoverinfo = "text",
                      text = ~paste0(title, ": ", format(x, big.mark = ","),
                                     "<br>Percentage: ", format(y * 100, digits = 2), "%")) %>%
        plotly::layout(title = list(text = title %+% " - CDF", font = list(size = 16)),
                       xaxis = list(title = title),
                       yaxis = list(title = "Cumulative %", tickformat = ".0%"))
    }
    
    #' Create a plotly bar plot for the given data
    #' @param dat data.frame. The raw data containing the field to plot.
    #' @param field Character string. The field name in dat to visualize.
    #' @param title Character string. The title for the plot.
    #' @param levels Character vector. Optional levels for factor variables.
    #' @return plotly object. A plotly visualization of the bar chart.
    bar_plot <- function(dat, field, title, levels = NULL) {
      # Handle empty or invalid data
      if (is.null(dat) || nrow2(dat) == 0 || !field %in% names(dat)) {
        return(plotly::plotly_empty(type = "bar") %>%
                 plotly::add_annotations(text = "No data available", showarrow = FALSE))
      }
      
      # Create the table/counts based on the field
      if (!is.null(levels) && field %in% names(dat)) {
        # Use specified levels
        cnt <- table(factor(dat[[field]], levels = levels))
      } else {
        # Default table
        cnt <- table(dat[[field]])
      }
      
      # Handle empty counts
      if (is.null(cnt) || !length(cnt)) {
        return(plotly::plotly_empty(type = "bar") %>%
                 plotly::add_annotations(text = "No valid count data", showarrow = FALSE))
      }
      
      # Convert to data frame for plotting
      df <- data.frame(x = names(cnt), y = as.numeric(cnt))
      total <- sum(df[["y"]])
      
      # For frequency plots or other numeric x-values, ensure proper ordering
      # Check if field is "f_value" (frequency) or all x values are numeric
      is_numeric_field <- field == "f_value" || all(!is.na(suppressWarnings(as.numeric(df$x))))
      
      if (is_numeric_field) {
        # Convert to numeric and sort
        df$x_num <- as.numeric(as.character(df$x))
        df <- df[order(df$x_num), ]
        # Keep the original x as factor for plotting but with correct order
        df$x <- factor(df$x, levels = df$x[order(df$x_num)])
      }
      
      # Create the plot
      plotly::plot_ly(df, x = ~x, y = ~y, type = "bar", marker = list(color = "#1F77B4"), hoverinfo = "text",
                      text = ~paste0("Value: ", x, "<br>Count: ", format(y, big.mark = ","),
                                     "<br>Percentage: ", round(y / total * 100, 1), "%")) %>%
        plotly::layout(title = list(text = title, font = list(size = 16)),
                       xaxis = list(title = "Value", categoryorder = "array", 
                                    categoryarray = if(is_numeric_field) df$x else if(!is.null(levels)) levels else df$x),
                       yaxis = list(title = "Count"))
    }

    #' Create a plotly bar plot from aggregated counts
    #' @param cnt_df data.frame. Must contain category and count columns.
    #' @param title Character. The title for the plot.
    #' @param levels Character vector. Optional levels for ordering.
    #' @return plotly object.
    bar_plot_counts <- function(cnt_df, title, levels = NULL) {
      if (is.null(cnt_df) || nrow2(cnt_df) == 0 || !"category" %in% names(cnt_df)) {
        return(plotly::plotly_empty(type = "bar") %>%
                 plotly::add_annotations(text = "No data available", showarrow = FALSE))
      }

      df <- cnt_df
      names(df)[names(df) == "category"] <- "x"
      names(df)[names(df) == "count"] <- "y"
      total <- sum(df[["y"]], na.rm = TRUE)

      is_numeric_field <- all(!is.na(suppressWarnings(as.numeric(df$x))))
      if (is_numeric_field) {
        df$x_num <- as.numeric(as.character(df$x))
        df <- df[order(df$x_num), ]
        df$x <- factor(df$x, levels = df$x[order(df$x_num)])
      } else if (!is.null(levels)) {
        df$x <- factor(df$x, levels = levels)
      }

      plotly::plot_ly(
        df, x = ~x, y = ~y, type = "bar",
        marker = list(color = "#1F77B4"), hoverinfo = "text",
        text = ~paste0("Value: ", x, "<br>Count: ", format(y, big.mark = ","),
                       "<br>Percentage: ", round(y / total * 100, 1), "%")
      ) %>%
        plotly::layout(
          title = list(text = title, font = list(size = 16)),
          xaxis = list(title = "Value", categoryorder = "array",
                       categoryarray = if (is_numeric_field) df$x else if(!is.null(levels)) levels else df$x),
          yaxis = list(title = "Count")
        )
    }
    
    # ------------ 資料存取輔助 ----------------------------------------------
    #' Debug logging for data verification
    #' 
    #' Prints sample data for debugging and monitoring
    observe({
      # Safely get and print debug information with proper error handling
      tryCatch({
        # Get base data
        dat <- get_metric_data("m_value")
        
        # Check if we have valid data to display
        if (!is.null(dat) && is.data.frame(dat) && nrow(dat) > 0) {
          # Print sample data for debugging
          print(paste("Loaded DNA data with", nrow(dat), "records"))
          print(head(dat))
        } else {
          print("No DNA data available or empty dataset")
        }
      }, error = function(e) {
        # Log the error but don't break the app
        warning("Debug observer error: ", e$message)
      })
    })
    
    # ------------ observer 工具 ----------------------------------------------
    #' Create an observer for rendering a metric visualization
    #'
    #' Factory function that creates an observer for a specific metric button.
    #' When the button is clicked, this creates the appropriate ECDF visualization.
    #'
    #' PERFORMANCE OPTIMIZATION (2026-01-26):
    #' - Uses get_ecdf_data() which computes ECDF at SQL level (CUME_DIST window function)
    #' - Only transfers unique values + cumulative percentages (~5,000 rows)
    #' - Instead of transferring all raw values (~124,000 rows)
    #' - Expected speedup: 10-20x for large datasets
    #'
    #' @param field Character string. The field name in the data to visualize (e.g., "M", "R", "F").
    #' @param btn Character string. The input ID of the button that triggers this visualization.
    #' @param ttl Character string. The title to display for this visualization.
    #' @return Observer. A Shiny observer that handles visualization when the button is clicked.
    render_metric <- function(field, btn, ttl) {
      observeEvent(input[[btn]], {
        # Update current visualization tracking
        current_visualization(btn)

        # Get pre-computed ECDF data from SQL (optimized path)
        ecdf_data <- get_ecdf_data(field)

        # Render the visualization using optimized function
        output$dna_distribution_plot <- renderPlotly({
          if (!is.null(ecdf_data) && nrow2(ecdf_data) > 0 && all(c("x", "y") %in% names(ecdf_data))) {
            # Use optimized path with SQL-computed ECDF
            ecdf_plot_optimized(ecdf_data, ttl)
          } else {
            # Fallback to legacy path (load raw data, compute in R)
            dat <- get_metric_data(field)
            ecdf_plot(dat, field, ttl, btn)
          }
        })

        # Render statistics
        output$metric_stats <- renderUI({
          stats_box(get_summary_stats(field), ttl)
        })
      })
    }
    
    render_metric("m_value", "m_ecdf",  "Purchase Amount (M)")
    render_metric("r_value", "r_ecdf",  "Recency (R)")
    render_metric("f_value", "f_ecdf",  "Frequency (F)")
    render_metric("ipt_mean", "ipt_ecdf", "Inter‑purchase Time")
    
    # barplots
    observeEvent(input$f_barplot, {
      current_visualization("f_barplot")
      dat <- get_metric_data("f_value")
      cnt <- get_category_counts("f_value")
      
      output$dna_distribution_plot <- renderPlotly({
        # Use SQL-level aggregated counts when available
        if (!is.null(cnt) && nrow2(cnt) > 0 && "category" %in% names(cnt)) {
          max_f <- suppressWarnings(max(as.numeric(cnt$category), na.rm = TRUE))
          freq_levels <- if (is.finite(max_f)) as.character(1:min(max_f, 50)) else NULL
          bar_plot_counts(cnt, "Purchase Frequency Distribution", levels = freq_levels)
        } else {
          bar_plot(dat, "f_value", "Purchase Frequency Distribution")
        }
      })
      
      output$metric_stats <- renderUI({
        stats_box(get_summary_stats("f_value"), "Frequency (F)")
      })
    })
    
    observeEvent(input$nes_barplot, {
      current_visualization("nes_barplot")
      cnt <- get_category_counts("nes_status")
      
      output$dna_distribution_plot <- renderPlotly({
        if (!is.null(cnt) && nrow2(cnt) > 0 && "category" %in% names(cnt)) {
          bar_plot_counts(cnt, "NES Status Distribution", levels = c("N", "E0", "S1", "S2", "S3"))
        } else {
          dat <- get_metric_data("nes_status")
          bar_plot(dat, "nes_status", "NES Status Distribution", levels = c("N", "E0", "S1", "S2", "S3"))
        }
      })
      
      output$metric_stats <- renderUI({
        # Re-fetch data within renderUI scope (dat from renderPlotly is not accessible here)
        nes_cnt <- get_category_counts("nes_status")
        nes_dat <- get_metric_data("nes_status")
        has_data <- (!is.null(nes_cnt) && nrow2(nes_cnt) > 0) ||
                    (!is.null(nes_dat) && nrow2(nes_dat) > 0 && "nes_status" %in% names(nes_dat))
        if (!has_data) {
          p("No data available")
        } else {
          NULL  # No stats for NES distribution
        }
      })
    })
    
    output$component_status <- renderText({
      switch(component_status(),
             idle = "Select a metric", loading = "Loading data...",
             ready = "Ready", computing = "Computing...", error = "Error", component_status())
    })
    
    # Initialize with default visualization (Purchase Amount M)
    # Following UI_R008: Default Selection Conventions
    # Primary business metric shown by default to reduce user effort
    #
    # PERFORMANCE OPTIMIZATION (2026-01-26):
    # Uses SQL-computed ECDF for faster initial load
    observe({
      # Check if this is the initial load (no visualization selected yet)
      if (current_visualization() == "none") {
        # Get pre-computed ECDF data (optimized path)
        ecdf_data <- get_ecdf_data("m_value")

        # Render Purchase Amount (M) visualization by default
        if (!is.null(ecdf_data) && is.data.frame(ecdf_data) && nrow(ecdf_data) > 0) {
          # Update the current visualization state
          current_visualization("m_ecdf")

          # Render the default ECDF plot using optimized function
          output$dna_distribution_plot <- renderPlotly({
            if (all(c("x", "y") %in% names(ecdf_data))) {
              ecdf_plot_optimized(ecdf_data, "Purchase Amount (M)")
            } else {
              # Fallback to legacy path
              dat <- get_metric_data("m_value")
              ecdf_plot(dat, "m_value", "Purchase Amount (M)", "m_ecdf")
            }
          })

          # Render the statistics
          output$metric_stats <- renderUI({
            stats_box(get_summary_stats("m_value"), "Purchase Amount (M)")
          })
        } else {
          # No data available - show informative empty state
          output$dna_distribution_plot <- renderPlotly({
            plotly::plotly_empty(type = "scatter") %>%
              plotly::add_annotations(
                text = "No data available. Please select a metric.",
                showarrow = FALSE,
                font = list(size = 16, color = "#666666")
              )
          })
          output$metric_stats <- renderUI({
            p(style = "text-align: center; color: #666666;", "Waiting for data...")
          })
        }
      }
    })
    
    list(current_visualization = current_visualization,
         component_status      = component_status,
         df_dna_by_customer    = df_dna_by_customer)
  })
}

# Component wrapper -----------------------------------------------------------
#' microDNADistributionComponent
#' 
#' Implements a visualization component for customer DNA distributions (M, R, F, IPT, NES)
#' following the Connected Component principle and using plotly for visualizations.
#' 
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. The data connection supporting Enhanced Data Access pattern (R116).
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Configuration parameters for customizing component behavior (optional).
#'        If reactive, will be re-evaluated when dependencies change.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return A list containing UI and server functions structured according to the Connected Component Principle (MP56).
#'         The UI element contains 'filter' and 'display' components, and the server function initializes component functionality.
#' @examples
#' # Basic usage with default settings
#' dnaComponent <- microDNADistributionComponent("dna_viz")
#' 
#' # Usage with database connection
#' dnaComponent <- microDNADistributionComponent(
#'   id = "dna_viz",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")  # Using character ID for Amazon platform
#' )
#'
#' # Usage with file path (tbl2 enhanced access)
#' dnaComponent <- microDNADistributionComponent(
#'   id = "dna_viz",
#'   app_data_connection = "path/to/data.csv",
#'   config = list(filters = list(platform_id = "all"))  # Using character ID for all platforms
#' )
#' @export
microDNADistributionComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = microDNADistributionFilterUI(id, translate),
              display = microDNADistributionDisplayUI(id)),
    server = function(input, output, session) {
      microDNADistributionServer(id, app_data_connection, config, session)
    }
  )
}

# For backwards compatibility - assigns the component function to the old name
#' @rdname microDNADistributionComponent
#' @usage microDNADistribution(id, app_data_connection = NULL, config = NULL, translate = identity)
#' @description Alias for microDNADistributionComponent. Provided for backward compatibility.
microDNADistribution <- microDNADistributionComponent
