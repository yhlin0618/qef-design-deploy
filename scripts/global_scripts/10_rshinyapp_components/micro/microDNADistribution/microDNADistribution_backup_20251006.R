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
    
    # active_tab <- reactive({
    #     cfg <- if (is.function(config)) config() else config
    #     cfg$active_tab %||% "dna"      # 預設自己就是 dna 分頁
    #   })
    
    # ------------ 資料存取 (R116) ------------------------------------
    #' Reactive data accessor for customer DNA data
    #' 
    #' Retrieves customer DNA data from the database or data source using the Enhanced Data Access
    #' Pattern (R116). Applies platform filtering if specified in the configuration.
    #' 
    #' @return data.frame. A reactive data frame containing customer DNA metrics.
    #'         Contains columns for customer_id, M, R, F, IPT_mean, and NES.
    df_dna_by_customer <- reactive({
      # Update component status to show data loading
      component_status("loading")
      
      # Get platform ID from config and ensure it's not NA
      plat <- platform_id()
      
      # Safely access data with proper error handling
      result <- tryCatch({
        # First check if we have a valid connection
        if (is.null(app_data_connection)) {
          warning("No valid database connection available")
          return(data.frame())
        }
        
        # Access data directly using tbl2 for enhanced data access (R116)
        if (!is.null(plat) && !is.na(plat) && plat != "all") {
          # Apply platform filter when a specific platform ID is provided
          # Use character platform_id directly (e.g., "amz" for Amazon, "eby" for eBay)
          # SLN08: Use !! to force evaluation of variables before SQL translation
          # This prevents "Cannot translate a shiny reactive to SQL" errors
          res <- tbl2(app_data_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plat)
        } else {
          # Use tbl2 without filter for "all" platform (all platforms)
          res <- tbl2(app_data_connection, "df_dna_by_customer")
        }
        
        # Collect results
        res_data <- res %>% collect()
        
        # Update component status
        component_status("ready")
        
        # Return the collected data
        res_data
      }, error = function(e) {
        warning("Error fetching DNA data: ", e$message)
        component_status("error")
        data.frame() # Return empty data frame on error
      })
      
      # Return the result
      return(result)
    })
    
    # ------------ helper ------------------------------------------------------
    #' Compute distribution for ECDF plotting
    #' @param v Numeric vector. The values to compute distribution for.
    #' @return data.frame with columns x (values), y (cumulative probabilities), and count (total count).
    compute_distribution <- function(v) {
      v <- v[!is.na(v)]; if (!length(v)) return(NULL)
      fn <- ecdf(v); x <- sort(unique(v)); data.frame(x = x, y = fn(x), count = length(v))
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
      
      # Create a named list for statistics
      stats_list <- list(
        Count = st[["n"]], 
        Mean = round(st[["mean"]], 2), 
        Median = round(st[["median"]], 2),
        `Std Dev` = round(st[["sd"]], 2)
      )
      
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
    
    #' Create a plotly ECDF plot for the given data
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
    
    # ------------ 資料存取輔助 ----------------------------------------------
    #' Debug logging for data verification
    #' 
    #' Prints sample data for debugging and monitoring
    observe({
      # Safely get and print debug information with proper error handling
      tryCatch({
        # Get base data
        dat <- df_dna_by_customer()
        
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
    #' @param field Character string. The field name in the data to visualize (e.g., "M", "R", "F").
    #' @param btn Character string. The input ID of the button that triggers this visualization.
    #' @param ttl Character string. The title to display for this visualization.
    #' @return Observer. A Shiny observer that handles visualization when the button is clicked.
    render_metric <- function(field, btn, ttl) {
      observeEvent(input[[btn]], {
        # Update current visualization tracking
        current_visualization(btn)
        
        # Get data
        dat <- df_dna_by_customer()
        
        # Render the visualization with access to the field and button
        output$dna_distribution_plot <- renderPlotly({
          ecdf_plot(dat, field, ttl, btn)
        })
        
        # Render statistics
        output$metric_stats <- renderUI({
          if (!is.null(dat) && nrow2(dat) > 0 && field %in% names(dat)) {
            stats_box(calc_stats(dat[[field]]), ttl)
          } else {
            p("No data available")
          }
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
      dat <- df_dna_by_customer()
      
      output$dna_distribution_plot <- renderPlotly({
        # Modify the bar_plot call to explicitly set integer orders for frequency
        # Create a vector of integer levels from 1 to max frequency (or a reasonable maximum)
        if (!is.null(dat) && nrow2(dat) > 0 && "f_value" %in% names(dat)) {
          # Find the maximum frequency in the data
          max_f <- max(dat[["f_value"]], na.rm = TRUE)
          # Create integer levels from 1 to max_f (capped at 50 to prevent excessive levels)
          freq_levels <- as.character(1:min(max_f, 50))
          bar_plot(dat, "f_value", "Purchase Frequency Distribution", levels = freq_levels)
        } else {
          bar_plot(dat, "f_value", "Purchase Frequency Distribution")
        }
      })
      
      output$metric_stats <- renderUI({
        if (!is.null(dat) && nrow2(dat) > 0 && "f_value" %in% names(dat)) {
          stats_box(calc_stats(dat[["f_value"]]), "Frequency (F)")
        } else {
          p("No data available")
        }
      })
    })
    
    observeEvent(input$nes_barplot, {
      current_visualization("nes_barplot")
      dat <- df_dna_by_customer()
      
      output$dna_distribution_plot <- renderPlotly({
        bar_plot(dat, "nes_status", "NES Status Distribution", levels = c("N", "E0", "S1", "S2", "S3"))
      })
      
      output$metric_stats <- renderUI({
        if (is.null(dat) || nrow2(dat) == 0 || !"nes_status" %in% names(dat)) {
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
    
    # 預設載入 M
    observe({ 
      # Isolate to prevent dependency chain issues
      isolate({ 
        # Safely check for data and load initial visualization
        tryCatch({
          data_available <- FALSE
          
          # Get data
          data <- df_dna_by_customer()
          
          # Verify we have data before triggering initial visualization
          if (!is.null(data) && is.data.frame(data) && nrow(data) > 0) {
            data_available <- TRUE
          }
          
          # Only trigger initial visualization if we have data and no visualization is currently active
          if (current_visualization() == "none" && data_available) {
            session$sendCustomMessage("shiny.button.click", list(id = session$ns("m_ecdf")))
          }
        }, error = function(e) {
          # Silently handle errors to avoid breaking UI
          warning("Error in initial visualization: ", e$message)
        })
      }) 
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