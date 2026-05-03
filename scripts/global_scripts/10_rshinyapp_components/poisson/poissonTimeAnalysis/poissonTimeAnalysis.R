#LOCK FILE
#
# poissonTimeAnalysis.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP73: Interactive Visualization Preference (DT and plotly for visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
# - UI_R019: AI Process Notification Rule (stage notifications for AI processes)
#
# Features:
#   • Time dimension Poisson regression analysis display
#   • Year, month, day, weekday effects visualization
#   • Incidence Rate Ratio (IRR) interpretation
#   • Confidence intervals and significance testing
#   • Product line filtering and comparison
# -----------------------------------------------------------------------------

# Source covariate exclusion utility (configuration-driven approach)
# Following: Configuration-Driven Development, DRY Principle
source("scripts/global_scripts/04_utils/fn_should_exclude_covariate.R")

# Stage notification messages for time insights analysis
# Following UI_R007: 標準化介面文字 (Traditional Chinese)
# Following UI_R019: AI Process Notification Rule
# Following MP088: Immediate Feedback Principle
TIME_ANALYSIS_STAGE_MESSAGES <- list(
  start = "⏰ 正在準備時間趨勢分析...",
  analyzing = "🤖 AI 正在分析時間模式...",
  complete = "✅ 時間趨勢分析完成！",
  error = "❌ AI 分析失敗"
)

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

# Filter UI -------------------------------------------------------------------
#' poissonTimeAnalysisFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the time analysis component.
poissonTimeAnalysisFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("時間區段分析篩選")),
    
    # Time dimension filter
    checkboxGroupInput(
      inputId = ns("time_dimensions"),
      label = translate("時間維度"),
      choices = list(
        "年度效應" = "year",
        "月份季節性" = "monthly",
        "日期效應" = "day", 
        "星期效應" = "weekday"
      ),
      selected = c("year", "monthly", "day", "weekday")
    ),
    
    # Significance filter
    checkboxInput(
      inputId = ns("show_significant_only"),
      label = translate("只顯示顯著結果 (p<0.05)"),
      value = FALSE
    ),
    
    
    # Reset button
    actionButton(
      inputId = ns("reset_filters"),
      label = translate("重置篩選"),
      class = "btn-outline-secondary btn-block mt-3"
    ),
    
    hr(),
    
    # AI Analysis button
    actionButton(
      inputId = ns("generate_time_insight"),
      label = translate("生成 AI 時段洞察"),
      class = "btn-primary btn-block mt-3",
      icon = icon("magic")
    ),
    
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' poissonTimeAnalysisDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @return shiny.tag. A Shiny UI component containing the display elements for the time analysis.
poissonTimeAnalysisDisplayUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    # FIX BUG 2: Add CSS for overflow handling (UI_R024: Component-specific styling)
    tags$style(HTML("
      .info-box-content h4 {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 100%;
        cursor: help;
      }

      .info-box {
        min-height: 100px;
        display: flex;
        align-items: center;
      }

      .info-box-content {
        width: 100%;
        padding: 10px;
      }

      .info-box-content h4 {
        margin-top: 0;
        margin-bottom: 5px;
        font-size: 1.2rem;
      }

      .info-box-content p {
        margin-bottom: 0;
        font-size: 0.9rem;
      }
    ")),

    div(class = "component-header mb-3 text-center",
        h3(translate("時間區段分析")),
        p(translate("分析時間因素對銷售的影響：年度趨勢、季節性、週期性效應"))),

    # TYPE B METADATA BANNER (MP135 v2.0 + UI_R024)
    uiOutput(ns("metadata_banner")),

    # Summary cards row
    fluidRow(
      column(3,
        div(class = "info-box bg-primary",
            div(class = "info-box-content",
                h4(textOutput(ns("total_analyses")), class = "text-white"),
                p(translate("總分析數"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-success", 
            div(class = "info-box-content",
                h4(textOutput(ns("significant_count")), class = "text-white"),
                p(translate("顯著結果"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-warning",
            div(class = "info-box-content",
                # FIX BUG 2: Add title attribute for full text tooltip (UI_R024)
                h4(textOutput(ns("strongest_effect")),
                   class = "text-white",
                   title = textOutput(ns("strongest_effect_full"))),
                p(translate("最強效應"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-info",
            div(class = "info-box-content",
                h4(textOutput(ns("product_line_count")), class = "text-white"),
                p(translate("產品線數"), class = "text-white"))))
    ),
    
    # Main visualization
    div(class = "component-output p-3",
        tabsetPanel(
          id = ns("analysis_tabs"),
          
          # Time effects overview
          tabPanel(
            title = translate("時間效應總覽"),
            value = "overview",
            br(),
            plotly::plotlyOutput(ns("time_effects_plot"), height = "500px")
          ),
          
          # Seasonal analysis
          tabPanel(
            title = translate("季節性分析"),
            value = "seasonal", 
            br(),
            fluidRow(
              column(6, plotly::plotlyOutput(ns("monthly_effects_plot"), height = "400px")),
              column(6, plotly::plotlyOutput(ns("weekday_effects_plot"), height = "400px"))
            )
          ),
          
          # Detailed table
          tabPanel(
            title = translate("詳細數據"),
            value = "table",
            br(),
            # Following UI_R018: Table Download Button Placement Rule
            # Following UI_R021: Dual Download Buttons for Statistical Data
            # Control bar ABOVE table with dual download buttons
            div(class = "table-control-bar",
                style = "display: flex; justify-content: flex-end; gap: 10px; margin-bottom: 15px;",

                # Download full data (all results with significance markers)
                downloadButton(ns("download_full"),
                               "下載完整數據",
                               class = "btn-primary btn-sm",
                               icon = icon("download")),

                # Download significant results only (p < 0.05)
                downloadButton(ns("download_significant"),
                               "下載顯著結果",
                               class = "btn-success btn-sm",
                               icon = icon("download"))),
            # Table BELOW control bar
            DTOutput(ns("time_analysis_table"), width = "100%")
          ),
          
          # AI Insights
          tabPanel(
            title = translate("AI 時段洞察"),
            value = "ai_insights",
            br(),
            div(
              class = "ai-insights-container",
              style = "min-height: 300px; padding: 20px; background-color: #f8f9fa; border-radius: 8px;",
              if (requireNamespace("shinycssloaders", quietly = TRUE)) {
                shinycssloaders::withSpinner(
                  htmlOutput(ns("time_insight_output")),
                  type = 6,
                  color = "#0d6efd"
                )
              } else {
                htmlOutput(ns("time_insight_output"))
              }
            )
          )
        ))
  )
}

# Server ----------------------------------------------------------------------
#' poissonTimeAnalysisServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data.
poissonTimeAnalysisServer <- function(id, app_data_connection = NULL, config = NULL,
                                     session = getDefaultReactiveDomain(),
                                     translate = identity) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ Status tracking ----------------------------------
    component_status <- reactiveVal("idle")
    
    # ------------ Extract configuration parameters -----------------
    platform_id <- reactive({
      tryCatch({
        if (is.null(config)) return("cbz")
        
        if (is.function(config)) {
          if (shiny::is.reactive(config) || "reactive" %in% class(config)) {
            cfg <- config()
          } else {
            cfg <- config
          }
        } else {
          cfg <- config
        }
        
        if (!is.null(cfg)) {
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
            return(as.character(cfg[["filters"]][["platform_id"]]))
          }
          if (!is.null(cfg[["platform_id"]])) {
            return(as.character(cfg[["platform_id"]]))
          }
        }
        
        "cbz"  # Default platform
      }, error = function(e) {
        warning("Error extracting platform_id: ", e$message)
        "cbz"
      })
    })
    
    product_line_id <- reactive({
      tryCatch({
        if (is.null(config)) return("all")
        
        if (is.function(config)) {
          if (shiny::is.reactive(config) || "reactive" %in% class(config)) {
            cfg <- config()
          } else {
            cfg <- config
          }
        } else {
          cfg <- config
        }
        
        if (!is.null(cfg)) {
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["product_line_id_chosen"]])) {
            return(as.character(cfg[["filters"]][["product_line_id_chosen"]]))
          }
        }
        
        "all"  # Default product line
      }, error = function(e) {
        warning("Error extracting product_line_id: ", e$message)
        "all"
      })
    })
    
    # ------------ Data access (R116) -----------------------------------
    poisson_data <- reactive({
      component_status("loading")
      
      result <- tryCatch({
        if (is.null(app_data_connection)) {
          warning("No valid database connection available")
          return(data.frame())
        }
        
        # Get platform - 時間分析固定使用 Cyberbiz 資料
        # platform <- platform_id()
        platform <- "cbz"  # 固定使用 Cyberbiz，因為只有 Cyberbiz 有時間區段資料
        
        # Access Poisson analysis results using tbl2
        table_name <- paste0("df_", platform, "_poisson_analysis_all")
        tbl <- tbl2(app_data_connection, table_name)
        
        # Filter for time features only (and successfully estimated)
        time_data <- tbl %>%
          dplyr::filter(predictor_type == "time_feature" &
                        estimation_status == "estimated") %>%
          collect()

        # ISSUE_115: Enrich with hierarchical time labels
        # Source enrichment function if not already loaded
        if (!exists("fn_enrich_time_labels")) {
          source(file.path("scripts", "global_scripts", "04_utils", "fn_enrich_time_labels.R"))
        }

        # Enrich time labels with year/month/hierarchy context
        time_data <- fn_enrich_time_labels(
          data = time_data,
          con = app_data_connection,
          platform_id = platform
        ) %>%
          # PHASE 4: Add display_name_safe with fallback
          # Priority: hierarchical_label (time-specific) > display_name > predictor
          mutate(display_name_safe = dplyr::coalesce(hierarchical_label, display_name, predictor))

        component_status("ready")
        return(time_data)
        
      }, error = function(e) {
        warning("Error fetching Poisson data: ", e$message)
        component_status("error")
        data.frame()
      })
      
      return(result)
    })
    
    # ------------ Filter Options -----------------------------------------
    # No additional filter options needed - platform and product line come from config
    
    # ------------ Filtered Data ----------------------------------------
    filtered_data <- reactive({
      data <- poisson_data()
      
      if (is.null(data) || nrow(data) == 0) return(data.frame())
      
      # Filter by product line from config
      current_product_line <- product_line_id()
      if (!is.null(current_product_line) && current_product_line != "all") {
        data <- data %>% dplyr::filter(product_line_id == current_product_line)
      }
      
      # Filter by time dimensions
      if (length(input$time_dimensions) > 0) {
        time_filters <- character()
        
        if ("year" %in% input$time_dimensions) {
          time_filters <- c(time_filters, "year")
        }
        if ("monthly" %in% input$time_dimensions) {
          time_filters <- c(time_filters, paste0("month_", 1:12))
        }
        if ("day" %in% input$time_dimensions) {
          time_filters <- c(time_filters, "day")
        }
        if ("weekday" %in% input$time_dimensions) {
          time_filters <- c(time_filters, "monday", "tuesday", "wednesday", 
                           "thursday", "friday", "saturday", "sunday")
        }
        
        data <- data %>% dplyr::filter(predictor %in% time_filters)
      }
      
      # Filter by significance
      # FIX: convergence is BOOLEAN (TRUE/FALSE), not string "converged"
      if (input$show_significant_only) {
        data <- data %>%
          dplyr::filter(!is.na(p_value) & p_value < 0.05 & convergence == "converged")
      }
      
      # Apply covariate exclusion rules for display purposes only
      # This preserves the full analysis but filters what users see
      if (nrow(data) > 0) {
        tryCatch({
          all_predictors <- unique(data$predictor)
          kept_predictors <- filter_covariates(
            var_names = all_predictors,
            app_type = "time_series_analysis",  # Use time series specific settings
            verbose = FALSE
          )
          
          # Filter data to keep only allowed predictors
          data <- data %>%
            dplyr::filter(predictor %in% kept_predictors)
          
          # Log exclusions if verbose
          excluded_count <- length(all_predictors) - length(kept_predictors)
          if (excluded_count > 0) {
            message(sprintf("Hiding %d time covariates from display based on exclusion rules", excluded_count))
          }
        }, error = function(e) {
          # If function not available, show all predictors
          warning("filter_covariates not available, showing all covariates: ", e$message)
        })
      }
      
      return(data)
    })

    # TYPE B METADATA BANNER (MP135 v2.0 + UI_R024)
    # Display metadata for steady-state analytics using all historical data
    output$metadata_banner <- renderUI({
      # Get analysis data
      data <- poisson_data()

      # If no data, don't show banner
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }

      # Extract Type B metadata from first row (all rows have same metadata)
      computed_at <- data$computed_at[1]
      data_version <- data$data_version[1]

      # FIX BUG 1: Check for NULL first, then check NA
      # Following MP031: Defensive Programming
      # is.na(NULL) returns logical(0), not TRUE/FALSE, causing error
      if (is.null(computed_at) || is.null(data_version) ||
          is.na(computed_at) || is.na(data_version)) {
        return(NULL)
      }

      # Convert to proper date/time types (after NULL/NA check)
      computed_at <- as.POSIXct(computed_at)
      data_version <- as.Date(data_version)

      # Additional safety check after conversion
      if (is.na(computed_at) || is.na(data_version)) {
        return(NULL)
      }

      # Render metadata banner (per UI_R024)
      # SECURITY FIX: Removed "數據至" field (sensitive business data)
      # UX FIX: Changed text color to #495057 for proper contrast (WCAG AA compliant)
      div(
        class = "alert alert-info",
        style = "margin-bottom: 15px; padding: 10px 15px; background-color: #e8f4f8; border-left: 4px solid #1e88e5; color: #495057;",

        div(
          style = "display: flex; align-items: center; gap: 20px; flex-wrap: wrap;",

          # Data scope
          div(
            tags$i(class = "fas fa-database", style = "color: #1565c0;"),
            strong(" 基於全部歷史數據")
          ),

          # Computed timestamp (only non-sensitive temporal metadata shown)
          div(
            tags$i(class = "fas fa-clock", style = "color: #1565c0;"),
            sprintf(" 計算時間: %s", format(computed_at, "%Y-%m-%d %H:%M"))
          )
        )
      )
    })

    # ------------ Summary Statistics --------------------------------
    output$total_analyses <- renderText({
      data <- filtered_data()
      if (nrow(data) == 0) return("0")
      formatC(nrow(data), format = "d", big.mark = ",")
    })
    
    output$significant_count <- renderText({
      data <- filtered_data()
      if (nrow(data) == 0) return("0")
      # FIX: convergence is BOOLEAN (TRUE/FALSE), not string "converged"
      sig_count <- sum(!is.na(data$p_value) & data$p_value < 0.05 & data$convergence == "converged", na.rm = TRUE)
      formatC(sig_count, format = "d", big.mark = ",")
    })
    
    # FIX BUG 2: Consistent truncation logic with better length calculation
    # Following MP031: Defensive Programming + UI_R024: Metadata Display
    output$strongest_effect <- renderText({
      data <- filtered_data()
      if (nrow(data) == 0) return("--")

      # Find strongest significant effect (highest absolute IRR deviation from 1)
      sig_data <- data %>%
        dplyr::filter(!is.na(incidence_rate_ratio) & !is.na(p_value) & p_value < 0.05) %>%
        dplyr::mutate(irr_deviation = abs(log(incidence_rate_ratio))) %>%
        dplyr::arrange(desc(irr_deviation))

      if (nrow(sig_data) == 0) return("無顯著")

      strongest <- sig_data[1, ]
      # PHASE 4: Use display_name_safe for user-friendly display
      full_text <- paste0(strongest$display_name_safe, " (", round(strongest$incidence_rate_ratio, 2), "×)")

      # Chinese characters take more space, use conservative limit
      max_display_chars <- 20  # Increased for better readability

      if (nchar(full_text) > max_display_chars) {
        # Truncate at max_display_chars - 3 (for "...")
        paste0(substr(full_text, 1, max_display_chars - 3), "...")
      } else {
        full_text
      }
    })

    # FIX BUG 2: Add full text output for tooltip
    output$strongest_effect_full <- renderText({
      data <- filtered_data()
      if (nrow(data) == 0) return("--")

      # Find strongest significant effect
      sig_data <- data %>%
        dplyr::filter(!is.na(incidence_rate_ratio) & !is.na(p_value) & p_value < 0.05) %>%
        dplyr::mutate(irr_deviation = abs(log(incidence_rate_ratio))) %>%
        dplyr::arrange(desc(irr_deviation))

      if (nrow(sig_data) == 0) return("無顯著")

      strongest <- sig_data[1, ]
      # PHASE 4: Use display_name_safe for tooltip (full text)
      paste0(strongest$display_name_safe, " (", round(strongest$incidence_rate_ratio, 2), "×)")  # Full text
    })
    
    output$product_line_count <- renderText({
      data <- filtered_data()
      if (nrow(data) == 0) return("0")
      length(unique(data$product_line_id))
    })
    
    # ------------ Visualizations ------------------------------------
    
    # Time effects overview plot
    output$time_effects_plot <- plotly::renderPlotly({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No data available"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      # Prepare data for plotting
      # FIX: convergence is BOOLEAN (TRUE/FALSE), not string "converged"
      plot_data <- data %>%
        dplyr::filter(!is.na(incidence_rate_ratio) & convergence == "converged") %>%
        dplyr::mutate(
          is_significant = !is.na(p_value) & p_value < 0.05,
          # ISSUE_115: Use hierarchical_label with full year/month context
          predictor_clean = dplyr::if_else(
            !is.na(hierarchical_label) & hierarchical_label != "",
            hierarchical_label,
            predictor  # Fallback to original predictor if enrichment failed
          )
        )
      
      # 固定使用 IRR (發生率比)
      y_var <- "incidence_rate_ratio"
      
      p <- ggplot2::ggplot(plot_data, ggplot2::aes(x = predictor_clean, y = .data[[y_var]], 
                                                  color = product_line_id, shape = is_significant)) +
        ggplot2::geom_point(size = 3, alpha = 0.7) +
        ggplot2::theme_minimal() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1)) +
        ggplot2::labs(
          title = "時間因素效應分析",
          x = "時間維度",
          y = "發生率比 (IRR)",
          color = "產品線",
          shape = "顯著性 (p<0.05)"
        ) +
        ggplot2::scale_shape_manual(values = c(1, 16), labels = c("否", "是"))
      
      # Add reference line for IRR = 1
      p <- p + ggplot2::geom_hline(yintercept = 1, linetype = "dashed", alpha = 0.5)
      
      plotly::ggplotly(p, tooltip = c("x", "y", "colour", "shape"))
    })
    
    # Monthly effects plot
    output$monthly_effects_plot <- plotly::renderPlotly({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No data available"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      # Filter for monthly data
      monthly_data <- data %>%
        dplyr::filter(grepl("^month_", predictor) & !is.na(incidence_rate_ratio) & convergence == "converged") %>%
        dplyr::mutate(
          month_num = as.numeric(gsub("month_", "", predictor)),
          # ISSUE_115: Use hierarchical_label for full year+month display
          month_label = dplyr::if_else(
            !is.na(hierarchical_label) & hierarchical_label != "",
            hierarchical_label,
            paste0(month_num, "月")  # Fallback
          ),
          is_significant = !is.na(p_value) & p_value < 0.05
        ) %>%
        dplyr::arrange(month_num)
      
      if (nrow(monthly_data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No monthly data"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      p <- ggplot2::ggplot(monthly_data, ggplot2::aes(x = factor(month_num), y = incidence_rate_ratio,
                                                     color = product_line_id, shape = is_significant)) +
        ggplot2::geom_point(size = 3, alpha = 0.7) +
        ggplot2::geom_line(ggplot2::aes(group = product_line_id), alpha = 0.5) +
        ggplot2::geom_hline(yintercept = 1, linetype = "dashed", alpha = 0.5) +
        ggplot2::theme_minimal() +
        ggplot2::labs(
          title = "月份季節性效應",
          x = "月份",
          y = "發生率比 (IRR)",
          color = "產品線",
          shape = "顯著性"
        ) +
        # ISSUE_115: Use hierarchical labels with year information
        ggplot2::scale_x_discrete(labels = monthly_data$month_label) +
        ggplot2::scale_shape_manual(values = c(1, 16), labels = c("否", "是"))
      
      plotly::ggplotly(p, tooltip = c("x", "y", "colour", "shape"))
    })
    
    # Weekday effects plot
    output$weekday_effects_plot <- plotly::renderPlotly({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No data available"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      # Filter for weekday data
      weekdays_order <- c("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
      weekdays_labels <- c("週一", "週二", "週三", "週四", "週五", "週六", "週日")
      
      weekday_data <- data %>%
        dplyr::filter(predictor %in% weekdays_order & !is.na(incidence_rate_ratio) & convergence == "converged") %>%
        dplyr::mutate(
          weekday_num = match(predictor, weekdays_order),
          weekday_name = weekdays_labels[weekday_num],
          is_significant = !is.na(p_value) & p_value < 0.05
        ) %>%
        dplyr::arrange(weekday_num)
      
      if (nrow(weekday_data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No weekday data"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      p <- ggplot2::ggplot(weekday_data, ggplot2::aes(x = factor(weekday_num), y = incidence_rate_ratio,
                                                     color = product_line_id, shape = is_significant)) +
        ggplot2::geom_point(size = 3, alpha = 0.7) +
        ggplot2::geom_line(ggplot2::aes(group = product_line_id), alpha = 0.5) +
        ggplot2::geom_hline(yintercept = 1, linetype = "dashed", alpha = 0.5) +
        ggplot2::theme_minimal() +
        ggplot2::labs(
          title = "星期效應分析",
          x = "星期",
          y = "發生率比 (IRR)",
          color = "產品線",
          shape = "顯著性"
        ) +
        ggplot2::scale_x_discrete(labels = weekdays_labels) +
        ggplot2::scale_shape_manual(values = c(1, 16), labels = c("否", "是"))
      
      plotly::ggplotly(p, tooltip = c("x", "y", "colour", "shape"))
    })
    
    # Detailed table
    output$time_analysis_table <- renderDT({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        return(data.frame(Message = "無可用資料"))
      }
      
      # Prepare table data
      # Apply basic filter and YAML-based exclusion
      table_data <- data %>%
        dplyr::filter(convergence == "converged")

      # Apply YAML-based covariate exclusion (presentation layer filtering)
      # Following DM_R043: Predictor Data Classification
      table_data <- filter_excluded_covariates(
        table_data,
        predictor_col = "predictor",
        app_type = "poisson_regression"
      ) %>%
        dplyr::select(product_line_id, predictor, hierarchical_label, coefficient, incidence_rate_ratio,
                     std_error, p_value, conf_low, conf_high, aic, sample_size) %>%
        dplyr::mutate(
          # ISSUE_115: Use hierarchical_label with full year/month context
          predictor_chinese = dplyr::if_else(
            !is.na(hierarchical_label) & hierarchical_label != "",
            hierarchical_label,
            predictor  # Fallback to original predictor
          ),
          coefficient = round(coefficient, 4),
          incidence_rate_ratio = round(incidence_rate_ratio, 4),
          std_error = round(std_error, 4),
          p_value = round(p_value, 6),
          conf_low = round(conf_low, 4),
          conf_high = round(conf_high, 4),
          significance = ifelse(!is.na(p_value) & p_value < 0.05, translate("Significant"), translate("Not Significant"))
        ) %>%
        dplyr::select(product_line_id, predictor_chinese, coefficient, incidence_rate_ratio,
                     std_error, p_value, significance, conf_low, conf_high, aic, sample_size)
      
      # Following DEV_R052 + Issue #288: translate column names via ui_terminology.csv
      colnames(table_data) <- sapply(c("Product line", "Time Dimension", "Coefficient",
                                       "Incidence Rate Ratio", "Standard Error", "P-Value",
                                       "Significance", "CI Lower Bound", "CI Upper Bound",
                                       "AIC", "Sample Size"), translate)

      # Following UI_R018: Removed embedded download button (now above table)
      datatable(table_data,
                options = list(
                  pageLength = 15,
                  scrollX = TRUE,
                  dom = 'frtip'  # Removed 'B' (buttons) per UI_R018
                ),
                # Removed extensions per UI_R018
                rownames = FALSE) %>%
        formatStyle(translate("Significance"),
                   backgroundColor = styleEqual(translate("Significant"), "#d4edda"),
                   color = styleEqual(translate("Significant"), "#155724"))
    })

    # Following UI_R021: Dual Download Buttons for Statistical Data
    # Download 1: Full Data (all results with significance markers)
    # Following UI_R018: Separate download button above table
    # Following ISSUE_245: UTF-8 BOM for Excel compatibility
    # Following UI_R020: All downloads use CSV with UTF-8 BOM
    output$download_full <- downloadHandler(
      filename = function() {
        paste0("時間區段完整分析_", format(Sys.Date(), "%Y%m%d"), ".csv")
      },
      content = function(file) {
        data <- filtered_data()

        if (nrow(data) == 0) {
          # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
          write_utf8_csv_with_bom(data.frame(Message = "無可用資料"), file)
        } else {
          # Apply basic filter and YAML-based exclusion
          table_data <- data %>%
            dplyr::filter(convergence == "converged")

          # Apply YAML-based covariate exclusion (presentation layer filtering)
          # Following DM_R043: Predictor Data Classification
          table_data <- filter_excluded_covariates(
            table_data,
            predictor_col = "predictor",
            app_type = "poisson_regression"
          ) %>%
            dplyr::select(product_line_id, predictor, hierarchical_label, coefficient, incidence_rate_ratio,
                         std_error, p_value, conf_low, conf_high, aic, sample_size) %>%
            dplyr::mutate(
              predictor_chinese = dplyr::if_else(
                !is.na(hierarchical_label) & hierarchical_label != "",
                hierarchical_label,
                predictor
              ),
              coefficient = round(coefficient, 4),
              incidence_rate_ratio = round(incidence_rate_ratio, 4),
              std_error = round(std_error, 4),
              p_value = round(p_value, 6),
              conf_low = round(conf_low, 4),
              conf_high = round(conf_high, 4),
              # Add significance marker for all data
              significance = case_when(
                is.na(p_value) ~ "",
                p_value < 0.05 ~ translate("Significant"),
                TRUE ~ translate("Not Significant")
              )
            ) %>%
            dplyr::select(product_line_id, predictor_chinese, coefficient, incidence_rate_ratio,
                         std_error, p_value, significance, conf_low, conf_high, aic, sample_size)

          colnames(table_data) <- sapply(c("Product line", "Time Dimension", "Coefficient",
                                           "Incidence Rate Ratio", "Standard Error", "P-Value",
                                           "Significance", "CI Lower Bound", "CI Upper Bound",
                                           "AIC", "Sample Size"), translate)

          # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
          write_utf8_csv_with_bom(table_data, file)
        }
      }
    )

    # Download 2: Significant Results Only (p < 0.05)
    # Following UI_R021: Dual Download Buttons for Statistical Data
    # Following ISSUE_245: UTF-8 BOM for Excel compatibility
    # Following UI_R020: All downloads use CSV with UTF-8 BOM
    output$download_significant <- downloadHandler(
      filename = function() {
        paste0("時間區段顯著結果_", format(Sys.Date(), "%Y%m%d"), ".csv")
      },
      content = function(file) {
        data <- filtered_data()

        # Filter for converged and significant results
        significant_data <- data %>%
          dplyr::filter(convergence == "converged" & !is.na(p_value) & p_value < 0.05)

        if (nrow(significant_data) == 0) {
          # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
          write_utf8_csv_with_bom(data.frame(Message = "無顯著結果"), file)
        } else {
          table_data <- significant_data %>%
            dplyr::select(product_line_id, predictor, hierarchical_label, coefficient, incidence_rate_ratio,
                         std_error, p_value, conf_low, conf_high, aic, sample_size) %>%
            dplyr::mutate(
              predictor_chinese = dplyr::if_else(
                !is.na(hierarchical_label) & hierarchical_label != "",
                hierarchical_label,
                predictor
              ),
              coefficient = round(coefficient, 4),
              incidence_rate_ratio = round(incidence_rate_ratio, 4),
              std_error = round(std_error, 4),
              p_value = round(p_value, 6),
              conf_low = round(conf_low, 4),
              conf_high = round(conf_high, 4),
              # All results here are significant
              significance = translate("Significant")
            ) %>%
            dplyr::select(product_line_id, predictor_chinese, coefficient, incidence_rate_ratio,
                         std_error, p_value, significance, conf_low, conf_high, aic, sample_size)

          colnames(table_data) <- sapply(c("Product line", "Time Dimension", "Coefficient",
                                           "Incidence Rate Ratio", "Standard Error", "P-Value",
                                           "Significance", "CI Lower Bound", "CI Upper Bound",
                                           "AIC", "Sample Size"), translate)

          # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
          write_utf8_csv_with_bom(table_data, file)
        }
      }
    )

    # AIC distribution plot
    output$aic_distribution_plot <- plotly::renderPlotly({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No data available"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      aic_data <- data %>%
        dplyr::filter(!is.na(aic) & convergence == "converged")
      
      if (nrow(aic_data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No AIC data"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      p <- ggplot2::ggplot(aic_data, ggplot2::aes(x = aic, fill = product_line_id)) +
        ggplot2::geom_histogram(bins = 30, alpha = 0.7, position = "identity") +
        ggplot2::theme_minimal() +
        ggplot2::labs(
          title = "AIC 分布 (模型品質指標)",
          x = "AIC 值",
          y = "頻率",
          fill = "產品線"
        )
      
      plotly::ggplotly(p)
    })
    
    # Convergence status plot
    output$convergence_status_plot <- plotly::renderPlotly({
      data <- filtered_data()
      
      if (nrow(data) == 0) {
        p <- ggplot2::ggplot() + 
          ggplot2::geom_text(ggplot2::aes(x = 0.5, y = 0.5, label = "No data available"), size = 6) +
          ggplot2::theme_void()
        return(plotly::ggplotly(p))
      }
      
      convergence_data <- data %>%
        dplyr::count(product_line_id, convergence) %>%
        dplyr::mutate(
          convergence_chinese = case_when(
            convergence == "converged" ~ "收斂",
            convergence == "failed" ~ "失敗",
            grepl("error", convergence) ~ "錯誤",
            TRUE ~ "其他"
          )
        )
      
      p <- ggplot2::ggplot(convergence_data, ggplot2::aes(x = product_line_id, y = n, fill = convergence_chinese)) +
        ggplot2::geom_col(position = "stack") +
        ggplot2::theme_minimal() +
        ggplot2::labs(
          title = "模型收斂狀態統計",
          x = "產品線",
          y = "模型數量",
          fill = "收斂狀態"
        ) +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
      
      plotly::ggplotly(p)
    })
    
    # Reset filters
    observeEvent(input$reset_filters, {
      updateCheckboxGroupInput(session, "time_dimensions", 
                             selected = c("year", "monthly", "day", "weekday"))
      updateCheckboxInput(session, "show_significant_only", value = FALSE)
    })
    
    # Component status
    output$component_status <- renderText({
      # MP031: Defensive programming - check for NULL/empty values before switch
      # R113: Error handling for reactive expressions
      status_val <- tryCatch({
        component_status()
      }, error = function(e) {
        warning("Error getting component status: ", e$message)
        "idle"
      })

      # MP099: Defensive check for NULL or empty status
      if (is.null(status_val) || length(status_val) == 0 || status_val == "") {
        return("準備顯示時間分析")
      }

      # Ensure status_val is character and length 1 for switch
      status_val <- as.character(status_val)[1]

      switch(status_val,
             idle = "準備顯示時間分析",
             loading = "載入分析資料中...",
             ready = paste0("已載入 ", nrow(poisson_data()), " 筆時間分析結果"),
             error = "載入資料時發生錯誤",
             status_val)  # Default: return the status value itself
    })
    
    # ------------ AI Time Insights Generation ----------------------------
    ai_insight_result <- reactiveVal(NULL)
    
    # Get OpenAI API key from environment
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
    if (!nzchar(gpt_key)) {
      gpt_key <- NULL
    }
    
    observeEvent(input$generate_time_insight, {
      data <- filtered_data()

      if (is.null(data) || nrow(data) == 0) {
        showNotification("無可用的時間分析資料", type = "warning")
        return()
      }

      if (is.null(gpt_key)) {
        showNotification("OpenAI API 金鑰未設定。AI 分析功能已停用。", type = "error")
        return()
      }

      # Following UI_R019: AI Process Notification Rule
      # Following MP088: Immediate Feedback Principle
      # Show initial notification without auto-dismiss
      showNotification(
        TIME_ANALYSIS_STAGE_MESSAGES$start,
        id = "time_insights_progress",
        type = "message",
        duration = NULL,  # Don't auto-dismiss
        closeButton = FALSE
      )

      # MP031: Defensive Programming - wrap in tryCatch
      tryCatch({

        # Prepare time effect data for AI analysis
        # Focus on significant monthly and weekday effects
        monthly_data <- data %>%
          dplyr::filter(grepl("^month_", predictor) & !is.na(incidence_rate_ratio)) %>%
          dplyr::mutate(
            month_num = as.numeric(gsub("month_", "", predictor)),
            month_name = month.abb[month_num],
            effect_strength = round(incidence_rate_ratio, 2),
            is_significant = !is.na(p_value) & p_value < 0.05
          ) %>%
          dplyr::filter(is_significant) %>%
          dplyr::arrange(desc(incidence_rate_ratio)) %>%
          dplyr::select(month_name, effect_strength)

        weekday_data <- data %>%
          dplyr::filter(predictor %in% c("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday") &
                 !is.na(incidence_rate_ratio)) %>%
          dplyr::mutate(
            weekday_chinese = dplyr::recode(predictor,
              "monday" = "週一", "tuesday" = "週二", "wednesday" = "週三",
              "thursday" = "週四", "friday" = "週五", "saturday" = "週六", "sunday" = "週日"),
            effect_strength = round(incidence_rate_ratio, 2),
            is_significant = !is.na(p_value) & p_value < 0.05
          ) %>%
          dplyr::filter(is_significant) %>%
          dplyr::arrange(desc(incidence_rate_ratio)) %>%
          dplyr::select(weekday_chinese, effect_strength)

        # Convert to JSON for GPT
        time_effects <- list(
          monthly_effects = if(nrow(monthly_data) > 0) {
            setNames(as.list(monthly_data$effect_strength), monthly_data$month_name)
          } else list(),
          weekday_effects = if(nrow(weekday_data) > 0) {
            setNames(as.list(weekday_data$effect_strength), weekday_data$weekday_chinese)
          } else list()
        )

        effects_json <- jsonlite::toJSON(time_effects, auto_unbox = TRUE)

        # OpenAI functions should already be loaded from union_production_test.R
        if (!exists("chat_api")) {
          stop("OpenAI functions not loaded. Please check union_production_test.R initialization.")
        }

        # Create prompt
        sys <- list(role = "system", content = "你是專業的電商數據分析師，擅長時間序列分析和銷售策略。請用繁體中文回答。")
        usr <- list(
          role = "user",
          content = paste0(
            "根據以下時間效應分析數據，提供時段驅動力洞察報告。",
            "\n\n## 時間效應數據（發生率比 Incidence Ratio）：",
            "\n", effects_json,
            "\n\n請按以下格式輸出：",
            "\n\n### 🕐 時段驅動力洞察",
            "\n\n#### 1. 正向影響銷售的關鍵時段",
            "\n列出效應值 > 1.2 的月份和星期，並說明銷售倍數。",
            "\n\n#### 2. 關鍵字廣告投放建議",
            "\n針對高峰時段，建議 3-5 個適合的關鍵字組合（如：開罐器 不鏽鋼、廚房用品 省力）。",
            "\n\n#### 3. 出價策略建議",
            "\n根據時段效應強度，提供具體的競價調整百分比建議。",
            "\n\n#### 4. 促銷活動建議",
            "\n結合時段特性，建議適合的促銷方式（如：Lightning Deal、Coupon、Subscribe & Save）。",
            "\n\n**注意**：",
            "\n- 保持專業但易懂的語言",
            "\n- 提供具體可執行的建議",
            "\n- 限制在 400 字內"
          )
        )

        # Update notification for AI analysis stage
        # Following UI_R019: Multi-stage notifications for processes > 10s
        showNotification(
          TIME_ANALYSIS_STAGE_MESSAGES$analyzing,
          id = "time_insights_progress",
          type = "message",
          duration = NULL,
          closeButton = FALSE
        )

        txt <- chat_api(list(sys, usr), gpt_key)

        ai_insight_result(txt)

        # Switch to AI insights tab
        updateTabsetPanel(session, "analysis_tabs", selected = "ai_insights")

        # Remove progress notification and show completion
        # Following MP088: Immediate Feedback Principle
        removeNotification("time_insights_progress")
        showNotification(
          TIME_ANALYSIS_STAGE_MESSAGES$complete,
          type = "message",
          duration = 3  # Auto-dismiss after 3 seconds
        )

      }, error = function(e) {
        # Following MP031: Defensive Programming
        # Remove progress notification and show error
        removeNotification("time_insights_progress")
        showNotification(
          paste(TIME_ANALYSIS_STAGE_MESSAGES$error, "：", e$message),
          type = "error",
          duration = 5
        )
      })
    })
    
    # Render AI insights
    output$time_insight_output <- renderUI({
      txt <- ai_insight_result()
      
      if (is.null(txt)) {
        return(HTML("<i style='color:gray;'>點擊「生成 AI 時段洞察」按鈕，獲得基於時間效應的銷售策略建議。</i>"))
      }
      
      # Clean and convert to HTML
      res <- strip_code_fence(txt)
      if (requireNamespace("markdown", quietly = TRUE)) {
        html <- markdown::markdownToHTML(text = res, fragment.only = TRUE)
        HTML(html)
      } else {
        # Fallback
        HTML(paste0("<pre>", res, "</pre>"))
      }
    })
    
    # Return reactive values
    return(list(
      poisson_data = poisson_data,
      filtered_data = filtered_data,
      component_status = component_status,
      ai_insight_result = ai_insight_result
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' poissonTimeAnalysisComponent
#' 
#' Implements a time dimension Poisson analysis component following the Connected Component principle.
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
#' # Basic usage
#' timeComp <- poissonTimeAnalysisComponent("time_analysis")
#' 
#' # Usage with database connection
#' timeComp <- poissonTimeAnalysisComponent(
#'   id = "time_analysis",
#'   app_data_connection = app_conn,
#'   config = list(platform_id = "cbz")
#' )
#' @export
poissonTimeAnalysisComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = poissonTimeAnalysisFilterUI(id, translate),
              display = poissonTimeAnalysisDisplayUI(id, translate)),
    server = function(input, output, session) {
      poissonTimeAnalysisServer(id, app_data_connection, config, session, translate = translate)
    }
  )
}