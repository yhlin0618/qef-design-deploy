#' Macro Section Sidebar Server Component
#'
#' This component provides server-side logic for the macro-level sidebar, handling
#' aggregation and comparison settings for the macro analysis view.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarMacroUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarMacroDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param data_source The data source specification, which can be:
#'   - NULL: Will use defaults
#'   - String: A table/query name (e.g., "metrics")
#'   - Array: Multiple related tables in specific order
#'   - Object: Multiple tables with specific roles (e.g., {metrics: "metrics_table"})
#'
#' @return None (server component with side effects)
#' @export
sidebarMacroServer <- function(id, data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get default values for all outputs
    defaults <- sidebarMacroDefaults()
    
    # Process data source using the utility function
    tables <- reactive({
      processDataSource(
        data_source = data_source, 
        table_names = c("metrics", "primary"),
        get_table_func = function(table_name) {
          tryCatch({
            if (is.null(data_source)) {
              return(data.frame())
            } else if (is.function(data_source[[table_name]])) {
              return(data_source[[table_name]]())
            } else {
              return(data.frame())
            }
          }, error = function(e) {
            message("Error retrieving table ", table_name, ": ", e$message)
            return(data.frame())
          })
        }
      )
    })
    
    # Populate metrics dropdown
    observe({
      metrics_data <- tables()$metrics
      
      if (!is.null(metrics_data) && nrow(metrics_data) > 0 && 
          all(c("metric_id", "metric_name") %in% names(metrics_data))) {
        
        # Create named list for dropdown
        metrics <- setNames(
          as.list(metrics_data$metric_id),
          metrics_data$metric_name
        )
        
        updateSelectInput(
          session,
          "primary_metric",
          choices = metrics,
          selected = metrics[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "primary_metric",
          choices = defaults$metrics,
          selected = names(defaults$metrics)[1]
        )
      }
    })
    
    # Create reactive for current macro analysis settings
    current_settings <- reactiveVal(list(
      primary_metric = NULL,
      aggregation_level = "product_category",
      enable_comparison = FALSE,
      comparison_type = "yoy",
      target_value = 100000,
      chart_type = "bar",
      display_options = c("show_labels")
    ))
    
    # Only update settings when Apply button is clicked
    observeEvent(input$apply_settings, {
      # Update the current settings
      current_settings(list(
        primary_metric = input$primary_metric,
        aggregation_level = input$aggregation_level,
        enable_comparison = input$enable_comparison,
        comparison_type = input$comparison_type,
        target_value = input$target_value,
        chart_type = input$chart_type,
        display_options = input$display_options
      ))
      
      # Store in session for access by macro modules
      session$userData$macro_settings <- current_settings()
      
      # Show notification
      showNotification(
        "分析設定已更新 (Analysis settings updated)",
        type = "message"
      )
    })
    
    # Initialize settings on load
    observe({
      # Only run once on initialization
      req(is.null(current_settings()$primary_metric))
      
      # Get first available metric or use default
      metrics_data <- tables()$metrics
      first_metric <- NULL
      
      if (!is.null(metrics_data) && nrow(metrics_data) > 0 && 
          "metric_id" %in% names(metrics_data)) {
        first_metric <- metrics_data$metric_id[1]
      } else {
        first_metric <- names(defaults$metrics)[1]
      }
      
      # Set initial settings
      current_settings(list(
        primary_metric = first_metric,
        aggregation_level = "product_category",
        enable_comparison = FALSE,
        comparison_type = "yoy",
        target_value = 100000,
        chart_type = "bar",
        display_options = c("show_labels")
      ))
      
      # Store in session
      session$userData$macro_settings <- current_settings()
    })
  })
}