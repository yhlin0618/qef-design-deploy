#' Hybrid Sidebar UI Component
#'
#' This component provides a sidebar with both global and context-specific sections.
#' The global section remains consistent across all modules, while the context-specific
#' section changes based on the active module.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarHybridServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#' @param active_module The currently active module (main, micro, macro, target)
#'
#' @return A sidebar UI component
#' @export
sidebarHybridUI <- function(id, active_module = "main") {
  ns <- NS(id)
  
  # Validate active_module parameter
  active_module <- match.arg(active_module, c("main", "micro", "macro", "target"))
  
  sidebar(
    title = translate("Application Settings"),
    
    # =====================================================
    # Part 1: Global controls (consistent across all modules)
    # =====================================================
    
    # Distribution channel selection from configuration
    radioButtons(
      inputId = ns("distribution_channel"),
      label = translate("Marketing Channel"),
      choices = if(exists("platform_dictionary")) {
        # Use the configured platforms from app_config
        platform_dictionary
      } else if(exists("source_dictionary")) {
        # Fallback to source_dictionary for backwards compatibility
        source_dictionary
      } else {
        # Fallback if configuration is not loaded
        list(
          "Amazon" = "amazon",
          "Official Website" = "officialwebsite"
        )
      },
      selected = if(exists("platform_vec") && length(platform_vec) > 0) {
        platform_vec[1]
      } else if(exists("source_vec") && length(source_vec) > 0) {
        source_vec[1] 
      } else {
        "amazon"  # Default to amazon if no config
      },
      width = "100%"
    ),
    
    # Product category selection
    selectInput(
      inputId = ns("product_category"),
      label = translate("Product Category"),
      choices = NULL, # Will be populated by server
      width = "100%"
    ),
    
    # Geographic filter
    selectizeInput(
      inputId = ns("geographic_region"), 
      label = translate("Region"),
      choices = NULL, # Will be populated by server
      multiple = FALSE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    ),
    
    # Visual separator
    tags$hr(style = "border-top: 1px solid #ddd; margin: 15px 0;"),
    
    # Module label
    tags$div(
      style = "text-align: center; margin-bottom: 10px;",
      tags$span(
        class = "badge bg-primary",
        style = "font-size: 0.9rem;",
        textOutput(ns("active_module_label"))
      )
    ),
    
    # =====================================================
    # Part 2: Context-specific controls based on active module
    # =====================================================
    
    # Main module - date range selector
    conditionalPanel(
      condition = sprintf("input['%s'] == 'main'", ns("active_module_hidden")),
      dateRangeInput(
        inputId = ns("date_range_main"),
        label = translate("Date Range"),
        start = Sys.Date() - 365,
        end = Sys.Date(),
        separator = " to "
      ),
      selectInput(
        inputId = ns("time_scale_main"),
        label = translate("Time Scale"),
        choices = list(
          "year" = "year", 
          "quarter" = "quarter", 
          "month" = "month"
        ) %>% setNames(translate(c("Year", "Quarter", "Month"))),
        selected = "quarter"
      )
    ),
    
    # Micro module - customer filters
    conditionalPanel(
      condition = sprintf("input['%s'] == 'micro'", ns("active_module_hidden")),
      textInput(
        inputId = ns("customer_search"),
        label = translate("Customer Search"),
        placeholder = translate("Enter Customer ID or Name")
      ),
      selectInput(
        inputId = ns("customer_segment"),
        label = translate("Customer Segment"),
        choices = NULL, # Will be populated by server
        multiple = TRUE,
        width = "100%"
      ),
      sliderInput(
        inputId = ns("recency_filter"),
        label = translate("Recency (R)"),
        min = 0,
        max = 365,
        value = c(0, 365),
        step = 1
      )
    ),
    
    # Macro module - aggregation and comparison
    conditionalPanel(
      condition = sprintf("input['%s'] == 'macro'", ns("active_module_hidden")),
      selectInput(
        inputId = ns("aggregation_level"),
        label = translate("Aggregation Level"),
        choices = list(
          "product_category" = "product_category",
          "region" = "region",
          "channel" = "channel",
          "customer_segment" = "customer_segment"
        ) %>% setNames(translate(c("Product Category", "Region", "Channel", "Customer Segment"))),
        selected = "product_category"
      ),
      checkboxInput(
        inputId = ns("enable_comparison"),
        label = translate("Enable Comparison"),
        value = FALSE
      ),
      conditionalPanel(
        condition = sprintf("input['%s'] == true", ns("enable_comparison")),
        selectInput(
          inputId = ns("comparison_type"),
          label = translate("Comparison Type"),
          choices = list(
            "yoy" = "yoy",
            "pop" = "pop",
            "target" = "target"
          ) %>% setNames(translate(c("Year-over-Year", "Period-over-Period", "Target Comparison"))),
          selected = "yoy"
        )
      )
    ),
    
    # Target module - campaign settings
    conditionalPanel(
      condition = sprintf("input['%s'] == 'target'", ns("active_module_hidden")),
      selectInput(
        inputId = ns("campaign_selector"),
        label = translate("Select Campaign"),
        choices = NULL,  # Will be populated by server
        width = "100%"
      ),
      actionButton(
        inputId = ns("new_campaign"),
        label = translate("Create New Campaign"),
        icon = icon("plus"),
        width = "100%",
        class = "btn-success mb-3"
      ),
      selectInput(
        inputId = ns("target_audience"),
        label = translate("Target Audience"),
        choices = NULL,  # Will be populated by server
        width = "100%"
      )
    ),
    
    # Hidden input to store current module
    tags$div(
      style = "display: none;",
      textInput(ns("active_module_hidden"), NULL, value = active_module)
    ),
    
    # Apply filters button
    actionButton(
      inputId = ns("apply_filters"),
      label = translate("Apply Filters"),
      width = "100%",
      class = "btn-primary mt-3"
    )
  )
}