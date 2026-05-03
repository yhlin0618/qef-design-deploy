#' Hybrid Sidebar Server Component
#'
#' This component provides server-side logic for the hybrid sidebar, handling both
#' global and module-specific filter updates.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarHybridUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarHybridDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param active_module The currently active module (main, micro, macro, target)
#' @param data_source The data source specification
#'
#' @return A reactive expression returning the current filter values
#' @export
sidebarHybridServer <- function(id, active_module = "main", data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get default values for all outputs
    defaults <- sidebarHybridDefaults()
    
    # Update the hidden input that drives conditional panels
    observe({
      updateTextInput(session, "active_module_hidden", value = active_module)
    })
    
    # Set the module label based on active module
    output$active_module_label <- renderText({
      module_labels <- list(
        "main" = "總覽過濾器",
        "micro" = "顧客分析過濾器",
        "macro" = "總體分析過濾器",
        "target" = "行銷活動過濾器"
      )
      module_labels[[active_module]]
    })
    
    # Process data source using the utility function
    tables <- reactive({
      processDataSource(
        data_source = data_source, 
        table_names = c("products", "regions", "customers", "segments", "campaigns", "audiences", "primary"),
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
    
    # ===========================================================
    # Global Controls - Shared across all modules
    # ===========================================================
    
    # Populate product categories dropdown
    observe({
      product_data <- tables()$products
      
      if (!is.null(product_data) && nrow(product_data) > 0 && 
          all(c("category_id", "category_name") %in% names(product_data))) {
        
        # Create named list for dropdown
        categories <- setNames(
          as.list(product_data$category_id),
          product_data$category_name
        )
        
        updateSelectInput(
          session,
          "product_category",
          choices = categories,
          selected = categories[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "product_category",
          choices = defaults$product_categories,
          selected = names(defaults$product_categories)[1]
        )
      }
    })
    
    # Populate geographic regions dropdown
    observe({
      region_data <- tables()$regions
      
      if (!is.null(region_data) && nrow(region_data) > 0 && 
          all(c("region_id", "region_name") %in% names(region_data))) {
        
        # Create named list for dropdown
        regions <- setNames(
          as.list(region_data$region_id),
          region_data$region_name
        )
        
        updateSelectizeInput(
          session,
          "geographic_region",
          choices = regions,
          selected = regions[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectizeInput(
          session,
          "geographic_region",
          choices = defaults$geographic_regions,
          selected = names(defaults$geographic_regions)[1]
        )
      }
    })
    
    # ===========================================================
    # Module-specific Controls
    # ===========================================================
    
    # MICRO: Customer segments for micro view
    observe({
      req(active_module == "micro")
      
      segment_data <- tables()$segments
      
      if (!is.null(segment_data) && nrow(segment_data) > 0 && 
          all(c("segment_id", "segment_name") %in% names(segment_data))) {
        
        # Create named list for dropdown
        segments <- setNames(
          as.list(segment_data$segment_id),
          segment_data$segment_name
        )
        
        updateSelectInput(
          session,
          "customer_segment",
          choices = segments
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "customer_segment",
          choices = defaults$customer_segments
        )
      }
    })
    
    # TARGET: Campaign selector for target view
    observe({
      req(active_module == "target")
      
      campaigns_data <- tables()$campaigns
      
      if (!is.null(campaigns_data) && nrow(campaigns_data) > 0 && 
          all(c("campaign_id", "campaign_name") %in% names(campaigns_data))) {
        
        # Create named list for dropdown with a "New Campaign" option
        campaign_choices <- c(
          "新建活動 (New Campaign)" = "new",
          setNames(
            as.list(campaigns_data$campaign_id),
            campaigns_data$campaign_name
          )
        )
        
        updateSelectInput(
          session,
          "campaign_selector",
          choices = campaign_choices,
          selected = campaign_choices[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "campaign_selector",
          choices = c(
            "新建活動 (New Campaign)" = "new",
            defaults$campaigns
          ),
          selected = "new"
        )
      }
    })
    
    # TARGET: Audience segments for target view
    observe({
      req(active_module == "target")
      
      audience_data <- tables()$audiences
      
      if (!is.null(audience_data) && nrow(audience_data) > 0 && 
          all(c("audience_id", "audience_name", "audience_size") %in% names(audience_data))) {
        
        # Create named list for dropdown with audience size in the labels
        audiences <- setNames(
          as.list(audience_data$audience_id),
          paste0(
            audience_data$audience_name, 
            " (", format(audience_data$audience_size, big.mark = ","), " 顧客)"
          )
        )
        
        updateSelectInput(
          session,
          "target_audience",
          choices = audiences,
          selected = audiences[[1]]
        )
      } else {
        # Use defaults if data not available
        updateSelectInput(
          session,
          "target_audience",
          choices = defaults$audiences,
          selected = names(defaults$audiences)[1]
        )
      }
    })
    
    # ===========================================================
    # Filter Management
    # ===========================================================
    
    # Reactive values to store current filters
    current_filters <- reactiveVal(list(
      global = list(
        distribution_channel = if(exists("source_vec") && length(source_vec) > 0) {
          source_vec[1]  # Use first source from config
        } else {
          "amazon"  # Default to amazon if no config
        },
        product_category = NULL,
        geographic_region = NULL
      ),
      main = list(
        date_range = list(start = Sys.Date() - 365, end = Sys.Date()),
        time_scale = "quarter"
      ),
      micro = list(
        customer_search = "",
        customer_segment = NULL,
        recency_filter = c(0, 365)
      ),
      macro = list(
        aggregation_level = "product_category",
        enable_comparison = FALSE,
        comparison_type = "yoy"
      ),
      target = list(
        campaign_selector = "new",
        target_audience = NULL
      )
    ))
    
    # Update filter values when Apply button is clicked
    observeEvent(input$apply_filters, {
      # Update global filters
      global_filters <- list(
        distribution_channel = input$distribution_channel,
        product_category = input$product_category,
        geographic_region = input$geographic_region
      )
      
      # Get current filter values
      filters <- current_filters()
      filters$global <- global_filters
      
      # Update module-specific filters based on current active module
      if (active_module == "main") {
        filters$main <- list(
          date_range = list(
            start = input$date_range_main[1],
            end = input$date_range_main[2]
          ),
          time_scale = input$time_scale_main
        )
      } else if (active_module == "micro") {
        filters$micro <- list(
          customer_search = input$customer_search,
          customer_segment = input$customer_segment,
          recency_filter = input$recency_filter
        )
      } else if (active_module == "macro") {
        filters$macro <- list(
          aggregation_level = input$aggregation_level,
          enable_comparison = input$enable_comparison,
          comparison_type = if(input$enable_comparison) input$comparison_type else NULL
        )
      } else if (active_module == "target") {
        filters$target <- list(
          campaign_selector = input$campaign_selector,
          target_audience = input$target_audience
        )
      }
      
      # Update the reactive value
      current_filters(filters)
      
      # Store in session$userData for access by other modules
      session$userData$filters <- filters
      
      # Show notification
      showNotification(
        "篩選條件已套用 (Filters applied)",
        type = "message"
      )
    })
    
    # Return current filters as reactive
    return(reactive({
      current_filters()
    }))
  })
}