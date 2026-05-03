#' Micro Section Sidebar Server Component
#'
#' This component provides server-side logic for the micro-level sidebar, handling customer 
#' filter updates and broadcasting selections to other micro components.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarMicroUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarMicroDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param data_source The data source specification, which can be:
#'   - NULL: Will use defaults
#'   - String: A table/query name (e.g., "customers")
#'   - Array: Multiple related tables in specific order
#'   - Object: Multiple tables with specific roles (e.g., {customers: "customers_table"})
#'
#' @return None (server component with side effects)
#' @export
sidebarMicroServer <- function(id, data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get default values for all outputs
    defaults <- sidebarMicroDefaults()
    
    # Process data source using the utility function
    tables <- reactive({
      processDataSource(
        data_source = data_source, 
        table_names = c("customers", "segments", "lifecycle_stages", "primary"),
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
    
    # Populate customer segments dropdown
    observe({
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
    
    # Populate lifecycle stages checkboxes
    observe({
      lifecycle_data <- tables()$lifecycle_stages
      
      if (!is.null(lifecycle_data) && nrow(lifecycle_data) > 0 && 
          all(c("stage_id", "stage_name") %in% names(lifecycle_data))) {
        
        # Create named list for dropdown
        stages <- setNames(
          as.list(lifecycle_data$stage_id),
          lifecycle_data$stage_name
        )
        
        updateCheckboxGroupInput(
          session,
          "lifecycle_stage",
          choices = stages,
          selected = stages[1:2] # Select first two by default
        )
      } else {
        # Use defaults if data not available
        updateCheckboxGroupInput(
          session,
          "lifecycle_stage",
          choices = defaults$lifecycle_stages,
          selected = names(defaults$lifecycle_stages)[1:2] # Select first two by default
        )
      }
    })
    
    # Create reactive for current filter state
    current_filters <- reactive({
      list(
        customer_search = input$customer_search,
        customer_segment = input$customer_segment,
        recency = list(
          min = input$recency_filter[1],
          max = input$recency_filter[2]
        ),
        frequency = list(
          min = input$frequency_filter[1],
          max = input$frequency_filter[2]
        ),
        monetary = list(
          min = input$monetary_filter[1],
          max = input$monetary_filter[2]
        ),
        lifecycle_stage = input$lifecycle_stage
      )
    })
    
    # Store filter values in session for access by all micro modules
    observe({
      session$userData$micro_filters <- current_filters()
    })
    
    # Show filter status indicator
    output$filter_status <- renderUI({
      filters <- current_filters()
      
      # Check which filters are active
      active_filters <- list()
      
      if (!is.null(filters$customer_search) && nchar(filters$customer_search) > 0) {
        active_filters <- c(active_filters, "顧客搜尋")
      }
      
      if (!is.null(filters$customer_segment) && length(filters$customer_segment) > 0) {
        active_filters <- c(active_filters, "顧客區隔")
      }
      
      if (!is.null(filters$recency) && 
          (filters$recency$min > 0 || filters$recency$max < 365)) {
        active_filters <- c(active_filters, "最近購買 (R)")
      }
      
      if (!is.null(filters$frequency) && 
          (filters$frequency$min > 0 || filters$frequency$max < 50)) {
        active_filters <- c(active_filters, "購買頻率 (F)")
      }
      
      if (!is.null(filters$monetary) && 
          (filters$monetary$min > 0 || filters$monetary$max < 10000)) {
        active_filters <- c(active_filters, "購買金額 (M)")
      }
      
      if (!is.null(filters$lifecycle_stage) && length(filters$lifecycle_stage) > 0 && 
          length(filters$lifecycle_stage) < length(defaults$lifecycle_stages)) {
        active_filters <- c(active_filters, "生命週期階段")
      }
      
      if (length(active_filters) > 0) {
        div(
          class = "filter-indicator",
          span(class = "badge bg-primary", length(active_filters)),
          " 篩選器啟用中",
          tags$ul(
            class = "filter-list",
            lapply(active_filters, function(filter) {
              tags$li(filter)
            })
          )
        )
      } else {
        div(
          class = "filter-indicator text-muted",
          "未啟用篩選器"
        )
      }
    })
    
    # Handle reset filters button
    observeEvent(input$reset_filters, {
      # Reset all filters to default values
      updateTextInput(session, "customer_search", value = "")
      
      updateSelectInput(
        session,
        "customer_segment",
        selected = character(0)
      )
      
      updateSliderInput(
        session,
        "recency_filter",
        value = c(0, 365)
      )
      
      updateSliderInput(
        session,
        "frequency_filter",
        value = c(0, 50)
      )
      
      updateSliderInput(
        session,
        "monetary_filter",
        value = c(0, 10000)
      )
      
      updateCheckboxGroupInput(
        session,
        "lifecycle_stage",
        selected = character(0)
      )
    })
  })
}