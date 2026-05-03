#' Main Sidebar Server Component
#'
#' This component provides server-side logic for the main sidebar, handling filter updates
#' and broadcasting filter changes to other components.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarMainUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarMainDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param data_source The data source specification, which can be:
#'   - NULL: Will use defaults
#'   - String: A table/query name (e.g., "product_categories")
#'   - Array: Multiple related tables in specific order
#'   - Object: Multiple tables with specific roles (e.g., {products: "products_table"})
#'
#' @return None (server component with side effects)
#' @export
sidebarMainServer <- function(id, data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Get default values for all outputs
    defaults <- sidebarMainDefaults()
    
    # Process data source using the utility function
    tables <- reactive({
      processDataSource(
        data_source = data_source, 
        table_names = c("products", "regions", "primary"),
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
    
    # Create reactive for current filter state
    current_filters <- reactive({
      list(
        distribution_channel = input$distribution_channel,
        product_category = input$product_category,
        time_scale = input$time_scale,
        geographic_region = input$geographic_region,
        date_range = list(
          start = input$date_range[1],
          end = input$date_range[2]
        )
      )
    })
    
    # Store filter values in session for access by all modules
    observe({
      session$userData$filters <- current_filters()
    })
    
    # Show filter status indicator
    output$filter_status <- renderUI({
      filters <- current_filters()
      
      # Count active filters
      active_count <- sum(
        !is.null(filters$distribution_channel),
        !is.null(filters$product_category),
        !is.null(filters$geographic_region),
        !is.null(filters$time_scale)
      )
      
      if (active_count > 0) {
        div(
          class = "filter-indicator",
          span(class = "badge bg-primary", active_count),
          " 篩選器啟用中"
        )
      } else {
        div(
          class = "filter-indicator text-muted",
          "未啟用篩選器"
        )
      }
    })
  })
}